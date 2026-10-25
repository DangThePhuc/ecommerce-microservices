// Customer Service - Customer & Membership Bounded Context. DB: PostgreSQL (Customer DB): customers, memberships.
// Giao tiep: gRPC (goi tu API Gateway). Co them HTTP /health cho Docker.
const path = require('path');
const http = require('http');
const { Pool } = require('pg');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PORT = Number(process.env.PORT || 3002);
const GRPC_PORT = Number(process.env.GRPC_PORT || 50052);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

class GrpcError extends Error {
  constructor(code, message) { super(message); this.grpcCode = code; }
}
const wrap = (fn) => async (call, cb) => {
  try { cb(null, await fn(call)); } catch (e) {
    if (e instanceof GrpcError) return cb({ code: e.grpcCode, details: e.message });
    console.error('[customer]', e);
    cb({ code: grpc.status.INTERNAL, details: 'Internal server error' });
  }
};
const caller = (call) => ({
  uid: Number((call.metadata.get('x-user-id') || [])[0]) || 0,
  role: (call.metadata.get('x-user-role') || [])[0] || '',
});
// chi chu tai khoan hoac ADMIN moi duoc truy cap
function assertAccess(call, uid) {
  const c = caller(call);
  if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
  if (c.role !== 'ADMIN' && c.uid !== uid) throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden');
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function retry(fn, name, tries = 30) {
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); } catch (e) {
      console.error(`[customer] ${name} chua san sang (${i}/${tries}): ${e.message}`);
      await sleep(2000);
    }
  }
  throw new Error(`${name} failed`);
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS memberships (
      mid SERIAL PRIMARY KEY,
      mname VARCHAR(50) UNIQUE NOT NULL,
      score INT NOT NULL
    )`);
  await pool.query(`
    INSERT INTO memberships(mname, score) VALUES ('Normal',10),('Silver',50),('Gold',100)
    ON CONFLICT (mname) DO NOTHING`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customers (
      uid INT PRIMARY KEY,
      fullname VARCHAR(100) NOT NULL,
      mid INT REFERENCES memberships(mid)
    )`);
  // mid cho phep NULL: khach hang chua dang ky member thi chua co hang
  await pool.query('ALTER TABLE customers ALTER COLUMN mid DROP NOT NULL');
}

const def = protoLoader.loadSync(path.join(__dirname, '../proto/customer.proto'), {
  keepCase: true, longs: Number, defaults: true,
});
const customerPkg = grpc.loadPackageDefinition(def).customer;

// Customer kem hang thanh vien (mname, score)
async function findCustomer(uid) {
  const r = await pool.query(
    `SELECT c.uid, c.fullname, c.mid, m.mname, m.score
     FROM customers c LEFT JOIN memberships m ON m.mid = c.mid WHERE c.uid=$1`, [uid]);
  return r.rows[0];
}

const handlers = {
  // goi boi Gateway ngay sau khi dang ky tai khoan: chi tao Customer, CHUA co hang thanh vien (mid = NULL)
  CreateCustomer: wrap(async (call) => {
    const { uid, fullname } = call.request;
    if (!uid || !fullname || !fullname.trim()) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'uid va fullname la bat buoc');
    await pool.query(
      `INSERT INTO customers(uid, fullname) VALUES ($1, $2) ON CONFLICT (uid) DO NOTHING`,
      [uid, fullname.trim().slice(0, 100)]
    );
    return findCustomer(uid);
  }),

  GetCustomer: wrap(async (call) => {
    const uid = call.request.uid;
    assertAccess(call, uid);
    const c = await findCustomer(uid);
    if (!c) throw new GrpcError(grpc.status.NOT_FOUND, 'Customer not found');
    return c;
  }),

  GetMembership: wrap(async (call) => {
    const uid = call.request.uid;
    assertAccess(call, uid);
    const c = await findCustomer(uid);
    if (!c) throw new GrpcError(grpc.status.NOT_FOUND, 'Customer not found');
    if (!c.mid) throw new GrpcError(grpc.status.NOT_FOUND, 'Customer has not registered membership');
    return { uid: c.uid, mid: c.mid, mname: c.mname, score: c.score };
  }),

  // khach hang dang ky de tro thanh member: bat dau o hang Normal (hang cao hon do ADMIN nang cap)
  RegisterMembership: wrap(async (call) => {
    const uid = call.request.uid;
    assertAccess(call, uid);
    const c = await findCustomer(uid);
    if (!c) throw new GrpcError(grpc.status.NOT_FOUND, 'Customer not found');
    if (c.mid) throw new GrpcError(grpc.status.ALREADY_EXISTS, 'Customer da la member');
    await pool.query(`UPDATE customers SET mid = (SELECT mid FROM memberships WHERE mname='Normal') WHERE uid=$1`, [uid]);
    return findCustomer(uid);
  }),

  // ADMIN: danh sach tat ca Customer (kem hang thanh vien)
  ListCustomers: wrap(async (call) => {
    const c = caller(call);
    if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
    if (c.role !== 'ADMIN') throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden: Admin only');
    const r = await pool.query(
      `SELECT c.uid, c.fullname, c.mid, m.mname, m.score
       FROM customers c LEFT JOIN memberships m ON m.mid = c.mid ORDER BY c.uid`);
    return { items: r.rows };
  }),

  // ADMIN: xem tat ca member = cac Customer da co mid
  ListMembers: wrap(async (call) => {
    const c = caller(call);
    if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
    if (c.role !== 'ADMIN') throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden: Admin only');
    const r = await pool.query(
      `SELECT c.uid, c.fullname, c.mid, m.mname, m.score
       FROM customers c JOIN memberships m ON m.mid = c.mid ORDER BY c.uid`);
    return { items: r.rows };
  }),

  UpdateCustomer: wrap(async (call) => {
    const q = call.request;
    assertAccess(call, q.uid);
    if (!q.has_fullname && !q.has_mid) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'Khong co du lieu cap nhat');
    if (q.has_fullname && !q.fullname.trim()) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'fullname khong hop le');
    if (q.has_mid) {
      if (caller(call).role !== 'ADMIN') throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Chi ADMIN moi duoc doi hang thanh vien');
      const m = await pool.query('SELECT 1 FROM memberships WHERE mid=$1', [q.mid]);
      if (!m.rows[0]) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'mid khong ton tai');
    }
    const r = await pool.query(
      `UPDATE customers SET fullname = COALESCE($2, fullname), mid = COALESCE($3, mid) WHERE uid=$1 RETURNING uid`,
      [q.uid, q.has_fullname ? q.fullname.trim().slice(0, 100) : null, q.has_mid ? q.mid : null]
    );
    if (!r.rows[0]) throw new GrpcError(grpc.status.NOT_FOUND, 'Customer not found');
    return findCustomer(q.uid); // thong tin Customer sau khi cap nhat
  }),
};

(async () => {
  await retry(initDb, 'postgres');
  const server = new grpc.Server();
  server.addService(customerPkg.CustomerService.service, handlers);
  server.bindAsync(`0.0.0.0:${GRPC_PORT}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`[customer] gRPC listening on ${GRPC_PORT}`);
  });
  http.createServer(async (req, res) => {
    try { await pool.query('SELECT 1'); res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"status":"UP","service":"customer-service"}'); }
    catch { res.writeHead(503); res.end('{"status":"DOWN"}'); }
  }).listen(PORT, () => console.log(`[customer] health on ${PORT}`));
})();
