// Auth Service - Identity & Access Bounded Context. DB: PostgreSQL (Auth DB): users, roles.
// Giao tiep: gRPC (goi tu API Gateway). Co them HTTP /health cho Docker.
const path = require('path');
const http = require('http');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PORT = Number(process.env.PORT || 3001);
const GRPC_PORT = Number(process.env.GRPC_PORT || 50051);
const JWT_SECRET = process.env.JWT_SECRET;
const JWT_EXPIRES_IN = Number(process.env.JWT_EXPIRES_IN || 3600);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

class GrpcError extends Error {
  constructor(code, message) { super(message); this.grpcCode = code; }
}
const wrap = (fn) => async (call, cb) => {
  try { cb(null, await fn(call)); } catch (e) {
    if (e instanceof GrpcError) return cb({ code: e.grpcCode, details: e.message });
    console.error('[auth]', e);
    cb({ code: grpc.status.INTERNAL, details: 'Internal server error' });
  }
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function retry(fn, name, tries = 30) {
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); } catch (e) {
      console.error(`[auth] ${name} chua san sang (${i}/${tries}): ${e.message}`);
      await sleep(2000);
    }
  }
  throw new Error(`${name} failed`);
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS roles (
      roleid SERIAL PRIMARY KEY,
      rolename VARCHAR(50) UNIQUE NOT NULL
    )`);
  await pool.query(`
    INSERT INTO roles(rolename) VALUES ('ADMIN'),('CUSTOMER'),('SHIPMENT_STAFF')
    ON CONFLICT (rolename) DO NOTHING`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      uid SERIAL PRIMARY KEY,
      username VARCHAR(50) UNIQUE NOT NULL,
      password VARCHAR(255) NOT NULL,
      roleid INT NOT NULL REFERENCES roles(roleid)
    )`);
  const seeds = [
    [process.env.ADMIN_USERNAME, process.env.ADMIN_PASSWORD, 'ADMIN'],
    [process.env.STAFF_USERNAME, process.env.STAFF_PASSWORD, 'SHIPMENT_STAFF'],
  ];
  for (const [u, p, role] of seeds) {
    if (!u || !p) continue;
    await pool.query(
      `INSERT INTO users(username, password, roleid)
       VALUES ($1, $2, (SELECT roleid FROM roles WHERE rolename=$3)) ON CONFLICT (username) DO NOTHING`,
      [u, await bcrypt.hash(p, 10), role]
    );
  }
}

const def = protoLoader.loadSync(path.join(__dirname, '../proto/auth.proto'), {
  keepCase: true, longs: Number, defaults: true,
});
const authPkg = grpc.loadPackageDefinition(def).auth;

const handlers = {
  Register: wrap(async (call) => {
    const { username, password } = call.request;
    if (!username || username.trim().length < 3 || !password || password.length < 6) {
      throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'username (>=3 ky tu) va password (>=6 ky tu) la bat buoc');
    }
    const hash = await bcrypt.hash(password, 10);
    try {
      const r = await pool.query(
        `INSERT INTO users(username, password, roleid)
         VALUES ($1, $2, (SELECT roleid FROM roles WHERE rolename='CUSTOMER')) RETURNING uid`,
        [username.trim(), hash]
      );
      return { uid: r.rows[0].uid };
    } catch (e) {
      if (e.code === '23505') throw new GrpcError(grpc.status.ALREADY_EXISTS, 'Username da ton tai');
      throw e;
    }
  }),

  Login: wrap(async (call) => {
    const { username, password } = call.request;
    if (!username || !password) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'username va password la bat buoc');
    const r = await pool.query(
      `SELECT u.uid, u.username, u.password, r.rolename
       FROM users u JOIN roles r ON r.roleid = u.roleid WHERE u.username=$1`, [username]);
    const u = r.rows[0];
    if (!u || !(await bcrypt.compare(password, u.password))) {
      throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Sai username hoac password');
    }
    const token = jwt.sign({ sub: String(u.uid), role: u.rolename, username: u.username }, JWT_SECRET, {
      algorithm: 'HS256', expiresIn: JWT_EXPIRES_IN,
    });
    return { access_token: token, token_type: 'Bearer', expires_in: JWT_EXPIRES_IN };
  }),

  VerifyToken: wrap(async (call) => {
    try {
      const p = jwt.verify(call.request.token, JWT_SECRET, { algorithms: ['HS256'] });
      return { uid: Number(p.sub), role: p.role, username: p.username || '' };
    } catch {
      throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Token khong hop le hoac het han');
    }
  }),

  DeleteUser: wrap(async (call) => {
    await pool.query('DELETE FROM users WHERE uid=$1', [call.request.uid]);
    return { success: true };
  }),
};

(async () => {
  await retry(initDb, 'postgres');
  const server = new grpc.Server();
  server.addService(authPkg.AuthService.service, handlers);
  server.bindAsync(`0.0.0.0:${GRPC_PORT}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`[auth] gRPC listening on ${GRPC_PORT}`);
  });
  http.createServer(async (req, res) => {
    try { await pool.query('SELECT 1'); res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"status":"UP","service":"auth-service"}'); }
    catch { res.writeHead(503); res.end('{"status":"DOWN"}'); }
  }).listen(PORT, () => console.log(`[auth] health on ${PORT}`));
})();
