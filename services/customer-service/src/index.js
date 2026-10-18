const { Pool } = require('pg');
const { loadProto, err, wrap, startServer, retry } = require('./grpc');

const PORT = process.env.GRPC_PORT || 50052;
const DEFAULT_MID = 1; // hạng mặc định: Normal, score = 10

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS memberships (
      mid   INT PRIMARY KEY,
      mname VARCHAR(50) NOT NULL,
      score INT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS customers (
      uid      INT PRIMARY KEY,                       -- tham chiếu User (Identity & Access), không có FK vì khác DB
      fullname VARCHAR(100) NOT NULL,
      mid      INT NOT NULL REFERENCES memberships(mid)
    );
  `);
  await pool.query(`
    INSERT INTO memberships (mid, mname, score) VALUES
      (1, 'Normal', 10), (2, 'Silver', 50), (3, 'Gold', 100)
    ON CONFLICT (mid) DO NOTHING`);
}

const SELECT = `
  SELECT c.uid, c.fullname, c.mid, m.mname, m.score
  FROM customers c JOIN memberships m ON m.mid = c.mid`;

async function findCustomer(uid) {
  if (!Number.isInteger(uid) || uid <= 0) throw err.invalid('uid must be a positive integer');
  const { rows } = await pool.query(`${SELECT} WHERE c.uid = $1`, [uid]);
  if (!rows[0]) throw err.notFound('Customer not found');
  return rows[0];
}

async function assertMembership(mid) {
  const { rowCount } = await pool.query('SELECT 1 FROM memberships WHERE mid = $1', [mid]);
  if (!rowCount) throw err.invalid(`membership mid=${mid} does not exist`);
}

const impl = {
  CreateCustomer: wrap('CreateCustomer', async ({ uid, fullname, mid }) => {
    if (!Number.isInteger(uid) || uid <= 0) throw err.invalid('uid must be a positive integer');
    if (!fullname || !fullname.trim() || fullname.length > 100) throw err.invalid('fullname is required (max 100 chars)');
    const m = mid || DEFAULT_MID;
    await assertMembership(m);
    try {
      await pool.query('INSERT INTO customers (uid, fullname, mid) VALUES ($1, $2, $3)', [uid, fullname.trim(), m]);
    } catch (e) {
      if (e.code === '23505') throw err.exists('Customer already exists');
      throw e;
    }
    return findCustomer(uid);
  }),

  GetCustomer: wrap('GetCustomer', async ({ uid }) => findCustomer(uid)),

  GetMembership: wrap('GetMembership', async ({ uid }) => findCustomer(uid)),

  UpdateCustomer: wrap('UpdateCustomer', async ({ uid, fullname, mid }) => {
    await findCustomer(uid);
    if (fullname && (!fullname.trim() || fullname.length > 100)) throw err.invalid('fullname must be 1-100 chars');
    if (mid) await assertMembership(mid);
    await pool.query(
      `UPDATE customers SET fullname = COALESCE(NULLIF($2::text, ''), fullname), mid = COALESCE(NULLIF($3::int, 0), mid) WHERE uid = $1`,
      [uid, fullname ? fullname.trim() : '', mid || 0]
    );
    return findCustomer(uid);
  }),

  Health: wrap('Health', async () => {
    await pool.query('SELECT 1');
    return { status: 'UP' };
  }),
};

(async () => {
  await retry('customer-db', migrate);
  const proto = loadProto('customer.proto');
  await startServer(proto.customer.CustomerService.service, impl, PORT);
})().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
