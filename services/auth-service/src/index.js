const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const { loadProto, err, wrap, startServer, retry } = require('./grpc');

const PORT = process.env.GRPC_PORT || 50051;
const JWT_SECRET = process.env.JWT_SECRET || 'change-me-in-production';
const JWT_EXPIRES_IN = parseInt(process.env.JWT_EXPIRES_IN || '3600', 10);

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Role: 1 = admin, 2 = normal, 3 = shipment_staff
const ROLES = ['admin', 'normal', 'shipment_staff'];

async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS roles (
      roleid   SERIAL PRIMARY KEY,
      rolename VARCHAR(50) UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS users (
      uid      SERIAL PRIMARY KEY,
      username VARCHAR(50)  UNIQUE NOT NULL,
      password VARCHAR(255) NOT NULL,
      roleid   INT NOT NULL REFERENCES roles(roleid)
    );
  `);
  for (const r of ROLES) {
    await pool.query('INSERT INTO roles (rolename) VALUES ($1) ON CONFLICT (rolename) DO NOTHING', [r]);
  }
  await seedUser(process.env.ADMIN_USERNAME || 'admin', process.env.ADMIN_PASSWORD || 'Admin@123', 'admin');
  await seedUser(process.env.STAFF_USERNAME || 'staff', process.env.STAFF_PASSWORD || 'Staff@123', 'shipment_staff');
}

async function seedUser(username, password, role) {
  const hash = await bcrypt.hash(password, 10);
  await pool.query(
    `INSERT INTO users (username, password, roleid)
     VALUES ($1, $2, (SELECT roleid FROM roles WHERE rolename = $3))
     ON CONFLICT (username) DO NOTHING`,
    [username, hash, role]
  );
}

const DUMMY_HASH = bcrypt.hashSync('dummy-password', 10);
const USERNAME_RE = /^[A-Za-z0-9_.-]{3,50}$/;

const impl = {
  Register: wrap('Register', async ({ username, password }) => {
    if (!USERNAME_RE.test(username || '')) throw err.invalid('username must be 3-50 chars (letters, digits, _ . -)');
    if (!password || password.length < 6 || password.length > 72) throw err.invalid('password must be 6-72 characters');
    const hash = await bcrypt.hash(password, 10);
    try {
      const { rows } = await pool.query(
        `INSERT INTO users (username, password, roleid)
         VALUES ($1, $2, (SELECT roleid FROM roles WHERE rolename = 'normal'))
         RETURNING uid`,
        [username, hash]
      );
      return { uid: rows[0].uid, role: 'normal' };
    } catch (e) {
      if (e.code === '23505') throw err.exists('username already exists');
      throw e;
    }
  }),

  Login: wrap('Login', async ({ username, password }) => {
    if (!username || !password) throw err.invalid('username and password are required');
    const { rows } = await pool.query(
      `SELECT u.uid, u.password, r.rolename FROM users u JOIN roles r ON r.roleid = u.roleid WHERE u.username = $1`,
      [username]
    );
    const user = rows[0];
    // So sánh hash cả khi không có user để tránh lộ thông tin qua thời gian phản hồi
    const ok = await bcrypt.compare(password, user ? user.password : DUMMY_HASH);
    if (!user || !ok) throw err.unauthenticated('invalid username or password');
    const token = jwt.sign({ sub: String(user.uid), role: user.rolename }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
    return { access_token: token, token_type: 'Bearer', expires_in: JWT_EXPIRES_IN };
  }),

  VerifyToken: wrap('VerifyToken', async ({ token }) => {
    try {
      const p = jwt.verify(token, JWT_SECRET);
      return { valid: true, uid: parseInt(p.sub, 10), role: p.role, exp: p.exp };
    } catch (_) {
      return { valid: false, uid: 0, role: '', exp: 0 };
    }
  }),

  DeleteUser: wrap('DeleteUser', async ({ uid }) => {
    const r = await pool.query('DELETE FROM users WHERE uid = $1', [uid]);
    return { deleted: r.rowCount > 0 };
  }),

  Health: wrap('Health', async () => {
    await pool.query('SELECT 1');
    return { status: 'UP' };
  }),
};

(async () => {
  await retry('auth-db', migrate);
  const proto = loadProto('auth.proto');
  await startServer(proto.auth.AuthService.service, impl, PORT);
})().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
