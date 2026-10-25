// API Gateway: nhan REST/JSON tu Client, chuyen thanh gRPC goi den cac service.
// Trach nhiem: routing, authentication (Auth Service verify JWT), authorization (role),
// rate limiting + cache token + cache du lieu (Redis), logging.
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const morgan = require('morgan');
const Redis = require('ioredis');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PORT = Number(process.env.PORT || 8080);
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX || 100);
const RATE_LIMIT_WINDOW = Number(process.env.RATE_LIMIT_WINDOW_SECONDS || 60);
const CACHE_TTL = Number(process.env.CACHE_TTL_SECONDS || 15);
const TOKEN_CACHE_TTL = 60;

const redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 2 });
redis.on('error', (e) => console.error('[redis]', e.message));

// ---------------- gRPC clients ----------------
const opts = { keepCase: true, longs: Number, defaults: true };
function makeClient(file, pkg, svc, url) {
  const def = protoLoader.loadSync(path.join(__dirname, '../proto', file), opts);
  const P = grpc.loadPackageDefinition(def)[pkg];
  return new P[svc](url, grpc.credentials.createInsecure());
}
const authC = makeClient('auth.proto', 'auth', 'AuthService', process.env.AUTH_GRPC_URL);
const customerC = makeClient('customer.proto', 'customer', 'CustomerService', process.env.CUSTOMER_GRPC_URL);
const productC = makeClient('product.proto', 'product', 'ProductService', process.env.PRODUCT_GRPC_URL);
const orderC = makeClient('order.proto', 'order', 'OrderService', process.env.ORDER_GRPC_URL);
const shipmentC = makeClient('shipment.proto', 'shipment', 'ShipmentService', process.env.SHIPMENT_GRPC_URL);

const healthUrls = {
  'auth-service': process.env.AUTH_HEALTH_URL,
  'customer-service': process.env.CUSTOMER_HEALTH_URL,
  'product-service': process.env.PRODUCT_HEALTH_URL,
  'order-service': process.env.ORDER_HEALTH_URL,
  'shipment-service': process.env.SHIPMENT_HEALTH_URL,
};

const rpc = (client, method, req, user) =>
  new Promise((resolve, reject) => {
    const md = new grpc.Metadata();
    if (user) { md.set('x-user-id', String(user.uid)); md.set('x-user-role', user.role); }
    client[method](req, md, { deadline: Date.now() + 8000 }, (e, r) => (e ? reject(e) : resolve(r)));
  });

// ---------------- helpers ----------------
const HTTP = { 3: 400, 16: 401, 7: 403, 5: 404, 6: 409, 9: 409, 14: 503, 4: 504 };
const bad = (msg) => ({ code: 3, details: msg });
const h = (fn) => async (req, res) => {
  try { await fn(req, res); } catch (e) {
    if (e && typeof e.code === 'number' && e.details !== undefined) {
      if (e.code === 14) console.error('[gateway] service unavailable:', e.details);
      return res.status(HTTP[e.code] || 500).json({ message: e.details });
    }
    console.error('[gateway]', e);
    res.status(500).json({ message: 'Internal server error' });
  }
};
const toInt = (v) => (/^\d+$/.test(String(v)) ? Number(v) : 0);

function normalizeItems(items) {
  if (!Array.isArray(items) || !items.length) return null;
  const out = [];
  for (const it of items) {
    if (!it || !Number.isInteger(it.pid) || !Number.isInteger(it.qty) || it.qty <= 0) return null;
    out.push({ pid: it.pid, qty: it.qty });
  }
  return out;
}

async function clearCache() {
  try {
    const keys = await redis.keys('cache:*');
    if (keys.length) await redis.del(keys);
  } catch (e) { console.error('[cache] clear loi:', e.message); }
}
async function cached(key, loader, res) {
  try {
    const hit = await redis.get(key);
    if (hit) { res.set('X-Cache', 'HIT'); return res.json(JSON.parse(hit)); }
  } catch (e) { console.error('[cache] doc loi:', e.message); }
  const data = await loader();
  redis.set(key, JSON.stringify(data), 'EX', CACHE_TTL).catch(() => {});
  res.set('X-Cache', 'MISS');
  res.json(data);
}

// ---------------- app ----------------
const app = express();
app.disable('x-powered-by');

morgan.token('user', (req) => (req.user ? `${req.user.uid}(${req.user.role})` : '-'));
app.use(morgan(':date[iso] :remote-addr :method :url :status :response-time ms user=:user cache=:res[x-cache]'));

// Health check (public)
app.get('/api/health', async (req, res) => {
  const deps = {};
  deps.redis = await redis.ping().then(() => 'UP').catch(() => 'DOWN');
  await Promise.all(Object.entries(healthUrls).map(async ([name, url]) => {
    try {
      const r = await fetch(url, { signal: AbortSignal.timeout(3000) });
      deps[name] = r.ok ? 'UP' : 'DOWN';
    } catch { deps[name] = 'DOWN'; }
  }));
  const up = Object.values(deps).every((v) => v === 'UP');
  res.status(up ? 200 : 503).json({
    status: up ? 'UP' : 'DOWN',
    message: up ? 'E-commerce API is running' : 'Some dependencies are unhealthy',
    dependencies: deps,
  });
});

// Rate limiting (Redis, fixed window theo IP)
app.use(async (req, res, next) => {
  try {
    const key = `rl:${req.ip}`;
    const count = await redis.incr(key);
    if (count === 1) await redis.expire(key, RATE_LIMIT_WINDOW);
    res.set('X-RateLimit-Limit', String(RATE_LIMIT_MAX));
    res.set('X-RateLimit-Remaining', String(Math.max(0, RATE_LIMIT_MAX - count)));
    if (count > RATE_LIMIT_MAX) {
      res.set('Retry-After', String(await redis.ttl(key)));
      return res.status(429).json({ message: 'Too many requests' });
    }
  } catch (e) { console.error('[rate-limit] redis loi, bo qua:', e.message); }
  next();
});

app.use(express.json());
app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ message: 'JSON khong hop le' });
  next(err);
});

// Authentication: Auth Service verify JWT (BPM buoc 2), ket qua cache trong Redis
async function authenticate(req, res, next) {
  const [scheme, token] = (req.headers.authorization || '').split(' ');
  if (scheme !== 'Bearer' || !token) return res.status(401).json({ message: 'Missing or invalid token' });
  const key = `token:${crypto.createHash('sha256').update(token).digest('hex')}`;
  try {
    const hit = await redis.get(key);
    if (hit) { req.user = JSON.parse(hit); return next(); }
  } catch (e) { console.error('[token-cache] doc loi:', e.message); }
  try {
    const r = await rpc(authC, 'VerifyToken', { token });
    req.user = { uid: r.uid, role: r.role };
    redis.set(key, JSON.stringify(req.user), 'EX', TOKEN_CACHE_TTL).catch(() => {});
    next();
  } catch (e) {
    if (e.code === 16) return res.status(401).json({ message: 'Missing or invalid token' });
    return res.status(503).json({ message: 'Auth service unavailable' });
  }
}
// Authorization theo role trong JWT
const need = (...roles) => [authenticate, (req, res, next) =>
  roles.includes(req.user.role) ? next() : res.status(403).json({ message: 'Forbidden: insufficient role' })];
const authed = [authenticate];

// ---------------- Auth ----------------
app.post('/api/auth/register', h(async (req, res) => {
  const { username, password, email, fullname } = req.body || {};
  if (typeof username !== 'string' || typeof password !== 'string') throw bad('username va password la bat buoc');
  if (email && !/^\S+@\S+\.\S+$/.test(email)) throw bad('email khong hop le');
  const { uid } = await rpc(authC, 'Register', { username, password });
  try {
    // tao Customer (hang Normal, score 10) tuong ung voi User moi
    await rpc(customerC, 'CreateCustomer', { uid, fullname: (typeof fullname === 'string' && fullname.trim()) || username });
  } catch (e) {
    await rpc(authC, 'DeleteUser', { uid }).catch(() => {}); // bu tru
    console.error('[gateway] tao Customer loi:', e.details || e.message);
    throw { code: 14, details: 'Customer service khong kha dung, dang ky that bai' };
  }
  res.status(201).json({ message: 'User registered successfully', uid });
}));

app.post('/api/auth/login', h(async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) throw bad('username va password la bat buoc');
  const r = await rpc(authC, 'Login', { username: String(username), password: String(password) });
  res.json({ accessToken: r.access_token, tokenType: r.token_type, expiresIn: r.expires_in });
}));

// ---------------- Customer & Member ----------------
// Customer chua dang ky member thi khong co mid, mname, score.
const customerView = (c) =>
  c.mid ? { uid: c.uid, fullname: c.fullname, mid: c.mid, mname: c.mname, score: c.score }
        : { uid: c.uid, fullname: c.fullname };
// danh sach Customer: chi uid, fullname va mid (neu co)
const listView = (c) =>
  c.mid ? { uid: c.uid, fullname: c.fullname, mid: c.mid } : { uid: c.uid, fullname: c.fullname };
// danh sach Member: them mid, mname, score
const memberView = (c) => ({ uid: c.uid, fullname: c.fullname, mid: c.mid, mname: c.mname, score: c.score });

// thong tin cua chinh khach hang: khong co uid
const selfView = (c) =>
  c.mid ? { fullname: c.fullname, mid: c.mid, mname: c.mname, score: c.score } : { fullname: c.fullname };

// ADMIN: xem tat ca Customer (uid, fullname, mid neu co)
app.get('/api/customers', need('ADMIN'), h(async (req, res) => {
  const r = await rpc(customerC, 'ListCustomers', {}, req.user);
  res.json({ items: r.items.map(listView) });
}));

// Xem membership:
//  - ADMIN: tat ca member cung luc (uid, fullname, mid, mname, score)
//  - Customer thuong: chi membership cua chinh minh, KHONG tra uid (danh sach rong neu chua dang ky)
app.get('/api/membership', authed, h(async (req, res) => {
  if (req.user.role === 'ADMIN') {
    const r = await rpc(customerC, 'ListMembers', {}, req.user);
    return res.json({ items: r.items.map(memberView) });
  }
  const c = await rpc(customerC, 'GetCustomer', { uid: req.user.uid }, req.user);
  res.json({ items: c.mid ? [selfView(c)] : [] });
}));

// Khach hang dang ky de tro thanh member (hang Normal). Nguoi dung lay tu token, khong can uid.
app.post('/api/membership', authed, h(async (req, res) => {
  const c = await rpc(customerC, 'RegisterMembership', { uid: req.user.uid }, req.user);
  res.status(201).json(selfView(c));
}));

// Khach hang xem thong tin ca nhan cua minh (KHONG tra uid). Phai dat truoc /api/customers/:uid
app.get('/api/customers/me', authed, h(async (req, res) => {
  const c = await rpc(customerC, 'GetCustomer', { uid: req.user.uid }, req.user);
  res.json(selfView(c));
}));

app.get('/api/customers/:uid', authed, h(async (req, res) => {
  const c = await rpc(customerC, 'GetCustomer', { uid: toInt(req.params.uid) }, req.user);
  res.json(customerView(c));
}));

app.put('/api/customers/:uid', authed, h(async (req, res) => {
  const { fullname, mid } = req.body || {};
  if (fullname !== undefined && typeof fullname !== 'string') throw bad('fullname khong hop le');
  if (mid !== undefined && !Number.isInteger(mid)) throw bad('mid khong hop le');
  const r = await rpc(customerC, 'UpdateCustomer', {
    uid: toInt(req.params.uid),
    has_fullname: fullname !== undefined, fullname: fullname || '',
    has_mid: mid !== undefined, mid: mid || 0,
  }, req.user);
  res.json(customerView(r));
}));

// ---------------- Product ----------------
app.get('/api/products', authed, h(async (req, res) => {
  await cached('cache:products:all', async () => {
    const r = await rpc(productC, 'ListProducts', {}, req.user);
    return { items: r.items };
  }, res);
}));

app.get('/api/products/:pid', authed, h(async (req, res) => {
  await cached(`cache:products:${req.params.pid}`, () => rpc(productC, 'GetProduct', { pid: toInt(req.params.pid) }, req.user), res);
}));

app.post('/api/products', need('ADMIN'), h(async (req, res) => {
  const { pname, price, quantity } = req.body || {};
  if (typeof pname !== 'string' || typeof price !== 'number' || !Number.isInteger(quantity)) {
    throw bad('pname (chuoi), price (so), quantity (so nguyen) la bat buoc');
  }
  await clearCache();
  const p = await rpc(productC, 'CreateProduct', { pname, price, quantity }, req.user);
  await clearCache();
  res.status(201).json(p);
}));

app.put('/api/products/:pid', need('ADMIN'), h(async (req, res) => {
  const { pname, price, quantity } = req.body || {};
  if ((pname !== undefined && typeof pname !== 'string') || (price !== undefined && typeof price !== 'number') ||
      (quantity !== undefined && !Number.isInteger(quantity))) throw bad('Du lieu khong hop le');
  await clearCache();
  const r = await rpc(productC, 'UpdateProduct', {
    pid: toInt(req.params.pid),
    has_pname: pname !== undefined, pname: pname || '',
    has_price: price !== undefined, price: price || 0,
    has_quantity: quantity !== undefined, quantity: quantity || 0,
  }, req.user);
  await clearCache();
  res.json({ message: r.message, pid: r.pid });
}));

app.delete('/api/products/:pid', need('ADMIN'), h(async (req, res) => {
  await clearCache();
  const r = await rpc(productC, 'DeleteProduct', { pid: toInt(req.params.pid) }, req.user);
  await clearCache();
  res.json({ message: r.message, pid: r.pid });
}));

// ---------------- Order (BPM: Dat hang) ----------------
app.post('/api/orders', authed, h(async (req, res) => {
  const items = normalizeItems((req.body || {}).items);
  if (!items) throw bad('items khong hop le (pid, qty phai la so nguyen duong)');
  // buoc 3: lay thong tin Customer (uid, fullname, mid) - 404 neu khong ton tai
  const cust = await rpc(customerC, 'GetCustomer', { uid: req.user.uid }, req.user);
  // buoc 4: lay diem thanh vien (score) - chua dang ky member thi score = 0 (khong giam gia)
  const m = cust.mid ? await rpc(customerC, 'GetMembership', { uid: req.user.uid }, req.user) : { score: 0 };
  // buoc 5-9: Order Service tinh discount, kiem tra/cap nhat ton kho, luu Order + Outbox, publish Kafka
  await clearCache();
  const o = await rpc(orderC, 'CreateOrder', { uid: req.user.uid, score: m.score, items }, req.user);
  await clearCache();
  // buoc 10: tra ket qua
  res.status(201).json({
    oid: o.oid, uid: o.uid, discount: o.discount, subtotal: o.subtotal, total_amount: o.total_amount,
    items: o.items, shipment: { status: o.shipment_status },
  });
}));

app.get('/api/orders', authed, h(async (req, res) => {
  const r = await rpc(orderC, 'ListOrders', {}, req.user);
  res.json({ items: r.items.map(({ oid, uid, discount, total_amount }) => ({ oid, uid, discount, total_amount })) });
}));

app.get('/api/orders/:oid', authed, h(async (req, res) => {
  const o = await rpc(orderC, 'GetOrder', { oid: toInt(req.params.oid) }, req.user);
  res.json({ oid: o.oid, uid: o.uid, discount: o.discount, subtotal: o.subtotal, total_amount: o.total_amount, items: o.items });
}));

// ---------------- Shipment ----------------
app.get('/api/shipments', authed, h(async (req, res) => {
  const has = req.query.oid !== undefined;
  const r = await rpc(shipmentC, 'ListShipments', { has_oid: has, oid: has ? toInt(req.query.oid) : 0 }, req.user);
  res.json({ items: r.items });
}));

app.get('/api/shipments/:shipid', authed, h(async (req, res) => {
  const s = await rpc(shipmentC, 'GetShipment', { shipid: toInt(req.params.shipid) }, req.user);
  res.json(s);
}));

app.put('/api/shipments/:shipid', need('SHIPMENT_STAFF', 'ADMIN'), h(async (req, res) => {
  const { status } = req.body || {};
  if (typeof status !== 'string') throw bad('status la bat buoc');
  const s = await rpc(shipmentC, 'UpdateShipmentStatus', { shipid: toInt(req.params.shipid), status }, req.user);
  res.json(s);
}));

app.use((req, res) => res.status(404).json({ message: 'Route not found' }));

app.listen(PORT, () => console.log(`[gateway] listening on ${PORT}`));
