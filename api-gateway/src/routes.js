const express = require('express');
const { call, HttpError } = require('./grpc');
const { redis, cacheGet, cacheSet, invalidate } = require('./redis');
const { authenticate, requireRole } = require('./middleware');

const CACHE_TTL = parseInt(process.env.CACHE_TTL_SEC || '30', 10);
const h = (fn) => (req, res, next) => Promise.resolve(fn(req, res)).catch(next); // bắt lỗi async

const isStr = (v) => typeof v === 'string';
const isInt = (v) => Number.isInteger(v);
const intParam = (v, name) => {
  if (!/^\d+$/.test(v)) throw new HttpError(400, `${name} must be a positive integer`);
  return parseInt(v, 10);
};

// ───────────────────────── Public: health, register, login ─────────────────────────
const publicRouter = express.Router();

const health = h(async (req, res) => {
  const check = async (fn) => { try { return await fn(); } catch (_) { return null; } };
  const [rd, auth, customer, product, order, shipment] = await Promise.all([
    check(async () => ((await redis.ping()) === 'PONG' ? { status: 'UP' } : null)),
    check(() => call('auth', 'Health', {}, 2000)),
    check(() => call('customer', 'Health', {}, 2000)),
    check(() => call('product', 'Health', {}, 2000)),
    check(() => call('order', 'Health', {}, 2000)),
    check(() => call('shipment', 'Health', {}, 2000)),
  ]);
  const s = (x) => (x && x.status === 'UP' ? 'UP' : 'DOWN');
  const components = {
    redis: s(rd),
    'auth-service': s(auth),
    'customer-service': s(customer),
    'product-service': s(product),
    'order-service': s(order),
    'shipment-service': s(shipment),
    kafka: order && shipment && order.kafka === 'UP' && shipment.kafka === 'UP' ? 'UP' : 'DOWN',
  };
  const up = Object.values(components).every((v) => v === 'UP');
  res.status(up ? 200 : 503).json({
    status: up ? 'UP' : 'DOWN',
    message: up ? 'E-commerce API is running' : 'Some required dependencies are unhealthy',
    components,
  });
});

publicRouter.post('/auth/register', h(async (req, res) => {
  const { username, password, fullname, email, phone } = req.body || {};
  if (!isStr(username) || !isStr(password) || !username || !password) throw new HttpError(400, 'username and password are required');
  if (email !== undefined && (!isStr(email) || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))) throw new HttpError(400, 'email is invalid');
  if (phone !== undefined && (!isStr(phone) || !/^[0-9+\-\s]{8,15}$/.test(phone))) throw new HttpError(400, 'phone is invalid');
  if (fullname !== undefined && !isStr(fullname)) throw new HttpError(400, 'fullname must be a string');

  const user = await call('auth', 'Register', { username, password });       // role mặc định = normal
  try {
    // Tự tạo Customer với hạng thành viên mặc định (mid = 1, score = 10)
    await call('customer', 'CreateCustomer', { uid: user.uid, fullname: (fullname && fullname.trim()) || username, mid: 0 });
  } catch (e) {
    await call('auth', 'DeleteUser', { uid: user.uid }).catch(() => {}); // bù trừ
    throw e;
  }
  res.status(201).json({ message: 'User registered successfully', uid: user.uid, role: user.role });
}));

publicRouter.post('/auth/login', h(async (req, res) => {
  const { username, password } = req.body || {};
  if (!isStr(username) || !isStr(password) || !username || !password) throw new HttpError(400, 'username and password are required');
  const r = await call('auth', 'Login', { username, password });
  res.json({ accessToken: r.access_token, tokenType: r.token_type, expiresIn: r.expires_in });
}));

// ───────────────────────── Protected (yêu cầu Bearer JWT) ─────────────────────────
const router = express.Router();
router.use(authenticate);

// Customers
const canAccess = (req, uid) => req.user.role === 'admin' || req.user.uid === uid;
const customerDto = (c) => ({
  uid: c.uid,
  fullname: c.fullname,
  mid: c.mid,
  membership: { mid: c.mid, mname: c.mname, score: c.score },
});

router.get('/customers/:uid', h(async (req, res) => {
  const uid = intParam(req.params.uid, 'uid');
  if (!canAccess(req, uid)) throw new HttpError(403, 'Forbidden');
  res.json(customerDto(await call('customer', 'GetCustomer', { uid })));
}));

router.put('/customers/:uid', h(async (req, res) => {
  const uid = intParam(req.params.uid, 'uid');
  if (!canAccess(req, uid)) throw new HttpError(403, 'Forbidden');
  const { fullname, mid } = req.body || {};
  if (fullname !== undefined && (!isStr(fullname) || !fullname.trim())) throw new HttpError(400, 'fullname must be a non-empty string');
  if (mid !== undefined && (!isInt(mid) || mid <= 0)) throw new HttpError(400, 'mid must be a positive integer');
  if (mid !== undefined && req.user.role !== 'admin') throw new HttpError(403, 'Only admin can change membership');
  await call('customer', 'UpdateCustomer', { uid, fullname: fullname || '', mid: mid || 0 });
  res.json({ message: 'Customer updated successfully', uid });
}));

// Products (cache trong Redis, vô hiệu hóa khi dữ liệu thay đổi)
router.get('/products', h(async (req, res) => {
  const key = 'cache:products:list';
  let data = await cacheGet(key);
  if (!data) {
    data = await call('product', 'ListProducts', {});
    await cacheSet(key, data, CACHE_TTL);
  }
  res.json(data);
}));

router.get('/products/:pid', h(async (req, res) => {
  const pid = intParam(req.params.pid, 'pid');
  const key = `cache:products:${pid}`;
  let data = await cacheGet(key);
  if (!data) {
    data = await call('product', 'GetProduct', { pid });
    await cacheSet(key, data, CACHE_TTL);
  }
  res.json(data);
}));

const checkProductBody = ({ pname, price, quantity }, partial) => {
  if ((!partial || pname !== undefined) && (!isStr(pname) || !pname.trim())) throw new HttpError(400, 'pname must be a non-empty string');
  if ((!partial || price !== undefined) && (typeof price !== 'number' || !Number.isFinite(price) || price < 0)) throw new HttpError(400, 'price must be a number >= 0');
  if ((!partial || quantity !== undefined) && (!isInt(quantity) || quantity < 0)) throw new HttpError(400, 'quantity must be an integer >= 0');
};

router.post('/products', requireRole('admin'), h(async (req, res) => {
  const body = req.body || {};
  checkProductBody(body, false);
  const p = await call('product', 'CreateProduct', { pname: body.pname, price: body.price, quantity: body.quantity });
  await invalidate('cache:products:*');
  res.status(201).json(p);
}));

router.put('/products/:pid', requireRole('admin'), h(async (req, res) => {
  const pid = intParam(req.params.pid, 'pid');
  const body = req.body || {};
  checkProductBody(body, true);
  const cur = await call('product', 'GetProduct', { pid }); // 404 nếu không tồn tại; các field không gửi sẽ giữ nguyên
  await call('product', 'UpdateProduct', {
    pid,
    pname: body.pname !== undefined ? body.pname : cur.pname,
    price: body.price !== undefined ? body.price : cur.price,
    quantity: body.quantity !== undefined ? body.quantity : cur.quantity,
  });
  await invalidate('cache:products:*');
  res.json({ message: 'Product updated successfully', pid });
}));

router.delete('/products/:pid', requireRole('admin'), h(async (req, res) => {
  const pid = intParam(req.params.pid, 'pid');
  await call('product', 'DeleteProduct', { pid });
  await invalidate('cache:products:*');
  res.json({ message: 'Product deleted successfully', pid });
}));

// Orders
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const shipmentOf = async (oid, user) => {
  try {
    const s = await call('shipment', 'GetShipmentByOrder', { oid, uid: user.uid, role: user.role });
    return { shipid: s.shipid, status: s.status };
  } catch (e) {
    if (e.status === 404) return null;
    throw e;
  }
};

router.post('/orders', h(async (req, res) => {
  const { items } = req.body || {};
  if (!Array.isArray(items) || !items.length || !items.every((i) => i && isInt(i.pid) && i.pid > 0 && isInt(i.qty) && i.qty > 0)) {
    throw new HttpError(400, 'items must be a non-empty array of { pid, qty } with positive integers');
  }
  const order = await call('order', 'CreateOrder', { uid: req.user.uid, items: items.map((i) => ({ pid: i.pid, qty: i.qty })) });
  await invalidate('cache:products:*'); // tồn kho đã đổi

  // Shipment được tạo bất đồng bộ qua Kafka -> chờ ngắn để trả kèm trong kết quả (nếu chưa có thì shipment = null)
  let shipment = null;
  for (let i = 0; i < 6 && !shipment; i++) {
    shipment = await shipmentOf(order.oid, req.user).catch(() => null);
    if (!shipment) await sleep(500);
  }
  res.status(201).json({ ...order, shipment });
}));

router.get('/orders', h(async (req, res) => {
  res.json(await call('order', 'ListOrders', { uid: req.user.uid, role: req.user.role }));
}));

router.get('/orders/:oid', h(async (req, res) => {
  const oid = intParam(req.params.oid, 'oid');
  const order = await call('order', 'GetOrder', { oid, uid: req.user.uid, role: req.user.role });
  res.json({ ...order, shipment: await shipmentOf(oid, req.user).catch(() => null) });
}));

// Shipments
router.get('/shipments/:shipid', h(async (req, res) => {
  const shipid = intParam(req.params.shipid, 'shipid');
  res.json(await call('shipment', 'GetShipment', { shipid, uid: req.user.uid, role: req.user.role }));
}));

router.patch('/shipments/:shipid/status', requireRole('admin', 'shipment_staff'), h(async (req, res) => {
  const shipid = intParam(req.params.shipid, 'shipid');
  const { status } = req.body || {};
  if (!isStr(status)) throw new HttpError(400, 'status is required');
  res.json(await call('shipment', 'UpdateShipmentStatus', { shipid, status }));
}));

module.exports = { publicRouter, router, health };
