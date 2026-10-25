// Chay: node tests/run-tests.js   (Node >= 18, he thong da `docker compose up`)
// Kiem tra cac test case trong 25_TestCases_5_BoundedContexts.xlsx (phan goi duoc qua REST).
// Cong 3003, moi duong dan deu co tien to /api.
// TC-SHP-02 va TC-SHP-05 can ban event thu cong vao Kafka (xem huong dan CLI).
const BASE = process.env.BASE_URL || 'http://localhost:3003';
const ADMIN = { username: process.env.ADMIN_USERNAME || 'admin', password: process.env.ADMIN_PASSWORD || 'Admin@123' };
const STAFF = { username: process.env.STAFF_USERNAME || 'staff', password: process.env.STAFF_PASSWORD || 'Staff@123' };

let pass = 0, fail = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, path, { token, body } = {}) {
  const res = await fetch(BASE + path, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch {}
  return { status: res.status, data };
}
function check(id, name, cond, extra = '') {
  if (cond) { pass++; console.log(`  PASS  ${id}  ${name}`); }
  else { fail++; console.log(`  FAIL  ${id}  ${name}  ${extra}`); }
}
const login = async (u) => (await api('POST', '/api/auth/login', { body: u })).data?.accessToken;
const uidOf = (t) => Number(JSON.parse(Buffer.from(t.split('.')[1], 'base64url').toString()).sub);

(async () => {
  console.log(`Base URL: ${BASE}\n`);
  const h = await api('GET', '/api/health');
  console.log(`Health: ${h.status} ${JSON.stringify(h.data)}\n`);

  const adminToken = await login(ADMIN);
  const staffToken = await login(STAFF);
  if (!adminToken || !staffToken) { console.log('Khong dang nhap duoc admin/staff - kiem tra .env'); process.exit(1); }

  // ---------- Identity & Access ----------
  console.log('[Identity & Access]');
  const username = `user_${Date.now()}`;
  const password = 'P@ss123';
  let r = await api('POST', '/api/auth/register', { body: { username, password, email: `${username}@example.com`, phone: '0900000000' } });
  check('TC-IAM-01', 'Dang ky hop le -> 201', r.status === 201 && r.data?.uid, JSON.stringify(r));
  r = await api('POST', '/api/auth/register', { body: { username, password, email: `x_${username}@example.com` } });
  check('TC-IAM-02', 'Username trung -> 409', r.status === 409, JSON.stringify(r));
  r = await api('POST', '/api/auth/register', { body: { username: `np_${Date.now()}`, password: '' } });
  check('TC-IAM-03', 'Thieu password -> 400', r.status === 400, JSON.stringify(r));
  r = await api('POST', '/api/auth/login', { body: { username, password } });
  check('TC-IAM-04', 'Login dung -> JWT', r.status === 200 && !!r.data?.accessToken, JSON.stringify(r));
  const token = r.data.accessToken;
  const uid = uidOf(token);
  r = await api('POST', '/api/auth/login', { body: { username, password: 'sai-mat-khau' } });
  check('TC-IAM-05', 'Login sai password -> 401', r.status === 401, JSON.stringify(r));

  // ---------- Customer & Membership ----------
  console.log('\n[Customer & Membership]');
  r = await api('GET', '/api/customers/me', { token });
  check('TC-CUS-01', 'Khach xem thong tin ca nhan (khong lo uid, chua la member: khong co mid)', r.status === 200 && r.data?.fullname && r.data?.uid === undefined && r.data?.mid === undefined, JSON.stringify(r));
  r = await api('GET', `/api/customers/${uid}`, { token: adminToken });
  check('TC-CUS-01a', 'ADMIN xem Customer theo uid', r.status === 200 && r.data?.uid === uid, JSON.stringify(r));
  r = await api('GET', '/api/customers/999999', { token: adminToken });
  check('TC-CUS-02', 'Customer khong ton tai -> 404', r.status === 404, JSON.stringify(r));
  r = await api('PUT', `/api/customers/${uid}`, { token, body: { fullname: 'Nguyen Van Test' } });
  check('TC-CUS-03', 'Cap nhat fullname -> tra thong tin Customer da cap nhat', r.status === 200 && r.data?.fullname === 'Nguyen Van Test' && r.data?.uid === uid, JSON.stringify(r));
  r = await api('GET', '/api/membership', { token });
  check('TC-CUS-04a', 'Chua dang ky member -> danh sach rong', r.status === 200 && r.data?.items?.length === 0, JSON.stringify(r));
  r = await api('POST', '/api/membership', { token });
  check('MEM-01', 'Dang ky member -> 201, hang Normal (khong lo uid)', r.status === 201 && r.data?.mname === 'Normal' && r.data?.score === 10 && r.data?.mid >= 1 && r.data?.uid === undefined, JSON.stringify(r));
  r = await api('POST', '/api/membership', { token });
  check('MEM-02', 'Dang ky member lan 2 -> 409', r.status === 409, JSON.stringify(r));
  r = await api('GET', '/api/membership', { token });
  check('TC-CUS-04', 'Lay Membership cua minh (mname, score)', r.status === 200 && r.data?.items?.length === 1 && r.data.items[0].uid === undefined && r.data.items[0].mname === 'Normal' && r.data.items[0].score === 10, JSON.stringify(r));
  r = await api('GET', '/api/customers/me', { token });
  check('TC-CUS-01b', 'Customer la member -> co mid, mname, score', r.status === 200 && r.data?.mid >= 1 && r.data?.mname === 'Normal' && r.data?.score === 10 && r.data?.uid === undefined, JSON.stringify(r));
  r = await api('GET', '/api/customers', { token: adminToken });
  const mine = r.data?.items?.find((c) => c.uid === uid);
  check('LIST-01', 'ADMIN xem tat ca Customer: chi uid, fullname, mid', r.status === 200 && mine && mine.mid >= 1 && mine.mname === undefined && mine.score === undefined, JSON.stringify(mine));
  r = await api('GET', '/api/membership', { token: adminToken });
  const mem = r.data?.items?.find((c) => c.uid === uid);
  check('LIST-02', 'ADMIN xem membership cua tat ca member: co mid, mname, score', r.status === 200 && mem && mem.mname === 'Normal' && mem.score === 10, JSON.stringify(mem));
  r = await api('GET', '/api/customers', { token });
  check('LIST-03', 'Customer thuong xem danh sach -> 403', r.status === 403, JSON.stringify(r));
  r = await api('GET', '/api/membership', { token });
  check('LIST-04', 'Customer thuong chi thay membership cua minh (khong lo uid)', r.status === 200 && r.data?.items?.length === 1 && r.data.items[0].uid === undefined, JSON.stringify(r));
  r = await api('GET', `/api/customers/${uid}`);
  check('TC-CUS-05', 'Khong co token -> 401', r.status === 401, JSON.stringify(r));

  // ---------- Product ----------
  console.log('\n[Product]');
  r = await api('GET', '/api/products', { token });
  check('TC-PRO-01', 'Lay danh sach Product', r.status === 200 && Array.isArray(r.data?.items) && r.data.items.length > 0, JSON.stringify(r));
  r = await api('GET', '/api/products/1', { token });
  check('TC-PRO-02', 'Lay Product ton tai', r.status === 200 && r.data?.pid === 1, JSON.stringify(r));
  r = await api('GET', '/api/products/999999', { token });
  check('TC-PRO-03', 'Product khong ton tai -> 404', r.status === 404, JSON.stringify(r));
  r = await api('POST', '/api/products', { token: adminToken, body: { pname: `Test-${Date.now()}`, price: 10, quantity: 5 } });
  check('TC-PRO-04', 'Tao Product -> 201', r.status === 201 && r.data?.pid, JSON.stringify(r));
  const newPid = r.data?.pid;
  const r403 = await api('POST', '/api/products', { token, body: { pname: 'X', price: 1, quantity: 1 } });
  check('TC-PRO-04b', 'Customer tao Product -> 403', r403.status === 403, JSON.stringify(r403));
  r = await api('PUT', `/api/products/${newPid}`, { token: adminToken, body: { quantity: 42 } });
  await sleep(200);
  const p = await api('GET', `/api/products/${newPid}`, { token });
  check('TC-PRO-05', 'Cap nhat ton kho chinh xac', r.status === 200 && p.data?.quantity === 42, JSON.stringify(p));

  // ---------- Order ----------
  console.log('\n[Order]');
  const before = (await api('GET', '/api/products/1', { token })).data.quantity;
  r = await api('POST', '/api/orders', { token, body: { items: [{ pid: 1, qty: 2 }, { pid: 2, qty: 1 }] } });
  const oid = r.data?.oid;
  check('TC-ORD-01', 'Tao Order + OrderDetail', r.status === 201 && oid && r.data.items?.length === 2, JSON.stringify(r));
  const od = await api('GET', `/api/orders/${oid}`, { token });
  check('TC-ORD-01b', 'Order co OrderDetail va unit_price', od.status === 200 && od.data?.items?.length === 2 && od.data.items[0].unit_price > 0, JSON.stringify(od));
  await sleep(200);
  const stock = (await api('GET', '/api/products/1', { token })).data.quantity;
  check('FR09', 'Ton kho giam sau khi dat hang', stock === before - 2, `truoc=${before} sau=${stock}`);
  r = await api('POST', '/api/orders', { body: { items: [{ pid: 1, qty: 1 }] } });
  check('TC-ORD-02', 'Khong co JWT -> 401', r.status === 401, JSON.stringify(r));
  r = await api('POST', '/api/orders', { token, body: { items: [{ pid: 1, qty: 999999 }] } });
  check('TC-ORD-03', 'Vuot ton kho -> 409', r.status === 409, JSON.stringify(r));
  check('TC-ORD-04', 'Normal (score 10) -> discount 10%', od.data?.discount === 10, `discount=${od.data?.discount}`);
  // doi sang Gold (score 100) -> discount 20%
  await api('PUT', `/api/customers/${uid}`, { token: adminToken, body: { mid: 3 } });
  r = await api('POST', '/api/orders', { token, body: { items: [{ pid: 2, qty: 1 }] } });
  check('TC-ORD-04b', 'Gold (score 100) -> discount 20%', r.status === 201 && r.data?.discount === 20, JSON.stringify(r));
  // khach chua dang ky member -> khong giam gia
  const u2 = `nm_${Date.now()}`;
  await api('POST', '/api/auth/register', { body: { username: u2, password } });
  const t2 = await login({ username: u2, password });
  r = await api('POST', '/api/orders', { token: t2, body: { items: [{ pid: 2, qty: 1 }] } });
  check('TC-ORD-04c', 'Chua la member -> discount 0%', r.status === 201 && r.data?.discount === 0, JSON.stringify(r));
  // Kafka -> Shipment
  let ship = null;
  for (let i = 0; i < 20 && !ship; i++) {
    const s = await api('GET', `/api/shipments?oid=${oid}`, { token });
    ship = s.data?.items?.[0];
    if (!ship) await sleep(1000);
  }
  check('TC-ORD-05', 'Order luu + event OrderCreated -> Kafka -> Shipment PENDING', !!ship && ship.status === 'PENDING', JSON.stringify(ship));

  // ---------- Shipment ----------
  console.log('\n[Shipment]');
  check('TC-SHP-01', 'Nhan OrderCreated -> tao Shipment', !!ship, '');
  if (ship) {
    const one = await api('GET', `/api/shipments/${ship.shipid}`, { token });
    check('API-SHP-01', 'Customer xem trang thai don hang cua minh', one.status === 200 && one.data?.oid === oid, JSON.stringify(one));
    r = await api('PUT', `/api/shipments/${ship.shipid}`, { token: staffToken, body: { status: 'SHIPPING' } });
    check('TC-SHP-03', 'Cap nhat SHIPPING', r.status === 200 && r.data?.status === 'SHIPPING', JSON.stringify(r));
    r = await api('PUT', `/api/shipments/${ship.shipid}`, { token, body: { status: 'DELIVERED' } });
    check('TC-SHP-03b', 'Customer doi trang thai -> 403', r.status === 403, JSON.stringify(r));
  }
  r = await api('PUT', '/api/shipments/999999', { token: staffToken, body: { status: 'SHIPPING' } });
  check('TC-SHP-04', 'Shipment khong ton tai -> 404', r.status === 404, JSON.stringify(r));

  console.log(`\nKET QUA: ${pass} PASS, ${fail} FAIL`);
  process.exit(fail ? 1 : 0);
})();
