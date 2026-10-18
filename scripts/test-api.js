#!/usr/bin/env node
// Kiểm thử end-to-end cho phiếu chấm 3 (Node.js >= 18, không cần cài thêm thư viện)
//   TC-PRO-01  Lấy danh sách Product
//   TC-CUS-04  Lấy thông tin Membership (mname, score)
//   TC-CUS-05  Truy vấn Customer không có token -> 401
//   TC-ORD-04  Tính discount theo score
//   TC-ORD-05  Tạo Order và phát sự kiện OrderCreated lên Kafka
//
// Chạy:  docker compose up -d --build   (đợi healthy)   rồi:   node scripts/test-api.js
// Biến môi trường: BASE_URL (mặc định http://localhost:8080), ADMIN_USERNAME, ADMIN_PASSWORD
const { execSync } = require('child_process');
const path = require('path');

const BASE = process.env.BASE_URL || 'http://localhost:8080';
const ADMIN = { username: process.env.ADMIN_USERNAME || 'admin', password: process.env.ADMIN_PASSWORD || 'Admin@123' };
const ROOT = path.join(__dirname, '..');

let passed = 0;
let failed = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, url, { token, body } = {}) {
  const res = await fetch(BASE + url, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch (_) { /* không có body */ }
  return { status: res.status, data };
}

function expect(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function test(id, title, fn) {
  try {
    const detail = await fn();
    passed++;
    console.log(`  ✔ ${id}: ${title}${detail ? `  (${detail})` : ''}`);
  } catch (e) {
    failed++;
    console.log(`  ✘ ${id}: ${title}\n      -> ${e.message}`);
  }
}

// Chạy lệnh trong container qua docker compose; trả về null nếu không dùng được Docker
function dockerExec(cmd) {
  try {
    return execSync(`docker compose exec -T ${cmd}`, { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 60000 });
  } catch (e) {
    return e.stdout ? String(e.stdout) : null;
  }
}

async function waitFor(fn, tries = 20, delay = 500) {
  for (let i = 0; i < tries; i++) {
    const v = await fn();
    if (v) return v;
    await sleep(delay);
  }
  return null;
}

(async () => {
  console.log(`\nE-commerce Microservices - kiểm thử phiếu chấm 3\nBASE_URL = ${BASE}\n`);

  // ───── Chuẩn bị dữ liệu ─────
  const health = await api('GET', '/api/health').catch(() => null);
  if (!health || health.status !== 200) {
    console.error('Hệ thống chưa sẵn sàng. Hãy chạy "docker compose up -d --build" và đợi các container healthy.\n', health && health.data);
    process.exit(2);
  }
  const adminLogin = await api('POST', '/api/auth/login', { body: ADMIN });
  expect(adminLogin.status === 200, `Không đăng nhập được admin (${adminLogin.status})`);
  const adminToken = adminLogin.data.accessToken;

  const run = Date.now().toString(36);
  const prodX = (await api('POST', '/api/products', { token: adminToken, body: { pname: `Laptop-${run}`, price: 1500, quantity: 100 } })).data;
  const prodY = (await api('POST', '/api/products', { token: adminToken, body: { pname: `Mouse-${run}`, price: 25, quantity: 100 } })).data;
  expect(prodX && prodX.pid && prodY && prodY.pid, 'Không tạo được sản phẩm X, Y');

  const username = `user_${run}`;
  const reg = await api('POST', '/api/auth/register', { body: { username, password: 'P@ss1234', fullname: 'Nguyen Van Test' } });
  expect(reg.status === 201, `Đăng ký thất bại (${reg.status})`);
  const uid = reg.data.uid;
  const token = (await api('POST', '/api/auth/login', { body: { username, password: 'P@ss1234' } })).data.accessToken;

  const order = (token_) => api('POST', '/api/orders', {
    token: token_,
    body: { items: [{ pid: prodX.pid, qty: 2 }, { pid: prodY.pid, qty: 5 }] },
  });
  const SUBTOTAL = 2 * 1500 + 5 * 25; // 3125

  console.log('Kết quả:');

  // ───── TC-PRO-01 ─────
  await test('TC-PRO-01', 'Lấy danh sách Product', async () => {
    const r = await api('GET', '/api/products', { token });
    expect(r.status === 200, `status = ${r.status}, mong đợi 200`);
    expect(Array.isArray(r.data.items), 'response thiếu mảng items');
    const x = r.data.items.find((p) => p.pid === prodX.pid);
    expect(x && x.pname === prodX.pname && x.price === 1500 && x.quantity === 100, 'danh sách không chứa sản phẩm X đúng dữ liệu');
    return `${r.data.items.length} sản phẩm`;
  });

  // ───── TC-CUS-04 ─────
  await test('TC-CUS-04', 'Lấy thông tin Membership', async () => {
    const r = await api('GET', `/api/customers/${uid}`, { token });
    expect(r.status === 200, `status = ${r.status}, mong đợi 200`);
    expect(r.data.mid && r.data.membership, 'Customer phải có mid và membership');
    expect(r.data.membership.mname === 'Normal' && r.data.membership.score === 10, `membership = ${JSON.stringify(r.data.membership)}`);
    return `mid=${r.data.mid}, mname=${r.data.membership.mname}, score=${r.data.membership.score}`;
  });

  // ───── TC-CUS-05 ─────
  await test('TC-CUS-05', 'Truy vấn Customer không có token -> 401', async () => {
    const r = await api('GET', `/api/customers/${uid}`);
    expect(r.status === 401, `status = ${r.status}, mong đợi 401`);
    return `401 "${r.data && r.data.message}"`;
  });

  // ───── TC-ORD-04 ─────
  await test('TC-ORD-04', 'Tính discount theo score', async () => {
    const out = [];
    // [mid, score, discount%] theo chính sách: >=100 -> 15, >=50 -> 10, >=10 -> 5
    for (const [mid, score, pct] of [[1, 10, 5], [2, 50, 10], [3, 100, 15]]) {
      if (mid !== 1) {
        const up = await api('PUT', `/api/customers/${uid}`, { token: adminToken, body: { mid } });
        expect(up.status === 200, `admin đổi hạng mid=${mid} thất bại (${up.status})`);
      }
      const r = await order(token);
      expect(r.status === 201, `đặt hàng (score ${score}) status = ${r.status}: ${JSON.stringify(r.data)}`);
      const expected = Math.round(SUBTOTAL * (1 - pct / 100) * 100) / 100;
      expect(r.data.discount === pct, `score ${score}: discount = ${r.data.discount}, mong đợi ${pct}`);
      expect(Math.abs(r.data.total_amount - expected) < 0.005, `score ${score}: total_amount = ${r.data.total_amount}, mong đợi ${expected}`);
      out.push(`score ${score} -> ${pct}%`);
    }
    await api('PUT', `/api/customers/${uid}`, { token: adminToken, body: { mid: 1 } }); // trả về hạng mặc định
    return out.join('; ');
  });

  // ───── TC-ORD-05 ─────
  await test('TC-ORD-05', 'Tạo Order và phát sự kiện OrderCreated', async () => {
    const before = (await api('GET', `/api/products/${prodX.pid}`, { token })).data.quantity;
    const r = await order(token);
    expect(r.status === 201, `status = ${r.status}: ${JSON.stringify(r.data)}`);
    const oid = r.data.oid;
    expect(oid && r.data.items.length === 2, 'Order phải có 2 OrderDetail');

    // 1) Order đã được lưu
    const saved = await api('GET', `/api/orders/${oid}`, { token });
    expect(saved.status === 200 && saved.data.oid === oid && saved.data.items.length === 2, 'không đọc lại được Order vừa tạo');
    // 2) Tồn kho đã giảm
    const after = (await api('GET', `/api/products/${prodX.pid}`, { token })).data.quantity;
    expect(after === before - 2, `tồn kho sản phẩm X: ${before} -> ${after}, mong đợi giảm 2`);
    // 3) Sự kiện đã được phát và Shipment (consumer) đã tạo với trạng thái PENDING
    const ship = await waitFor(async () => {
      const s = await api('GET', `/api/orders/${oid}`, { token });
      return s.data && s.data.shipment;
    });
    expect(ship && ship.status === 'PENDING', 'Shipment chưa được tạo từ sự kiện OrderCreated');
    const notes = [`oid=${oid}`, `shipid=${ship.shipid} PENDING`];

    // 4) (tùy chọn, cần Docker) kiểm tra trực tiếp message trong Kafka và bảng outbox
    const kafkaOut = dockerExec('kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-created --from-beginning --timeout-ms 8000');
    if (kafkaOut !== null) {
      const hit = kafkaOut.split('\n').map((l) => { try { return JSON.parse(l); } catch (_) { return null; } }).find((m) => m && m.oid === oid);
      expect(hit, `không thấy message OrderCreated của oid=${oid} trong topic order-created`);
      expect(hit.eventType === 'OrderCreated', 'message không có eventType = OrderCreated');
      notes.push('Kafka topic order-created có message');
    }
    const ob = dockerExec(`order-db psql -U postgres -d order_db -tAc "SELECT status FROM outbox_events WHERE aggregate_id = ${oid}"`);
    if (ob !== null && ob.trim()) {
      expect(ob.trim() === 'PUBLISHED', `outbox status = ${ob.trim()}, mong đợi PUBLISHED`);
      notes.push('outbox = PUBLISHED');
    }
    return notes.join('; ');
  });

  console.log(`\nTổng: ${passed} đạt, ${failed} lỗi\n`);
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error('Lỗi khi chạy test:', e.message);
  process.exit(2);
});
