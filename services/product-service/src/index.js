// Product Service - Product Catalog Bounded Context. DB: MongoDB (Product DB): products.
// Giao tiep: gRPC (Gateway goi CRUD, Order Service goi kiem tra / cap nhat ton kho). Co them HTTP /health.
const path = require('path');
const http = require('http');
const { MongoClient } = require('mongodb');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PORT = Number(process.env.PORT || 3003);
const GRPC_PORT = Number(process.env.GRPC_PORT || 50053);
const client = new MongoClient(process.env.MONGO_URL);
let products, counters;

class GrpcError extends Error {
  constructor(code, message) { super(message); this.grpcCode = code; }
}
const wrap = (fn) => async (call, cb) => {
  try { cb(null, await fn(call)); } catch (e) {
    if (e instanceof GrpcError) return cb({ code: e.grpcCode, details: e.message });
    console.error('[product]', e);
    cb({ code: grpc.status.INTERNAL, details: 'Internal server error' });
  }
};
const role = (call) => (call.metadata.get('x-user-role') || [])[0] || '';
const requireAdmin = (call) => {
  if (role(call) !== 'ADMIN') throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden: Admin only');
};
const noId = { projection: { _id: 0 } };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function retry(fn, name, tries = 30) {
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); } catch (e) {
      console.error(`[product] ${name} chua san sang (${i}/${tries}): ${e.message}`);
      await sleep(2000);
    }
  }
  throw new Error(`${name} failed`);
}

async function nextPid() {
  const r = await counters.findOneAndUpdate({ _id: 'pid' }, { $inc: { seq: 1 } }, { upsert: true, returnDocument: 'after' });
  return r.seq;
}

async function initDb() {
  await client.connect();
  const db = client.db('product_db');
  products = db.collection('products');
  counters = db.collection('counters');
  await products.createIndex({ pid: 1 }, { unique: true });
  if ((await products.countDocuments()) === 0) {
    const seed = [
      { pname: 'Laptop', price: 1500, quantity: 10 },
      { pname: 'Mouse', price: 25, quantity: 100 },
      { pname: 'Keyboard', price: 80, quantity: 50 },
      { pname: 'Monitor', price: 300, quantity: 20 },
    ];
    for (const s of seed) await products.insertOne({ pid: await nextPid(), ...s });
    console.log('[product] da seed du lieu mau');
  }
}

const def = protoLoader.loadSync(path.join(__dirname, '../proto/product.proto'), {
  keepCase: true, longs: Number, defaults: true,
});
const productPkg = grpc.loadPackageDefinition(def).product;

const validPrice = (v) => typeof v === 'number' && Number.isFinite(v) && v >= 0;
const validQty = (v) => Number.isInteger(v) && v >= 0;

async function restore(items) {
  for (const { pid, qty } of items) await products.updateOne({ pid }, { $inc: { quantity: qty } });
}

const handlers = {
  ListProducts: wrap(async () => ({ items: await products.find({}, noId).sort({ pid: 1 }).toArray() })),

  GetProduct: wrap(async (call) => {
    const p = await products.findOne({ pid: call.request.pid }, noId);
    if (!p) throw new GrpcError(grpc.status.NOT_FOUND, 'Product not found');
    return p;
  }),

  CreateProduct: wrap(async (call) => {
    requireAdmin(call);
    const { pname, price, quantity } = call.request;
    if (!pname || !pname.trim() || !validPrice(price) || !validQty(quantity)) {
      throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'pname, price (>=0), quantity (so nguyen >=0) la bat buoc');
    }
    const doc = { pid: await nextPid(), pname: pname.trim().slice(0, 100), price, quantity };
    await products.insertOne({ ...doc });
    return doc;
  }),

  UpdateProduct: wrap(async (call) => {
    requireAdmin(call);
    const q = call.request;
    const set = {};
    if (q.has_pname) { if (!q.pname.trim()) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'pname khong hop le'); set.pname = q.pname.trim().slice(0, 100); }
    if (q.has_price) { if (!validPrice(q.price)) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'price khong hop le'); set.price = q.price; }
    if (q.has_quantity) { if (!validQty(q.quantity)) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'quantity khong hop le'); set.quantity = q.quantity; }
    if (!Object.keys(set).length) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'Khong co du lieu cap nhat');
    const r = await products.updateOne({ pid: q.pid }, { $set: set });
    if (!r.matchedCount) throw new GrpcError(grpc.status.NOT_FOUND, 'Product not found');
    return { message: 'Product updated successfully', pid: q.pid };
  }),

  DeleteProduct: wrap(async (call) => {
    requireAdmin(call);
    const r = await products.deleteOne({ pid: call.request.pid });
    if (!r.deletedCount) throw new GrpcError(grpc.status.NOT_FOUND, 'Product not found');
    return { message: 'Product deleted successfully', pid: call.request.pid };
  }),

  // BPM buoc 6: Order Service hoi gia + so luong ton
  CheckProducts: wrap(async (call) => {
    const pids = [...new Set(call.request.pids)];
    const found = await products.find({ pid: { $in: pids } }, noId).toArray();
    const missing = pids.filter((p) => !found.some((f) => f.pid === p));
    if (missing.length) throw new GrpcError(grpc.status.NOT_FOUND, `Product ${missing.join(', ')} not found`);
    return { items: found };
  }),

  // BPM buoc 7: tru ton kho all-or-nothing (guard $gte chong ban vuot ton khi dat dong thoi)
  UpdateInventory: wrap(async (call) => {
    const items = call.request.items;
    const done = [];
    try {
      for (const { pid, qty } of items) {
        if (qty <= 0) throw new GrpcError(grpc.status.INVALID_ARGUMENT, 'qty phai > 0');
        const r = await products.updateOne({ pid, quantity: { $gte: qty } }, { $inc: { quantity: -qty } });
        if (!r.matchedCount) {
          const exists = await products.findOne({ pid });
          throw exists
            ? new GrpcError(grpc.status.FAILED_PRECONDITION, `Khong du ton kho cho san pham ${pid}`)
            : new GrpcError(grpc.status.NOT_FOUND, `Product ${pid} not found`);
        }
        done.push({ pid, qty });
      }
      return { success: true };
    } catch (e) {
      await restore(done).catch(() => {});
      throw e;
    }
  }),

  RestoreInventory: wrap(async (call) => {
    await restore(call.request.items);
    return { success: true };
  }),
};

(async () => {
  await retry(initDb, 'mongodb');
  const server = new grpc.Server();
  server.addService(productPkg.ProductService.service, handlers);
  server.bindAsync(`0.0.0.0:${GRPC_PORT}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`[product] gRPC listening on ${GRPC_PORT}`);
  });
  http.createServer(async (req, res) => {
    try { await client.db('product_db').command({ ping: 1 }); res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"status":"UP","service":"product-service"}'); }
    catch { res.writeHead(503); res.end('{"status":"DOWN"}'); }
  }).listen(PORT, () => console.log(`[product] health on ${PORT}`));
})();
