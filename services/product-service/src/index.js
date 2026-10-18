const { MongoClient } = require('mongodb');
const { loadProto, err, wrap, startServer, retry } = require('./grpc');

const PORT = process.env.GRPC_PORT || 50053;
const client = new MongoClient(process.env.MONGO_URL || 'mongodb://localhost:27017');
let products, counters;

async function connect() {
  await client.connect();
  const db = client.db(process.env.MONGO_DB || 'product_db');
  products = db.collection('products');
  counters = db.collection('counters');
  await products.createIndex({ pid: 1 }, { unique: true });
}

// pid tự tăng (1, 2, 3, ...) bằng collection counters
async function nextPid() {
  const r = await counters.findOneAndUpdate(
    { _id: 'pid' },
    { $inc: { seq: 1 } },
    { upsert: true, returnDocument: 'after' }
  );
  return r.seq;
}

const dto = (d) => ({ pid: d.pid, pname: d.pname, price: d.price, quantity: d.quantity });

function validate({ pname, price, quantity }) {
  if (typeof pname !== 'string' || !pname.trim() || pname.length > 100) throw err.invalid('pname is required (max 100 chars)');
  if (typeof price !== 'number' || !Number.isFinite(price) || price < 0) throw err.invalid('price must be a number >= 0');
  if (!Number.isInteger(quantity) || quantity < 0) throw err.invalid('quantity must be an integer >= 0');
}

// Gộp các dòng trùng pid và kiểm tra đầu vào
function normalizeItems(items) {
  if (!items || !items.length) throw err.invalid('items must not be empty');
  const map = new Map();
  for (const { pid, qty } of items) {
    if (!Number.isInteger(pid) || pid <= 0 || !Number.isInteger(qty) || qty <= 0) throw err.invalid('each item needs positive integer pid and qty');
    map.set(pid, (map.get(pid) || 0) + qty);
  }
  return [...map].map(([pid, qty]) => ({ pid, qty }));
}

async function loadLines(items) {
  const docs = await products.find({ pid: { $in: items.map((i) => i.pid) } }).toArray();
  const byPid = new Map(docs.map((d) => [d.pid, d]));
  return items.map(({ pid, qty }) => {
    const d = byPid.get(pid);
    if (!d) throw err.notFound(`Product ${pid} not found`);
    if (d.quantity < qty) throw err.conflict(`Insufficient stock for product ${pid} (available ${d.quantity}, requested ${qty})`);
    return { pid, pname: d.pname, unit_price: d.price, qty };
  });
}

const impl = {
  ListProducts: wrap('ListProducts', async () => {
    const docs = await products.find({}).sort({ pid: 1 }).toArray();
    return { items: docs.map(dto) };
  }),

  GetProduct: wrap('GetProduct', async ({ pid }) => {
    const d = await products.findOne({ pid });
    if (!d) throw err.notFound('Product not found');
    return dto(d);
  }),

  CreateProduct: wrap('CreateProduct', async (req) => {
    validate(req);
    const doc = { pid: await nextPid(), pname: req.pname.trim(), price: req.price, quantity: req.quantity, createdAt: new Date() };
    await products.insertOne(doc);
    return dto(doc);
  }),

  UpdateProduct: wrap('UpdateProduct', async (req) => {
    validate(req);
    const r = await products.findOneAndUpdate(
      { pid: req.pid },
      { $set: { pname: req.pname.trim(), price: req.price, quantity: req.quantity, updatedAt: new Date() } },
      { returnDocument: 'after' }
    );
    if (!r) throw err.notFound('Product not found');
    return dto(r);
  }),

  DeleteProduct: wrap('DeleteProduct', async ({ pid }) => {
    const r = await products.deleteOne({ pid });
    if (!r.deletedCount) throw err.notFound('Product not found');
    return { pid };
  }),

  // Bước 6: kiểm tra tồn tại + đủ số lượng (không thay đổi dữ liệu)
  CheckStock: wrap('CheckStock', async ({ items }) => ({ items: await loadLines(normalizeItems(items)) })),

  // Bước 7: trừ tồn kho theo kiểu all-or-nothing (điều kiện quantity >= qty nằm trong câu update nên không bị race)
  ReserveStock: wrap('ReserveStock', async ({ items }) => {
    const list = normalizeItems(items);
    const done = [];
    try {
      const lines = [];
      for (const { pid, qty } of list) {
        const r = await products.findOneAndUpdate(
          { pid, quantity: { $gte: qty } },
          { $inc: { quantity: -qty } },
          { returnDocument: 'after' }
        );
        if (!r) {
          const exists = await products.findOne({ pid });
          if (!exists) throw err.notFound(`Product ${pid} not found`);
          throw err.conflict(`Insufficient stock for product ${pid} (available ${exists.quantity}, requested ${qty})`);
        }
        done.push({ pid, qty });
        lines.push({ pid, pname: r.pname, unit_price: r.price, qty });
      }
      return { items: lines };
    } catch (e) {
      for (const { pid, qty } of done) await products.updateOne({ pid }, { $inc: { quantity: qty } }); // hoàn lại
      throw e;
    }
  }),

  ReleaseStock: wrap('ReleaseStock', async ({ items }) => {
    for (const { pid, qty } of normalizeItems(items)) await products.updateOne({ pid }, { $inc: { quantity: qty } });
    return { released: true };
  }),

  Health: wrap('Health', async () => {
    await client.db().command({ ping: 1 });
    return { status: 'UP' };
  }),
};

(async () => {
  await retry('product-db', connect);
  const proto = loadProto('product.proto');
  await startServer(proto.product.ProductService.service, impl, PORT);
})().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
