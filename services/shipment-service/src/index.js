const { MongoClient } = require('mongodb');
const { loadProto, err, wrap, startServer, retry } = require('./grpc');
const consumer = require('./consumer');

const PORT = process.env.GRPC_PORT || 50055;
const STATUSES = ['PENDING', 'SHIPPING', 'DELIVERED', 'CANCELLED'];

const client = new MongoClient(process.env.MONGO_URL || 'mongodb://localhost:27017');
let shipments, counters, failedEvents;

async function connect() {
  await client.connect();
  const db = client.db(process.env.MONGO_DB || 'shipment_db');
  shipments = db.collection('shipments');
  counters = db.collection('counters');
  failedEvents = db.collection('failed_events');
  await shipments.createIndex({ shipid: 1 }, { unique: true });
  await shipments.createIndex({ oid: 1 }, { unique: true }); // đảm bảo idempotent: 1 order = 1 shipment
}

async function nextShipId() {
  const r = await counters.findOneAndUpdate({ _id: 'shipid' }, { $inc: { seq: 1 } }, { upsert: true, returnDocument: 'after' });
  return 5000 + r.seq; // shipid bắt đầu từ 5001
}

// Store dùng cho consumer
const store = {
  async createForOrder(ev) {
    if (await shipments.findOne({ oid: ev.oid })) return null;
    const doc = { shipid: await nextShipId(), oid: ev.oid, uid: ev.uid, status: 'PENDING', eventId: ev.eventId || null, createdAt: new Date() };
    try {
      await shipments.insertOne(doc);
      return doc;
    } catch (e) {
      if (e.code === 11000) return null; // event trùng đến đồng thời
      throw e;
    }
  },
  async recordFailure(reason, payload) {
    console.error(`[shipment] event rejected: ${reason}`, typeof payload === 'string' ? payload : JSON.stringify(payload));
    await failedEvents.insertOne({ reason, payload, at: new Date() });
  },
};

const dto = (d) => ({ shipid: d.shipid, oid: d.oid, status: d.status });

// Khách hàng chỉ xem được shipment của mình; admin và shipment_staff xem tất cả
function assertAccess(doc, uid, role) {
  if (role === 'admin' || role === 'shipment_staff') return;
  if (doc.uid !== uid) throw err.forbidden('You cannot access this shipment');
}

const impl = {
  GetShipment: wrap('GetShipment', async ({ shipid, uid, role }) => {
    const d = await shipments.findOne({ shipid });
    if (!d) throw err.notFound('Shipment not found');
    assertAccess(d, uid, role);
    return dto(d);
  }),

  GetShipmentByOrder: wrap('GetShipmentByOrder', async ({ oid, uid, role }) => {
    const d = await shipments.findOne({ oid });
    if (!d) throw err.notFound('Shipment not found');
    assertAccess(d, uid, role);
    return dto(d);
  }),

  UpdateShipmentStatus: wrap('UpdateShipmentStatus', async ({ shipid, status }) => {
    if (!STATUSES.includes(status)) throw err.invalid(`status must be one of: ${STATUSES.join(', ')}`);
    const d = await shipments.findOneAndUpdate({ shipid }, { $set: { status, updatedAt: new Date() } }, { returnDocument: 'after' });
    if (!d) throw err.notFound('Shipment not found');
    return dto(d);
  }),

  Health: wrap('Health', async () => {
    await client.db().command({ ping: 1 });
    return { status: 'UP', kafka: consumer.isConnected() ? 'UP' : 'DOWN' };
  }),
};

(async () => {
  await retry('shipment-db', connect);
  const proto = loadProto('shipment.proto');
  await startServer(proto.shipment.ShipmentService.service, impl, PORT);
  await retry('kafka-consumer', () => consumer.start(store));
})().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
