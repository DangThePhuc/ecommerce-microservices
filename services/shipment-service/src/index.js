// Shipment Service - Shipment Bounded Context. DB: MongoDB (Shipment DB): shipments, trackings.
// Kafka consumer (OrderCreated -> tao Shipment PENDING) + gRPC (Gateway goi). Co them HTTP /health.
const path = require('path');
const http = require('http');
const { MongoClient } = require('mongodb');
const { Kafka } = require('kafkajs');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PORT = Number(process.env.PORT || 3005);
const GRPC_PORT = Number(process.env.GRPC_PORT || 50055);
const TOPIC = process.env.ORDER_CREATED_TOPIC || 'order-created';
const STATUSES = ['PENDING', 'SHIPPING', 'DELIVERED', 'CANCELLED'];

const mongo = new MongoClient(process.env.MONGO_URL);
let shipments, trackings, counters, failedEvents;
const kafka = new Kafka({ clientId: 'shipment-service', brokers: [process.env.KAFKA_BROKER], retry: { retries: 10 } });
const consumer = kafka.consumer({ groupId: 'shipment-service' });
let consumerReady = false;

class GrpcError extends Error {
  constructor(code, message) { super(message); this.grpcCode = code; }
}
const wrap = (fn) => async (call, cb) => {
  try { cb(null, await fn(call)); } catch (e) {
    if (e instanceof GrpcError) return cb({ code: e.grpcCode, details: e.message });
    console.error('[shipment]', e);
    cb({ code: grpc.status.INTERNAL, details: 'Internal server error' });
  }
};
const caller = (call) => ({
  uid: Number((call.metadata.get('x-user-id') || [])[0]) || 0,
  role: (call.metadata.get('x-user-role') || [])[0] || '',
});
const privileged = (c) => c.role === 'ADMIN' || c.role === 'SHIPMENT_STAFF';
const view = ({ shipid, oid, status }) => ({ shipid, oid, status });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function retry(fn, name, tries = 30) {
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); } catch (e) {
      console.error(`[shipment] ${name} chua san sang (${i}/${tries}): ${e.message}`);
      await sleep(2000);
    }
  }
  throw new Error(`${name} failed`);
}

async function initDb() {
  await mongo.connect();
  const db = mongo.db('shipment_db');
  shipments = db.collection('shipments');
  trackings = db.collection('trackings');
  counters = db.collection('counters');
  failedEvents = db.collection('failed_events');
  await shipments.createIndex({ shipid: 1 }, { unique: true });
  await shipments.createIndex({ oid: 1 }, { unique: true }); // chong tao shipment trung (TC-SHP-05)
  await trackings.createIndex({ shipid: 1 });
}

async function nextShipId() {
  const r = await counters.findOneAndUpdate({ _id: 'shipid' }, { $inc: { seq: 1 } }, { upsert: true, returnDocument: 'after' });
  return 5000 + r.seq; // shipid bat dau tu 5001
}

// ----- xu ly event OrderCreated (BPM: Consume Event -> Tao Shipment status = PENDING) -----
async function handleOrderCreated(raw) {
  let evt;
  try { evt = JSON.parse(raw); } catch {
    console.error('[shipment] event khong phai JSON hop le:', raw);
    return failedEvents.insertOne({ reason: 'INVALID_JSON', raw, at: new Date() });
  }
  if (!evt || !Number.isInteger(evt.oid)) { // TC-SHP-02
    console.error('[shipment] event thieu oid, bo qua:', raw);
    return failedEvents.insertOne({ reason: 'MISSING_OID', raw, at: new Date() });
  }
  if (await shipments.findOne({ oid: evt.oid })) { // TC-SHP-05
    console.log(`[shipment] event trung cho oid=${evt.oid}, bo qua`);
    return;
  }
  const doc = { shipid: await nextShipId(), oid: evt.oid, status: 'PENDING' };
  try {
    await shipments.insertOne({ ...doc });
    await trackings.insertOne({ shipid: doc.shipid, oid: doc.oid, uid: evt.uid ?? null, status: 'PENDING', at: new Date() });
    console.log(`[shipment] tao shipment shipid=${doc.shipid} (PENDING) cho oid=${evt.oid}`);
  } catch (e) {
    if (e.code === 11000) console.log(`[shipment] oid=${evt.oid} da co shipment (race), bo qua`);
    else throw e;
  }
}

async function initKafka() {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }] });
  await admin.disconnect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => handleOrderCreated(message.value ? message.value.toString() : ''),
  });
  consumerReady = true;
  console.log(`[shipment] Kafka consumer san sang, topic=${TOPIC}`);
}

const def = protoLoader.loadSync(path.join(__dirname, '../proto/shipment.proto'), {
  keepCase: true, longs: Number, defaults: true,
});
const shipmentPkg = grpc.loadPackageDefinition(def).shipment;

// khach hang chi xem duoc shipment cua don hang cua minh (uid luu trong tracking)
async function assertCanView(c, shipid) {
  if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
  if (privileged(c)) return;
  if (!(await trackings.findOne({ shipid, uid: c.uid }))) throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden');
}

const handlers = {
  GetShipment: wrap(async (call) => {
    const c = caller(call);
    if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
    const s = await shipments.findOne({ shipid: call.request.shipid });
    if (!s) throw new GrpcError(grpc.status.NOT_FOUND, 'Shipment not found');
    await assertCanView(c, s.shipid);
    return view(s);
  }),

  ListShipments: wrap(async (call) => {
    const c = caller(call);
    if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
    const q = {};
    if (call.request.has_oid) q.oid = call.request.oid;
    if (!privileged(c)) {
      const own = await trackings.distinct('shipid', { uid: c.uid });
      q.shipid = { $in: own };
    }
    const items = await shipments.find(q).sort({ shipid: 1 }).toArray();
    return { items: items.map(view) };
  }),

  UpdateShipmentStatus: wrap(async (call) => {
    const c = caller(call);
    if (!c.uid) throw new GrpcError(grpc.status.UNAUTHENTICATED, 'Unauthorized');
    if (!privileged(c)) throw new GrpcError(grpc.status.PERMISSION_DENIED, 'Forbidden');
    const { shipid, status } = call.request;
    if (!STATUSES.includes(status)) {
      throw new GrpcError(grpc.status.INVALID_ARGUMENT, `status phai thuoc ${STATUSES.join(', ')}`);
    }
    const s = await shipments.findOneAndUpdate({ shipid }, { $set: { status } }, { returnDocument: 'after' });
    if (!s) throw new GrpcError(grpc.status.NOT_FOUND, 'Shipment not found');
    const first = await trackings.findOne({ shipid }, { sort: { at: 1 } });
    await trackings.insertOne({ shipid, oid: s.oid, uid: first ? first.uid : null, status, at: new Date() });
    return view(s);
  }),
};

(async () => {
  await retry(initDb, 'mongodb');
  await retry(initKafka, 'kafka');
  const server = new grpc.Server();
  server.addService(shipmentPkg.ShipmentService.service, handlers);
  server.bindAsync(`0.0.0.0:${GRPC_PORT}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`[shipment] gRPC listening on ${GRPC_PORT}`);
  });
  http.createServer(async (req, res) => {
    try {
      await mongo.db('shipment_db').command({ ping: 1 });
      if (!consumerReady) throw new Error('kafka');
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"status":"UP","service":"shipment-service"}');
    } catch { res.writeHead(503); res.end('{"status":"DOWN"}'); }
  }).listen(PORT, () => console.log(`[shipment] health on ${PORT}`));
})();
