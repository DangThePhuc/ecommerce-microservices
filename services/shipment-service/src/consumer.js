// Consumer: nhận sự kiện OrderCreated từ Kafka (topic order-created) và tạo Shipment (status = PENDING)
const { Kafka, logLevel } = require('kafkajs');

const TOPIC = process.env.KAFKA_TOPIC || 'order-created';
const kafka = new Kafka({
  clientId: 'shipment-service',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 8 },
});
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'shipment-service' });

let connected = false;
consumer.on(consumer.events.GROUP_JOIN, () => { connected = true; });
consumer.on(consumer.events.STOP, () => { connected = false; });
consumer.on(consumer.events.DISCONNECT, () => { connected = false; });
consumer.on(consumer.events.CRASH, () => { connected = false; });

const isConnected = () => connected;

// Xử lý 1 message. Lỗi dữ liệu (thiếu oid...) -> ghi nhận vào failed_events, KHÔNG tạo Shipment, KHÔNG retry.
// Lỗi hạ tầng (DB down) -> throw để kafkajs retry.
async function handleMessage(raw, store) {
  let ev;
  try {
    ev = JSON.parse(raw);
  } catch (_) {
    return store.recordFailure('Invalid JSON', raw);
  }
  if (!Number.isInteger(ev.oid) || ev.oid <= 0) return store.recordFailure('Missing or invalid oid', ev);
  if (!Number.isInteger(ev.uid) || ev.uid <= 0) return store.recordFailure('Missing or invalid uid', ev);

  const created = await store.createForOrder(ev); // idempotent: cùng oid chỉ tạo 1 Shipment
  if (created) console.log(`[shipment] created shipid=${created.shipid} for oid=${ev.oid} (PENDING)`);
  else console.log(`[shipment] duplicate event for oid=${ev.oid} ignored`);
}

async function start(store) {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => handleMessage(message.value ? message.value.toString() : '', store),
  });
  console.log(`Kafka consumer started, topic = ${TOPIC}`);
}

module.exports = { start, isConnected, handleMessage };
