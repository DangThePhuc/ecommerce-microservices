const { Kafka, logLevel } = require('kafkajs');
const { retry } = require('./grpc');

const TOPIC = process.env.KAFKA_TOPIC || 'order-created';
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 8 },
});

const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
let connected = false;
producer.on(producer.events.CONNECT, () => { connected = true; });
producer.on(producer.events.DISCONNECT, () => { connected = false; });

async function init() {
  await retry('kafka', async () => {
    const admin = kafka.admin();
    await admin.connect();
    try {
      // Tạo topic order-created (3 partition) nếu chưa có
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }] });
    } finally {
      await admin.disconnect();
    }
    await producer.connect();
  });
  console.log(`Kafka producer connected, topic = ${TOPIC}`);
}

const isConnected = () => connected;

module.exports = { init, producer, TOPIC, isConnected };
