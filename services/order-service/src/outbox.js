// Outbox Pattern: Order + OrderDetail + OutboxEvent được lưu trong CÙNG 1 transaction.
// Publisher nền đọc các event PENDING và gửi lên Kafka; chỉ khi gửi thành công mới đánh dấu PUBLISHED.
// Nếu Kafka lỗi, event vẫn ở trạng thái PENDING và sẽ được gửi lại (at-least-once).
const { producer, TOPIC } = require('./kafka');

let running = false;
let timer = null;

async function publishPending(pool) {
  if (running) return;
  running = true;
  try {
    for (;;) {
      const client = await pool.connect();
      let sent = 0;
      let failure = null;
      try {
        await client.query('BEGIN');
        const { rows } = await client.query(
          `SELECT id, aggregate_id, event_type, payload FROM outbox_events
           WHERE status = 'PENDING' ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED`
        );
        for (const ev of rows) {
          try {
            await producer.send({
              topic: TOPIC,
              messages: [{ key: String(ev.aggregate_id), value: JSON.stringify(ev.payload), headers: { 'event-type': ev.event_type } }],
            });
            await client.query(`UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW() WHERE id = $1`, [ev.id]);
            sent++;
          } catch (e) {
            failure = e;
            break;
          }
        }
        await client.query('COMMIT'); // giữ lại các event đã gửi thành công
        if (failure) throw failure;
        if (rows.length === 0) return;
        console.log(`[outbox] published ${sent} event(s) to Kafka topic ${TOPIC}`);
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        console.error('[outbox] publish failed, will retry:', e.message);
        return;
      } finally {
        client.release();
      }
    }
  } finally {
    running = false;
  }
}

function start(pool, intervalMs) {
  timer = setInterval(() => publishPending(pool), intervalMs);
  publishPending(pool);
}

// Gọi ngay sau khi commit để event được gửi nhanh, không phải chờ chu kỳ
const kick = (pool) => { publishPending(pool); };

module.exports = { start, kick, publishPending, stop: () => clearInterval(timer) };
