// Redis: cache token, cache dữ liệu sản phẩm, rate limiting. Nếu Redis lỗi, gateway vẫn chạy (fail-open).
const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379', { maxRetriesPerRequest: 1, enableOfflineQueue: false });
let warned = false;
redis.on('error', (e) => {
  if (!warned) console.error('[redis] error:', e.message);
  warned = true;
});
redis.on('ready', () => { warned = false; console.log('[redis] ready'); });

async function cacheGet(key) {
  try {
    const v = await redis.get(key);
    return v ? JSON.parse(v) : null;
  } catch (_) {
    return null;
  }
}

async function cacheSet(key, value, ttlSec) {
  try {
    await redis.set(key, JSON.stringify(value), 'EX', Math.max(1, Math.floor(ttlSec)));
  } catch (_) { /* bỏ qua */ }
}

async function invalidate(pattern) {
  try {
    const stream = redis.scanStream({ match: pattern, count: 100 });
    for await (const keys of stream) if (keys.length) await redis.del(...keys);
  } catch (_) { /* bỏ qua */ }
}

module.exports = { redis, cacheGet, cacheSet, invalidate };
