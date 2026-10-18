const crypto = require('crypto');
const { call, HttpError } = require('./grpc');
const { redis, cacheGet, cacheSet } = require('./redis');

const RATE_MAX = parseInt(process.env.RATE_LIMIT_MAX || '100', 10);
const RATE_WINDOW = parseInt(process.env.RATE_LIMIT_WINDOW_SEC || '60', 10);

// Logging: mỗi request 1 dòng JSON
function logger(req, res, next) {
  const start = process.hrtime.bigint();
  res.on('finish', () => {
    console.log(JSON.stringify({
      time: new Date().toISOString(),
      method: req.method,
      url: req.originalUrl,
      status: res.statusCode,
      ms: Number(process.hrtime.bigint() - start) / 1e6,
      ip: req.ip,
      uid: req.user ? req.user.uid : null,
    }));
  });
  next();
}

// Rate limiting (fixed window) lưu trong Redis theo IP
async function rateLimit(req, res, next) {
  try {
    const window = Math.floor(Date.now() / 1000 / RATE_WINDOW);
    const key = `ratelimit:${req.ip}:${window}`;
    const count = await redis.incr(key);
    if (count === 1) await redis.expire(key, RATE_WINDOW + 1);
    res.set('X-RateLimit-Limit', String(RATE_MAX));
    res.set('X-RateLimit-Remaining', String(Math.max(0, RATE_MAX - count)));
    if (count > RATE_MAX) return next(new HttpError(429, 'Too many requests, please try again later'));
  } catch (_) { /* Redis lỗi -> bỏ qua giới hạn */ }
  next();
}

// Xác thực JWT: hỏi Auth Service (gRPC), cache kết quả trong Redis để giảm tải
async function authenticate(req, res, next) {
  try {
    const m = /^Bearer\s+(\S+)$/i.exec(req.get('Authorization') || '');
    if (!m) throw new HttpError(401, 'Missing or invalid Authorization header');
    const token = m[1];
    const key = `token:${crypto.createHash('sha256').update(token).digest('hex')}`;
    let user = await cacheGet(key);
    if (!user) {
      const r = await call('auth', 'VerifyToken', { token });
      if (!r.valid) throw new HttpError(401, 'Invalid or expired token');
      user = { uid: r.uid, role: r.role };
      await cacheSet(key, user, Math.min(300, r.exp - Math.floor(Date.now() / 1000)));
    }
    req.user = user;
    next();
  } catch (e) {
    next(e);
  }
}

// Phân quyền theo role trong JWT
const requireRole = (...roles) => (req, res, next) =>
  roles.includes(req.user.role) ? next() : next(new HttpError(403, 'Forbidden: insufficient role'));

module.exports = { logger, rateLimit, authenticate, requireRole };
