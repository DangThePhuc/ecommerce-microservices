const express = require('express');
const { HttpError } = require('./grpc');
const { logger, rateLimit } = require('./middleware');
const { publicRouter, router, health } = require('./routes');

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(logger);
app.use(express.json({ limit: '100kb' }));

// Health check không bị giới hạn tốc độ (Docker gọi định kỳ)
app.get('/api/health', health);
app.use('/api', rateLimit);
app.use('/api', publicRouter);
app.use('/api', router);

app.use((req, res, next) => next(new HttpError(404, `Route not found: ${req.method} ${req.originalUrl}`)));

// eslint-disable-next-line no-unused-vars
app.use((e, req, res, next) => {
  let status = e.status || 500;
  let message = e.message;
  if (e.type === 'entity.parse.failed') { status = 400; message = 'Invalid JSON body'; }
  if (status === 500) { console.error('Unhandled error:', e); message = 'Internal server error'; }
  res.status(status).json({ status, error: statusText(status), message });
});

function statusText(s) {
  return ({ 400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden', 404: 'Not Found', 409: 'Conflict', 429: 'Too Many Requests', 503: 'Service Unavailable', 504: 'Gateway Timeout' })[s] || 'Internal Server Error';
}

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => console.log(`API Gateway listening on :${PORT}`));
