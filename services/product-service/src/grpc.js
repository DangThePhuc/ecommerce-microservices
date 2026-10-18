// Tiện ích gRPC dùng chung (mỗi service giữ một bản để độc lập khi build Docker)
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_DIR = process.env.PROTO_DIR || path.join(__dirname, '..', 'proto');

function loadProto(file) {
  const def = protoLoader.loadSync(path.join(PROTO_DIR, file), {
    keepCase: true,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(def);
}

// Lỗi nghiệp vụ -> gRPC status code (gateway sẽ map sang HTTP status)
class RpcError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const err = {
  invalid: (m) => new RpcError(grpc.status.INVALID_ARGUMENT, m),
  unauthenticated: (m) => new RpcError(grpc.status.UNAUTHENTICATED, m),
  forbidden: (m) => new RpcError(grpc.status.PERMISSION_DENIED, m),
  notFound: (m) => new RpcError(grpc.status.NOT_FOUND, m),
  exists: (m) => new RpcError(grpc.status.ALREADY_EXISTS, m),
  conflict: (m) => new RpcError(grpc.status.FAILED_PRECONDITION, m),
};

// Bọc handler async: trả lỗi đúng chuẩn gRPC, lỗi lạ -> INTERNAL
const wrap = (name, fn) => async (call, callback) => {
  try {
    callback(null, await fn(call.request));
  } catch (e) {
    if (e instanceof RpcError) return callback({ code: e.code, message: e.message });
    console.error(`[${name}] unexpected error:`, e);
    callback({ code: grpc.status.INTERNAL, message: 'Internal server error' });
  }
};

function startServer(serviceDef, impl, port) {
  const server = new grpc.Server();
  server.addService(serviceDef, impl);
  return new Promise((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (e) => {
      if (e) return reject(e);
      console.log(`gRPC server listening on :${port}`);
      resolve(server);
    });
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Thử lại cho tới khi hạ tầng (DB/Kafka) sẵn sàng
async function retry(name, fn, tries = 30, delayMs = 2000) {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i >= tries) throw e;
      console.log(`[${name}] chưa sẵn sàng (${i}/${tries}): ${e.message}`);
      await sleep(delayMs);
    }
  }
}

module.exports = { grpc, loadProto, RpcError, err, wrap, startServer, retry, sleep };
