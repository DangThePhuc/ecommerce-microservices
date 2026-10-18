// gRPC clients tới 5 service + chuyển mã lỗi gRPC -> HTTP
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_DIR = process.env.PROTO_DIR || path.join(__dirname, '..', 'proto');

function load(file) {
  return grpc.loadPackageDefinition(
    protoLoader.loadSync(path.join(PROTO_DIR, file), { keepCase: true, longs: Number, enums: String, defaults: true, oneofs: true })
  );
}

const cred = grpc.credentials.createInsecure();
const clients = {
  auth: new (load('auth.proto').auth.AuthService)(process.env.AUTH_GRPC_URL || 'auth-service:50051', cred),
  customer: new (load('customer.proto').customer.CustomerService)(process.env.CUSTOMER_GRPC_URL || 'customer-service:50052', cred),
  product: new (load('product.proto').product.ProductService)(process.env.PRODUCT_GRPC_URL || 'product-service:50053', cred),
  order: new (load('order.proto').order.OrderService)(process.env.ORDER_GRPC_URL || 'order-service:50054', cred),
  shipment: new (load('shipment.proto').shipment.ShipmentService)(process.env.SHIPMENT_GRPC_URL || 'shipment-service:50055', cred),
};

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

const GRPC_TO_HTTP = {
  [grpc.status.INVALID_ARGUMENT]: 400,
  [grpc.status.UNAUTHENTICATED]: 401,
  [grpc.status.PERMISSION_DENIED]: 403,
  [grpc.status.NOT_FOUND]: 404,
  [grpc.status.ALREADY_EXISTS]: 409,
  [grpc.status.FAILED_PRECONDITION]: 409,
  [grpc.status.UNAVAILABLE]: 503,
  [grpc.status.DEADLINE_EXCEEDED]: 504,
};

// call('order', 'CreateOrder', {...}) -> Promise
function call(service, method, request, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    clients[service][method](request, { deadline: Date.now() + timeoutMs }, (e, res) => {
      if (!e) return resolve(res);
      const status = GRPC_TO_HTTP[e.code] || 500;
      reject(new HttpError(status, status === 500 ? 'Internal server error' : e.details || e.message));
    });
  });
}

module.exports = { call, HttpError };
