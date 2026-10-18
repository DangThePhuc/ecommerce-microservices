# E-commerce MVP – Microservices (Node.js, gRPC, Kafka, Docker Compose)

Bài tập **PROMPT 3 – Microservice**. Hệ thống thương mại điện tử MVP theo kiến trúc **Microservices + DDD + Database-per-Service**,
bám theo 5 tài liệu thiết kế (ERD, Bounded Context, Microservice Architecture, Business Process Model, API Specification).

```
Client ──REST──► API Gateway ──gRPC──► Auth · Customer · Order · Product · Shipment
                 (Redis)                 │        │       │  ▲       │       ▲
                                         │        │       │  └gRPC───┘       │
                                         │        │       └─ Outbox ► Kafka ─┘  (order-created)
                                      Postgres Postgres Postgres  MongoDB   MongoDB
```

## 1. Cấu trúc dự án

```
.
├── docker-compose.yml            # 13 container (xem mục 3)
├── .env                          # biến môi trường (đã nằm trong .gitignore, không commit lên GitHub)
├── .gitignore
├── proto/                        # HỢP ĐỒNG gRPC giữa các service (mục 2)
│   ├── auth.proto  customer.proto  product.proto  order.proto  shipment.proto
├── api-gateway/                  # REST → gRPC, JWT, phân quyền, rate limit, cache, logging
│   └── src/ index.js · routes.js · middleware.js · grpc.js · redis.js
├── services/
│   ├── auth-service/             # Identity & Access   – PostgreSQL (roles, users)
│   ├── customer-service/         # Customer & Membership – PostgreSQL (customers, memberships)
│   ├── product-service/          # Product Catalog     – MongoDB (products)
│   ├── order-service/            # Order Management    – PostgreSQL (orders, order_details, outbox_events) + Kafka producer
│   │   └── src/ index.js · discount.js · outbox.js · kafka.js · grpc.js
│   └── shipment-service/         # Shipment            – MongoDB (shipments) + Kafka consumer
├── scripts/test-api.js           # test tự động cho 5 test case của phiếu chấm 3
└── postman/Ecommerce-Microservices.postman_collection.json
```

Mỗi service chỉ sở hữu **database riêng**, không truy cập DB của service khác; dữ liệu liên service chỉ đi qua **gRPC** (đồng bộ) hoặc **Kafka** (bất đồng bộ).
Dockerfile của mỗi service build với context là thư mục gốc để copy thư mục `proto/` dùng chung vào image.

## 2. Giải thích các proto

| File | Service (port gRPC) | RPC chính | Ai gọi |
|---|---|---|---|
| `auth.proto` | Auth (50051) | `Register`, `Login`, `VerifyToken`, `DeleteUser`, `Health` | Gateway |
| `customer.proto` | Customer (50052) | `CreateCustomer`, `GetCustomer`, `UpdateCustomer`, `GetMembership`, `Health` | Gateway; **Order** gọi `GetMembership` để lấy score |
| `product.proto` | Product (50053) | `List/Get/Create/Update/DeleteProduct`, `CheckStock`, `ReserveStock`, `ReleaseStock`, `Health` | Gateway; **Order** gọi `CheckStock` → `ReserveStock` (và `ReleaseStock` để bù trừ) |
| `order.proto` | Order (50054) | `CreateOrder`, `GetOrder`, `ListOrders`, `Health` | Gateway |
| `shipment.proto` | Shipment (50055) | `GetShipment`, `GetShipmentByOrder`, `UpdateShipmentStatus`, `Health` | Gateway |

Điểm cần nêu khi giải thích:
- **Contract-first**: `.proto` là hợp đồng duy nhất; client và server cùng nạp file này (`@grpc/proto-loader`), nên đổi field là phải đổi ở một chỗ.
- **Mỗi bounded context một proto/package** → khớp 5 Bounded Context trong sơ đồ.
- `uid` trong `customer.proto`, `pid`/`qty` trong `order.proto` là *tham chiếu theo ID* chứ không nhúng object của context khác (đúng DDD, không có FK liên DB).
- `ReserveStock` trừ kho **all-or-nothing** ngay trong câu lệnh Mongo (`quantity >= qty`) nên không bị race khi nhiều đơn đặt cùng lúc; nếu lưu Order lỗi thì Order Service gọi `ReleaseStock` để hoàn kho.
- Mã lỗi gRPC được Gateway đổi sang HTTP: `INVALID_ARGUMENT→400`, `UNAUTHENTICATED→401`, `PERMISSION_DENIED→403`, `NOT_FOUND→404`, `ALREADY_EXISTS→409`, `FAILED_PRECONDITION→409` (hết hàng), `UNAVAILABLE→503`.

## 3. Chạy hệ thống và liệt kê container

Yêu cầu: Docker + Docker Compose v2.

```bash
docker compose up -d --build
docker compose ps               # hoặc:
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
curl http://localhost:3003/api/health
```

Phải thấy đúng **13 container** như thiết kế:

| Nhóm | Container |
|---|---|
| Gateway | `api-gateway` (cổng 3003), `redis` |
| Identity & Access | `auth-service`, `auth-db` (PostgreSQL) |
| Customer & Membership | `customer-service`, `customer-db` (PostgreSQL) |
| Order Management | `order-service`, `order-db` (PostgreSQL) |
| Product Catalog | `product-service`, `product-db` (MongoDB) |
| Shipment | `shipment-service`, `shipment-db` (MongoDB) |
| Message Broker | `kafka` (KRaft, không cần Zookeeper) |

Health check: `GET /api/health` trả `200 {"status":"UP","message":"E-commerce API is running","components":{...}}`
(kiểm tra Redis, 5 service/DB và Kafka; có thành phần lỗi thì trả `503`).
Dừng: `docker compose down` (thêm `-v` để xóa dữ liệu).

**Tài khoản có sẵn** (seed khi Auth Service khởi động): `admin / Admin@123` (role `admin`), `staff / Staff@123` (role `shipment_staff`).
Người đăng ký mới tự có role `normal` và hạng thành viên mặc định *Normal* (score 10).

## 4. Kafka – giải thích và kiểm tra

**Vai trò**: tách Order Service khỏi Shipment Service. Order Service không gọi trực tiếp Shipment; nó phát sự kiện `OrderCreated`
lên topic **`order-created`**, Shipment Service tự consume rồi tạo Shipment (`PENDING`). Shipment chậm/chết thì đặt hàng vẫn thành công,
khởi động lại sẽ xử lý tiếp các sự kiện còn lại.

**Luồng (Outbox Pattern)**
1. Order Service mở 1 transaction PostgreSQL: lưu `orders` + `order_details` + `outbox_events(status=PENDING)` → commit. Nếu lỗi thì rollback tất cả, không có đơn "ma".
2. Publisher nền (`outbox.js`) đọc event `PENDING`, gửi lên Kafka (key = `oid`, producer idempotent), thành công mới đổi sang `PUBLISHED`. Kafka lỗi → event vẫn `PENDING` và được gửi lại (at-least-once).
3. Shipment Service (consumer group `shipment-service`) nhận event và tạo Shipment. **Idempotent**: unique index trên `oid` nên nhận cùng event 2 lần không tạo trùng. Event thiếu `oid`/`uid` bị từ chối, ghi vào collection `failed_events`.

Nội dung message:
```json
{"eventId":"…","eventType":"OrderCreated","occurredAt":"…","oid":1001,"uid":3,"discount":5,"totalAmount":2968.75,
 "items":[{"pid":1,"qty":2,"unit_price":1500},{"pid":2,"qty":5,"unit_price":25}]}
```

Lệnh kiểm tra:
```bash
# topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic order-created
# xem message đã phát
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-created --from-beginning --timeout-ms 5000
# consumer group của Shipment (LAG = 0 nghĩa là đã xử lý hết)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group shipment-service
# outbox trong Order DB
docker compose exec order-db psql -U postgres -d order_db -c "SELECT id, aggregate_id, event_type, status, published_at FROM outbox_events ORDER BY id DESC LIMIT 5;"
# Shipment do consumer tạo
docker compose exec shipment-db mongosh shipment_db --quiet --eval "db.shipments.find().sort({shipid:-1}).limit(5)"
# log
docker compose logs --tail 20 order-service shipment-service
```

## 5. Các test case của phiếu chấm 3

Chạy tự động (Node ≥ 18, không cần cài gì thêm): `node scripts/test-api.js`
hoặc import `postman/Ecommerce-Microservices.postman_collection.json` (chạy thư mục *0. Setup* trước, rồi từng thư mục `TC-*`).

| Test case | Cách kiểm tra | Kết quả mong đợi |
|---|---|---|
| **TC-PRO-01** Lấy danh sách Product | `GET /api/products` + Bearer token | `200`, `{"items":[{pid,pname,price,quantity},…]}` |
| **TC-CUS-04** Lấy thông tin Membership | `GET /api/customers/:uid` + Bearer token | `200`, có `mid` và `membership.mname`, `membership.score` (mặc định `Normal`, `10`) |
| **TC-CUS-05** Customer không có token | `GET /api/customers/:uid` không gửi JWT | `401 Unauthorized` |
| **TC-ORD-04** Discount theo score | Đặt cùng đơn (X×2 giá 1500, Y×5 giá 25 → 3125) với score 10 / 50 / 100 | discount `5%` / `10%` / `15%`; `total_amount` = 2968.75 / 2812.5 / 2656.25 |
| **TC-ORD-05** Tạo Order và phát sự kiện | `POST /api/orders` rồi xem DB, Kafka, Shipment | `201`; Order + OrderDetail trong DB; tồn kho giảm; `outbox_events.status = PUBLISHED`; message trong topic `order-created`; Shipment `PENDING` |

Ví dụ bằng curl:
```bash
B=http://localhost:3003
ADMIN=$(curl -s $B/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"Admin@123"}' | sed 's/.*"accessToken":"\([^"]*\)".*/\1/')
curl -s $B/api/products -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{"pname":"Laptop","price":1500,"quantity":100}'
curl -s $B/api/products -H "Authorization: Bearer $ADMIN"                                   # TC-PRO-01
curl -s -o /dev/null -w '%{http_code}\n' $B/api/customers/1                                  # TC-CUS-05 -> 401
```

**Chính sách discount** (`services/order-service/src/discount.js`): score ≥ 100 → 15%; ≥ 50 → 10%; ≥ 10 → 5%; còn lại 0%.
`discount` lưu trong bảng `orders` là **phần trăm**; `total_amount = tổng(qty × unit_price) × (1 − discount/100)`.
Hạng thành viên: `1 Normal (10)`, `2 Silver (50)`, `3 Gold (100)`; chỉ **admin** được đổi `mid` của khách hàng (`PUT /api/customers/:uid`).

## 6. Bảng API (theo API Specification)

| Method & Endpoint | Auth | Ghi chú |
|---|---|---|
| `POST /api/auth/register` · `POST /api/auth/login` | Không | bcrypt, JWT (`sub`, `role`, hết hạn 3600s) |
| `GET/PUT /api/customers/:uid` | JWT | chủ tài khoản hoặc admin |
| `GET /api/products` · `GET /api/products/:pid` | JWT | cache Redis |
| `POST/PUT/DELETE /api/products[/:pid]` | JWT admin | |
| `POST /api/orders` · `GET /api/orders` · `GET /api/orders/:oid` | JWT | `409` khi hết hàng; khách chỉ xem đơn của mình |
| `GET /api/shipments/:shipid` | JWT | |
| `PATCH /api/shipments/:shipid/status` | JWT `shipment_staff`/admin | `PENDING`, `SHIPPING`, `DELIVERED`, `CANCELLED` |
| `GET /api/health` | Không | |

API Gateway: xác thực JWT bằng cách hỏi Auth Service (kết quả cache trong Redis), phân quyền theo role, rate limit theo IP (mặc định 100 request/60 giây, trả `429`), ghi log JSON mỗi request, cache danh sách/chi tiết sản phẩm (tự xóa khi sản phẩm hoặc tồn kho thay đổi).

## 7. Lưu ý thiết kế

- `POST /api/orders` trả kèm `shipment` nếu consumer kịp tạo trong ~3 giây; nếu chưa kịp thì `shipment` là `null` (đúng bản chất bất đồng bộ) và có thể xem lại bằng `GET /api/orders/:oid`.
- Theo ERD, bảng `users` không có email/phone nên Gateway chỉ **kiểm tra định dạng** hai trường này nếu được gửi, không lưu. Vì vậy lỗi `409` khi đăng ký chỉ xét trùng `username`.
- `uid` của Customer chính là `uid` của User (tham chiếu theo ID, không có FK vì khác database). Đăng ký là thao tác 2 bước (tạo User rồi tạo Customer); nếu bước 2 lỗi, Gateway xóa User vừa tạo (bù trừ).
- Mật khẩu/secret mặc định chỉ dành cho môi trường học tập; khi triển khai thật phải đặt `JWT_SECRET`, mật khẩu DB trong `.env`.
