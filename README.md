# NaviRoute

NaviRoute is a Spring Boot ride-hailing and mobility backend with driver management, ride matching, fare estimation, payments, ratings, and real-time notifications.

It is built as a **modular monolith** using PostgreSQL, Redis, Kafka, WebSocket (STOMP), and Prometheus/Grafana.

> **Project status:** NaviRoute is an existing ride-hailing backend now being developed under the NaviRoute name. It is under active development and is **not production-ready**: payments are simulated, and several security issues are still open. See [Feature Status](#feature-status) and [Known Limitations](#known-limitations).

### Feature status

"Verified" means covered by the automated integration tests, which run the application over HTTP against embedded PostgreSQL 14, an embedded Kafka broker, and an embedded Redis 5.0.14 server (see [Testing](#testing)). These are real processes started by the test suite. They are not the Docker Compose stack or production infrastructure.

| Area | Status |
|---|---|
| Registration, login, JWT issuance | Verified |
| Driver vehicle and document submission, going online after approval | Verified (documents are stored as URLs only) |
| Admin login | Verified, using the configurable admin bootstrap (`NAVIROUTE_ADMIN_EMAIL` / `NAVIROUTE_ADMIN_PASSWORD`). The account seeded by V7 is deactivated by V11 unless its password was changed |
| Driver approval: pending list, approve, reject, re-approve | Verified |
| Fare estimation with Redis fare-rule caching | Verified |
| Ride request and driver matching (by vehicle type) | Verified |
| Ride details, history, start, complete, cancel | Verified |
| Payment record creation after ride completion (via Kafka) | Verified. Payments are **simulated**: there is no gateway, and paying only changes the status |
| Ratings | Verified |
| Real-time notifications (WebSocket delivery) | Implemented, **not verified** |
| Surge pricing, monitoring dashboards, Docker Compose stack | Implemented, **not verified** |

### Origin and attribution

NaviRoute is based on the **Ride-Engine** project: <https://github.com/sasimohith/Ride-Engine>. The original architecture, modules, and most of the code come from that repository. Internal identifiers from the original project (the `com.ridesharing` Java package, the `RideSharingApplication` class, database and Kafka/Redis names) are intentionally unchanged for now.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              NaviRoute (Spring Boot Application)        │
│                                                         │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌──────────────┐  │
│  │  Auth  │  │  Ride  │  │ Driver │  │   Admin      │  │
│  │ Module │  │ Module │  │ Module │  │   Module     │  │
│  └────────┘  └────────┘  └────────┘  └──────────────┘  │
│                                                         │
│  ┌────────┐  ┌────────────────┐  ┌───────────────────┐  │
│  │Pricing │  │  Notification  │  │    Payment        │  │
│  │ Module │  │(WebSocket+Kafka│  │  Module (Kafka)   │  │
│  └────────┘  └────────────────┘  └───────────────────┘  │
│                                                         │
│  ┌────────┐  ┌────────────────────────────────────────┐  │
│  │ Rating │  │       Shared Infrastructure            │  │
│  │ Module │  │   Kafka | Redis | Async | Exceptions   │  │
│  └────────┘  └────────────────────────────────────────┘  │
└────────────────────┬────────────┬───────────────────────┘
                     │            │
         ┌───────────┼────────────┼───────────┐
         ▼           ▼            ▼           ▼
    PostgreSQL     Redis        Kafka    Prometheus
    (Data Store)   (GEO +      (Event    + Grafana
                    Cache)     Stream)   (Monitoring)
```

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Java 17** | Core language |
| **Spring Boot 3.2** | Application framework |
| **Spring Security + JWT (JJWT)** | Stateless authentication & role-based authorization |
| **Spring Data JPA + Hibernate** | ORM for database operations |
| **PostgreSQL** | Primary relational database |
| **Redis** | Driver location tracking (GEO), fare rule caching, surge multipliers |
| **Apache Kafka** | Event-driven communication between modules |
| **WebSocket (STOMP over SockJS)** | Real-time push notifications to riders/drivers |
| **CompletableFuture + @Async** | Asynchronous driver matching |
| **Flyway** | Versioned database migrations (V1–V11) |
| **Prometheus + Grafana** | Metrics collection and monitoring dashboards |
| **Docker + Docker Compose** | Containerized local infrastructure |
| **Maven** | Build and dependency management |
| **JUnit 5 + Mockito, embedded PostgreSQL/Kafka/Redis** | Unit and integration tests (205 tests) |

## Modules

### 1. Auth Module
- User registration (Rider / Driver) with BCrypt password hashing
- JWT-based stateless authentication — login/register return an access token and a refresh token (there is **no** token-refresh endpoint yet)
- Admin self-registration blocked; an administrator is created from configuration at startup (`NAVIROUTE_ADMIN_EMAIL` / `NAVIROUTE_ADMIN_PASSWORD`, no default password)
- Spring Security filter chain; role checks enforced for `/api/admin/**` and payment refunds

### 2. Driver Module
- Vehicle registration with duplicate plate number prevention
- Document submission — stores a document type and a document **URL** (no file upload/storage)
- Location tracking using **Redis GEO** (`GEOADD`, radius search) — only approved, active, ONLINE drivers are added to the matching pool
- Online/Offline/Busy availability management (going online requires admin approval)

### 3. Admin Module
- List drivers, view driver details, list pending drivers (new drivers start as `PENDING`)
- Approve/Reject drivers (publishes a Kafka event); rejecting takes a driver offline, re-approving restores the account
- Suspend/Reactivate user accounts
- Protected by `ROLE_ADMIN`

### 4. Pricing Module
- Fare estimation using the **Haversine formula** (straight-line distance, assumed average speed — no routing/maps integration)
- Per-vehicle-type fare rules (AUTO, BIKE, SEDAN, SUV)
- **Redis cache-aside pattern** for fare rules
- Surge multiplier recalculated every 30 seconds via `@Scheduled`, based on the ratio of pending ride requests to drivers in the location index (one global value applied to all vehicle types)

### 5. Ride Module
- Ride lifecycle: `REQUESTED → ACCEPTED → IN_PROGRESS → COMPLETED / CANCELLED`
- **Asynchronous driver matching** on a custom thread pool: nearest drivers within 5 km via Redis GEO with the requested vehicle type, up to 3 attempts with a fixed 5-second delay; driver claim and ride assignment are atomic conditional updates
- Drivers are **auto-assigned** — there is no driver accept/decline step
- Duplicate active ride prevention
- Kafka events on request, assignment, completion, and cancellation (no event on ride start)
- Final fare is currently set equal to the estimated fare

### 6. Notification Module
- WebSocket (STOMP) push notifications via an in-memory broker
- Kafka consumers for `ride-accepted`, `ride-completed`, `ride-cancelled`, and `driver-approved`
- Async notification delivery via `@Async`

### 7. Payment Module (simulated)
- **Event-driven** payment record creation (Kafka consumer on `ride-completed`), idempotent per ride
- Payment methods recorded: CASH, UPI, WALLET
- **No payment gateway is integrated.** "Paying" marks the record `COMPLETED`; "refunding" marks it `REFUNDED`. No money is moved.
- Rider payment history and driver earnings listing
- Admin refund endpoint (status change only)

### 8. Rating Module
- Bidirectional ratings (rider rates driver, driver rates rider)
- 1–5 star scale with optional comments
- One rating per rater per ride (unique constraint); only COMPLETED rides can be rated
- **Denormalized average rating** on the User entity

### 9. Shared Infrastructure
- Kafka producer/consumer configuration with retry error handler
- Redis configuration with JSON serialization
- Custom `ThreadPoolTaskExecutor` for async operations
- Global exception handler (`@ControllerAdvice`)
- Standard response DTOs (`ApiResponse<T>`, `ErrorResponse`) and custom exceptions

### 10. Monitoring + Deployment
- Multi-stage Dockerfile (build + run, non-root user)
- Docker Compose for local infrastructure: PostgreSQL, Redis, Kafka, Zookeeper, Prometheus, Grafana (the application itself runs on the host)
- Prometheus scraping Spring Boot Actuator metrics
- Pre-provisioned Grafana dashboard **"NaviRoute - Overview"** (HTTP rate, p95 latency, JVM memory, threads, DB pool, error rate, GC)
- Profile-based config (`application-dev.yml`, `application-prod.yml`); see [`.env.example`](.env.example) for environment variables

## Project Structure

```
<repository root>/
├── src/main/java/com/ridesharing/           # Java package name kept from the original project
│   ├── RideSharingApplication.java          # Entry point (@EnableAsync, @EnableScheduling)
│   ├── auth/         # controller, service (AuthService, JwtService), model, repository, filter, config, dto
│   ├── driver/       # DriverController, DriverService, DriverLocationService, Vehicle, DriverDocument
│   ├── admin/        # AdminController, AdminService, AdminDriverResponse
│   ├── pricing/      # PricingController, PricingService, SurgePricingScheduler, FareRule
│   ├── ride/         # RideController, RideService, RideMatchingService, Ride, RideEvent
│   ├── notification/ # NotificationService, Kafka consumers, WebSocketConfig
│   ├── payment/      # PaymentController, PaymentService, PaymentEventConsumer, Payment
│   ├── rating/       # RatingController, RatingService, Rating
│   └── shared/       # kafka, redis, async, exceptions, dto, enums
├── src/main/resources/
│   ├── application.yml, application-dev.yml, application-prod.yml
│   └── db/migration/V1__…V11__*.sql
├── src/test/java/com/ridesharing/           # 191 unit + 14 integration tests
├── docker-compose.yml                       # Local infrastructure
├── Dockerfile                               # Multi-stage build
├── .env.example                             # Environment variable reference (placeholders only)
├── monitoring/                              # Prometheus config + Grafana provisioning
├── scripts/e2e-test.ps1                     # PowerShell API smoke script (see Known Limitations)
└── pom.xml
```

## API Endpoints

"Authenticated" means any valid JWT; the service layer adds further checks where noted.

### Authentication
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| POST | `/api/auth/register` | Public | Register rider/driver |
| POST | `/api/auth/login` | Public | Login, get JWT tokens |

### Driver
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| POST | `/api/driver/vehicle` | Driver | Add vehicle — body `{vehicleType, plateNumber, model, color}` |
| POST | `/api/driver/documents` | Driver | Submit document — body `{documentType, documentUrl}` |
| PUT | `/api/driver/availability` | Driver | Go online/offline — **query params** `status=ONLINE\|OFFLINE\|BUSY`, optional `latitude`, `longitude` |
| PUT | `/api/driver/location` | Driver | Update GPS location — body `{latitude, longitude}` |
| GET | `/api/driver/profile` | Driver | View profile |

### Admin
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| GET | `/api/admin/drivers/pending` | Admin | List pending drivers |
| GET | `/api/admin/drivers` | Admin | List all drivers |
| GET | `/api/admin/drivers/{id}` | Admin | Driver details |
| PUT | `/api/admin/drivers/{id}/approve` | Admin | Approve driver |
| PUT | `/api/admin/drivers/{id}/reject` | Admin | Reject driver |
| PUT | `/api/admin/users/{id}/suspend` | Admin | Suspend user |
| PUT | `/api/admin/users/{id}/reactivate` | Admin | Reactivate user |

### Pricing
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| POST | `/api/pricing/estimate` | Authenticated | Get fare estimate |
| GET | `/api/pricing/fare-rules` | Authenticated | List active fare rules |
| DELETE | `/api/pricing/cache/{type}` | Authenticated (intended for admins; not yet enforced) | Invalidate fare cache |

### Rides
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| POST | `/api/rides/request` | Rider | Request a ride |
| PUT | `/api/rides/{id}/start` | Assigned driver | Start ride |
| PUT | `/api/rides/{id}/complete` | Assigned driver | Complete ride |
| PUT | `/api/rides/{id}/cancel` | Rider/Driver of the ride | Cancel ride |
| GET | `/api/rides/{id}` | Participant | Get ride details |
| GET | `/api/rides/history/rider` | Authenticated | Own ride history as rider |
| GET | `/api/rides/history/driver` | Authenticated | Own ride history as driver |

### Payments (simulated — no gateway)
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| PUT | `/api/payments/{id}/pay` | Rider of the ride | Mark payment completed — body `{paymentMethod: CASH\|UPI\|WALLET}` |
| GET | `/api/payments/ride/{rideId}` | Participant | Payment for ride |
| GET | `/api/payments/history/rider` | Authenticated | Own payment history |
| GET | `/api/payments/history/driver` | Authenticated | Own earnings history |
| PUT | `/api/payments/{id}/refund` | Admin | Mark payment refunded |

### Ratings
| Method | Endpoint | Access | Description |
|--------|----------|--------|-------------|
| POST | `/api/ratings` | Rider/Driver of a completed ride | Submit rating (1-5) |
| GET | `/api/ratings/received` | Authenticated | Ratings received |
| GET | `/api/ratings/given` | Authenticated | Ratings given |
| GET | `/api/ratings/average/{userId}` | Authenticated | Average rating |

## Data Flow — Ride Lifecycle

```
Rider registers → PostgreSQL
Driver registers → PostgreSQL → Admin approves → Kafka → WebSocket notification

Rider requests ride:
  → PricingService: Redis cache → Haversine → fare estimate
  → Save to PostgreSQL (status = REQUESTED)
  → Kafka: publish ride-requested (no consumer yet)
  → Async: Redis GEO radius search (drivers within 5 km)
  → Driver auto-assigned → PostgreSQL (status = ACCEPTED, driver = BUSY)
  → Kafka: publish ride-accepted → WebSocket: "Driver Found!"

Driver starts ride → PostgreSQL (IN_PROGRESS)
Driver completes  → PostgreSQL (COMPLETED, actual fare = estimated fare)
                  → Kafka: ride-completed
                  → PaymentConsumer: creates PENDING payment record
                  → NotificationConsumer: WebSocket push to both

Rider "pays" (simulated) → PostgreSQL (payment COMPLETED)
Rider rates driver        → PostgreSQL (rating + user.average_rating updated)
```

## Getting Started

### Prerequisites
- Java 17+
- Maven 3.8+ (no Maven wrapper is included)
- Docker Desktop (for local infrastructure)

### Quick Start

```bash
# 1. Clone your NaviRoute repository and enter it
git clone <your-naviroute-repository-url>
cd <repository-directory>

# 2. Review environment variables (placeholders only — never commit real values)
#    See .env.example. Spring Boot does not load .env files automatically.
#    To get an administrator account, set NAVIROUTE_ADMIN_EMAIL and
#    NAVIROUTE_ADMIN_PASSWORD (at least 12 characters) before starting the app.
#    There is no default admin password.

# 3. Start infrastructure (PostgreSQL, Redis, Zookeeper, Kafka, Prometheus, Grafana)
docker compose up -d

# 4. Run the application (dev profile by default)
export NAVIROUTE_ADMIN_EMAIL=admin@example.com
export NAVIROUTE_ADMIN_PASSWORD='<choose-a-strong-password>'
mvn spring-boot:run

# 5. Verify
# App:        http://localhost:8080/actuator/health
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000 (default local credentials are set in docker-compose.yml)

# 6. Run unit + integration tests (no Docker needed; see Testing)
mvn verify
```

Local containers: `naviroute-postgres`, `naviroute-redis`, `naviroute-zookeeper`, `naviroute-kafka`, `naviroute-prometheus`, `naviroute-grafana`.
Local database (dev profile): `ridesharing_dev` on `localhost:5432`, user `ridesharing`.

### Test the API

```bash
# Register a rider
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name":"Arun","email":"arun@test.com","password":"pass123","phone":"9876543210","role":"RIDER"}'

# Login
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"arun@test.com","password":"pass123"}'
```

See [SETUP-AND-TESTING-GUIDE.md](SETUP-AND-TESTING-GUIDE.md) for the complete manual testing walkthrough.

## Testing

`mvn verify` runs 205 tests:

```
191 unit tests (JUnit 5 + Mockito; repositories, Kafka and Redis mocked)
├── Shared Infrastructure (incl. Redis serializer round-trip with the real serializer)
├── Auth Module (incl. admin bootstrap)
├── Driver, Pricing, Admin, Ride, Notification, Payment, Rating modules
└── Service-level flow test (e2e/RideSharingE2ETest — Mockito, no running infrastructure)

14 integration tests (full Spring context + HTTP, real infrastructure processes)
├── AdminAndDriverApprovalIntegrationTest — migrations V1–V11, admin login, pending/reject/re-approve
├── RideLifecycleIntegrationTest          — request → match → start → complete → Kafka → payment → rating,
│                                           Redis fare-rule cache, vehicle-type matching, duplicate events
└── MatchingConcurrencyIntegrationTest    — concurrent assignment, cancellation vs. matching (row locks),
                                            after-commit event publishing
```

The integration tests start **embedded PostgreSQL 14** (via `io.zonky.test:embedded-postgres`), an **embedded Kafka** broker (`spring-kafka-test`), and an **embedded Redis 5.0.14** server (`com.github.codemonstur:embedded-redis`). The database and Redis binaries are downloaded as Maven test dependencies, so Docker is not required. The integration tests do not cover WebSocket delivery or the Docker Compose stack.

## Database Schema

11 Flyway migrations. V1–V9 create the schema and seed data; V10 backfills driver approval status, and V11 deactivates the unusable V7 seed admin. The tables:
- `users` — riders, drivers, admins with roles and ratings
- `vehicles` — driver vehicles (type, plate, model, color)
- `driver_documents` — document type + URL
- `fare_rules` — per-vehicle-type pricing (base, per-km, per-minute)
- `rides` — ride lifecycle with timestamps and fare data
- `payments` — one payment record per ride
- `ratings` — bidirectional rider-driver ratings (1-5 stars)

## Event-Driven Architecture (Kafka Topics)

| Topic | Publisher | Consumer(s) |
|-------|-----------|-------------|
| `ride-requested` | RideService | — |
| `ride-accepted` | RideMatchingService | NotificationService |
| `ride-completed` | RideService | PaymentService, NotificationService |
| `ride-cancelled` | RideService, RideMatchingService | NotificationService |
| `driver-approved` | AdminService | NotificationService |

## Monitoring

- **Prometheus** scrapes `/actuator/prometheus` every 15 seconds (job `naviroute`)
- **Grafana** dashboard "NaviRoute - Overview" (folder "NaviRoute") includes HTTP request rate, p95 response time, JVM memory, active threads, DB connection pool, HTTP error rate, and GC pause time

## Known Limitations

NaviRoute is in early development.

### Recently fixed defects

These were found by running the application, and are now covered by regression tests:

- Admin login failed because of the V7 seed password hash. Fixed by the configurable admin bootstrap and V11.
- Ride details, history, start/complete, payment reads, and ratings-received returned HTTP 500 (`LazyInitializationException`). Fixed with bounded service transactions and fetch graphs.
- Ride completion failed before `ride-completed` was published, so no payment was created and the driver stayed `BUSY`. The event is now published only after the completion commits.
- `FareRule` could not be cached in Redis (`LocalDateTime` serialization).
- New drivers had no `PENDING` status. Registration now sets it, and V10 backfills existing drivers.
- Matching ignored the requested vehicle type, and could resurrect a cancelled ride or double-assign a driver under concurrency.
- Rejected-then-approved drivers could not log in.

### Remaining functional risks

- **Event delivery is not durable.** Kafka events are sent after the database commit, so a rolled-back change is never announced. But if the broker is unreachable at that moment, the event is lost: there is no transactional outbox. A lost `ride-completed` means no payment record is created for that ride.
- **Duplicate events.** A redelivered `ride-completed` does not create a second payment, but the WebSocket notification is sent again.
- **Driver location after a ride.** A driver becomes `ONLINE` after completing or cancelling a ride, but rejoins the matching pool only on their next location update.
- **Manual availability.** A driver can set themselves `ONLINE` through the availability endpoint while still on an active ride. Matching skips drivers with an active ride, but this check is not atomic with the driver claim.
- **Vehicle types are free text.** Matching compares the driver's registered vehicle type with the requested type (case-insensitive). A driver registered as `CAR` never matches `SEDAN` or `SUV` requests.

### Security issues (do not deploy publicly until fixed)

- **Fallback JWT signing key.** If `JWT_SECRET` is unset, a built-in default key is used, including in the `prod` profile.
- **Refresh tokens are accepted as access tokens** (confirmed).
- **Suspended users' existing tokens remain valid** until they expire (confirmed).
- **Fare-cache invalidation is not restricted.** Any authenticated user can call `DELETE /api/pricing/cache/{type}` (confirmed).
- **Actuator endpoints are public**, including `/actuator/metrics` (confirmed) and detailed health output.
- **WebSocket subscriptions are unauthenticated**, so any client can subscribe to other users' ride topics.
- **Default admin account from migration V7.** It is deactivated by V11 if its password was never changed. A deployment where it was changed keeps it active.
- **Account enumeration.** Login reports whether an email is registered, and emails differing only by letter case can be registered separately.

### Not implemented or simulated

- **Payments are simulated**: there is no payment gateway, and pay/refund only change status.
- **No token refresh endpoint**: refresh tokens are issued but cannot be exchanged.
- **No driver accept/decline**: matching auto-assigns drivers.
- **No routing/maps integration**: distance is straight-line (Haversine), time assumes a fixed average speed, and the final fare equals the estimate.
- **Documents** are stored as URLs only; there is no file upload or storage.
- `scripts/e2e-test.ps1` sends some request payloads that do not match the current API (availability, documents, driver ID).

### Not yet verified

- WebSocket notification delivery, surge pricing, the Docker Compose / Prometheus / Grafana stack, and behaviour against production-grade PostgreSQL, Kafka, or Redis deployments (the tests use embedded single-node instances; the embedded Redis is version 5.0.14, while Compose uses Redis 7).

## License

No license file is included. The original Ride-Engine README stated: "This project is for educational purposes." Permission to modify and redistribute the original code has not been established — confirm licensing with the original author before any public redistribution of NaviRoute.
