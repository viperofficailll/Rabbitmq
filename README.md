# RabbitMQ — Complete Guide with Node.js & TypeScript

> A deep-dive reference covering architecture, message flow, queue types, exchanges, bindings, channels, and a full Todo + Notification service example in TypeScript.

---

## Table of Contents

1. [What is RabbitMQ?](#1-what-is-rabbitmq)
2. [Core Components](#2-core-components)
   - 2.1 [Broker](#21-broker)
   - 2.2 [Producer](#22-producer)
   - 2.3 [Consumer](#23-consumer)
   - 2.4 [Connection](#24-connection)
   - 2.5 [Channel](#25-channel)
   - 2.6 [Exchange](#26-exchange)
   - 2.7 [Queue](#27-queue)
   - 2.8 [Binding](#28-binding)
   - 2.9 [Routing Key](#29-routing-key)
   - 2.10 [Virtual Host (vHost)](#210-virtual-host-vhost)
3. [Message Flow — End to End](#3-message-flow--end-to-end)
4. [Exchange Types](#4-exchange-types)
   - 4.1 [Direct Exchange](#41-direct-exchange)
   - 4.2 [Fanout Exchange](#42-fanout-exchange)
   - 4.3 [Topic Exchange](#43-topic-exchange)
   - 4.4 [Headers Exchange](#44-headers-exchange)
   - 4.5 [Default (Nameless) Exchange](#45-default-nameless-exchange)
5. [Queue Types & Properties](#5-queue-types--properties)
   - 5.1 [Classic Queue](#51-classic-queue)
   - 5.2 [Quorum Queue](#52-quorum-queue)
   - 5.3 [Stream Queue](#53-stream-queue)
   - 5.4 [Dead Letter Queue (DLQ)](#54-dead-letter-queue-dlq)
   - 5.5 [Priority Queue](#55-priority-queue)
   - 5.6 [Lazy Queue](#56-lazy-queue)
6. [Message Acknowledgements & Durability](#6-message-acknowledgements--durability)
7. [Prefetch & Quality of Service (QoS)](#7-prefetch--quality-of-service-qos)
8. [RabbitMQ Configuration](#8-rabbitmq-configuration)
   - 8.1 [rabbitmq.conf](#81-rabbitmqconf)
   - 8.2 [advanced.config](#82-advancedconfig)
   - 8.3 [Docker Compose Setup](#83-docker-compose-setup)
9. [Project Setup — Node.js TypeScript](#9-project-setup--nodejs-typescript)
10. [Shared Infrastructure Code](#10-shared-infrastructure-code)
11. [Todo App Service](#11-todo-app-service)
12. [Notification Service](#12-notification-service)
13. [Running Everything](#13-running-everything)
14. [Advanced Patterns](#14-advanced-patterns)
15. [Monitoring & Management](#15-monitoring--management)
16. [Common Pitfalls](#16-common-pitfalls)

---

## 1. What is RabbitMQ?

RabbitMQ is an open-source **message broker** that implements the **AMQP 0-9-1** (Advanced Message Queuing Protocol) specification. It acts as a middleman between services — a producer sends a message to RabbitMQ and a consumer picks it up later, fully decoupled in time and space.

```
┌──────────────────────────────────────────────────────────┐
│                     RabbitMQ Broker                      │
│                                                          │
│  Producer ──▶ Exchange ──▶ Queue ──▶ Consumer            │
│                    │                                     │
│                    └──▶ Queue ──▶ Consumer               │
└──────────────────────────────────────────────────────────┘
```

**Why use RabbitMQ?**

- **Decoupling** — services don't need to know about each other
- **Buffering** — absorb traffic spikes; consumers work at their own pace
- **Reliability** — messages can be persisted and acknowledged
- **Routing** — flexible rules to route messages to one or many queues
- **Language agnostic** — clients in every major language

---

## 2. Core Components

### 2.1 Broker

The **broker** is the RabbitMQ server process itself. It receives messages from producers, routes them through exchanges, stores them in queues, and delivers them to consumers. A broker can host multiple virtual hosts, and clusters of brokers provide high availability.

### 2.2 Producer

A **producer** is any application that publishes (sends) messages to the broker. The producer never sends directly to a queue — it sends to an **exchange** and lets RabbitMQ do the routing.

```
Producer ──publish(exchange, routingKey, message)──▶ Broker
```

### 2.3 Consumer

A **consumer** is any application that subscribes to a queue and receives messages from it. A consumer registers its interest by calling `consume(queueName, callback)`. Multiple consumers can subscribe to the same queue (competing consumers pattern — RabbitMQ round-robins delivery).

### 2.4 Connection

A **connection** is a TCP socket between your application and the RabbitMQ broker. Connections are expensive to open; keep one per application process and multiplex work over **channels**.

```typescript
// One connection per application process
const connection = await amqplib.connect('amqp://localhost');
```

### 2.5 Channel

A **channel** is a lightweight virtual connection inside a real TCP connection. All AMQP operations (declaring exchanges, publishing, consuming) happen on a channel. Think of a connection as a highway and channels as individual lanes.

```
TCP Connection
├── Channel 1  (publisher)
├── Channel 2  (consumer A)
└── Channel 3  (consumer B)
```

**Key rules:**
- Never share a channel across threads/async contexts concurrently — channels are not thread-safe.
- Use a **dedicated channel per consumer** (RabbitMQ tracks per-channel prefetch state).
- Use a **dedicated channel per publisher** when publishing from multiple coroutines.
- If a channel throws an error (e.g., declaring a mis-configured queue), it closes permanently — recreate it.

```typescript
// One channel per logical operation
const publisherChannel = await connection.createChannel();
const consumerChannel  = await connection.createChannel();
```

### 2.6 Exchange

An **exchange** receives messages from producers and routes them to queues based on its **type** and **bindings**. Exchanges are declared with:

| Property | Description |
|---|---|
| `name` | Identifier string (empty string = default exchange) |
| `type` | `direct`, `fanout`, `topic`, `headers` |
| `durable` | Survives broker restart when `true` |
| `autoDelete` | Deletes itself when last binding is removed |
| `internal` | Only reachable from other exchanges (exchange-to-exchange binding) |

```typescript
await channel.assertExchange('todo.events', 'topic', { durable: true });
```

### 2.7 Queue

A **queue** is a buffer that stores messages until a consumer retrieves them. Queues are declared with:

| Property | Description |
|---|---|
| `durable` | Queue and its messages survive restart (requires messages be `persistent`) |
| `exclusive` | Only the declaring connection can use it; auto-deleted on disconnect |
| `autoDelete` | Deleted when last consumer unsubscribes |
| `arguments` | Extra settings: TTL, max length, dead-letter exchange, etc. |

```typescript
await channel.assertQueue('todo.created', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': 'dlx',
    'x-message-ttl': 60000,   // messages expire after 60 seconds
    'x-max-length': 10000,    // max 10 000 messages in queue
  }
});
```

### 2.8 Binding

A **binding** is a rule that connects an exchange to a queue (or another exchange). It includes an optional **binding key** (pattern) that must match the message's routing key for delivery (depends on exchange type).

```
Exchange ──[binding with key "todo.#"]──▶ Queue "notifications"
```

```typescript
// Bind queue "notifications" to exchange "todo.events"
// Only messages with routing keys matching "todo.#" are delivered
await channel.bindQueue('notifications', 'todo.events', 'todo.#');
```

Multiple bindings allow one queue to receive from many exchanges, and one exchange to route to many queues.

### 2.9 Routing Key

A **routing key** is a string the producer attaches to a message when publishing. The exchange uses it (together with binding keys) to decide which queues receive the message. For `direct` exchanges it must match exactly; for `topic` exchanges it can use wildcard patterns.

```typescript
channel.publish('todo.events', 'todo.created', Buffer.from(JSON.stringify(payload)));
//                              ↑ routing key
```

### 2.10 Virtual Host (vHost)

A **virtual host** provides logical isolation inside a single RabbitMQ broker — separate exchanges, queues, users, and permissions. Different applications or environments (dev/staging/prod) can share one broker while remaining completely isolated.

```
amqp://user:pass@localhost:5672/my-app-vhost
```

---

## 3. Message Flow — End to End

```
 ┌────────────┐   publish(exchange, routingKey, body, options)
 │  Producer  │ ──────────────────────────────────────────────────────▶┐
 └────────────┘                                                         │
                                                                        ▼
                                                          ┌─────────────────────┐
                                                          │      Exchange        │
                                                          │  type: topic         │
                                                          │  name: todo.events   │
                                                          └──────┬──────┬────────┘
                                                  binding        │      │  binding
                                              "todo.created"     │      │  "todo.#"
                                                                 ▼      ▼
                                                   ┌──────────────┐  ┌────────────────┐
                                                   │     Queue    │  │     Queue      │
                                                   │  todo-worker │  │  notifications │
                                                   └──────┬───────┘  └───────┬────────┘
                                                          │                  │
                                                          ▼                  ▼
                                                   ┌────────────┐   ┌──────────────────┐
                                                   │  Consumer  │   │     Consumer     │
                                                   │ Todo Worker│   │ Notification Svc │
                                                   └────────────┘   └──────────────────┘
```

**Step-by-step:**

1. **Producer** opens a channel on an existing TCP connection.
2. Producer calls `channel.publish(exchange, routingKey, buffer, options)`.
3. **Exchange** receives the message and evaluates all bindings.
4. For each binding whose key matches `routingKey`, the exchange **enqueues a copy** into the bound queue.
5. If no binding matches and the exchange has no alternate exchange, the message is **dropped** (or returned if `mandatory: true`).
6. Each message sits in the queue in FIFO order (unless priority is set).
7. **Consumer** calls `channel.consume(queue, handler)` — RabbitMQ pushes messages as they arrive.
8. Consumer processes the message and calls `channel.ack(msg)` to remove it from the queue.
9. If processing fails, consumer calls `channel.nack(msg, false, requeue)` to re-queue or discard.

---

## 4. Exchange Types

### 4.1 Direct Exchange

Routes a message to queues whose **binding key exactly matches** the routing key.

```
Exchange: orders.direct
  Binding: "order.paid"    → queue: payment-processor
  Binding: "order.shipped" → queue: shipping-tracker
  Binding: "order.paid"    → queue: notification-service  (multiple queues, same key)
```

```typescript
await ch.assertExchange('orders.direct', 'direct', { durable: true });
await ch.assertQueue('payment-processor', { durable: true });
await ch.bindQueue('payment-processor', 'orders.direct', 'order.paid');

// Publish — only payment-processor and notification-service receive this
ch.publish('orders.direct', 'order.paid', Buffer.from(JSON.stringify(order)));
```

**Use when:** you need deterministic, one-to-one or one-to-few routing based on a fixed string.

---

### 4.2 Fanout Exchange

**Ignores the routing key entirely** — delivers a copy of every message to **all** bound queues.

```
Exchange: broadcast.fanout
  → queue: service-A
  → queue: service-B
  → queue: service-C
  (all three receive every message)
```

```typescript
await ch.assertExchange('broadcast', 'fanout', { durable: true });
await ch.bindQueue('service-a', 'broadcast', '');  // routing key ignored
await ch.bindQueue('service-b', 'broadcast', '');

ch.publish('broadcast', '', Buffer.from('system maintenance in 5 min'));
```

**Use when:** one event must be delivered to many independent consumers (e.g., cache invalidation, live sports scores).

---

### 4.3 Topic Exchange

Routes using **pattern matching** on dot-separated routing keys.

| Wildcard | Meaning |
|---|---|
| `*` | Exactly one word |
| `#` | Zero or more words |

```
Routing keys published:
  "todo.created.urgent"
  "todo.updated.normal"
  "user.registered"

Bindings:
  "todo.#"         → queue: todo-all-events     (matches first two)
  "todo.*.urgent"  → queue: urgent-handler      (matches first only)
  "#"              → queue: audit-log            (matches everything)
```

```typescript
await ch.assertExchange('app.events', 'topic', { durable: true });

await ch.bindQueue('todo-all',     'app.events', 'todo.#');
await ch.bindQueue('urgent',       'app.events', 'todo.*.urgent');
await ch.bindQueue('audit',        'app.events', '#');

ch.publish('app.events', 'todo.created.urgent', Buffer.from(JSON.stringify(msg)));
```

**Use when:** you need flexible, hierarchical routing based on event taxonomy.

---

### 4.4 Headers Exchange

Routes based on **message header attributes** instead of the routing key. Use `x-match: all` (all headers must match) or `x-match: any` (at least one must match).

```typescript
await ch.assertExchange('reports.headers', 'headers', { durable: true });

await ch.bindQueue('pdf-reports', 'reports.headers', '', {
  'x-match': 'all',
  format: 'pdf',
  type: 'report',
});

ch.publish('reports.headers', '', Buffer.from(data), {
  headers: { format: 'pdf', type: 'report', priority: 'high' },
});
```

**Use when:** routing logic is based on multiple metadata attributes, not a single key string.

---

### 4.5 Default (Nameless) Exchange

Every RabbitMQ broker has a built-in direct exchange with the name `""` (empty string). Every queue is automatically bound to it with its own name as the routing key. This lets you publish directly to a queue by name without declaring an exchange.

```typescript
// Sends directly to queue named "my-queue"
ch.sendToQueue('my-queue', Buffer.from('hello'));
// equivalent to:
ch.publish('', 'my-queue', Buffer.from('hello'));
```

---

## 5. Queue Types & Properties

### 5.1 Classic Queue

The original RabbitMQ queue. Stores messages on a single node (mirroring optional with classic mirrored queues, deprecated in 3.x).

```typescript
await ch.assertQueue('classic-queue', {
  durable: true,           // persist across restarts
  exclusive: false,        // accessible by other connections
  autoDelete: false,       // don't delete when unused
});
```

### 5.2 Quorum Queue

Introduced in RabbitMQ 3.8. Uses the **Raft consensus algorithm** — messages are replicated to a quorum (majority) of nodes before being considered written. Much more reliable than classic mirrored queues.

```typescript
await ch.assertQueue('quorum-tasks', {
  durable: true,           // must be true for quorum queues
  arguments: {
    'x-queue-type': 'quorum',
    'x-delivery-limit': 5, // max redelivery attempts before DLQ
  }
});
```

**Use when:** data safety and high availability matter more than raw throughput.

### 5.3 Stream Queue

Introduced in RabbitMQ 3.9. An **append-only log** — messages are never removed on acknowledgement; consumers track their own offset. Supports replay from any offset, like Apache Kafka.

```typescript
await ch.assertQueue('event-stream', {
  durable: true,
  arguments: {
    'x-queue-type': 'stream',
    'x-max-length-bytes': 1_000_000_000, // 1 GB retention
    'x-stream-max-segment-size-bytes': 100_000_000,
  }
});
// Consumer must set x-stream-offset
await ch.consume('event-stream', handler, {
  arguments: { 'x-stream-offset': 'first' } // replay from beginning
});
```

**Use when:** you need event replay, fan-out to many consumers each reading independently, or time-series retention.

### 5.4 Dead Letter Queue (DLQ)

A **dead-letter exchange (DLX)** is a regular exchange. When a message is:
- Rejected with `requeue: false`
- Expired (TTL elapsed)
- Dropped because the queue reached `x-max-length`

…RabbitMQ republishes it to the configured DLX. You bind a queue to that exchange to capture failed messages.

```typescript
// Declare the dead-letter exchange and queue
await ch.assertExchange('dlx', 'direct', { durable: true });
await ch.assertQueue('dead-letters', { durable: true });
await ch.bindQueue('dead-letters', 'dlx', 'todo-worker'); // routing key = original queue name

// Declare the main queue pointing to the DLX
await ch.assertQueue('todo-worker', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': 'dlx',
    'x-dead-letter-routing-key': 'todo-worker', // optional; defaults to original routing key
    'x-message-ttl': 30000,                     // messages dead-lettered after 30 s if unacked
  }
});
```

**Use when:** you need guaranteed delivery with a safety net for poison messages or processing failures.

### 5.5 Priority Queue

Consumers receive higher-priority messages first. Priority range is `0` (lowest) to the declared max.

```typescript
await ch.assertQueue('priority-tasks', {
  durable: true,
  arguments: { 'x-max-priority': 10 }
});

// Publish with priority
ch.sendToQueue('priority-tasks', Buffer.from(JSON.stringify(task)), {
  priority: 8  // 0–10; higher number = higher priority
});
```

**Caution:** Priority has overhead — only use when you genuinely need it.

### 5.6 Lazy Queue

In lazy mode the queue writes messages to disk immediately instead of keeping them in RAM. Useful for very long queues where memory is a concern.

```typescript
await ch.assertQueue('bulk-import', {
  durable: true,
  arguments: { 'x-queue-mode': 'lazy' }
});
```

> **Note:** In RabbitMQ 3.12+ lazy mode is the default for classic queues. The `x-queue-mode` argument is deprecated; use `x-queue-type: quorum` for modern workloads instead.

---

## 6. Message Acknowledgements & Durability

A message is **not removed from the queue** until it is acknowledged. This ensures no data loss even if the consumer crashes mid-processing.

```typescript
// Manual acknowledgement (safest)
ch.consume('my-queue', async (msg) => {
  if (!msg) return;
  try {
    await processMessage(msg);
    ch.ack(msg);                    // ✅ remove from queue
  } catch (err) {
    ch.nack(msg, false, false);     // ❌ don't requeue — send to DLQ
    // ch.nack(msg, false, true);   // 🔁 requeue (careful: infinite loop risk)
  }
}, { noAck: false });               // ALWAYS false for production
```

**For messages to survive a broker restart**, both the queue AND the message must be durable/persistent:

```typescript
// Queue must be durable: true (shown above)

// Message must have deliveryMode: 2 (persistent)
ch.publish('exchange', 'key', Buffer.from(data), {
  persistent: true,       // shorthand for deliveryMode: 2
  contentType: 'application/json',
});
```

---

## 7. Prefetch & Quality of Service (QoS)

By default RabbitMQ floods a consumer with all available messages. **Prefetch** limits how many unacknowledged messages a consumer holds at once — this is critical for fair work distribution and back-pressure.

```typescript
// Consume at most 1 message at a time per channel
// RabbitMQ won't send the next until the current one is acked
await ch.prefetch(1);

// Or allow up to 10 in-flight messages
await ch.prefetch(10);
```

**Rule of thumb:** Start with `prefetch(1)` for CPU-bound work, and tune upward for I/O-bound tasks where you want parallelism.

---

## 8. RabbitMQ Configuration

### 8.1 rabbitmq.conf

The main configuration file (Erlang new-style sysctl format):

```ini
# Network
listeners.tcp.default = 5672
management.tcp.port   = 15672

# Authentication
default_user     = admin
default_pass     = secret
default_vhost    = /

# Memory and disk alarms
vm_memory_high_watermark.relative = 0.6   # pause publishers at 60% RAM usage
disk_free_limit.relative           = 1.5  # alarm when disk < 1.5× RAM

# Message TTL (default, can be overridden per-queue)
# None set here; set per-queue via x-message-ttl argument

# Logging
log.console      = true
log.console.level = info
log.file         = /var/log/rabbitmq/rabbit.log
log.file.level   = info

# Heartbeat (seconds; 0 = disable)
heartbeat = 60

# Max message size (bytes) — default 128 MB
max_message_size = 134217728
```

### 8.2 advanced.config

For settings not exposed in rabbitmq.conf (Erlang term format):

```erlang
[
  {rabbit, [
    %% Allow larger frame sizes
    {frame_max, 131072},
    %% Channel max per connection
    {channel_max, 2047}
  ]},
  {rabbitmq_management, [
    {rates_mode, basic}
  ]}
].
```

### 8.3 Docker Compose Setup

```yaml
# docker-compose.yml
version: "3.9"

services:
  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    container_name: rabbitmq
    hostname: rabbitmq-node1
    ports:
      - "5672:5672"    # AMQP
      - "15672:15672"  # Management UI
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: secret
      RABBITMQ_DEFAULT_VHOST: /
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  rabbitmq_data:
```

Start with:
```bash
docker-compose up -d
# Management UI: http://localhost:15672  (admin / secret)
```

---

## 9. Project Setup — Node.js TypeScript

```
rabbitmq-demo/
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── src/
│   ├── config/
│   │   └── rabbitmq.ts          # connection + channel factory
│   ├── infrastructure/
│   │   ├── exchanges.ts         # declare all exchanges
│   │   ├── queues.ts            # declare all queues + bindings
│   │   └── dlq.ts               # dead-letter setup
│   ├── todo-service/
│   │   ├── producer.ts          # publish todo events
│   │   ├── consumer.ts          # process todo tasks
│   │   └── types.ts             # shared types
│   └── notification-service/
│       ├── consumer.ts          # receive & send notifications
│       └── types.ts
```

### package.json

```json
{
  "name": "rabbitmq-demo",
  "version": "1.0.0",
  "scripts": {
    "build": "tsc",
    "dev:todo": "ts-node src/todo-service/producer.ts",
    "dev:todo-worker": "ts-node src/todo-service/consumer.ts",
    "dev:notifications": "ts-node src/notification-service/consumer.ts"
  },
  "dependencies": {
    "amqplib": "^0.10.4",
    "uuid": "^9.0.0"
  },
  "devDependencies": {
    "@types/amqplib": "^0.10.5",
    "@types/node": "^20.0.0",
    "@types/uuid": "^9.0.0",
    "ts-node": "^10.9.0",
    "typescript": "^5.4.0"
  }
}
```

### tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "commonjs",
    "lib": ["ES2022"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "resolveJsonModule": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

---

## 10. Shared Infrastructure Code

### `src/config/rabbitmq.ts`

```typescript
/**
 * RabbitMQ connection and channel factory.
 *
 * We keep a single TCP connection (expensive to create) and create
 * new channels on demand (cheap, lightweight virtual connections).
 *
 * The module uses a module-level singleton so all imports share
 * the same connection object — no duplicate TCP sockets.
 */
import amqplib, { Connection, Channel, ConfirmChannel } from 'amqplib';

// ─── Configuration ──────────────────────────────────────────────────────────
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://admin:secret@localhost:5672';

// Retry settings — broker may not be ready immediately on startup
const MAX_RETRIES = 10;
const RETRY_DELAY_MS = 2000;

// ─── Module-level singleton ──────────────────────────────────────────────────
let connection: Connection | null = null;

/**
 * Returns an existing connection or creates a new one.
 * Implements an exponential-ish retry loop for Docker startup races.
 */
export async function getConnection(): Promise<Connection> {
  if (connection) return connection;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`[RabbitMQ] Connecting (attempt ${attempt}/${MAX_RETRIES})…`);
      connection = await amqplib.connect(RABBITMQ_URL);

      // React to unexpected connection closure — clear the cached reference
      // so the next call to getConnection() creates a fresh one.
      connection.on('close', () => {
        console.warn('[RabbitMQ] Connection closed unexpectedly');
        connection = null;
      });

      connection.on('error', (err) => {
        console.error('[RabbitMQ] Connection error:', err.message);
      });

      console.log('[RabbitMQ] Connected ✓');
      return connection;
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      console.warn(`[RabbitMQ] Connection failed, retrying in ${RETRY_DELAY_MS}ms…`);
      await sleep(RETRY_DELAY_MS);
    }
  }

  throw new Error('Could not connect to RabbitMQ after maximum retries');
}

/**
 * Creates a standard channel.
 * Use for publishers and simple consumers.
 *
 * Always create ONE channel per logical consumer — RabbitMQ tracks
 * per-channel prefetch counts and unacked message counts.
 */
export async function createChannel(): Promise<Channel> {
  const conn = await getConnection();
  const channel = await conn.createChannel();

  channel.on('error', (err) => {
    console.error('[Channel] Error:', err.message);
  });
  channel.on('close', () => {
    console.warn('[Channel] Channel closed');
  });

  return channel;
}

/**
 * Creates a confirm channel.
 * Confirm channels allow the publisher to receive acks/nacks from
 * the broker confirming that a message was successfully enqueued.
 * Use this whenever you need guaranteed publish delivery.
 */
export async function createConfirmChannel(): Promise<ConfirmChannel> {
  const conn = await getConnection();
  return conn.createConfirmChannel();
}

/**
 * Gracefully close the connection.
 * Call this in your process shutdown handler.
 */
export async function closeConnection(): Promise<void> {
  if (connection) {
    await connection.close();
    connection = null;
    console.log('[RabbitMQ] Connection closed gracefully');
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
```

---

### `src/infrastructure/exchanges.ts`

```typescript
/**
 * Declares all exchanges used by the application.
 *
 * "assertExchange" is idempotent — safe to call on every startup.
 * If the exchange already exists with the same parameters, it's a no-op.
 * If it exists with DIFFERENT parameters, RabbitMQ throws an error (by design).
 *
 * Exchange naming convention: <domain>.<type-hint>
 */
import { Channel } from 'amqplib';

export const EXCHANGES = {
  TODO_EVENTS:   'todo.events',   // topic  — all todo domain events
  NOTIFICATIONS: 'notifications', // fanout — broadcast to all notification consumers
  DLX:           'dlx',           // direct — dead-letter exchange
} as const;

export async function declareExchanges(channel: Channel): Promise<void> {
  // Topic exchange for todo domain events.
  // Routing keys follow the pattern: todo.<action>.<priority>
  //   Examples: todo.created.normal, todo.completed.urgent, todo.deleted.normal
  await channel.assertExchange(EXCHANGES.TODO_EVENTS, 'topic', {
    durable: true,    // survives broker restart
    autoDelete: false // do not delete when no bindings exist
  });

  // Fanout exchange for notifications.
  // Every message is broadcast to ALL notification consumers regardless of routing key.
  // This is useful if you later add SMS, push, email as separate queue bindings.
  await channel.assertExchange(EXCHANGES.NOTIFICATIONS, 'fanout', {
    durable: true,
    autoDelete: false,
  });

  // Dead-letter exchange (direct).
  // Failed messages from any queue get routed here.
  // The DLX is a normal direct exchange — we treat it specially by convention.
  await channel.assertExchange(EXCHANGES.DLX, 'direct', {
    durable: true,
  });

  console.log('[Exchanges] All exchanges declared ✓');
}
```

---

### `src/infrastructure/queues.ts`

```typescript
/**
 * Declares all queues and creates the bindings connecting them to exchanges.
 *
 * Queue naming convention: <service>.<action> or <service>-<purpose>
 *
 * Binding keys (for topic exchange) follow: <domain>.<action>.<priority?>
 */
import { Channel } from 'amqplib';
import { EXCHANGES } from './exchanges';

export const QUEUES = {
  // Todo domain — processed by the todo worker service
  TODO_CREATED:   'todo.created',
  TODO_UPDATED:   'todo.updated',
  TODO_DELETED:   'todo.deleted',
  TODO_COMPLETED: 'todo.completed',

  // Notification service queue
  NOTIFICATIONS:  'notification.inbox',

  // Dead-letter queues — hold unprocessable messages for inspection
  DLQ_TODO:       'dlq.todo',
  DLQ_NOTIFY:     'dlq.notifications',
} as const;

export async function declareQueues(channel: Channel): Promise<void> {
  // ── Dead-letter queues (declare first so they're ready when main queues reference them) ──
  //
  // These are plain durable queues bound to the DLX exchange.
  // Messages end up here when they're nack'd, expired, or overflow the main queue.
  await channel.assertQueue(QUEUES.DLQ_TODO, {
    durable: true,
    // No further dead-lettering for DLQ messages — we just want them parked here
  });
  await channel.bindQueue(QUEUES.DLQ_TODO, EXCHANGES.DLX, QUEUES.TODO_CREATED);
  await channel.bindQueue(QUEUES.DLQ_TODO, EXCHANGES.DLX, QUEUES.TODO_UPDATED);
  await channel.bindQueue(QUEUES.DLQ_TODO, EXCHANGES.DLX, QUEUES.TODO_COMPLETED);
  await channel.bindQueue(QUEUES.DLQ_TODO, EXCHANGES.DLX, QUEUES.TODO_DELETED);

  await channel.assertQueue(QUEUES.DLQ_NOTIFY, { durable: true });
  await channel.bindQueue(QUEUES.DLQ_NOTIFY, EXCHANGES.DLX, QUEUES.NOTIFICATIONS);

  // ── Main todo queues ──────────────────────────────────────────────────────
  //
  // Common options used by all todo queues:
  //   durable: true          — queue survives restart
  //   x-dead-letter-exchange — failed messages go to DLX
  //   x-dead-letter-routing-key — DLX routing key = queue name (so DLQ bindings above work)
  //   x-message-ttl          — messages auto-expire after 5 minutes if not consumed
  //   x-max-length           — prevent unbounded memory growth
  const todoQueueOptions = (queueName: string) => ({
    durable: true,
    arguments: {
      'x-dead-letter-exchange':    EXCHANGES.DLX,
      'x-dead-letter-routing-key': queueName,
      'x-message-ttl':             5 * 60 * 1000, // 5 minutes
      'x-max-length':              50_000,
    },
  });

  await channel.assertQueue(QUEUES.TODO_CREATED,   todoQueueOptions(QUEUES.TODO_CREATED));
  await channel.assertQueue(QUEUES.TODO_UPDATED,   todoQueueOptions(QUEUES.TODO_UPDATED));
  await channel.assertQueue(QUEUES.TODO_DELETED,   todoQueueOptions(QUEUES.TODO_DELETED));
  await channel.assertQueue(QUEUES.TODO_COMPLETED, todoQueueOptions(QUEUES.TODO_COMPLETED));

  // Bind todo queues to the topic exchange using specific routing key patterns.
  // "todo.created" binding key matches routing key "todo.created.normal", "todo.created.urgent", etc.
  // Using exact keys here (not wildcards) for clarity; you could use "todo.created.*" to be explicit.
  await channel.bindQueue(QUEUES.TODO_CREATED,   EXCHANGES.TODO_EVENTS, 'todo.created.#');
  await channel.bindQueue(QUEUES.TODO_UPDATED,   EXCHANGES.TODO_EVENTS, 'todo.updated.#');
  await channel.bindQueue(QUEUES.TODO_DELETED,   EXCHANGES.TODO_EVENTS, 'todo.deleted.#');
  await channel.bindQueue(QUEUES.TODO_COMPLETED, EXCHANGES.TODO_EVENTS, 'todo.completed.#');

  // ── Notification queue ────────────────────────────────────────────────────
  //
  // Bound to the fanout exchange — receives ALL messages published to that exchange.
  // The binding key "" is ignored for fanout exchanges (any string would work).
  await channel.assertQueue(QUEUES.NOTIFICATIONS, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange':    EXCHANGES.DLX,
      'x-dead-letter-routing-key': QUEUES.NOTIFICATIONS,
      'x-message-ttl':             10 * 60 * 1000, // 10 minutes
    },
  });
  await channel.bindQueue(QUEUES.NOTIFICATIONS, EXCHANGES.NOTIFICATIONS, '');

  console.log('[Queues] All queues and bindings declared ✓');
}
```

---

## 11. Todo App Service

### `src/todo-service/types.ts`

```typescript
/**
 * Shared TypeScript types for the Todo domain.
 *
 * These types describe the shape of messages on the wire.
 * Both producer and consumer import from here so any change
 * causes a compile-time error everywhere it matters.
 */

export type TodoPriority = 'low' | 'normal' | 'urgent';
export type TodoAction   = 'created' | 'updated' | 'deleted' | 'completed';

export interface TodoItem {
  id:          string;
  title:       string;
  description: string;
  priority:    TodoPriority;
  completed:   boolean;
  userId:      string;
  createdAt:   string; // ISO 8601
  updatedAt:   string;
}

/**
 * The envelope wrapping every todo event message on the queue.
 * Adding metadata (correlationId, timestamp, version) here makes
 * debugging and tracing dramatically easier.
 */
export interface TodoEventMessage {
  correlationId: string;    // unique ID for tracing the message through the system
  action:        TodoAction;
  payload:       TodoItem;
  timestamp:     string;    // when the event was generated (ISO 8601)
  version:       number;    // schema version — increment when the shape changes
}
```

---

### `src/todo-service/producer.ts`

```typescript
/**
 * Todo Service — Producer
 *
 * Simulates a web API handler that creates/updates/completes todos
 * and publishes domain events to RabbitMQ.
 *
 * Key design decisions:
 *  - Uses a ConfirmChannel so we know the broker received the message.
 *  - Sets persistent: true on every message for durability.
 *  - Constructs the routing key as "todo.<action>.<priority>" so topic
 *    bindings can filter by action AND/OR priority.
 *  - Also publishes a copy to the fanout notification exchange
 *    so the notification service receives all events.
 */
import { v4 as uuidv4 } from 'uuid';
import { ConfirmChannel } from 'amqplib';

import { createConfirmChannel, closeConnection } from '../config/rabbitmq';
import { declareExchanges, EXCHANGES } from '../infrastructure/exchanges';
import { declareQueues } from '../infrastructure/queues';
import { TodoEventMessage, TodoItem, TodoAction } from './types';

// ─── Helper: build routing key ───────────────────────────────────────────────
// Routing key format: "todo.<action>.<priority>"
// This lets consumers bind with:
//   "todo.#"            → all todo events
//   "todo.created.#"    → only created events
//   "todo.*.urgent"     → only urgent events of any action
function buildRoutingKey(action: TodoAction, priority: string): string {
  return `todo.${action}.${priority}`;
}

// ─── Helper: publish with confirmation ──────────────────────────────────────
/**
 * Publishes a message and waits for broker confirmation (publisher confirm mode).
 *
 * Without a confirm channel, publish() returns true/false immediately but
 * the message may still be lost if the broker crashes before persisting it.
 * With a confirm channel, the broker sends an ack once the message is safely
 * written to disk (for persistent messages).
 */
async function publishWithConfirm(
  channel: ConfirmChannel,
  exchange: string,
  routingKey: string,
  message: TodoEventMessage
): Promise<void> {
  const body = Buffer.from(JSON.stringify(message));

  return new Promise((resolve, reject) => {
    const published = channel.publish(exchange, routingKey, body, {
      persistent:    true,              // deliveryMode: 2 — survive broker restart
      contentType:  'application/json',
      correlationId: message.correlationId,
      timestamp:     Date.now(),
      appId:        'todo-service',
    }, (err) => {
      // This callback is invoked after broker confirmation
      if (err) {
        console.error('[Producer] Broker NACK'd message:', err.message);
        reject(err);
      } else {
        resolve();
      }
    });

    // publish() can return false when the write buffer is full (back-pressure).
    // In production you should await the 'drain' event before sending more messages.
    if (!published) {
      console.warn('[Producer] Write buffer full — back-pressure applied');
    }
  });
}

// ─── Main: simulate todo operations ──────────────────────────────────────────
async function main(): Promise<void> {
  // Get a confirm channel for reliable publishing
  const channel = await createConfirmChannel();

  // Declare infrastructure — idempotent, safe to call every startup
  await declareExchanges(channel);
  await declareQueues(channel);

  // Sample todos to simulate
  const todos: TodoItem[] = [
    {
      id:          uuidv4(),
      title:       'Buy groceries',
      description: 'Milk, eggs, bread',
      priority:    'normal',
      completed:   false,
      userId:      'user-001',
      createdAt:   new Date().toISOString(),
      updatedAt:   new Date().toISOString(),
    },
    {
      id:          uuidv4(),
      title:       'Fix critical bug',
      description: 'Production is down — P0 incident',
      priority:    'urgent',
      completed:   false,
      userId:      'user-002',
      createdAt:   new Date().toISOString(),
      updatedAt:   new Date().toISOString(),
    },
  ];

  // ── Publish "created" events ─────────────────────────────────────────────
  for (const todo of todos) {
    const event: TodoEventMessage = {
      correlationId: uuidv4(),
      action:        'created',
      payload:       todo,
      timestamp:     new Date().toISOString(),
      version:       1,
    };

    const routingKey = buildRoutingKey('created', todo.priority);

    // 1. Publish to the topic exchange — routed to specific todo queues
    await publishWithConfirm(channel, EXCHANGES.TODO_EVENTS, routingKey, event);
    console.log(`[Producer] Published todo.created → ${routingKey}  (id: ${todo.id})`);

    // 2. Also publish to the fanout notification exchange
    //    Routing key is ignored for fanout, so we use '' by convention
    await publishWithConfirm(channel, EXCHANGES.NOTIFICATIONS, '', event);
    console.log(`[Producer] Published notification for todo ${todo.id}`);
  }

  // ── Simulate completing a todo ────────────────────────────────────────────
  const completedTodo = { ...todos[0], completed: true, updatedAt: new Date().toISOString() };
  const completedEvent: TodoEventMessage = {
    correlationId: uuidv4(),
    action:        'completed',
    payload:       completedTodo,
    timestamp:     new Date().toISOString(),
    version:       1,
  };

  await publishWithConfirm(channel, EXCHANGES.TODO_EVENTS, buildRoutingKey('completed', completedTodo.priority), completedEvent);
  await publishWithConfirm(channel, EXCHANGES.NOTIFICATIONS, '', completedEvent);
  console.log(`[Producer] Published todo.completed for ${completedTodo.id}`);

  // Close the channel (not the connection) once publishing is done.
  // The connection is kept alive if other parts of the app still need it.
  await channel.close();
  await closeConnection();
}

main().catch(console.error);
```

---

### `src/todo-service/consumer.ts`

```typescript
/**
 * Todo Worker — Consumer
 *
 * Processes todos from the various todo queues.
 *
 * Key design decisions:
 *  - Separate channel per queue — RabbitMQ tracks per-channel prefetch independently.
 *  - prefetch(1) per channel — worker finishes one task before getting another.
 *    This prevents one slow task from holding many messages hostage.
 *  - Manual ack — messages are removed only after successful processing.
 *  - nack with requeue:false on unrecoverable errors — sends to DLQ.
 *  - Graceful shutdown — wait for in-flight messages before closing.
 */
import { Channel, ConsumeMessage } from 'amqplib';

import { createChannel, closeConnection, getConnection } from '../config/rabbitmq';
import { declareExchanges, EXCHANGES } from '../infrastructure/exchanges';
import { declareQueues, QUEUES } from '../infrastructure/queues';
import { TodoEventMessage } from './types';

// ─── Message processor ───────────────────────────────────────────────────────
/**
 * Simulates actual business logic processing a todo event.
 * In production this would: write to a database, call other services, etc.
 *
 * Throws on permanent errors (bad data, schema mismatch) so the caller
 * can nack and dead-letter the message rather than retrying forever.
 */
async function processTodoEvent(event: TodoEventMessage): Promise<void> {
  console.log(`[Worker] Processing ${event.action} for todo "${event.payload.title}" (corr: ${event.correlationId})`);

  // Validate schema version — reject messages we can't process
  if (event.version !== 1) {
    throw new Error(`Unsupported schema version: ${event.version}`);
  }

  // Simulate async work (database write, etc.)
  await new Promise((resolve) => setTimeout(resolve, 50));

  switch (event.action) {
    case 'created':
      // await db.todos.insert(event.payload);
      console.log(`  ✅ Todo created: "${event.payload.title}" [${event.payload.priority}]`);
      break;
    case 'updated':
      // await db.todos.update(event.payload.id, event.payload);
      console.log(`  ✏️  Todo updated: "${event.payload.title}"`);
      break;
    case 'completed':
      // await db.todos.markComplete(event.payload.id);
      console.log(`  🏁 Todo completed: "${event.payload.title}"`);
      break;
    case 'deleted':
      // await db.todos.delete(event.payload.id);
      console.log(`  🗑️  Todo deleted: "${event.payload.title}"`);
      break;
    default:
      throw new Error(`Unknown action: ${(event as TodoEventMessage).action}`);
  }
}

// ─── Generic queue consumer factory ──────────────────────────────────────────
/**
 * Creates a consumer on a single queue with:
 *   - Its own dedicated channel (required for independent prefetch)
 *   - prefetch(1) — process one message at a time
 *   - Error handling with DLQ routing (nack requeue:false)
 *
 * Returns a cleanup function to cancel the consumer gracefully.
 */
async function startQueueConsumer(queueName: string): Promise<() => Promise<void>> {
  // Each consumer gets its OWN channel — never share channels between consumers
  const channel = await createChannel();

  // Limit in-flight messages to 1 per channel.
  // RabbitMQ won't send message N+1 until message N is acked.
  await channel.prefetch(1);

  const { consumerTag } = await channel.consume(
    queueName,
    async (msg: ConsumeMessage | null) => {
      // msg is null when the consumer is cancelled by the server
      if (!msg) return;

      let event: TodoEventMessage;

      try {
        // Parse JSON body — throws SyntaxError on invalid JSON
        event = JSON.parse(msg.content.toString()) as TodoEventMessage;
      } catch (parseErr) {
        console.error(`[Worker:${queueName}] Failed to parse message — sending to DLQ`);
        // nack(msg, allUpTo, requeue)
        //   allUpTo: false  — only this message
        //   requeue: false  — don't put back, send to DLQ
        channel.nack(msg, false, false);
        return;
      }

      try {
        await processTodoEvent(event);
        // ✅ Success — acknowledge the message so RabbitMQ removes it from the queue
        channel.ack(msg);
      } catch (processingErr) {
        const errMsg = processingErr instanceof Error ? processingErr.message : String(processingErr);
        console.error(`[Worker:${queueName}] Processing failed: ${errMsg}`);

        // Decide whether to retry or dead-letter based on the error type.
        // For demo purposes we check the redelivered flag —
        // if this is already a redelivery, dead-letter it to avoid infinite loops.
        const shouldRequeue = !msg.fields.redelivered;

        // nack with requeue:true puts it back at the front of the queue (retry once).
        // nack with requeue:false sends it to the DLQ.
        channel.nack(msg, false, shouldRequeue);

        if (!shouldRequeue) {
          console.warn(`[Worker:${queueName}] Message dead-lettered after retry`);
        }
      }
    },
    { noAck: false } // ALWAYS false — we want manual acknowledgement
  );

  console.log(`[Worker] Consuming "${queueName}" with tag "${consumerTag}"`);

  // Return a cleanup function for graceful shutdown
  return async () => {
    await channel.cancel(consumerTag);
    await channel.close();
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  // Declare infrastructure — must exist before consuming
  const infraChannel = await createChannel();
  await declareExchanges(infraChannel);
  await declareQueues(infraChannel);
  await infraChannel.close(); // done with setup; close this channel

  // Start a consumer for each todo queue.
  // Each consumer is isolated with its own channel + prefetch.
  const cleanups = await Promise.all([
    startQueueConsumer(QUEUES.TODO_CREATED),
    startQueueConsumer(QUEUES.TODO_UPDATED),
    startQueueConsumer(QUEUES.TODO_COMPLETED),
    startQueueConsumer(QUEUES.TODO_DELETED),
  ]);

  console.log('[Worker] All consumers running. Press CTRL+C to stop.');

  // ── Graceful shutdown ────────────────────────────────────────────────────
  // On SIGINT/SIGTERM: cancel consumers (stops receiving new messages),
  // wait for in-flight messages to finish, then close the connection.
  const shutdown = async (signal: string) => {
    console.log(`\n[Worker] Received ${signal} — shutting down gracefully…`);
    await Promise.all(cleanups.map((cleanup) => cleanup()));
    await closeConnection();
    process.exit(0);
  };

  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch(console.error);
```

---

## 12. Notification Service

### `src/notification-service/types.ts`

```typescript
/**
 * Notification service types.
 *
 * The notification service receives TodoEventMessage payloads
 * (imported from the todo domain) and maps them to notifications.
 * We keep a separate type here for what a "rendered" notification looks like.
 */
import { TodoEventMessage } from '../todo-service/types';

export type NotificationChannel = 'email' | 'push' | 'sms' | 'in-app';

export interface Notification {
  id:       string;
  userId:   string;
  channel:  NotificationChannel;
  title:    string;
  body:     string;
  sentAt:   string;
  sourceEvent: TodoEventMessage; // the original event that triggered this notification
}
```

---

### `src/notification-service/consumer.ts`

```typescript
/**
 * Notification Service — Consumer
 *
 * Subscribes to the notification.inbox queue (bound to the fanout exchange).
 * Receives ALL todo events and decides which ones warrant a notification.
 *
 * Design decisions:
 *  - Separate channel with its own prefetch (10 here — notifications are fast I/O)
 *  - Idempotency check via correlationId to prevent duplicate notifications
 *    if a message is redelivered.
 *  - Dead-lettering on permanent failures (bad payload, unknown user).
 *  - Simulates multiple notification channels (email, push) with a dispatcher.
 */
import { v4 as uuidv4 } from 'uuid';
import { Channel, ConsumeMessage } from 'amqplib';

import { createChannel, closeConnection } from '../config/rabbitmq';
import { declareExchanges } from '../infrastructure/exchanges';
import { declareQueues, QUEUES } from '../infrastructure/queues';
import { TodoEventMessage } from '../todo-service/types';
import { Notification, NotificationChannel } from './types';

// ─── Idempotency store (in-memory for demo; use Redis in production) ──────────
const processedCorrelationIds = new Set<string>();

// ─── Notification dispatcher ──────────────────────────────────────────────────
/**
 * Decides which notification channels to use based on event priority and action,
 * then "sends" the notification (simulated with console.log).
 *
 * In production: call SendGrid for email, FCM/APNs for push, Twilio for SMS.
 */
async function dispatchNotification(
  event: TodoEventMessage,
  channel: NotificationChannel
): Promise<void> {
  const notification: Notification = {
    id:      uuidv4(),
    userId:  event.payload.userId,
    channel,
    title:   buildTitle(event),
    body:    buildBody(event),
    sentAt:  new Date().toISOString(),
    sourceEvent: event,
  };

  // Simulate sending the notification
  await new Promise((r) => setTimeout(r, 20));
  console.log(
    `  📬 [${channel.toUpperCase()}] → user ${notification.userId}: "${notification.title}"`
  );
}

function buildTitle(event: TodoEventMessage): string {
  const actionVerb: Record<string, string> = {
    created:   'New todo added',
    updated:   'Todo updated',
    completed: 'Todo completed! 🎉',
    deleted:   'Todo deleted',
  };
  return actionVerb[event.action] ?? 'Todo event';
}

function buildBody(event: TodoEventMessage): string {
  return `"${event.payload.title}" — ${event.payload.priority} priority`;
}

// ─── Processing logic ─────────────────────────────────────────────────────────
/**
 * Core processing function.
 *
 * Determines which channels to notify based on business rules:
 *   - urgent todos → email + push + in-app
 *   - normal todos → in-app only
 *   - low todos    → in-app only on creation; silent otherwise
 *   - completed    → always email (positive reinforcement!)
 */
async function processNotification(event: TodoEventMessage): Promise<void> {
  const { priority } = event.payload;

  // Idempotency: skip if we've already processed this exact event
  // (can happen if the consumer crashes after processing but before acking)
  if (processedCorrelationIds.has(event.correlationId)) {
    console.log(`[Notify] Skipping duplicate event ${event.correlationId}`);
    return;
  }

  console.log(`[Notify] Processing ${event.action} event (priority: ${priority})`);

  const channels: NotificationChannel[] = [];

  if (event.action === 'completed') {
    // Always send email on completion
    channels.push('email', 'in-app');
  } else if (priority === 'urgent') {
    channels.push('email', 'push', 'in-app');
  } else if (priority === 'normal' && event.action === 'created') {
    channels.push('push', 'in-app');
  } else {
    channels.push('in-app');
  }

  // Dispatch to all relevant channels concurrently
  await Promise.all(channels.map((ch) => dispatchNotification(event, ch)));

  // Mark as processed
  processedCorrelationIds.add(event.correlationId);

  // Prevent the set from growing indefinitely in long-running processes
  // (In production, use a Redis SET with TTL instead)
  if (processedCorrelationIds.size > 10_000) {
    const oldest = processedCorrelationIds.values().next().value;
    if (oldest) processedCorrelationIds.delete(oldest);
  }
}

// ─── Consumer setup ───────────────────────────────────────────────────────────
async function startNotificationConsumer(): Promise<() => Promise<void>> {
  const channel = await createChannel();

  // Notification dispatch is fast (mostly I/O).
  // Allow up to 10 concurrent in-flight messages per channel for throughput.
  await channel.prefetch(10);

  const { consumerTag } = await channel.consume(
    QUEUES.NOTIFICATIONS,
    async (msg: ConsumeMessage | null) => {
      if (!msg) return;

      let event: TodoEventMessage;

      try {
        event = JSON.parse(msg.content.toString()) as TodoEventMessage;
      } catch {
        console.error('[Notify] Invalid JSON payload — dead-lettering');
        channel.nack(msg, false, false); // poison message → DLQ
        return;
      }

      try {
        await processNotification(event);
        channel.ack(msg); // ✅ done — remove from queue
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error('[Notify] Failed to process notification:', message);
        // Retry once; dead-letter on second failure
        channel.nack(msg, false, !msg.fields.redelivered);
      }
    },
    { noAck: false }
  );

  console.log(`[Notify] Consuming "${QUEUES.NOTIFICATIONS}" with tag "${consumerTag}"`);

  return async () => {
    await channel.cancel(consumerTag);
    await channel.close();
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  const infraChannel = await createChannel();
  await declareExchanges(infraChannel);
  await declareQueues(infraChannel);
  await infraChannel.close();

  const cleanup = await startNotificationConsumer();

  console.log('[Notify] Notification service running. Press CTRL+C to stop.');

  const shutdown = async (signal: string) => {
    console.log(`\n[Notify] Received ${signal} — shutting down…`);
    await cleanup();
    await closeConnection();
    process.exit(0);
  };

  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch(console.error);
```

---

## 13. Running Everything

### Install dependencies

```bash
npm install
```

### Start RabbitMQ

```bash
docker-compose up -d
# Wait for health check to pass
docker-compose ps

# Management UI: http://localhost:15672  (admin / secret)
```

### Terminal 1 — Todo Worker (consumer)

```bash
npx ts-node src/todo-service/consumer.ts
# Output:
# [RabbitMQ] Connected ✓
# [Exchanges] All exchanges declared ✓
# [Queues] All queues and bindings declared ✓
# [Worker] Consuming "todo.created" with tag "amq.ctag-..."
# [Worker] Consuming "todo.updated" with tag "amq.ctag-..."
# [Worker] Consuming "todo.completed" with tag "amq.ctag-..."
# [Worker] Consuming "todo.deleted" with tag "amq.ctag-..."
# [Worker] All consumers running. Press CTRL+C to stop.
```

### Terminal 2 — Notification Service (consumer)

```bash
npx ts-node src/notification-service/consumer.ts
# Output:
# [RabbitMQ] Connected ✓
# [Notify] Consuming "notification.inbox" with tag "amq.ctag-..."
# [Notify] Notification service running. Press CTRL+C to stop.
```

### Terminal 3 — Publish Todo Events (producer)

```bash
npx ts-node src/todo-service/producer.ts
# Output:
# [RabbitMQ] Connected ✓
# [Producer] Published todo.created → todo.created.normal  (id: ...)
# [Producer] Published notification for todo ...
# [Producer] Published todo.created → todo.created.urgent  (id: ...)
# [Producer] Published notification for todo ...
# [Producer] Published todo.completed for ...
```

### Expected combined output

In Terminal 1 (worker):
```
[Worker] Processing created for todo "Buy groceries" (corr: ...)
  ✅ Todo created: "Buy groceries" [normal]
[Worker] Processing created for todo "Fix critical bug" (corr: ...)
  ✅ Todo created: "Fix critical bug" [urgent]
[Worker] Processing completed for todo "Buy groceries" (corr: ...)
  🏁 Todo completed: "Buy groceries"
```

In Terminal 2 (notifications):
```
[Notify] Processing created event (priority: normal)
  📬 [PUSH] → user user-001: "New todo added"
  📬 [IN-APP] → user user-001: "New todo added"
[Notify] Processing created event (priority: urgent)
  📬 [EMAIL] → user user-002: "New todo added"
  📬 [PUSH] → user user-002: "New todo added"
  📬 [IN-APP] → user user-002: "New todo added"
[Notify] Processing completed event (priority: normal)
  📬 [EMAIL] → user user-001: "Todo completed! 🎉"
  📬 [IN-APP] → user user-001: "Todo completed! 🎉"
```

---

## 14. Advanced Patterns

### Delayed Messages (Retry with Backoff)

Use the **RabbitMQ Delayed Message Plugin** or a TTL+DLQ trick:

```typescript
// Publish to a "delay" queue with a per-message TTL.
// When the TTL expires, the DLX routes it to the actual work queue.
async function retryWithDelay(
  channel: Channel,
  originalMsg: TodoEventMessage,
  delayMs: number
): Promise<void> {
  await channel.assertExchange('retry.delay', 'direct', { durable: true });
  await channel.assertQueue('retry.holding', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange':    'todo.events',           // route back after TTL
      'x-dead-letter-routing-key': `todo.${originalMsg.action}.${originalMsg.payload.priority}`,
      'x-message-ttl':             delayMs,
      'x-max-length':              10_000,
    },
  });
  await channel.bindQueue('retry.holding', 'retry.delay', 'retry');

  channel.publish(
    'retry.delay', 'retry',
    Buffer.from(JSON.stringify(originalMsg)),
    { persistent: true }
  );
  console.log(`[Retry] Message will be retried in ${delayMs}ms`);
}
```

### Request-Reply (RPC Pattern)

```typescript
// ── RPC Client (caller) ──────────────────────────────────────────────────────
async function callRpc(payload: object): Promise<object> {
  const channel = await createChannel();

  // Exclusive, auto-delete queue for the reply — unique per call
  const { queue: replyQueue } = await channel.assertQueue('', {
    exclusive: true,  // only this connection can use it
    autoDelete: true, // deleted when connection closes
  });

  const correlationId = uuidv4();

  return new Promise((resolve, reject) => {
    // Listen for the response on our temporary reply queue
    channel.consume(replyQueue, (msg) => {
      if (!msg) return;
      if (msg.properties.correlationId === correlationId) {
        resolve(JSON.parse(msg.content.toString()));
        channel.ack(msg);
      }
    }, { noAck: false });

    // Send the RPC request, telling the server where to reply
    channel.sendToQueue('rpc.server', Buffer.from(JSON.stringify(payload)), {
      correlationId,
      replyTo: replyQueue,
      persistent: false, // RPC calls are ephemeral
    });

    // Timeout guard
    setTimeout(() => reject(new Error('RPC timeout')), 5000);
  });
}

// ── RPC Server (handler) ─────────────────────────────────────────────────────
async function startRpcServer(): Promise<void> {
  const channel = await createChannel();
  await channel.assertQueue('rpc.server', { durable: false });
  await channel.prefetch(1);

  channel.consume('rpc.server', async (msg) => {
    if (!msg) return;
    const request = JSON.parse(msg.content.toString());

    // Process the request
    const result = { processed: true, input: request };

    // Send the response to the reply queue the client specified
    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(JSON.stringify(result)),
      { correlationId: msg.properties.correlationId }
    );

    channel.ack(msg);
  });
}
```

### Competing Consumers (Horizontal Scale)

Simply start multiple instances of the consumer — RabbitMQ round-robins messages across them automatically:

```bash
# Terminal A
WORKER_ID=A npx ts-node src/todo-service/consumer.ts

# Terminal B
WORKER_ID=B npx ts-node src/todo-service/consumer.ts

# Both workers share the same queue — RabbitMQ distributes messages between them
```

---

## 15. Monitoring & Management

### Management UI

Navigate to `http://localhost:15672` (admin / secret).

Key sections:
- **Overview** — message rates, connection count, memory/disk usage
- **Connections** — all active TCP connections
- **Channels** — all open channels with message counts
- **Exchanges** — all exchanges; click one to publish a test message
- **Queues** — queue depth, consumer count, message rates; click to inspect/purge messages
- **Admin** — manage users, virtual hosts, permissions

### CLI

```bash
# List queues
docker exec rabbitmq rabbitmqctl list_queues name messages consumers

# List exchanges
docker exec rabbitmq rabbitmqctl list_exchanges name type durable

# List bindings
docker exec rabbitmq rabbitmqctl list_bindings

# Check node health
docker exec rabbitmq rabbitmq-diagnostics ping
docker exec rabbitmq rabbitmq-diagnostics status

# Purge a queue (delete all messages)
docker exec rabbitmq rabbitmqctl purge_queue todo.created
```

### Key Metrics to Monitor

| Metric | Warning Threshold | Description |
|---|---|---|
| `queue_messages` | > 10 000 | Consumers can't keep up |
| `queue_messages_unacknowledged` | > prefetch × workers | Workers are stuck |
| `mem_used` | > 60% of `vm_memory_high_watermark` | Approaching publish pause |
| `disk_free` | < `disk_free_limit` | Broker will stop accepting messages |
| `connection_count` | Spiky/growing | Possible connection leak |
| `channel_count` | Growing unbounded | Channel leak — not closing channels |

---

## 16. Common Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| **Sharing a channel across concurrent consumers** | Out-of-order acks, protocol errors | One channel per consumer |
| **Missing prefetch** | Consumer OOM; single slow message blocks queue | `channel.prefetch(N)` before consume |
| **`noAck: true` in production** | Messages lost on crash | Always use manual ack (`noAck: false`) |
| **Publishing to wrong exchange type** | Messages dropped silently | Verify exchange type matches your routing intent |
| **Not declaring infrastructure before consuming** | `NOT_FOUND` channel error | Always `assertQueue`/`assertExchange` on startup |
| **Requeuing failed messages unconditionally** | Infinite poison-pill loop | Dead-letter after N retries or check `msg.fields.redelivered` |
| **Opening a new connection per request** | Connection exhaustion | Reuse one connection; create channels as needed |
| **Not handling channel close events** | Silent failures after errors | Register `channel.on('close', handler)` and recreate |
| **Using `sendToQueue` for cross-service events** | Tight coupling; bypasses exchange routing | Always route through exchanges for inter-service events |
| **Non-durable queues with persistent messages** | Data loss on restart | Queue durability and message persistence must both be `true` |

---

*Generated for RabbitMQ 3.13 with amqplib 0.10 and TypeScript 5. Management UI: http://localhost:15672*