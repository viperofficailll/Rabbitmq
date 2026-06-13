# RabbitMQ Interview Masterclass
## Answering Every Hard Question — With Code, Diagrams, and Trade-offs

> Built around real backend interview questions. Not just *what* to use — but *how it works*, *why it breaks*, and *how you design around failure*.

---

## Table of Contents

1. [Q1 — What happens if your consumer crashes before processing a message?](#q1--what-happens-if-your-consumer-crashes-before-processing-a-message)
2. [Q2 — How is RabbitMQ different from Redis Pub/Sub?](#q2--how-is-rabbitmq-different-from-redis-pubsub)
3. [Q3 — How do microservices communicate reliably with each other?](#q3--how-do-microservices-communicate-reliably-with-each-other)
4. [Q4 — What will you do if Service A calls Service B and Service B is down?](#q4--what-will-you-do-if-service-a-calls-service-b-and-service-b-is-down)
5. [Q5 — How do you scale RabbitMQ consumers in production?](#q5--how-do-you-scale-rabbitmq-consumers-in-production)
6. [Q6 — How do you design retry logic with exponential backoff?](#q6--how-do-you-design-retry-logic-with-exponential-backoff)
7. [Q7 — Dead Letter Queues — what are they really for?](#q7--dead-letter-queues--what-are-they-really-for)
8. [Q8 — How do you guarantee exactly-once processing?](#q8--how-do-you-guarantee-exactly-once-processing)
9. [Q9 — RabbitMQ vs Kafka — when to use which?](#q9--rabbitmq-vs-kafka--when-to-use-which)
10. [Q10 — How do you handle message ordering?](#q10--how-do-you-handle-message-ordering)
11. [Q11 — How do you monitor RabbitMQ in production?](#q11--how-do-you-monitor-rabbitmq-in-production)
12. [Q12 — How do you design a complete reliable messaging system?](#q12--how-do-you-design-a-complete-reliable-messaging-system)
13. [The Mental Model — How to Think About All of This](#the-mental-model--how-to-think-about-all-of-this)

---

## Q1 — What happens if your consumer crashes before processing a message?

### The Wrong Answer (and why interviewers hate it)

> "It's fine, the message is in the queue."

This is wrong. Whether the message is lost depends entirely on how you configured acknowledgements. Most junior developers don't know this.

### What Actually Happens

When a consumer receives a message, RabbitMQ marks it as **Unacknowledged (Unacked)**. The message is still in the queue — just in a "delivered but not confirmed" state. What happens next depends on `noAck`:

```
noAck: true  → RabbitMQ removes the message THE MOMENT it's delivered.
               Consumer crash = message GONE FOREVER.

noAck: false → Message stays Unacked until consumer calls ack() or nack().
               Consumer crash = message goes back to READY state → redelivered.
```

```
Queue state visualization:

READY         UNACKED        ACKNOWLEDGED
┌──────────┐  ┌──────────┐   (removed from queue)
│  msg-1   │  │  msg-2   │
│  msg-3   │  │  msg-4   │  ← consumer received these
│  msg-5   │  └──────────┘
└──────────┘
     ↑
     consumers pull from here

If consumer crashes → msg-2 and msg-4 move back to READY
```

### The Three Outcomes

```typescript
// server/src/consumers/safe-consumer.ts

import amqplib, { ConsumeMessage } from 'amqplib';

async function startConsumer() {
  const conn    = await amqplib.connect('amqp://localhost');
  const channel = await conn.createChannel();

  await channel.assertQueue('orders', { durable: true });

  // CRITICAL: prefetch limits how many unacked messages this consumer holds.
  // Without this, RabbitMQ floods the consumer with ALL messages at once.
  // If it crashes holding 10,000 messages — all 10,000 get redelivered.
  await channel.prefetch(1); // process ONE at a time

  channel.consume('orders', async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const order = JSON.parse(msg.content.toString());
    console.log('Processing order:', order.id);

    try {
      await processOrder(order); // your actual business logic

      // ✅ OUTCOME 1: Success — remove message from queue permanently
      channel.ack(msg);

    } catch (err) {
      if (isTransientError(err)) {
        // 🔁 OUTCOME 2: Temporary failure (DB timeout, network blip)
        // nack(msg, allUpTo, requeue)
        // requeue: true → put message back at front of queue → retry
        // WARNING: this can cause infinite loops if the error is permanent
        channel.nack(msg, false, !msg.fields.redelivered); // retry ONCE, then dead-letter

      } else {
        // ❌ OUTCOME 3: Permanent failure (bad data, schema mismatch)
        // requeue: false → message goes to Dead Letter Exchange (DLQ)
        channel.nack(msg, false, false);
      }
    }
  }, { noAck: false }); // ALWAYS false in production
}

function isTransientError(err: unknown): boolean {
  // Network errors, DB timeouts = transient (worth retrying)
  // Validation errors, parse errors = permanent (don't retry)
  const message = err instanceof Error ? err.message : '';
  return message.includes('ECONNREFUSED') || message.includes('timeout');
}
```

### The `msg.fields.redelivered` Flag

This is the most important field developers miss in interviews:

```typescript
channel.consume('orders', async (msg) => {
  if (!msg) return;

  // redelivered = true means this message was delivered before and nack'd
  // It tells you: "this message has already failed at least once"
  if (msg.fields.redelivered) {
    console.warn('Message is a redelivery — handling with caution');
    // Option A: send straight to DLQ after one retry
    // Option B: check a retry count header (see Q6)
    // Option C: log and ack to prevent poison-pill loop
  }
  // ...
}, { noAck: false });
```

### What to Say in an Interview

> "By default with `noAck: false`, if a consumer crashes before calling `ack()`, RabbitMQ requeues the message and redelivers it to another consumer. But you need three things working together: `noAck: false` on consume, `durable: true` on the queue, and `persistent: true` on published messages. Without all three, a broker restart can still lose messages. You also need a `prefetch` limit — otherwise a crashing consumer was holding thousands of unacked messages and all of them requeue at once, creating a thundering herd."

---

## Q2 — How is RabbitMQ different from Redis Pub/Sub?

This is a fundamental question about **delivery guarantees** and **reliability**. Here's the complete breakdown:

### Side-by-Side Comparison

| Property | RabbitMQ | Redis Pub/Sub |
|---|---|---|
| **Persistence** | Messages stored on disk | Messages exist only in memory, in-flight |
| **Delivery** | At-least-once (with ack) | At-most-once (fire and forget) |
| **If consumer is offline** | Message waits in queue | Message is **dropped** |
| **Acknowledgements** | ✅ Yes — broker knows if message was processed | ❌ No |
| **Message replay** | ❌ No (use Kafka Streams for this) | ❌ No |
| **Routing** | Complex (exchanges, bindings, wildcards) | Simple channel name match only |
| **Consumer groups** | ✅ Competing consumers share load | ❌ ALL subscribers get ALL messages |
| **Backpressure** | ✅ prefetch + queue length limits | ❌ None |
| **Dead letters** | ✅ Built-in DLQ | ❌ No |
| **Use case** | Task queues, reliable events, workflows | Live notifications, real-time feeds, cache invalidation signals |

### The Core Difference in Code

```typescript
// ─── Redis Pub/Sub — FIRE AND FORGET ─────────────────────────────────────────
// If no subscriber is listening when you publish → message is GONE.
// Perfect for: "hey, invalidate your cache" — it's OK if some instances miss it.

import { createClient } from 'redis';

const publisher  = createClient();
const subscriber = createClient();

await publisher.connect();
await subscriber.connect();

// Publisher
await publisher.publish('user:updated', JSON.stringify({ userId: '123' }));
// Message delivered to all CURRENTLY CONNECTED subscribers.
// If a service is restarting at this exact moment → it misses this event. Accepted.

// Subscriber
await subscriber.subscribe('user:updated', (message) => {
  const { userId } = JSON.parse(message);
  cache.delete(`user:${userId}`); // invalidate local cache
  // No ack needed — if this fails, it's fine (cache will refresh on next read)
});
```

```typescript
// ─── RabbitMQ — RELIABLE DELIVERY ─────────────────────────────────────────────
// Message is stored until a consumer acks it.
// If all consumers are down → message waits in queue.
// Perfect for: "charge this customer's card" — you CANNOT miss this.

import amqplib from 'amqplib';

const conn    = await amqplib.connect('amqp://localhost');
const channel = await conn.createChannel();

// Publisher — message persisted to disk
channel.publish('payments', 'charge.card', Buffer.from(JSON.stringify({
  orderId: 'order-999',
  amount:  4999,
})), {
  persistent: true, // survive broker restart
});
// Message sits safely in queue even if payment service is restarting.

// Consumer — guaranteed delivery
channel.consume('payment-processor', async (msg) => {
  if (!msg) return;
  const { orderId, amount } = JSON.parse(msg.content.toString());
  await chargeCard(orderId, amount);
  channel.ack(msg); // only removed after successful charge
}, { noAck: false });
```

### Decision Framework

```
Ask yourself: "What happens if the message is lost?"

Lost message = minor UI glitch, stale data, no big deal
→ Use Redis Pub/Sub (simpler, faster, lower operational cost)

Lost message = money not charged, order not processed, data corrupted
→ Use RabbitMQ (reliability, persistence, acks, DLQ)

Lost message + need to replay history from 6 months ago
→ Use Kafka (append-only log, consumer offsets, retention)
```

### The Interviewer's Follow-up

> "Can Redis Streams be used instead of RabbitMQ?"

Yes, and it's worth knowing. Redis Streams (`XADD`, `XREAD`, consumer groups) give you persistent, acknowledged delivery similar to RabbitMQ. The trade-off: you're adding messaging responsibilities to your cache infrastructure, which creates operational coupling. RabbitMQ is purpose-built for messaging with richer routing. Redis Streams work well for simpler use cases where you already run Redis and want to avoid another service.

---

## Q3 — How do microservices communicate reliably with each other?

There are two fundamentally different patterns. Getting these wrong is the most common architecture mistake.

### Pattern 1: Synchronous — Direct HTTP/gRPC Call

```
Service A ──HTTP POST──▶ Service B
Service A ◀──200 OK───── Service B
```

```typescript
// Direct HTTP call — simplest but fragile
async function createOrderAndNotify(order: Order) {
  // Step 1: Save order
  await db.orders.insert(order);

  // Step 2: Call notification service synchronously
  // PROBLEM: If notification service is down → this throws → order creation "fails"
  // even though the order WAS saved. Now you have inconsistent state.
  await axios.post('http://notification-service/notify', { orderId: order.id });
}
```

**When synchronous is appropriate:**
- You need the response to continue (e.g., checking inventory before accepting an order)
- The operation is a single logical unit that should fail together
- Latency SLA requires an immediate answer

**The problem:** Synchronous calls create **temporal coupling**. Service A cannot succeed unless Service B is up, fast, and healthy at that exact moment.

### Pattern 2: Asynchronous — Message Queue

```
Service A ──publish(event)──▶ RabbitMQ Queue ──deliver──▶ Service B
Service A continues immediately (doesn't wait for Service B)
```

```typescript
// server/src/patterns/reliable-communication.ts

import amqplib from 'amqplib';

// ─── Order Service (Producer) ─────────────────────────────────────────────────
// Publishes an event AFTER saving to its own database.
// Does NOT care whether downstream services are up.
async function createOrder(orderData: CreateOrderDTO) {
  const conn           = await amqplib.connect('amqp://localhost');
  const confirmChannel = await conn.createConfirmChannel(); // wait for broker ack

  await confirmChannel.assertExchange('order.events', 'topic', { durable: true });

  const order: Order = {
    id:        generateId(),
    ...orderData,
    status:    'pending',
    createdAt: new Date().toISOString(),
  };

  // Step 1: Save to database (own source of truth)
  await db.orders.insert(order);

  // Step 2: Publish event — broker confirms receipt before we proceed
  await new Promise<void>((resolve, reject) => {
    const published = confirmChannel.publish(
      'order.events',
      'order.created',
      Buffer.from(JSON.stringify({ orderId: order.id, userId: order.userId, amount: order.total })),
      { persistent: true, contentType: 'application/json' },
      (err) => err ? reject(err) : resolve()
    );
    if (!published) {
      // Back-pressure: write buffer full. In production, implement a retry/drain loop.
      console.warn('Buffer full — apply back-pressure');
    }
  });

  console.log('Order created and event published:', order.id);
  return order;
  // Notification service, inventory service, analytics — all receive this event
  // independently and at their own pace. None of them can break order creation.
}

// ─── Notification Service (Consumer) ─────────────────────────────────────────
// Receives order events and sends notifications.
// Completely decoupled — can be deployed, restarted, scaled independently.
async function startNotificationConsumer() {
  const conn    = await amqplib.connect('amqp://localhost');
  const channel = await conn.createChannel();

  await channel.assertExchange('order.events', 'topic', { durable: true });
  await channel.assertQueue('notifications.orders', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx',        // failed messages go here
      'x-message-ttl': 10 * 60 * 1000,        // expire after 10 min if not processed
    }
  });
  await channel.bindQueue('notifications.orders', 'order.events', 'order.#');
  await channel.prefetch(5); // process up to 5 notifications concurrently

  channel.consume('notifications.orders', async (msg) => {
    if (!msg) return;

    try {
      const event = JSON.parse(msg.content.toString());
      await sendOrderConfirmationEmail(event.userId, event.orderId);
      channel.ack(msg);
    } catch (err) {
      // Notification failed — don't crash the whole system
      // just dead-letter this one message
      channel.nack(msg, false, false);
    }
  }, { noAck: false });
}
```

### The Outbox Pattern — the Most Important Reliability Pattern

The problem with the code above: what if the app crashes AFTER saving to the DB but BEFORE publishing the event? The order exists but no downstream service knows about it.

```typescript
// server/src/patterns/outbox.ts
/**
 * THE OUTBOX PATTERN
 *
 * Instead of publishing directly to RabbitMQ, write the event
 * to an "outbox" table in the SAME database transaction as your business data.
 * A separate process polls the outbox and publishes to RabbitMQ.
 *
 * This gives you atomic "save data AND guarantee event is published"
 * using only database ACID guarantees — no distributed transaction needed.
 */

// Step 1: In your business logic — one atomic DB transaction
async function createOrderWithOutbox(orderData: CreateOrderDTO) {
  await db.transaction(async (trx) => {
    // Save the order
    const order = await trx('orders').insert({
      id:        generateId(),
      ...orderData,
      status:    'pending',
      createdAt: new Date().toISOString(),
    }).returning('*');

    // Save the event to the outbox table — SAME transaction
    // If either insert fails, BOTH roll back. Atomicity guaranteed.
    await trx('outbox_events').insert({
      id:          generateId(),
      event_type:  'order.created',
      payload:     JSON.stringify({ orderId: order[0].id, userId: order[0].userId }),
      created_at:  new Date().toISOString(),
      published:   false,   // not yet sent to RabbitMQ
    });
  });
  // Transaction committed: order exists in DB AND outbox event exists in DB
  // Even if the server crashes NOW, the outbox relay will publish the event later
}

// Step 2: Outbox relay — runs as a separate process / scheduled job
async function outboxRelay() {
  const channel = await getConfirmChannel();

  setInterval(async () => {
    // Find unpublished events (oldest first)
    const events = await db('outbox_events')
      .where({ published: false })
      .orderBy('created_at', 'asc')
      .limit(100);

    for (const event of events) {
      try {
        await publishWithConfirm(channel, event.event_type, event.payload);
        // Only mark as published AFTER broker confirms receipt
        await db('outbox_events').where({ id: event.id }).update({ published: true });
      } catch (err) {
        console.error('Failed to publish outbox event:', event.id, err);
        // Will retry on next interval — no message lost
      }
    }
  }, 1000); // poll every 1 second
}
```

### Synchronous vs Async — The Decision

```
┌─────────────────────────────────────────────────────────┐
│ SYNCHRONOUS (HTTP/gRPC)          ASYNCHRONOUS (MQ)       │
│                                                          │
│ ✅ Need immediate response       ✅ Don't need response  │
│ ✅ Read operations (GET)         ✅ Write/side-effect ops │
│ ✅ Simple request-reply          ✅ Fan-out to N services │
│ ✅ Query inventory before order  ✅ Send email after order│
│                                                          │
│ ❌ Tight coupling                ❌ Eventual consistency  │
│ ❌ Cascading failures            ❌ Harder to debug       │
│ ❌ Must handle Service B down    ❌ More infrastructure   │
└─────────────────────────────────────────────────────────┘
```

---

## Q4 — What will you do if Service A calls Service B and Service B is down?

This is a systems design question. The answer has three layers: immediate handling, recovery, and architecture.

### Layer 1: Immediate Handling — The Circuit Breaker

A circuit breaker watches call failure rates and "opens" (stops trying) when a threshold is breached — giving the failing service time to recover instead of hammering it with requests.

```
CLOSED state → calls pass through normally
    ↓ (failure threshold exceeded, e.g. 50% in 10 seconds)
OPEN state   → calls fail immediately without touching Service B
    ↓ (after timeout, e.g. 30 seconds)
HALF-OPEN    → ONE test call allowed through
    ↓ success                    ↓ failure
CLOSED again                 OPEN again
```

```typescript
// server/src/resilience/circuit-breaker.ts
/**
 * Manual circuit breaker implementation.
 * In production, use 'opossum' npm package — battle-tested, same concept.
 */

type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

interface CircuitBreakerOptions {
  failureThreshold: number;   // how many failures before opening
  successThreshold: number;   // how many successes in HALF_OPEN before closing
  timeout:          number;   // ms to wait in OPEN before trying HALF_OPEN
}

class CircuitBreaker {
  private state:          CircuitState = 'CLOSED';
  private failureCount:   number       = 0;
  private successCount:   number       = 0;
  private lastFailureTime: number      = 0;
  private readonly opts:  CircuitBreakerOptions;

  constructor(opts: CircuitBreakerOptions) {
    this.opts = opts;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === 'OPEN') {
      const elapsed = Date.now() - this.lastFailureTime;

      if (elapsed < this.opts.timeout) {
        // Circuit is open — fail fast without calling downstream
        throw new Error(`Circuit OPEN — Service unavailable. Retry in ${
          Math.ceil((this.opts.timeout - elapsed) / 1000)
        }s`);
      }

      // Timeout elapsed — try one request (HALF_OPEN probe)
      this.state = 'HALF_OPEN';
      console.log('[CircuitBreaker] Entering HALF_OPEN state — probing...');
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;

    } catch (err) {
      this.onFailure();
      throw err;
    }
  }

  private onSuccess(): void {
    this.failureCount = 0;

    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      if (this.successCount >= this.opts.successThreshold) {
        this.state        = 'CLOSED';
        this.successCount = 0;
        console.log('[CircuitBreaker] → CLOSED (service recovered)');
      }
    }
  }

  private onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.state === 'HALF_OPEN') {
      this.state        = 'OPEN';
      this.successCount = 0;
      console.log('[CircuitBreaker] → OPEN (probe failed)');
      return;
    }

    if (this.failureCount >= this.opts.failureThreshold) {
      this.state = 'OPEN';
      console.log(`[CircuitBreaker] → OPEN after ${this.failureCount} failures`);
    }
  }

  getState(): CircuitState { return this.state; }
}

// Usage
const paymentCircuit = new CircuitBreaker({
  failureThreshold: 5,
  successThreshold: 2,
  timeout:          30_000, // 30 seconds in OPEN state
});

async function callPaymentService(payload: ChargePayload) {
  return paymentCircuit.execute(async () => {
    const response = await axios.post('http://payment-service/charge', payload, {
      timeout: 3000, // don't wait more than 3 seconds
    });
    return response.data;
  });
}
```

### Layer 2: Recovery — Retry with Exponential Backoff via RabbitMQ

When a synchronous call fails and the circuit opens, don't just throw an error — offload the work to a message queue and retry asynchronously.

```typescript
// server/src/resilience/retry-queue.ts
/**
 * RETRY WITH EXPONENTIAL BACKOFF USING DLQ CHAINS
 *
 * Architecture:
 *   Main Queue → fails → Retry Queue 1 (TTL: 5s) → Retry Queue 2 (TTL: 30s) → Retry Queue 3 (TTL: 5min) → DLQ
 *
 * Each retry queue has a TTL. When a message expires, RabbitMQ dead-letters it
 * back to the main exchange for another attempt. After max retries, it goes to the
 * final DLQ for manual inspection.
 */

async function setupRetryInfrastructure(channel: amqplib.Channel) {
  // Main exchange — where work is published
  await channel.assertExchange('work.exchange', 'direct', { durable: true });

  // Dead Letter Exchange — where expired retry messages return to work exchange
  await channel.assertExchange('retry.dlx', 'direct', { durable: true });

  // Final DLQ — for messages that exhausted all retries
  await channel.assertExchange('dlq.exchange', 'direct', { durable: true });
  await channel.assertQueue('dlq.payment', { durable: true });
  await channel.bindQueue('dlq.payment', 'dlq.exchange', 'payment.charge');

  // Main work queue
  await channel.assertQueue('payment.charge', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange':    'retry.dlx',   // on nack → go to retry
      'x-dead-letter-routing-key': 'retry.1',
    }
  });
  await channel.bindQueue('payment.charge', 'work.exchange', 'payment.charge');

  // Retry Queue 1 — wait 5 seconds then try again
  await channel.assertQueue('retry.queue.1', {
    durable: true,
    arguments: {
      'x-message-ttl':             5_000,           // 5 second delay
      'x-dead-letter-exchange':    'work.exchange', // after TTL → back to work queue
      'x-dead-letter-routing-key': 'payment.charge',
    }
  });
  await channel.bindQueue('retry.queue.1', 'retry.dlx', 'retry.1');

  // Retry Queue 2 — wait 30 seconds then try again
  await channel.assertQueue('retry.queue.2', {
    durable: true,
    arguments: {
      'x-message-ttl':             30_000,
      'x-dead-letter-exchange':    'work.exchange',
      'x-dead-letter-routing-key': 'payment.charge',
    }
  });
  await channel.bindQueue('retry.queue.2', 'retry.dlx', 'retry.2');

  // Retry Queue 3 — wait 5 minutes then final attempt
  await channel.assertQueue('retry.queue.3', {
    durable: true,
    arguments: {
      'x-message-ttl':             300_000,
      'x-dead-letter-exchange':    'work.exchange',
      'x-dead-letter-routing-key': 'payment.charge',
    }
  });
  await channel.bindQueue('retry.queue.3', 'retry.dlx', 'retry.3');

  console.log('[Retry] Infrastructure ready: 3 retry tiers + DLQ');
}

// Consumer that routes to the correct retry tier
async function startPaymentConsumerWithRetry(channel: amqplib.Channel) {
  await channel.prefetch(1);

  channel.consume('payment.charge', async (msg) => {
    if (!msg) return;

    const payload = JSON.parse(msg.content.toString());

    // Track retry count in message headers
    const headers      = msg.properties.headers ?? {};
    const retryCount   = (headers['x-retry-count'] as number) ?? 0;
    const MAX_RETRIES  = 3;

    console.log(`[Payment] Attempt ${retryCount + 1} for order ${payload.orderId}`);

    try {
      await callPaymentService(payload);
      channel.ack(msg);
      console.log(`[Payment] ✅ Success on attempt ${retryCount + 1}`);

    } catch (err) {
      console.error(`[Payment] ❌ Attempt ${retryCount + 1} failed:`, (err as Error).message);

      if (retryCount >= MAX_RETRIES) {
        // Exhausted retries → final DLQ
        console.error('[Payment] Max retries reached — sending to DLQ for manual review');
        channel.nack(msg, false, false); // dies to DLQ via current queue's dead-letter config
        return;
      }

      // Republish with incremented retry count header
      // Route to the appropriate retry tier based on attempt number
      const retryRoutingKey = `retry.${retryCount + 1}`;

      channel.publish(
        'retry.dlx',
        retryRoutingKey,
        msg.content,
        {
          persistent: true,
          headers: { ...headers, 'x-retry-count': retryCount + 1 },
        }
      );
      channel.ack(msg); // ack the original — the retry queue now holds it
    }
  }, { noAck: false });
}
```

### Layer 3: Architecture — Async Instead of Sync

The deepest answer: if Service B being down causes Service A to fail, you've created a distributed monolith. The real fix is to redesign the communication as an event.

```
BEFORE (fragile):
  Order Service ──HTTP──▶ Payment Service
  (order creation fails if payment service is down)

AFTER (resilient):
  Order Service publishes "order.placed" event to RabbitMQ
  Payment Service consumes "order.placed" event
  (order creation NEVER fails because of payment service health)
  (payment will be processed when service recovers)
```

### What to Say in an Interview

> "My answer has three layers. First, immediate: circuit breakers stop cascading failures by failing fast when a downstream service is unhealthy, buying it time to recover. Second, retry: for idempotent operations I use exponential backoff via RabbitMQ delay queues — 5s, 30s, 5min — with a retry count header, and a DLQ for exhausted messages. Third, architectural: if Service A's core function requires Service B to be up, that's a design smell. I'd redesign it so Service A publishes an event and Service B processes it asynchronously. The Outbox Pattern guarantees the event gets published even if the server crashes between saving data and publishing."

---

## Q5 — How do you scale RabbitMQ consumers in production?

### Competing Consumers Pattern

The simplest scaling mechanism: multiple instances of the same consumer all read from the same queue. RabbitMQ round-robins delivery.

```
                      ┌─────────────┐
                      │    Queue    │
                      │  [msg][msg] │
                      │  [msg][msg] │
                      └──┬──┬──┬───┘
                         │  │  │
              ┌──────────┘  │  └──────────┐
              ▼             ▼             ▼
        Consumer A    Consumer B    Consumer C
        (worker-1)    (worker-2)    (worker-3)
        processing    processing    processing
        msg-1         msg-2         msg-3
```

```typescript
// server/src/scaling/worker.ts
/**
 * Start this file as multiple processes — they all compete for messages.
 * In Docker: scale with "docker-compose up --scale worker=5"
 * In Kubernetes: set replicas: 5 in your Deployment spec
 */

const WORKER_ID = process.env.WORKER_ID ?? Math.random().toString(36).slice(2, 6);

async function startWorker() {
  const conn    = await amqplib.connect(process.env.RABBITMQ_URL!);
  const channel = await conn.createChannel();

  await channel.assertQueue('heavy-jobs', { durable: true });

  // CRITICAL: Each worker must set its own prefetch.
  // prefetch(1) = fair dispatch — worker only gets next message after finishing current one.
  // prefetch(10) = higher throughput for fast, I/O-bound tasks.
  // prefetch(0) = unlimited — dangerous, worker can OOM
  await channel.prefetch(
    parseInt(process.env.PREFETCH_COUNT ?? '1')
  );

  console.log(`[Worker ${WORKER_ID}] Ready for jobs`);

  channel.consume('heavy-jobs', async (msg) => {
    if (!msg) return;

    const job = JSON.parse(msg.content.toString());
    console.log(`[Worker ${WORKER_ID}] Processing job:`, job.id);

    const start = Date.now();
    await processJob(job);
    const duration = Date.now() - start;

    console.log(`[Worker ${WORKER_ID}] Done in ${duration}ms`);
    channel.ack(msg);
  }, { noAck: false });
}
```

### How Prefetch Affects Throughput

```
prefetch(1) — worker processes one job at a time
  Worker A: [job1] ─── slow job (10s) ───▶ done, get job4
  Worker B: [job2] ─ fast job (1s) ─▶ done, get job3
  Worker C: [job3] ─ medium (3s) ──────▶ done, get job5
  Total throughput: limited by slowest job per worker
  Fairness: ✅ fast workers get more jobs

prefetch(10) — worker holds up to 10 jobs
  Worker A: [job1,job2,job3...job10] (some idle while first runs)
  Worker B: [job11...job20]
  Problem: if a worker crashes, 10 messages requeue at once
  Fairness: ❌ round-robined in batches, not per-job

Rule: prefetch(1) for CPU-bound or slow tasks.
      prefetch(N) where N > 1 for fast I/O-bound tasks.
```

### Consumer Cancellation and Graceful Shutdown

```typescript
// server/src/scaling/graceful-shutdown.ts
/**
 * In Kubernetes, when you scale down a pod receives SIGTERM.
 * If you kill the process immediately, in-flight messages requeue.
 * Graceful shutdown: stop accepting new messages, finish current ones, then exit.
 */

let consumerTag: string;

async function startWithGracefulShutdown() {
  const conn    = await amqplib.connect('amqp://localhost');
  const channel = await conn.createChannel();

  await channel.assertQueue('jobs', { durable: true });
  await channel.prefetch(1);

  // Track whether we're processing a message right now
  let processing = false;
  let shouldStop = false;

  const result = await channel.consume('jobs', async (msg) => {
    if (!msg) return;

    processing = true;
    try {
      await processJob(JSON.parse(msg.content.toString()));
      channel.ack(msg);
    } catch (err) {
      channel.nack(msg, false, false);
    } finally {
      processing = false;
      if (shouldStop) {
        console.log('[Worker] Graceful shutdown: finished last job, exiting.');
        await conn.close();
        process.exit(0);
      }
    }
  }, { noAck: false });

  consumerTag = result.consumerTag;

  // SIGTERM from Kubernetes / Docker stop
  process.on('SIGTERM', async () => {
    console.log('[Worker] SIGTERM received — finishing current job before stopping...');
    shouldStop = true;

    // Cancel consumer: stop receiving NEW messages from the broker
    await channel.cancel(consumerTag);

    if (!processing) {
      await conn.close();
      process.exit(0);
    }
    // If processing, the finally{} block above will close and exit
  });
}
```

---

## Q6 — How do you design retry logic with exponential backoff?

### The Full Pattern With All Edge Cases

```typescript
// server/src/patterns/smart-retry.ts
/**
 * Production-grade retry system using RabbitMQ message headers.
 *
 * Each message carries metadata in its headers:
 *   x-retry-count:   how many times this message has been attempted
 *   x-first-seen:    when the message was first published (ISO string)
 *   x-last-error:    the error message from the last failure
 *   x-original-queue: which queue this message belongs to
 *
 * Retry schedule: 5s → 30s → 2min → 10min → DLQ
 */

interface RetryOptions {
  maxRetries:       number;
  retryDelaysMs:    number[];  // [5000, 30000, 120000, 600000]
  dlqExchange:      string;
  retryExchange:    string;
}

const RETRY_OPTIONS: RetryOptions = {
  maxRetries:    4,
  retryDelaysMs: [5_000, 30_000, 120_000, 600_000],  // 5s, 30s, 2min, 10min
  dlqExchange:   'dlq',
  retryExchange: 'retry.delayed',
};

async function handleFailedMessage(
  channel: amqplib.Channel,
  msg:     amqplib.ConsumeMessage,
  error:   Error,
  opts:    RetryOptions = RETRY_OPTIONS
) {
  const headers   = (msg.properties.headers ?? {}) as Record<string, unknown>;
  const retryCount = Number(headers['x-retry-count'] ?? 0);
  const firstSeen  = (headers['x-first-seen'] as string) ?? new Date().toISOString();

  if (retryCount >= opts.maxRetries) {
    // ── DEAD LETTER ────────────────────────────────────────────────────────
    // Log full context for manual inspection / alerting
    console.error('[Retry] Exhausted retries — dead lettering', {
      retryCount,
      firstSeen,
      lastError:    error.message,
      queue:        headers['x-original-queue'],
      payload:      msg.content.toString().slice(0, 500), // truncate for logs
    });

    // Republish to DLQ with full audit trail in headers
    channel.publish(
      opts.dlqExchange,
      msg.fields.routingKey,
      msg.content,
      {
        persistent: true,
        headers: {
          ...headers,
          'x-retry-count':    retryCount,
          'x-first-seen':     firstSeen,
          'x-last-error':     error.message,
          'x-dead-lettered-at': new Date().toISOString(),
        }
      }
    );
    channel.ack(msg); // ack original to remove from main queue
    return;
  }

  // ── SCHEDULE RETRY ─────────────────────────────────────────────────────
  const delayMs = opts.retryDelaysMs[retryCount] ?? opts.retryDelaysMs.at(-1)!;

  console.warn(`[Retry] Scheduling retry ${retryCount + 1}/${opts.maxRetries} in ${delayMs / 1000}s`, {
    error: error.message,
  });

  // Use a per-message TTL on a holding queue to implement the delay.
  // The message sits in a queue with no consumers, expires after delayMs,
  // then gets dead-lettered back to the original exchange.
  const holdingQueue = `retry.hold.${delayMs}ms`;

  await channel.assertQueue(holdingQueue, {
    durable: true,
    arguments: {
      'x-message-ttl':             delayMs,
      'x-dead-letter-exchange':    msg.fields.exchange || '',
      'x-dead-letter-routing-key': msg.fields.routingKey,
      'x-expires':                 delayMs + 60_000, // auto-delete queue if unused for 1 min after TTL
    }
  });

  channel.sendToQueue(holdingQueue, msg.content, {
    persistent: true,
    headers: {
      ...headers,
      'x-retry-count':     retryCount + 1,
      'x-first-seen':      firstSeen,
      'x-last-error':      error.message,
      'x-original-queue':  msg.fields.routingKey,
      'x-retry-scheduled': new Date().toISOString(),
    },
  });

  channel.ack(msg); // remove original from main queue — retry queue holds it now
}

// Usage in a consumer
channel.consume('payment.charge', async (msg) => {
  if (!msg) return;
  try {
    await processPayment(JSON.parse(msg.content.toString()));
    channel.ack(msg);
  } catch (err) {
    await handleFailedMessage(channel, msg, err as Error);
  }
}, { noAck: false });
```

---

## Q7 — Dead Letter Queues — What Are They Really For?

Most developers answer "DLQ is where failed messages go." Correct but incomplete. Here's the full picture.

### Three Reasons a Message Gets Dead-Lettered

```typescript
// server/src/patterns/dlq-setup.ts

async function setupDLQInfrastructure(channel: amqplib.Channel) {

  // ── 1. SETUP: DLX and DLQ ─────────────────────────────────────────────────
  await channel.assertExchange('dlx', 'topic', { durable: true });
  await channel.assertQueue('dlq.all', {
    durable: true,
    // DLQ has no further dead-lettering — messages park here permanently
  });
  // Catch-all binding — any routing key
  await channel.bindQueue('dlq.all', 'dlx', '#');

  // ── 2. MAIN QUEUE configured to dead-letter ───────────────────────────────
  await channel.assertQueue('critical.jobs', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange':    'dlx',

      // Reason 1: Consumer calls nack(msg, false, false) — explicitly rejected
      // (message moves to DLX immediately)

      // Reason 2: TTL — message sits unprocessed too long
      'x-message-ttl': 60_000, // 1 minute max wait time

      // Reason 3: Queue length overflow — too many messages backed up
      'x-max-length':  10_000,
      // When queue is full, the oldest message is dead-lettered to make room
      'x-overflow':    'reject-publish', // or 'drop-head' (drop oldest)
    }
  });

  console.log('[DLQ] Infrastructure ready — DLX → dlq.all');
}

// ── DLQ Consumer — for monitoring, alerting, and reprocessing ─────────────────
async function startDLQConsumer(channel: amqplib.Channel) {
  await channel.prefetch(1);

  channel.consume('dlq.all', async (msg) => {
    if (!msg) return;

    const headers     = msg.properties.headers as Record<string, unknown>;
    const deathInfo   = headers['x-death'] as Array<Record<string, unknown>> | undefined;
    const reason      = deathInfo?.[0]?.reason as string;     // 'rejected' | 'expired' | 'maxlen'
    const originalQ   = deathInfo?.[0]?.queue as string;
    const deathCount  = deathInfo?.[0]?.count as number;

    console.error('[DLQ] Dead-lettered message received', {
      reason,
      originalQueue: originalQ,
      deathCount,
      lastError:     headers['x-last-error'],
      firstSeen:     headers['x-first-seen'],
      payload:       msg.content.toString().slice(0, 200),
    });

    // ── What to do with DLQ messages ─────────────────────────────────────
    // Option A: Alert engineering team (PagerDuty, Slack webhook)
    await sendAlert({
      channel: '#backend-alerts',
      message: `☠️ DLQ: ${deathCount} dead letter(s) from ${originalQ}. Reason: ${reason}`,
    });

    // Option B: Store in DB for manual review / replay UI
    await db('dead_letters').insert({
      id:            generateId(),
      reason,
      original_queue: originalQ,
      headers:       JSON.stringify(headers),
      payload:       msg.content.toString(),
      created_at:    new Date().toISOString(),
    });

    // Option C: Attempt fix and republish (only if reason is known + fixable)
    if (reason === 'expired' && deathCount === 1) {
      // Message just timed out — maybe a temporary outage. Try once more.
      channel.publish(originalQ.split('.')[0], originalQ, msg.content, {
        persistent: true,
        headers: { ...headers, 'x-retry-count': 0 }, // reset counter
      });
    }

    channel.ack(msg); // Always ack DLQ messages — you've handled them
  }, { noAck: false });
}
```

### The DLQ is a Diagnostic Tool

```
A growing DLQ is a symptom — it means:

DLQ growing fast + reason: rejected
  → Consumer is throwing errors → check error logs, fix the bug

DLQ growing fast + reason: expired
  → Consumers too slow or down → scale up consumers, check health

DLQ growing fast + reason: maxlen
  → Queue overflowing → consumers can't keep up with producers → scale consumers or slow producers

DLQ message count = 0
  → Healthy system
```

---

## Q8 — How do you guarantee exactly-once processing?

### The Honest Answer

True exactly-once is impossible in distributed systems. What you can achieve is **at-least-once delivery + idempotent consumers = effectively once**.

```
DELIVERY GUARANTEES:

At-most-once:   Message might be lost. Ack before processing.
                Use for: metrics, non-critical notifications

At-least-once:  Message might be processed MULTIPLE TIMES. Ack after processing.
                Use for: most production workloads WITH idempotent handlers

Exactly-once:   Requires distributed transactions. Very expensive.
                Kafka transactions + transactional outbox can get close.
```

```typescript
// server/src/patterns/idempotent-consumer.ts
/**
 * IDEMPOTENT CONSUMER PATTERN
 *
 * Before processing a message, check if we've already processed this exact message.
 * If yes: ack and skip. If no: process, record, ack.
 *
 * The idempotency key can be:
 *   - msg.properties.messageId (set by producer)
 *   - A business-level ID from the payload (orderId, paymentId)
 *   - correlationId + action combination
 */

import { createClient } from 'redis';

const redis = createClient();
await redis.connect();

const IDEMPOTENCY_TTL = 24 * 60 * 60; // 24 hours in seconds

async function processMessageIdempotently(
  channel: amqplib.Channel,
  msg:     amqplib.ConsumeMessage,
  handler: (payload: unknown) => Promise<void>
) {
  const payload   = JSON.parse(msg.content.toString());

  // Use messageId from AMQP properties, or fall back to a business ID
  const messageId = msg.properties.messageId
                  ?? payload.idempotencyKey
                  ?? payload.orderId;   // something unique per business event

  if (!messageId) {
    // No idempotency key — process and hope for the best (log a warning)
    console.warn('[Idempotency] No messageId found — processing without dedup');
    await handler(payload);
    channel.ack(msg);
    return;
  }

  const redisKey = `processed:${messageId}`;

  // Atomic check-and-set using Redis SET NX (Set if Not eXists)
  // Returns null if key already exists (already processed)
  // Returns 'OK' if key was set (first time processing)
  const acquired = await redis.set(redisKey, '1', {
    NX:  true,               // only set if not exists
    EX:  IDEMPOTENCY_TTL,    // auto-expire after 24h
  });

  if (!acquired) {
    // Duplicate delivery — we've already processed this message
    console.log(`[Idempotency] Skipping duplicate message: ${messageId}`);
    channel.ack(msg); // ack it so it's removed from the queue
    return;
  }

  try {
    await handler(payload);
    channel.ack(msg);
    console.log(`[Idempotency] Processed: ${messageId}`);

  } catch (err) {
    // Processing failed — delete the Redis key so we can retry
    await redis.del(redisKey);
    channel.nack(msg, false, !msg.fields.redelivered);
    throw err;
  }
}

// Producer side: always set messageId
channel.publish('orders', 'order.created', Buffer.from(JSON.stringify(order)), {
  persistent:  true,
  messageId:   order.id,        // ← ALWAYS set this
  contentType: 'application/json',
  timestamp:   Date.now(),
});
```

---

## Q9 — RabbitMQ vs Kafka — When to Use Which?

```
┌────────────────────────────────────────────────────────────────────┐
│                  RABBITMQ                                           │
│                                                                    │
│  Model:    Smart broker, dumb consumer                             │
│            Broker routes, filters, expires, dead-letters messages  │
│                                                                    │
│  Delivery: Push-based (broker pushes to consumers)                 │
│  Storage:  Messages deleted after ack                              │
│  Replay:   ❌ Not supported                                        │
│  Ordering: Per-queue FIFO, no global ordering                      │
│  Routing:  Rich (exchanges, bindings, wildcards, headers)          │
│  Latency:  Sub-millisecond                                         │
│                                                                    │
│  Best for: Task queues, RPC, complex routing, work distribution    │
│  Examples: Send emails, process payments, resize images            │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│                    KAFKA                                            │
│                                                                    │
│  Model:    Dumb broker, smart consumer                             │
│            Broker stores log, consumers track their own offset     │
│                                                                    │
│  Delivery: Pull-based (consumers poll the log)                     │
│  Storage:  Messages retained for days/weeks/forever                │
│  Replay:   ✅ Replay from any offset, any time                    │
│  Ordering: Guaranteed within a partition                           │
│  Routing:  Simple (topic + partition key)                          │
│  Latency:  Low but higher than RabbitMQ (batch-oriented)           │
│                                                                    │
│  Best for: Event sourcing, audit logs, stream processing, replay   │
│  Examples: User activity streams, CDC, data pipelines              │
└────────────────────────────────────────────────────────────────────┘
```

### The One-Question Decision Framework

> "Do you need to replay events from the past?"
> - YES → Kafka
> - NO  → RabbitMQ

### When the Answer Is Nuanced

```typescript
// Scenario 1: Processing payments
// "Charge this card" — once, reliably, no replay needed
// → RABBITMQ ✅

// Scenario 2: Audit log of all user actions
// "We need to know everything a user did in the last 90 days"
// "New analytics service needs to catch up on 3 months of history"
// → KAFKA ✅

// Scenario 3: Microservice event bus
// New services need to replay historical events to build their state
// → KAFKA ✅

// Scenario 4: Background job queue
// Resize images, send emails, generate reports
// → RABBITMQ ✅ (simpler, purpose-built for tasks)

// Scenario 5: Real-time notifications to users
// "Tell user their order shipped"
// No replay needed, complex routing (per-user queues)
// → RABBITMQ ✅
```

---

## Q10 — How do you handle message ordering?

### The Problem

```
Producer publishes: order-A (t=0s), order-B (t=1s), order-C (t=2s)
Multiple consumers:
  Consumer 1 gets order-A (slow — takes 5s)
  Consumer 2 gets order-B (fast — takes 1s) → processed FIRST
  Consumer 3 gets order-C (fast — takes 1s) → processed SECOND
  Consumer 1 finishes order-A                → processed THIRD

Result: B, C, A — OUT OF ORDER
```

### Solution 1: Single Consumer (simplest, limited scale)

```typescript
// One consumer = guaranteed FIFO order
// Trade-off: throughput limited to one worker
await channel.prefetch(1);
// Start only ONE consumer process
```

### Solution 2: Partition by Key (scales + ordered per entity)

```typescript
// server/src/patterns/ordered-processing.ts
/**
 * CONSISTENT HASHING ROUTING
 *
 * Messages for the same entity (e.g., same userId, same orderId)
 * always go to the same queue — processed by the same consumer.
 * Different entities can still be processed in parallel.
 *
 * This is the same technique Kafka uses with partition keys.
 */

const NUM_PARTITIONS = 8; // 8 queues = up to 8x parallelism, ordered per entity

function getPartition(entityId: string): number {
  // Simple hash — distribute entity IDs evenly across queues
  let hash = 0;
  for (let i = 0; i < entityId.length; i++) {
    hash = (hash * 31 + entityId.charCodeAt(i)) & 0xffffffff;
  }
  return Math.abs(hash) % NUM_PARTITIONS;
}

// Publisher: route to partition based on orderId
function publishOrdered(channel: amqplib.Channel, order: Order) {
  const partition    = getPartition(order.id);
  const queueName    = `orders.partition.${partition}`;

  channel.sendToQueue(queueName, Buffer.from(JSON.stringify(order)), {
    persistent: true,
    headers: { 'x-partition': partition },
  });

  // All messages for order-123 → always partition 3
  // All messages for order-456 → always partition 7
  // Within partition 3: messages are FIFO
}

// Start one consumer per partition
async function startPartitionedConsumers(channel: amqplib.Channel) {
  for (let i = 0; i < NUM_PARTITIONS; i++) {
    const queueName = `orders.partition.${i}`;
    await channel.assertQueue(queueName, { durable: true });
    await channel.prefetch(1); // One at a time within a partition = ordered

    channel.consume(queueName, async (msg) => {
      if (!msg) return;
      await processOrder(JSON.parse(msg.content.toString()));
      channel.ack(msg);
    }, { noAck: false });

    console.log(`[Partition] Consumer started on ${queueName}`);
  }
}
```

---

## Q11 — How do you monitor RabbitMQ in production?

### Key Metrics and What They Mean

```typescript
// server/src/monitoring/rabbitmq-monitor.ts
/**
 * RabbitMQ exposes a full REST API via the management plugin.
 * Poll it regularly and push metrics to your monitoring system.
 */

interface QueueMetrics {
  name:              string;
  messages:          number;  // total messages (ready + unacked)
  messages_ready:    number;  // waiting for consumers
  messages_unacked:  number;  // delivered but not yet acked
  consumers:         number;  // how many active consumers
  message_stats?: {
    deliver_details: { rate: number }; // messages/second being delivered
    ack_details:     { rate: number }; // messages/second being acked
    publish_details: { rate: number }; // messages/second arriving
  };
}

async function fetchQueueMetrics(): Promise<QueueMetrics[]> {
  const response = await fetch('http://localhost:15672/api/queues/%2F', {
    headers: {
      Authorization: 'Basic ' + Buffer.from('admin:secret').toString('base64'),
    }
  });
  return response.json();
}

async function checkHealth() {
  const queues = await fetchQueueMetrics();

  for (const queue of queues) {
    // ALERT 1: Queue depth too high — consumers can't keep up
    if (queue.messages > 10_000) {
      await alert(`🚨 Queue "${queue.name}" has ${queue.messages} messages — consumers overwhelmed`);
    }

    // ALERT 2: No consumers — queue accumulating with nobody processing
    if (queue.messages > 0 && queue.consumers === 0) {
      await alert(`🚨 Queue "${queue.name}" has ${queue.messages} messages but ZERO consumers`);
    }

    // ALERT 3: Too many unacked messages — consumers are stuck or slow
    if (queue.messages_unacked > 1000) {
      await alert(`⚠️ Queue "${queue.name}" has ${queue.messages_unacked} unacked — consumers may be stuck`);
    }

    // ALERT 4: Consume rate far below publish rate — backlog building
    const publishRate = queue.message_stats?.publish_details?.rate ?? 0;
    const ackRate     = queue.message_stats?.ack_details?.rate ?? 0;
    if (publishRate > 0 && ackRate < publishRate * 0.5) {
      await alert(`⚠️ Queue "${queue.name}" consuming at ${ackRate.toFixed(1)}/s but publishing at ${publishRate.toFixed(1)}/s`);
    }
  }
}

// Run health check every 30 seconds
setInterval(checkHealth, 30_000);
```

### The Monitoring Dashboard Queries

```bash
# CLI equivalents for quick checks

# List all queues with depth and consumer count
docker exec rabbitmq rabbitmqctl list_queues \
  name messages messages_ready messages_unacknowledged consumers

# Find queues with zero consumers (danger signal)
docker exec rabbitmq rabbitmqctl list_queues name messages consumers \
  | awk '$3 == 0 && $2 > 0'

# Check memory and disk alarms
docker exec rabbitmq rabbitmq-diagnostics status | grep -E 'alarm|memory|disk'

# Count connections (sudden spike = connection leak)
docker exec rabbitmq rabbitmqctl list_connections | wc -l

# Check if any queues are in DLQ
docker exec rabbitmq rabbitmqctl list_queues name messages \
  | grep dlq
```

---

## Q12 — How do you design a complete reliable messaging system?

### The Full Architecture — Everything Together

```typescript
// server/src/complete-system/index.ts
/**
 * COMPLETE PRODUCTION-READY MESSAGING SYSTEM
 *
 * This pulls together every concept from this guide:
 *   ✅ Durable queues + persistent messages
 *   ✅ Publisher confirms (broker ack)
 *   ✅ Consumer acks (at-least-once delivery)
 *   ✅ Prefetch (fair dispatch + back-pressure)
 *   ✅ Dead Letter Queue (poison message handling)
 *   ✅ Retry with exponential backoff
 *   ✅ Idempotency via Redis
 *   ✅ Outbox pattern (atomic publish)
 *   ✅ Circuit breaker for downstream calls
 *   ✅ Graceful shutdown
 *   ✅ Health monitoring
 */

import amqplib, { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

// ─── BOOTSTRAP — declare all infrastructure ───────────────────────────────────
async function bootstrap(): Promise<ConfirmChannel> {
  const conn    = await amqplib.connect(process.env.RABBITMQ_URL ?? 'amqp://localhost');
  const channel = await conn.createConfirmChannel();

  // Dead-letter infrastructure (declare first)
  await channel.assertExchange('dlx', 'topic',  { durable: true });
  await channel.assertExchange('dlq', 'direct', { durable: true });

  await channel.assertQueue('dlq.orders', { durable: true });
  await channel.bindQueue('dlq.orders', 'dlq', 'order.#');

  // Main business exchange
  await channel.assertExchange('app.events', 'topic', { durable: true });

  // Order processing queue with all protections
  await channel.assertQueue('order.processor', {
    durable: true,
    arguments: {
      // Failed messages go to DLX
      'x-dead-letter-exchange':    'dlx',
      'x-dead-letter-routing-key': 'order.failed',

      // Message expires if not consumed in 10 minutes (consumer outage protection)
      'x-message-ttl': 10 * 60 * 1000,

      // Cap queue size to prevent memory explosion
      'x-max-length': 100_000,
      'x-overflow':   'reject-publish',

      // Quorum queue for HA (requires RabbitMQ 3.8+)
      'x-queue-type': 'quorum',
    }
  });
  await channel.bindQueue('order.processor', 'app.events', 'order.#');

  console.log('[Bootstrap] ✅ All exchanges, queues, and bindings declared');
  return channel;
}

// ─── PRODUCER — with publisher confirms + outbox ──────────────────────────────
async function publishEvent(
  channel:    ConfirmChannel,
  routingKey: string,
  payload:    Record<string, unknown>
): Promise<void> {
  const messageId = uuidv4();
  const body      = Buffer.from(JSON.stringify(payload));

  return new Promise((resolve, reject) => {
    const flushed = channel.publish(
      'app.events',
      routingKey,
      body,
      {
        persistent:   true,           // survive broker restart
        messageId,                    // for idempotency on consumer side
        contentType:  'application/json',
        timestamp:    Date.now(),
        appId:        'order-service',
        headers: {
          'x-published-at': new Date().toISOString(),
          'x-retry-count':  0,
        }
      },
      (err) => {
        if (err) {
          console.error('[Publisher] Broker NACK:', err.message);
          reject(err);
        } else {
          console.log(`[Publisher] ✅ Confirmed: ${routingKey} (${messageId})`);
          resolve();
        }
      }
    );

    if (!flushed) {
      // Back-pressure: channel write buffer is full.
      // In production: queue locally and drain on 'drain' event.
      channel.once('drain', () => console.log('[Publisher] Buffer drained — resuming'));
    }
  });
}

// ─── CONSUMER — with all reliability guarantees ───────────────────────────────
async function startConsumer(channel: ConfirmChannel): Promise<void> {
  // Fair dispatch: worker only gets the next message after finishing current one
  await channel.prefetch(1);

  const { consumerTag } = await channel.consume(
    'order.processor',
    async (msg: ConsumeMessage | null) => {
      if (!msg) return;

      const startTime    = Date.now();
      const messageId    = msg.properties.messageId ?? 'unknown';
      const headers      = (msg.properties.headers ?? {}) as Record<string, unknown>;
      const retryCount   = Number(headers['x-retry-count'] ?? 0);

      let payload: Record<string, unknown>;
      try {
        payload = JSON.parse(msg.content.toString());
      } catch {
        // Unparseable message — dead-letter immediately, don't retry
        console.error(`[Consumer] ❌ Unparseable message ${messageId} — dead lettering`);
        channel.nack(msg, false, false);
        return;
      }

      // Idempotency check
      const alreadyProcessed = await checkIdempotency(messageId);
      if (alreadyProcessed) {
        console.log(`[Consumer] ↩️  Skipping duplicate ${messageId}`);
        channel.ack(msg);
        return;
      }

      try {
        console.log(`[Consumer] ▶ Processing ${msg.fields.routingKey} (attempt ${retryCount + 1})`);

        await processBusinessLogic(payload, msg.fields.routingKey);
        await markProcessed(messageId);

        channel.ack(msg);
        console.log(`[Consumer] ✅ Done in ${Date.now() - startTime}ms`);

      } catch (err) {
        const error    = err as Error;
        const isPermanent = isPermanentError(error);

        console.error(`[Consumer] ❌ Failed (${isPermanent ? 'permanent' : 'transient'}):`, error.message);

        if (isPermanent || retryCount >= 3) {
          // Dead-letter — won't recover from retrying
          await deleteIdempotencyKey(messageId); // clean up so manual replay works
          channel.nack(msg, false, false);
        } else {
          // Schedule retry via delay queue
          const delayMs = [5_000, 30_000, 120_000][retryCount] ?? 120_000;
          await scheduleRetry(channel, msg, retryCount + 1, delayMs, error.message);
          channel.ack(msg); // ack original; retry queue holds the copy
        }
      }
    },
    { noAck: false }
  );

  console.log(`[Consumer] ✅ Listening on "order.processor" (tag: ${consumerTag})`);

  // Graceful shutdown
  process.on('SIGTERM', async () => {
    console.log('[Consumer] SIGTERM — cancelling consumer...');
    await channel.cancel(consumerTag);
    // In-flight message (if any) will finish naturally; ack/nack will fire
  });
}

// ─── Business logic dispatcher ─────────────────────────────────────────────────
async function processBusinessLogic(
  payload:    Record<string, unknown>,
  routingKey: string
): Promise<void> {
  switch (routingKey) {
    case 'order.created':
      await handleOrderCreated(payload);
      break;
    case 'order.payment.requested':
      await handlePaymentRequest(payload);
      break;
    case 'order.fulfilled':
      await handleOrderFulfilled(payload);
      break;
    default:
      console.warn('[Consumer] Unknown routing key:', routingKey);
      // Don't throw — just log and ack to avoid infinite dead-letter loop
  }
}

function isPermanentError(err: Error): boolean {
  const permanent = [
    'ValidationError',
    'SchemaMismatch',
    'NotFound',
    'Forbidden',
  ];
  return permanent.some((name) => err.constructor.name === name || err.message.includes(name));
}

// ─── Entry point ──────────────────────────────────────────────────────────────
async function main() {
  try {
    const channel = await bootstrap();
    await startConsumer(channel);

    // Test: publish a sample event
    await publishEvent(channel, 'order.created', {
      orderId: uuidv4(),
      userId:  'user-001',
      amount:  4999,
      items:   [{ sku: 'PROD-1', qty: 2 }],
    });

  } catch (err) {
    console.error('[Main] Fatal error:', err);
    process.exit(1);
  }
}

main();
```

---

## The Mental Model — How to Think About All of This

Every RabbitMQ interview question is really asking one of three things:

### 1. What happens when things go wrong?

```
Your message pipeline has failure points at every step:

Producer crashes after writing to DB but before publish
  → Outbox Pattern

Broker crashes before persisting the message
  → persistent:true + durable:true + publisher confirms

Consumer crashes after receiving but before processing
  → noAck:false + prefetch

Consumer crashes during processing
  → nack + retry with backoff + idempotent handler

Downstream service (that consumer calls) is down
  → Circuit breaker + async retry via delay queues

Message is fundamentally broken (bad data, schema mismatch)
  → Dead Letter Queue + alert + manual replay
```

### 2. How does it scale?

```
More messages per second needed:
  → Add consumer instances (competing consumers)
  → Increase prefetch (higher throughput per consumer)
  → Shard into multiple queues (consistent hash routing)

Broker becomes a bottleneck:
  → RabbitMQ cluster (3 or more nodes)
  → Quorum queues for data safety
  → Redis adapter / multiple vHosts

Need ordered processing at scale:
  → Partition by entity key (N queues, N consumers)
```

### 3. What are the trade-offs?

```
noAck:true vs false
  Fast but lossy  vs  Reliable but slower

prefetch(1) vs prefetch(N)
  Fair but lower throughput  vs  Higher throughput but unfair on crash

Sync HTTP vs Async MQ
  Simple but coupled  vs  Resilient but eventually consistent

Classic queue vs Quorum queue
  Faster but single-node risk  vs  Safe but 3x write amplification

Retry requeue:true vs delay queue
  Fast retry but potential loop  vs  Backoff but more infrastructure
```

---

## Quick Reference Card

```
QUESTION IN INTERVIEW              WHAT THEY WANT TO HEAR
────────────────────────────────────────────────────────────────
Consumer crashes                → noAck:false + prefetch + DLQ
Message queue vs Pub/Sub        → Persistence + acks + delivery guarantees
Microservice reliability        → Outbox + async events + idempotency
Service B is down               → Circuit breaker + retry + async fallback
Scale consumers                 → Competing consumers + prefetch + partitioning
Exactly-once processing         → At-least-once + idempotent consumer + Redis NX
RabbitMQ vs Kafka               → Task queues vs event log + replay
Message ordering                → Single consumer or partition by key
Monitor in production           → Queue depth + unacked count + consumer count
Design reliable system          → All of the above, working together
```

---

*This README is built around real backend interview questions. The code is production-oriented — every pattern here solves a real failure mode you will encounter.*