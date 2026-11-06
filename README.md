# Zero-Dependency Idempotent Webhooks & Background Retries in Modern Node.js with Built-In `node:sqlite` (WAL Mode)

**By Huzaifa (`@khuxaiphi-stack`)** • Companion Repository & Verified Test Suite (`Node.js v22.5+ / v26+`)

---

## Introduction: The Hidden Cost of "Just Add Redis"

Every production backend eventually faces the same reliability problem: **at-least-once webhook delivery and transient upstream failures**.

When Stripe, GitHub, Shopify, or a Solana webhook indexer sends an HTTP event to your server, network timeouts or `502/503` responses cause the sender to retry. Without strict idempotency, your service risks:

1. **Double Side-Effects:** Charging a customer twice, minting duplicate tokens, or sending duplicate notifications when a retry arrives after the first request succeeded right before a socket timeout.
2. **Concurrent Worker Race Conditions:** Two identical webhook deliveries arriving within 5 milliseconds of each other, both passing a naive `SELECT` check before either writes a completion record.
3. **Silent Payload Tampering:** A client reusing an `Idempotency-Key` header with a modified request body (`{ amountCents: 9900 }` instead of `{ amountCents: 4900 }`).

Historically, teams reached immediately for Redis (`SET key NX EX`) plus BullMQ. While Redis is fantastic at scale, introducing a separate in-memory datastore creates **split-brain failure modes** (what happens when your database commits, your process crashes before updating Redis, and the retry runs?) and unnecessary operational overhead for single-node, edge, or modular services.

With the built-in **`node:sqlite` (`DatabaseSync`)** module in modern Node.js (Node 22.5+ and Node 26), we can build a **zero-dependency, ACID-compliant idempotency lock, SHA-256 payload conflict detector, exponential backoff scheduler, and dead-letter queue (DLQ)** in ~115 lines of pure JavaScript.

---

## 1. Architecture & State Machine

Our `IdempotentJobEngine` enforces a deterministic 3-state machine backed by SQLite in **Write-Ahead Logging (`WAL`)** mode:

```
Incoming Event (idempotencyKey + payload)
                  │
                  ▼
      ┌───────────────────────┐
      │ SHA-256 Payload Check │──(Mismatch!)──► Throw IdempotencyPayloadConflictError
      └───────────┬───────────┘
                  │
     ┌────────────┼────────────┐
     ▼            ▼            ▼
[New Key]   [COMPLETED]    [FAILED]
     │            │            │
     │      Return cached      ├── If attempts >= max_attempts ──► Throw DeadLetterQueueError
     │      JSON result        ├── If now < next_retry_at      ──► Throw RetryBackoffActiveError
     │      (0 side effects)   └── Atomic UPDATE reclaim       ──► Increment attempts & execute
     ▼                                                                    │
Insert 'PROCESSING' (attempts = 1) ◄──────────────────────────────────────┘
     │
     ▼
Execute Handler(payload, { attempt })
     ├── Success ──► UPDATE status = 'COMPLETED', result_json = ?
     └── Error   ──► UPDATE status = 'FAILED', next_retry_at = now + baseDelayMs * 2^(attempt-1)
```

---

## 2. Why `PRAGMA journal_mode = WAL` Matters

By default, SQLite uses a rollback journal that locks the database file during writes. Enabling **Write-Ahead Logging (`WAL`)** changes the concurrency model:
- Readers do not block writers, and writers do not block readers.
- Combined with `PRAGMA synchronous = NORMAL`, SQLite achieves tens of thousands of atomic state transitions per second on local NVMe storage with zero external network hops.
- Most importantly, `UPDATE ... WHERE status = 'FAILED' AND attempts < max_attempts AND next_retry_at <= ?` executes atomically inside SQLite—eliminating the time-of-check to time-of-use (`TOCTOU`) race condition between concurrent workers.

---

## 3. Complete Implementation (`code/idempotent-worker.mjs`)

```javascript
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

export class IdempotentJobEngine {
  constructor(dbPath = ':memory:') {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS idempotent_jobs (
        idempotency_key TEXT PRIMARY KEY,
        payload_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        result_json TEXT,
        last_error TEXT,
        next_retry_at INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL
      );
    `);

    this._getStmt = this.db.prepare(`SELECT * FROM idempotent_jobs WHERE idempotency_key = ?`);
    this._insertStmt = this.db.prepare(`
      INSERT INTO idempotent_jobs (idempotency_key, payload_hash, status, attempts, max_attempts, next_retry_at, updated_at)
      VALUES (?, ?, 'PROCESSING', 1, ?, 0, ?)
    `);
    this._completeStmt = this.db.prepare(`
      UPDATE idempotent_jobs
      SET status = 'COMPLETED', result_json = ?, last_error = NULL, updated_at = ?
      WHERE idempotency_key = ?
    `);
    this._failStmt = this.db.prepare(`
      UPDATE idempotent_jobs
      SET status = 'FAILED', last_error = ?, next_retry_at = ?, updated_at = ?
      WHERE idempotency_key = ?
    `);
    this._reclaimStmt = this.db.prepare(`
      UPDATE idempotent_jobs
      SET status = 'PROCESSING', attempts = attempts + 1, updated_at = ?
      WHERE idempotency_key = ? AND status = 'FAILED' AND attempts < max_attempts AND next_retry_at <= ?
    `);
  }

  static hashPayload(payload) {
    return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  }

  async process({ idempotencyKey, payload, maxAttempts = 3, baseDelayMs = 1000, now = Date.now() }, handler) {
    const payloadHash = IdempotentJobEngine.hashPayload(payload);
    const existing = this._getStmt.get(idempotencyKey);

    if (existing) {
      if (existing.payload_hash !== payloadHash) {
        throw new Error(
          `IdempotencyPayloadConflictError: Key "${idempotencyKey}" was already used with a different payload`
        );
      }
      if (existing.status === 'COMPLETED') {
        return {
          deduplicated: true,
          status: 'COMPLETED',
          attempts: existing.attempts,
          result: JSON.parse(existing.result_json),
        };
      }
      if (existing.status === 'PROCESSING') {
        throw new Error(`IdempotencyLockError: Key "${idempotencyKey}" is currently in flight`);
      }
      if (existing.attempts >= existing.max_attempts) {
        throw new Error(`DeadLetterQueueError: Key "${idempotencyKey}" exhausted all ${existing.max_attempts} attempts`);
      }
      if (now < existing.next_retry_at) {
        throw new Error(`RetryBackoffActiveError: Key "${idempotencyKey}" cannot retry until ${existing.next_retry_at}`);
      }

      const reclaim = this._reclaimStmt.run(now, idempotencyKey, now);
      if (reclaim.changes === 0) {
        throw new Error(`IdempotencyLockError: Concurrent worker claimed "${idempotencyKey}"`);
      }
    } else {
      this._insertStmt.run(idempotencyKey, payloadHash, maxAttempts, now);
    }

    const current = this._getStmt.get(idempotencyKey);
    try {
      const result = await handler(payload, { attempt: current.attempts });
      this._completeStmt.run(JSON.stringify(result ?? null), Date.now(), idempotencyKey);
      return {
        deduplicated: false,
        status: 'COMPLETED',
        attempts: current.attempts,
        result,
      };
    } catch (err) {
      const backoffMs = baseDelayMs * 2 ** (current.attempts - 1);
      const nextRetryAt = now + backoffMs;
      this._failStmt.run(err.message, nextRetryAt, now, idempotencyKey);
      throw err;
    }
  }
}
```

---

## 4. Verifying Edge Cases with `node:test` (`code/idempotent-worker.test.mjs`)

Run the test suite with zero external packages:

```bash
node --test code/idempotent-worker.test.mjs
```

### Verified Test Output (Node.js v26.8.1)
```text
✔ IdempotentJobEngine executes handler once and deduplicates retries with identical payload (33.4ms)
✔ IdempotentJobEngine rejects payload tampering on the same idempotency key (5.5ms)
✔ IdempotentJobEngine enforces exponential backoff and dead-letter cap across transient failures (5.0ms)
ℹ tests 3 | pass 3 | fail 0
```
