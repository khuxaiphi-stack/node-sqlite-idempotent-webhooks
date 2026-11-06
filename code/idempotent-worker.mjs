/**
 * Zero-Dependency Idempotent Webhook & Background Job Engine using Node.js 26 Built-in node:sqlite (WAL Mode)
 * Companion production code for the $500 Technical Writing Tutorial (Honeybadger / Draft.dev)
 */
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

export class IdempotentJobEngine {
  /**
   * @param {string} [dbPath=':memory:']
   */
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

  /**
   * Execute a webhook or job handler exactly once per idempotencyKey, with deterministic exponential backoff.
   */
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
