import test from 'node:test';
import assert from 'node:assert/strict';
import { IdempotentJobEngine } from './idempotent-worker.mjs';

test('IdempotentJobEngine executes handler once and deduplicates retries with identical payload', async () => {
  const engine = new IdempotentJobEngine(':memory:');
  let sideEffects = 0;

  const first = await engine.process(
    { idempotencyKey: 'evt_stripe_001', payload: { amountCents: 4900, currency: 'USD' } },
    async (data) => {
      sideEffects += 1;
      return { charged: data.amountCents };
    }
  );

  assert.equal(first.deduplicated, false);
  assert.equal(first.attempts, 1);
  assert.deepEqual(first.result, { charged: 4900 });

  const second = await engine.process(
    { idempotencyKey: 'evt_stripe_001', payload: { amountCents: 4900, currency: 'USD' } },
    async () => {
      sideEffects += 1;
      return { charged: 9999 };
    }
  );

  assert.equal(second.deduplicated, true);
  assert.equal(sideEffects, 1);
  assert.deepEqual(second.result, { charged: 4900 });
});

test('IdempotentJobEngine rejects payload tampering on the same idempotency key', async () => {
  const engine = new IdempotentJobEngine(':memory:');
  await engine.process(
    { idempotencyKey: 'evt_002', payload: { userId: 10, role: 'viewer' } },
    async () => ({ ok: true })
  );

  await assert.rejects(
    () =>
      engine.process(
        { idempotencyKey: 'evt_002', payload: { userId: 10, role: 'admin' } },
        async () => ({ ok: true })
      ),
    /IdempotencyPayloadConflictError/
  );
});

test('IdempotentJobEngine enforces exponential backoff and dead-letter cap across transient failures', async () => {
  const engine = new IdempotentJobEngine(':memory:');
  let callCount = 0;

  // Attempt 1 fails at t = 1000 (next retry at 1000 + 500 = 1500)
  await assert.rejects(
    () =>
      engine.process(
        { idempotencyKey: 'evt_flaky', payload: { job: 1 }, maxAttempts: 2, baseDelayMs: 500, now: 1000 },
        async () => {
          callCount += 1;
          throw new Error('Upstream 503 Gateway Timeout');
        }
      ),
    /Upstream 503 Gateway Timeout/
  );

  // Early retry at t = 1200 is blocked by backoff window
  await assert.rejects(
    () =>
      engine.process(
        { idempotencyKey: 'evt_flaky', payload: { job: 1 }, maxAttempts: 2, baseDelayMs: 500, now: 1200 },
        async () => ({ recovered: true })
      ),
    /RetryBackoffActiveError/
  );

  // Retry at t = 1600 succeeds on attempt 2
  const recovered = await engine.process(
    { idempotencyKey: 'evt_flaky', payload: { job: 1 }, maxAttempts: 2, baseDelayMs: 500, now: 1600 },
    async (_payload, { attempt }) => {
      callCount += 1;
      return { recoveredOnAttempt: attempt };
    }
  );

  assert.equal(recovered.deduplicated, false);
  assert.equal(recovered.attempts, 2);
  assert.deepEqual(recovered.result, { recoveredOnAttempt: 2 });
  assert.equal(callCount, 2);
});
