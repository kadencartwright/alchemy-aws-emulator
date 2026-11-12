import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Effect } from 'effect';
import { ListBucketsCommand, GetObjectCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { request } from 'node:https';
import { AwsEmulatorSession, sessionLayer, assertLocalEndpoint, type Session } from '../src/session.js';
import { provision, destroy } from '../src/infra.js';
import { produce, consume } from '../src/app.js';
const run = <A,E>(body: Effect.Effect<A,E,AwsEmulatorSession>) => Effect.runPromise(Effect.scoped(body.pipe(Effect.provide(sessionLayer()))));
test('local endpoint guard rejects cloud/HTTP/credentials', () => {
  for (const value of ['https://s3.amazonaws.com', 'http://127.0.0.1:9297', 'https://user:pass@127.0.0.1:9297', 'https://localhost:9297']) assert.throws(() => assertLocalEndpoint(value));
});
test('two sessions are isolated; cleanup leaves other session running; restart reseeds with fresh keys', async () => {
  let first!: Session;
  await run(Effect.gen(function* () {
    const outer = yield* AwsEmulatorSession;
    const resources = yield* provision;
    yield* Effect.tryPromise(async () => {
      await run(Effect.gen(function* () {
        first = yield* AwsEmulatorSession;
        assert.notEqual(first.endpoint, outer.endpoint);
        assert.notEqual(first.credentials.accessKeyId, outer.credentials.accessKeyId);
        const empty = yield* Effect.tryPromise(() => first.s3.send(new ListBucketsCommand({}))); assert.deepEqual(empty.Buckets, []);
        const infra = yield* provision;
        yield* produce(infra, 'restart fixture');
        const result = yield* consume(infra); assert.equal(result?.content, 'RESTART FIXTURE');
        yield* destroy();
      }));
      await outer.s3.send(new HeadBucketCommand({ Bucket: resources.bucket }));
    });
    yield* destroy();
  }));
  await run(Effect.gen(function* () {
    const next = yield* AwsEmulatorSession;
    assert.notEqual(first.credentials.accessKeyId, next.credentials.accessKeyId);
    const buckets = yield* Effect.tryPromise(() => next.s3.send(new ListBucketsCommand({})));
    assert.deepEqual(buckets.Buckets, []);
    const infra = yield* provision;
    yield* produce(infra, 'restart fixture');
    const result = yield* consume(infra); assert.equal(result?.content, 'RESTART FIXTURE');
    yield* destroy();
  }));
});
test('CA trust is required; Alchemy resources and runtime share trusted transport; failure-before-ack is idempotent', async () => {
  await run(Effect.gen(function* () {
    const session = yield* AwsEmulatorSession;
    yield* Effect.tryPromise(() => assert.rejects(new Promise((resolve,reject) => {
      const req = request(`${session.endpoint}/__admin/health`, res => { res.resume(); resolve(res.statusCode); }); req.on('error', reject); req.end();
    }), /certificate|self.signed/i));
    const infra = yield* provision;
    const job = yield* produce(infra, 'checksum fixture');
    yield* consume(infra, true).pipe(Effect.match({ onFailure: e => assert.match(String(e), /Injected failure/), onSuccess: () => assert.fail('must inject failure') }));
    yield* Effect.sleep('1200 millis');
    const result = yield* consume(infra);
    assert.equal(result?.duplicate, true); assert.equal(result?.sha256, job.sha256); assert.equal(result?.content, 'CHECKSUM FIXTURE');
    const stored = yield* Effect.tryPromise(() => session.s3.send(new GetObjectCommand({ Bucket: infra.bucket, Key: result!.resultKey })));
    assert.equal(JSON.parse(yield* Effect.tryPromise(() => stored.Body!.transformToString())).sha256, job.sha256);
    assert.equal(yield* consume(infra), undefined);
    yield* destroy();
  }));
});
