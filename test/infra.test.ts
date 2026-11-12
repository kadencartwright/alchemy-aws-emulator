import { test } from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:https';
import { Effect } from 'effect';
import { ListBucketsCommand } from '@aws-sdk/client-s3';
import { GetQueueAttributesCommand, ListQueuesCommand } from '@aws-sdk/client-sqs';
import { AwsEmulatorSession, sessionLayer } from '../src/session.js';
import { makeInfrastructureWith, providers, LocalBucket, LocalQueue } from '../src/infra.js';

test('Alchemy lifecycle uses trusted local transport; reconcile updates and destroy cleans resources', async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* AwsEmulatorSession;
    yield* Effect.tryPromise(async () => {
      await assert.rejects(new Promise<void>((resolve, reject) => {
        const req = request(session.endpoint, { agent: false }, response => { response.resume(); resolve(); });
        req.on('error', reject); req.end();
      }), /certificate|self.signed|issuer/i);
    });
    const desired = { visibility: 1 };
    const controller = yield* makeInfrastructureWith(desired);
    const first = yield* controller.deploy;
    const registered = yield* providers;
    const bucketProvider = yield* Effect.gen(function* () { return yield* LocalBucket.Provider; }).pipe(Effect.provide(registered));
    const queueProvider = yield* Effect.gen(function* () { return yield* LocalQueue.Provider; }).pipe(Effect.provide(registered));
    const readBucket = { id: "objects", fqn: "objects", instanceId: "test", olds: { name: first.bucket }, output: { name: first.bucket } };
    const readQueue = { id: "jobs", fqn: "jobs", instanceId: "test", olds: { name: `jobs-${session.id}` }, output: { url: first.queueUrl, arn: first.queueArn } };
    assert.deepEqual(yield* bucketProvider.read!(readBucket), { name: first.bucket });
    assert.deepEqual(yield* queueProvider.read!(readQueue), { url: first.queueUrl, arn: first.queueArn });
    const second = yield* controller.deploy;
    assert.deepEqual(second, first);
    desired.visibility = 3;
    const updated = yield* controller.deploy;
    assert.deepEqual(updated, first);
    yield* Effect.tryPromise(async () => {
      const attrs = await session.sqs.send(new GetQueueAttributesCommand({ QueueUrl: first.queueUrl, AttributeNames: ['VisibilityTimeout'] }));
      assert.equal(attrs.Attributes?.VisibilityTimeout, '3');
      const buckets = await session.s3.send(new ListBucketsCommand({}));
      assert.ok(buckets.Buckets?.some(b => b.Name === first.bucket));
    });
    yield* controller.destroy;
    assert.equal(yield* bucketProvider.read!(readBucket), undefined);
    assert.equal(yield* queueProvider.read!(readQueue), undefined);
    yield* Effect.tryPromise(async () => {
      const buckets = await session.s3.send(new ListBucketsCommand({}));
      assert.ok(!buckets.Buckets?.some(b => b.Name === first.bucket));
      const queues = await session.sqs.send(new ListQueuesCommand({}));
      assert.ok(!queues.QueueUrls?.includes(first.queueUrl));
      assert.ok(!queues.QueueUrls?.includes(first.dlqUrl));
    });
  }).pipe(Effect.provide(sessionLayer()))));
});
