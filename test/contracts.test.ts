import test from 'node:test';
import assert from 'node:assert/strict';
import { Effect } from 'effect';
import {
  CreateBucketCommand, PutObjectCommand, GetObjectCommand, ListObjectsV2Command,
  PutBucketNotificationConfigurationCommand,
} from '@aws-sdk/client-s3';
import {
  CreateQueueCommand, GetQueueAttributesCommand, SendMessageCommand,
  ReceiveMessageCommand, DeleteMessageCommand, SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs';
import { AwsEmulatorSession, sessionLayer, type Session } from '../src/session.js';

const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const run = (f: (s: Session) => Promise<void>) => Effect.runPromise(
  Effect.gen(function* () { const s = yield* AwsEmulatorSession; yield* Effect.tryPromise(() => f(s)); })
    .pipe(Effect.provide(sessionLayer()), Effect.scoped),
);
const status = (e: unknown): number | undefined => (e as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode;

// Every assertion below talks to the official emulator over CA-verified HTTPS.
test('S3 basic operations and concurrent conditional writes preserve data', () => run(async ({ s3 }) => {
  const Bucket = 'conditional-contract';
  await s3.send(new CreateBucketCommand({ Bucket }));
  const Key = 'counter';
  const creates = await Promise.allSettled(Array.from({ length: 12 }, (_, n) => s3.send(new PutObjectCommand({ Bucket, Key, Body: `create-${n}`, IfNoneMatch: '*' }))));
  assert.equal(creates.filter(r => r.status === 'fulfilled').length, 1, 'If-None-Match creation must have exactly one winner');
  for (const r of creates) if (r.status === 'rejected') assert.equal(status(r.reason), 412);
  const initial = await s3.send(new GetObjectCommand({ Bucket, Key }));
  assert.match(await initial.Body!.transformToString(), /^create-\d+$/);
  const updates = await Promise.allSettled(['update-a', 'update-b'].map(Body => s3.send(new PutObjectCommand({ Bucket, Key, Body, IfMatch: initial.ETag! }))));
  assert.equal(updates.filter(r => r.status === 'fulfilled').length, 1, 'two distinct updates sharing an ETag must have one winner');
  for (const r of updates) if (r.status === 'rejected') assert.equal(status(r.reason), 412);
  const updated = await s3.send(new GetObjectCommand({ Bucket, Key }));
  const body = await updated.Body!.transformToString();
  assert.ok(['update-a', 'update-b'].includes(body));
  await assert.rejects(s3.send(new PutObjectCommand({ Bucket, Key, Body: 'lost-write', IfMatch: initial.ETag! })), e => status(e) === 412);
  const preserved = await s3.send(new GetObjectCommand({ Bucket, Key }));
  assert.equal(await preserved.Body!.transformToString(), body);
  assert.equal(preserved.ETag, updated.ETag);
  assert.deepEqual((await s3.send(new ListObjectsV2Command({ Bucket }))).Contents?.map(o => o.Key), [Key]);
}));

test('SQS visibility expiry redelivers, acknowledgement deletes, repeated failure reaches DLQ', () => run(async ({ sqs }) => {
  const dlq = await sqs.send(new CreateQueueCommand({ QueueName: 'contract-dlq' }));
  const dlqAttrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl!, AttributeNames: ['QueueArn'] }));
  const main = await sqs.send(new CreateQueueCommand({ QueueName: 'contract-queue', Attributes: {
    VisibilityTimeout: '1', RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqAttrs.Attributes!.QueueArn!, maxReceiveCount: '2' }),
  } }));
  const QueueUrl = main.QueueUrl!;
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: 'redelivery' }));
  const first = (await sqs.send(new ReceiveMessageCommand({ QueueUrl, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }))).Messages?.[0];
  assert.ok(first);
  assert.equal((await sqs.send(new ReceiveMessageCommand({ QueueUrl }))).Messages?.length ?? 0, 0, 'inflight message must be invisible');
  await pause(1200);
  const second = (await sqs.send(new ReceiveMessageCommand({ QueueUrl, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }))).Messages?.[0];
  assert.ok(second);
  assert.equal(second.MessageId, first.MessageId);
  assert.equal(second.Attributes?.ApproximateReceiveCount, '2');
  await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: second.ReceiptHandle! }));
  await pause(1200);
  assert.equal((await sqs.send(new ReceiveMessageCommand({ QueueUrl }))).Messages?.length ?? 0, 0);

  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: 'poison' }));
  for (let n = 0; n < 4; n++) {
    await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    await pause(1200);
  }
  const dead = (await sqs.send(new ReceiveMessageCommand({ QueueUrl: dlq.QueueUrl! }))).Messages?.[0];
  assert.equal(dead?.Body, 'poison', 'maxReceiveCount must route the poison job to the DLQ');
  await sqs.send(new DeleteMessageCommand({ QueueUrl: dlq.QueueUrl!, ReceiptHandle: dead!.ReceiptHandle! }));
}));

test('real S3 object-created notification is delivered to SQS', () => run(async ({ s3, sqs }) => {
  const Bucket = 'notification-contract';
  await s3.send(new CreateBucketCommand({ Bucket }));
  const queue = await sqs.send(new CreateQueueCommand({ QueueName: 'notification-contract' }));
  const QueueUrl = queue.QueueUrl!;
  const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }));
  const QueueArn = attrs.Attributes!.QueueArn!;
  await sqs.send(new SetQueueAttributesCommand({ QueueUrl, Attributes: { Policy: JSON.stringify({ Version: '2012-10-17', Statement: [{ Effect: 'Allow', Principal: { Service: 's3.amazonaws.com' }, Action: 'sqs:SendMessage', Resource: QueueArn, Condition: { ArnEquals: { 'aws:SourceArn': `arn:aws:s3:::${Bucket}` } } }] }) } }));
  await s3.send(new PutBucketNotificationConfigurationCommand({ Bucket, NotificationConfiguration: { QueueConfigurations: [{ Id: 'created', QueueArn, Events: ['s3:ObjectCreated:*'] }] } }));
  await s3.send(new PutObjectCommand({ Bucket, Key: 'notification.txt', Body: 'actual-notification' }));
  let found = false;
  for (let n = 0; n < 10 && !found; n++) {
    const response = await sqs.send(new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10 }));
    for (const msg of response.Messages ?? []) {
      const event = JSON.parse(msg.Body!);
      if (event.Records?.some((r: { eventName: string; s3: { bucket: { name: string }; object: { key: string } } }) => r.eventName.startsWith('ObjectCreated:') && r.s3.bucket.name === Bucket && r.s3.object.key === 'notification.txt')) found = true;
      await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: msg.ReceiptHandle! }));
    }
  }
  assert.ok(found, 'S3 must emit the object-created event without an explicit SendMessage call');
}));

test('If-Match on an absent S3 key rejects with 412 and leaves no object', () => run(async ({ s3 }) => {
  const Bucket = 'absent-if-match-repro';
  await s3.send(new CreateBucketCommand({ Bucket }));
  await assert.rejects(s3.send(new PutObjectCommand({ Bucket, Key: 'absent', Body: 'must-not-exist', IfMatch: '"not-an-existing-etag"' })), e => status(e) === 412);
  assert.deepEqual((await s3.send(new ListObjectsV2Command({ Bucket }))).Contents ?? [], []);
}));

test('S3 notification suffix filter excludes a nonmatching object', () => run(async ({ s3, sqs }) => {
  const Bucket = 'notification-filter-contract';
  await s3.send(new CreateBucketCommand({ Bucket }));
  const queue = await sqs.send(new CreateQueueCommand({ QueueName: 'notification-filter-contract' }));
  const QueueUrl = queue.QueueUrl!;
  const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }));
  const QueueArn = attrs.Attributes!.QueueArn!;
  await sqs.send(new SetQueueAttributesCommand({ QueueUrl, Attributes: { Policy: JSON.stringify({ Version: '2012-10-17', Statement: [{ Effect: 'Allow', Principal: { Service: 's3.amazonaws.com' }, Action: 'sqs:SendMessage', Resource: QueueArn, Condition: { ArnEquals: { 'aws:SourceArn': `arn:aws:s3:::${Bucket}` } } }] }) } }));
  await s3.send(new PutBucketNotificationConfigurationCommand({ Bucket, NotificationConfiguration: { QueueConfigurations: [{
    Id: 'json-only', QueueArn, Events: ['s3:ObjectCreated:*'], Filter: { Key: { FilterRules: [{ Name: 'suffix', Value: '.json' }] } },
  }] } }));
  // Drain a possible configuration-test message before creating either object.
  for (const msg of (await sqs.send(new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10 }))).Messages ?? []) {
    await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: msg.ReceiptHandle! }));
  }
  await s3.send(new PutObjectCommand({ Bucket, Key: 'ignored.txt', Body: 'must-not-notify' }));
  await s3.send(new PutObjectCommand({ Bucket, Key: 'included.json', Body: '{}' }));
  const keys: string[] = [];
  for (let n = 0; n < 3; n++) {
    const response = await sqs.send(new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    for (const msg of response.Messages ?? []) {
      const event = JSON.parse(msg.Body!);
      for (const record of event.Records ?? []) keys.push(record.s3.object.key);
      await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle: msg.ReceiptHandle! }));
    }
  }
  assert.deepEqual(keys, ['included.json']);
}));
