import { Effect } from 'effect';
import { createHash } from 'node:crypto';
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { AwsEmulatorSession } from './session.js';
import type { Infrastructure } from './infra.js';
const api = <A>(f: () => Promise<A>) => Effect.tryPromise({ try: f, catch: e => e instanceof Error ? e : new Error(String(e)) });
export const checksum = (content: string) => createHash('sha256').update(content).digest('hex');
export interface Job { key: string; sha256: string }
export const produce = (infra: Infrastructure, content: string, mode: 'explicit' | 'notification' = 'explicit') => Effect.gen(function* () {
  const { s3, sqs } = yield* AwsEmulatorSession;
  const sha256 = checksum(content); const key = `input/${sha256}.txt`;
  yield* api(async () => {
    await s3.send(new PutObjectCommand({ Bucket: infra.bucket, Key: key, Body: content, Metadata: { sha256 } }));
    if (mode === 'explicit') await sqs.send(new SendMessageCommand({ QueueUrl: infra.queueUrl, MessageBody: JSON.stringify({ key, sha256 }) }));
  });
  return { key, sha256 };
});
export const consume = (infra: Infrastructure, failBeforeAck = false) => Effect.gen(function* () {
  const { s3, sqs } = yield* AwsEmulatorSession;
  return yield* api(async () => {
    const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: infra.queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 1, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
    const message = received.Messages?.[0]; if (!message) return undefined;
    const body = JSON.parse(message.Body!);
    if (body.Event === 's3:TestEvent') {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: infra.queueUrl, ReceiptHandle: message.ReceiptHandle! }));
      return undefined;
    }
    // Real notification bodies are distinct from explicitly queued jobs.
    const key: string = body.Records ? decodeURIComponent(body.Records[0].s3.object.key.replace(/\+/g, ' ')) : body.key;
    const object = await s3.send(new GetObjectCommand({ Bucket: infra.bucket, Key: key }));
    const content = await object.Body!.transformToString(); const actual = checksum(content);
    const expected = body.Records ? object.Metadata?.sha256 : body.sha256;
    if (!expected || actual !== expected || object.Metadata?.sha256 !== actual) throw new Error('Object checksum mismatch');
    const resultKey = `results/${actual}.json`;
    const result = JSON.stringify({ input: key, sha256: actual, content: content.toUpperCase() });
    let duplicate = false;
    try { await s3.send(new PutObjectCommand({ Bucket: infra.bucket, Key: resultKey, Body: result, IfNoneMatch: '*' })); }
    catch (e) {
      if ((e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode !== 412) throw e;
      const stored = await s3.send(new GetObjectCommand({ Bucket: infra.bucket, Key: resultKey }));
      if (await stored.Body!.transformToString() !== result) throw new Error('Existing result differs');
      duplicate = true;
    }
    if (failBeforeAck) throw new Error('Injected failure after result persisted, before acknowledgement');
    await sqs.send(new DeleteMessageCommand({ QueueUrl: infra.queueUrl, ReceiptHandle: message.ReceiptHandle! }));
    return { resultKey, duplicate, sha256: actual, content: content.toUpperCase(), receiveCount: Number(message.Attributes?.ApproximateReceiveCount) };
  });
});
