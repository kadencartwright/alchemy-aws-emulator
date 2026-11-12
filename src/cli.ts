import { Effect } from 'effect';
import { NodeRuntime } from '@effect/platform-node';
import { sessionLayer } from './session.js';
import { provision, destroy } from './infra.js';
import { produce, consume } from './app.js';
import { PutBucketNotificationConfigurationCommand } from '@aws-sdk/client-s3';
import { SetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { AwsEmulatorSession } from './session.js';
const args = process.argv.slice(2);
const mode = args.includes('--notifications') ? 'notification' : 'explicit';
const portIndex = args.indexOf('--port');
const run = Effect.gen(function* () {
  const session = yield* AwsEmulatorSession;
  const infra = yield* provision;
  if (mode === 'notification') yield* Effect.tryPromise(() => session.sqs.send(new SetQueueAttributesCommand({ QueueUrl: infra.queueUrl, Attributes: { Policy: JSON.stringify({ Version: '2012-10-17', Statement: [{ Effect: 'Allow', Principal: { Service: 's3.amazonaws.com' }, Action: 'sqs:SendMessage', Resource: infra.queueArn, Condition: { ArnEquals: { 'aws:SourceArn': `arn:aws:s3:::${infra.bucket}` } } }] }) } })));
  if (mode === 'notification') yield* Effect.tryPromise(() => session.s3.send(new PutBucketNotificationConfigurationCommand({ Bucket: infra.bucket, NotificationConfiguration: { QueueConfigurations: [{ QueueArn: infra.queueArn, Events: ['s3:ObjectCreated:*'], Filter: { Key: { FilterRules: [{ Name: 'prefix', Value: 'input/' }] } } }] } })));
  const job = yield* produce(infra, 'hello from real Node and Effect', mode);
  if (mode === 'explicit') {
    yield* consume(infra, true).pipe(Effect.catch(e => String(e).includes('Injected failure') ? Effect.void : Effect.fail(e)));
    yield* Effect.sleep('1200 millis');
  }
  let result;
  for (let attempt = 0; attempt < 10 && !result; attempt++) result = yield* consume(infra);
  if (!result || result.sha256 !== job.sha256) return yield* Effect.fail(new Error('No verified result'));
  console.log(JSON.stringify({ backend: 'official-aws-emulator', mode, ...result }, null, 2));
  yield* destroy(infra);
});
NodeRuntime.runMain(Effect.scoped(run.pipe(Effect.provide(sessionLayer({ port: portIndex < 0 ? 0 : Number(args[portIndex + 1]) })))));
