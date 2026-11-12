import { Effect, Layer } from "effect";
import { isResolved } from "alchemy/Diff";
import { Resource } from "alchemy/Resource";
import * as Provider from "alchemy/Provider";
import { provideFreshArtifactStore } from "alchemy/Artifacts";
import { AuthProvidersLive } from "alchemy/Auth/Registry";
import { layerNonInteractive } from "alchemy/Interaction";
import { Stack } from "alchemy/Stack";
import { deploy } from "alchemy/Deploy";
import { destroy as destroyStack } from "alchemy/Destroy";
import { inMemoryState } from "alchemy/State/InMemoryState";
import { CreateBucketCommand, HeadBucketCommand, DeleteBucketCommand, ListObjectsV2Command, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { CreateQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SetQueueAttributesCommand, DeleteQueueCommand } from "@aws-sdk/client-sqs";
import { AwsEmulatorSession } from "./session.js";

export class LocalApiError extends Error {
  readonly _tag = "LocalApiError";
  constructor(readonly cause: unknown) { super(cause instanceof Error ? cause.message : String(cause)); }
}
const api = <A>(f: () => Promise<A>) => Effect.tryPromise({ try: f, catch: (e) => new LocalApiError(e) });
const missing = (e: unknown) => {
  const cause = e instanceof LocalApiError ? e.cause : e;
  return typeof cause === "object" && cause !== null && ("$metadata" in cause && (cause.$metadata as { httpStatusCode?: number }).httpStatusCode === 404 || "name" in cause && ["NoSuchBucket", "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue"].includes(String(cause.name)));
};

export interface LocalBucket extends Resource<"AwsEmulator.Bucket", { name: string }, { name: string }> {}
export const LocalBucket = Resource<LocalBucket>("AwsEmulator.Bucket");
export interface LocalQueue extends Resource<"AwsEmulator.Queue", { name: string; visibility?: number; redriveArn?: string }, { url: string; arn: string }> {}
export const LocalQueue = Resource<LocalQueue>("AwsEmulator.Queue");

/** Both lifecycle and application calls use these exact session-owned SDK clients. */
export const providers = Effect.gen(function* () {
  const session = yield* AwsEmulatorSession;
  const bucket = Provider.succeed(LocalBucket, {
    read: ({ olds }) => api(async () => {
      try { await session.s3.send(new HeadBucketCommand({ Bucket: olds.name })); return { name: olds.name }; }
      catch (e) { if (missing(e)) return undefined; throw e; }
    }),
    diff: ({ olds, news }) => Effect.succeed(isResolved(news) && olds.name !== news.name ? { action: "replace" as const } : undefined),
    reconcile: ({ news }) => api(async () => {
      await session.s3.send(new CreateBucketCommand({ Bucket: news.name, ...(session.region === "us-east-1" ? {} : { CreateBucketConfiguration: { LocationConstraint: session.region as "us-west-2" } }) }));
      return { name: news.name };
    }),
    delete: ({ output }) => api(async () => {
      try {
        for (;;) {
          const page = await session.s3.send(new ListObjectsV2Command({ Bucket: output.name }));
          if (!page.Contents?.length) break;
          for (const item of page.Contents) await session.s3.send(new DeleteObjectCommand({ Bucket: output.name, Key: item.Key! }));
        }
        await session.s3.send(new DeleteBucketCommand({ Bucket: output.name }));
      } catch (e) { if (!missing(e)) throw e; }
    }),
  });
  const queue = Provider.succeed(LocalQueue, {
    read: ({ olds }) => api(async () => {
      try {
        const { QueueUrl } = await session.sqs.send(new GetQueueUrlCommand({ QueueName: olds.name }));
        const { Attributes } = await session.sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ["QueueArn"] }));
        return { url: QueueUrl!, arn: Attributes!.QueueArn! };
      } catch (e) { if (missing(e)) return undefined; throw e; }
    }),
    diff: ({ olds, news }) => Effect.succeed(isResolved(news) && olds.name !== news.name ? { action: "replace" as const } : undefined),
    reconcile: ({ news, output }) => api(async () => {
      const attrs = { VisibilityTimeout: String(news.visibility ?? 1), ...(news.redriveArn ? { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: news.redriveArn, maxReceiveCount: "2" }) } : {}) };
      const url = output?.url ?? (await session.sqs.send(new CreateQueueCommand({ QueueName: news.name, Attributes: attrs }))).QueueUrl!;
      await session.sqs.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: attrs }));
      const { Attributes } = await session.sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["QueueArn"] }));
      return { url, arn: Attributes!.QueueArn! };
    }),
    delete: ({ output }) => api(async () => { try { await session.sqs.send(new DeleteQueueCommand({ QueueUrl: output.url })); } catch (e) { if (!missing(e)) throw e; } }),
  });
  return Layer.mergeAll(bucket, queue);
});

export interface Infrastructure { bucket: string; queueUrl: string; queueArn: string; dlqUrl: string; dlqArn: string }

/** A controller owns ephemeral Alchemy state for precisely one emulator session. */
export const makeInfrastructureWith = (desired: { visibility?: number } = {}) => Effect.gen(function* () {
  const session = yield* AwsEmulatorSession;
  const providerLayer = yield* providers;
  const state = inMemoryState();
  const stack = Stack(`aws-emulator-${session.id}`, { providers: providerLayer, state }, Effect.gen(function* () {
    const bucket = yield* LocalBucket("objects", { name: `objects-${session.id}` });
    const dlq = yield* LocalQueue("dead-letters", { name: `dlq-${session.id}` });
    const queue = yield* LocalQueue("jobs", { name: `jobs-${session.id}`, visibility: desired.visibility ?? 1, redriveArn: dlq.arn });
    return { bucket: bucket.name, queueUrl: queue.url, queueArn: queue.arn, dlqUrl: dlq.url, dlqArn: dlq.arn };
  }));
  const engine = Layer.mergeAll(state, AuthProvidersLive, layerNonInteractive());
  return {
    deploy: deploy({ stack, stage: "local" }).pipe(provideFreshArtifactStore, Effect.provide(engine)),
    destroy: destroyStack({ stack, stage: "local" }).pipe(provideFreshArtifactStore, Effect.provide(engine)),
  };
});

export const makeInfrastructure = makeInfrastructureWith();

const controllers = new WeakMap<object, Effect.Success<typeof makeInfrastructure>>();
/** Retains session-local state across successive reconciliations. Never reuses state after restart. */
export const provision = Effect.gen(function* () {
  const session = yield* AwsEmulatorSession;
  let controller = controllers.get(session);
  if (!controller) { controller = yield* makeInfrastructure; controllers.set(session, controller); }
  return yield* controller.deploy;
});
export const destroy = (_resources?: Infrastructure) => Effect.gen(function* () {
  const session = yield* AwsEmulatorSession;
  const controller = controllers.get(session);
  if (controller) { yield* controller.destroy; controllers.delete(session); }
});
