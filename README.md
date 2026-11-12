# Alchemy + official AWS Emulator

A working local S3/SQS proof of concept with Effect 4 and Alchemy v2. The official
AWS Emulator runs as a scoped native child; the application runs in **real Node.js**.
No AWS account, real credentials, `.env`, Docker daemon, or cloud provisioning.

## Run

Prerequisites: Linux x86_64 with glibc ≥2.17, Node 24+, pnpm 11.20.0,
`curl`, `dpkg-deb`, and internet for dependency/package downloads. Bootstrap
extracts a prebuilt Debian package under this checkout; no sudo/system install.

```sh
git clone https://github.com/kadencartwright/alchemy-aws-emulator.git
cd alchemy-aws-emulator
pnpm install --frozen-lockfile
pnpm run bootstrap
pnpm typecheck
pnpm build
pnpm test
pnpm demo
pnpm demo --notifications
```

Use `pnpm run bootstrap` explicitly: pnpm's bare `bootstrap` command can refer to
its own install workflow. The download is pinned by version **and SHA-256**.
No full Rust workspace build is involved.

`pnpm dev` runs the explicit demo. `pnpm reset` starts a new session, declares
resources, reseeds the fixture and reruns. Each command destroys its Alchemy
resources and closes its emulator before exiting; Ctrl+C/SIGTERM interrupts the
Effect scope. `pnpm cleanup` removes only this project's downloaded/build
artifacts. It never kills processes by name or port. A forcibly killed parent
(SIGKILL) cannot finalize; stop only its known child PID if that happens.

Use `pnpm demo --port 9297` to request the documented default HTTPS port;
by default port `0` lets the OS allocate a free port. An occupied requested port
fails. The current package bootstrap supports Linux x86_64 only; container
networking, attaching to existing sessions and Floci comparison are not implemented.

## What the demo does

Alchemy declares a bucket, a job queue and a DLQ. A producer writes an input
object with SHA-256 metadata. Explicit mode sends a JSON job to SQS. The consumer
checks object bytes against both the job checksum and metadata, uppercases the
content and conditionally creates a result object. It intentionally fails after
saving the result but before acknowledgement. After visibility expiry it reads
the same job, verifies the existing result, reports `duplicate: true`, and deletes
the message. Tests also inspect the stored result.

Notification mode installs a real queue policy and S3 object-created
notification filtered to `input/`, then writes the object **without explicit
enqueue**. It validates bytes against metadata. Configuration test events are
acknowledged separately. The result prefix does not trigger another input job.
Both modes print the result checksum and transformed content.

This is idempotent storage of a deterministic result in this session. It does
not promise exactly-once arbitrary external effects. Poison jobs reach a DLQ
after two receives; the strict test exercises that routing.

## Reuse and transport boundary

```ts
import { Effect } from 'effect';
import { NodeRuntime } from '@effect/platform-node';
import { sessionLayer, provision, produce, consume, destroy } from './src/index.js';

NodeRuntime.runMain(Effect.scoped(Effect.gen(function* () {
  const infra = yield* provision;
  yield* produce(infra, 'another application');
  console.log(yield* consume(infra));
  yield* destroy();
}).pipe(Effect.provide(sessionLayer()))));
```

The build exports the same API from `dist/src/index.js` with declarations, so
another workspace can use a local path dependency (this package is private,
not published to npm).

- `AwsEmulatorSession` / `sessionLayer({ binary?, port?, region? })`: owns one
  emulator, readiness check, random session identity, endpoint, region/account,
  generated credentials, CA, S3/SQS clients, and explicit `childEnv` for spawned
  callers. The binary override is for compatible manually installed builds;
  the validated bootstrap version remains the default.
- `LocalBucket`, `LocalQueue`, `providers`: genuine custom Alchemy resource and
  provider layers, reusable in other stacks. `makeInfrastructureWith(desired)`
  exposes deploy/destroy and supports queue visibility updates.
- `provision`, `destroy`: session-local controller convenience APIs.
- `produce`, `consume`, `checksum`: ordinary Effect application functions.

These are explicitly named custom resources, **not** transparent overrides of
`AWS.S3.Bucket` or built-in AWS bindings/Actions. Lifecycle calls have an explicit
HTTPS agent trusting the generated CA; the application uses the same configured
clients. Tests independently reject untrusted Node HTTPS and verify Alchemy
create/read/update/delete plus runtime access. No claim is made about configuring
Alchemy's separate built-in Distilled transport. See [adapter seam](docs/alchemy-seam.md).

`NODE_EXTRA_CA_CERTS` is only supplied in `childEnv` for **new processes**.
It is never set after Node starts to configure an existing transport. Current
clients use explicit `https.Agent({ ca, rejectUnauthorized: true })`. S3 uses
path-style addressing and host prefix injection is disabled. SQS may return
AWS-looking queue URLs; `useQueueUrlAsEndpoint: false` keeps requests at the
explicit loopback endpoint. Endpoint validation rejects non-HTTPS/non-loopback
hosts. No live/cloud providers are registered; unsupported resources cannot fall
back to AWS through this adapter. Credentials are explicit, session generated,
and no credential-provider chain is used.

## Restarts, ownership, versions

The emulator is memory-only. Each session gets a private temporary home and
fresh keys/CA. Alchemy state is in-memory and keyed to that session; nothing
persists across restarts. Restart means declare and seed again, including fresh
resource names. Repeated deploys inside a session retain state and reconcile.
Tests actually stop/start and reseed, and run two isolated sessions while closing
one without affecting the other. This strategy deliberately sacrifices durable
local resource identity. Only owned child processes and temporary files are
released. Telemetry is disabled at startup.

Pinned: AWS Emulator package `1.0.336` (versions index entry `1.0.336.1`),
commit `f515581569b98a9dcbd2e686c12f912fdce51bba`; Alchemy `2.0.0-beta.86`,
Effect/platform-node `4.1.4`, AWS SDK S3/SQS `3.1166.0`. Full transitive versions
are in `pnpm-lock.yaml`.

[Measured results](docs/results.md) records local checks and limits;
[contract details](docs/contracts.md) distinguishes these tests from upstream scores.
GitHub Actions runs the same emulator tests without cloud credentials.

Original code: Apache-2.0. The downloaded upstream emulator is not committed or
relicensed: its source is CC0, with Apache-licensed vendor code and bundled
third-party notices in the extracted package. Dependencies retain their licenses.

References: [official emulator](https://github.com/aws/aws-emulator),
[Linux setup](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-linux.html),
[SDK configuration](https://docs.aws.amazon.com/emulator/latest/userguide/point-sdk-to-emulator.html),
[Alchemy local providers](https://alchemy.run/infrastructure-as-code/local-provider/).
