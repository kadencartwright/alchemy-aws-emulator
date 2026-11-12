# Local verification results — 2026-11-12

Test host: Linux x86_64, Node 24.19.0, pnpm 11.20.0. Official emulator
1.0.336, commit f515581569b98a9dcbd2e686c12f912fdce51bba.
Downloaded Debian package SHA-256:
`e2c621ec12c8a4bb46c884792d72f189378ea83ffcce180dc837b4c5e6b97f1c`.
The version index calls this build 1.0.336.1; its pinned package filename uses
`aws-emulator_1.0.336-1_amd64.deb` and the binary reports 1.0.336.

Executed `pnpm run bootstrap`, `pnpm typecheck`, `pnpm build`, `pnpm test`,
`pnpm demo`, and `pnpm demo --notifications`. Combined suite: **9 passed,
0 failed, 0 skipped**, approximately 10.6 seconds. This is contract validation,
not a throughput benchmark.

| Check | Observed result |
| --- | --- |
| S3 create/put/get/list | Passed over CA-verified HTTPS |
| Concurrent conditional create | 12 calls: one success, eleven HTTP 412 |
| Distinct writes sharing original ETag | One success, one HTTP 412; stale write preserves bytes/ETag |
| Absent-key If-Match | HTTP 412, key remains absent |
| SQS create/send/receive/delete | Passed |
| Visibility expiry | Redelivered with same message ID, receive count 2 |
| DLQ | Poison message routed after configured maxReceiveCount 2 |
| Genuine S3 → SQS | Delivered without explicit enqueue; suffix exclusion passed |
| Alchemy/runtime consistency | Runtime sees Alchemy's bucket and queues |
| Alchemy lifecycle | Create, repeated no-op deploy, provider read, visibility update 1→3, destroy all pass |
| CA trust | Default untrusted Node HTTPS rejects; configured provider/runtime succeed |
| Failure after result save, before ack | Redelivery recognizes existing result, verifies content, acknowledges |
| Restart/reseed | New process has fresh credentials, empty state; redeclare/reseed/rerun succeeds |
| Two isolated sessions | Distinct endpoints/keys, no resource visibility; closing inner leaves outer usable |
| Endpoint validation | Cloud, HTTP, userinfo, and non-loopback endpoints rejected |

Both demos produce `HELLO FROM REAL NODE AND EFFECT` with verified SHA-256
`007a82265d370200a1003008a90f95b348382875f6a416dcb7d1dcfbe5bc315a`.
Explicit mode reports `duplicate: true`, receiveCount 2 after injected failure;
notification mode reports receiveCount 1. Result contents are also checked in
automated tests.

No emulator fidelity failure was found by these probes. Previously reported
upstream absent-key If-Match and notification-filter failures did not reproduce.
The first notification CLI/filter setup lacked an SQS destination policy and
correctly received HTTP 400; fixed application/test configuration. Other initial
implementation failures (Effect error wrapping and provider service access) were
fixed before the combined run. Tests retain strict assertions, with no mock
backend, skips or expected failures. See [contract details](contracts.md).

Limits: custom Alchemy resource seam, not transparent built-in AWS bindings or
Distilled transport configuration. One account (000000000000)/region per process,
unversioned S3, local loopback Linux amd64 only. No attachment/container networking,
Floci comparison, Lambda/ECS execution or production deployment. A new session
intentionally gets fresh Alchemy memory state, names and fixtures. Deterministic
S3 result idempotence does not cover arbitrary external side effects. No real AWS
credentials were supplied, no live providers registered, and telemetry was off.

CI is configured to repeat these local checks on Ubuntu without cloud credentials;
local results do not claim a completed hosted CI run.
