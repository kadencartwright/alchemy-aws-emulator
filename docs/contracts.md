# Verified local contracts

Executed against the official AWS Emulator 1.0.336 Linux amd64 binary (release
package 1.0.336.1, commit f515581569b98a9dcbd2e686c12f912fdce51bba), using
AWS SDK for JavaScript 3.1166.0 over HTTPS with the generated CA explicitly
configured in the HTTP agent. These are this project's observations, not
upstream evaluation scores.

Run `pnpm exec tsx --test test/contracts.test.ts` after `pnpm run bootstrap`.
All five tests passed in approximately 10.7 seconds on November 12, 2026:

- S3 create bucket, put/get/list; twelve concurrent `PutObject` calls with
  `If-None-Match: *` produce exactly one winner and eleven HTTP 412 responses.
- Two distinct object updates sharing the original ETag produce one winner and
  one HTTP 412. A later stale-ETag write returns 412 and preserves both body and
  ETag.
- SQS send/receive, one-second visibility timeout, redelivery with receive count
  two, delete acknowledgement, and poison-message DLQ routing with
  `maxReceiveCount: 2`.
- Genuine S3 `ObjectCreated` notification reaches SQS without an explicit
  application `SendMessage` call. The queue grants S3 permission to publish from
  the configured bucket.
- `If-Match` for an absent object returns 412 and leaves no object. A suffix
  notification filter includes `.json` and excludes `.txt`. Both checks pass in
  this package; previously published upstream failures must not be assumed to
  persist.

The suffix-filter test initially omitted the destination queue policy. Emulator
validation correctly rejected notification configuration with HTTP 400
`InvalidArgument`. Adding the required policy fixed the test; this was test
configuration error, not an emulator fidelity gap. No test is skipped or marked
as an expected failure.

These checks cover specific contracts and do not establish service-wide fidelity,
production readiness, or performance. Session isolation, restart/reseed,
provisioning, and the application's idempotency/checksum behavior are exercised
separately by the lifecycle/demo tests.
