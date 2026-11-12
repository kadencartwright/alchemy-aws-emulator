# Alchemy adapter boundary

Validated against `alchemy@2.0.0-beta.86`, Effect 4.1.4. `LocalBucket` and
`LocalQueue` are original custom Alchemy `Resource` types with real
`Provider.succeed` lifecycle implementations. `Stack`, `Deploy.deploy`, and
`Destroy.destroy` execute the dependency graph, resolve queue ARN outputs,
retain state, and drive read/diff/reconcile/delete. These are deliberately named
`AwsEmulator.*`: this project does not claim transparent replacement of built-in
`AWS.S3.Bucket` or built-in AWS bindings.

The public `providers` Effect captures `AwsEmulatorSession` and builds the two
provider layers. Every API operation uses that session's explicitly configured
AWS SDK clients, including HTTPS CA trust; the provider transport is therefore
exactly the same transport implementation as the application transport, with
separate lifecycle requests verified by integration tests. There is no ambient
AWS credential or endpoint lookup, and no registered live/cloud provider.

## APIs investigated

`alchemy/Local/ProviderLayer.dual` registers live/local variants. Its `dataPlane`
callback installs the cloud endpoint/credential/region service layer for
**built-in bindings** invoking a local resource, including deployment actions.
It does not itself configure TLS trust. Reusing built-in AWS reconcilers would
also require configuring the Distilled/Effect HTTP transport and all its
credential/environment services; configuring an AWS SDK client alone would not
configure that separate transport. We use the explicit custom-provider seam,
so no unverified built-in transport or live-mode fallback is present.

`alchemy/Local/LocalProvider.make` manages one process per resource in the dev
RPC sidecar, with a registry and restart detection. A bucket or queue is not a
process, and all resources must share one authenticated emulator. The
`AwsEmulatorSession` scoped Effect layer owns that process instead. Application
execution remains ordinary Node.js, outside the emulator's Lambda engines.

## State and ownership

`makeInfrastructure` returns a controller with `deploy` and `destroy` Effects.
Its Alchemy `inMemoryState()` layer is private to one session; `provision` retains
the controller in a weak map keyed by the session object for repeated deploys.
A new session receives new state and new resource names, so memory-only emulator
restarts never encounter persisted stale state or credentials. Destroy empties
the owned unversioned bucket before deleting it and deletes only declared
queues. Closing the emulator session independently releases its process.

This proof of concept supports bucket name replacement and queue name
replacement, visibility/redrive updates, read, and deletion. Bucket versioning,
IAM, arbitrary AWS bindings, resource imports, and production deployment are
outside this adapter's contract. API failures propagate as tagged
`LocalApiError`; missing resources alone map to an absent read.

`makeInfrastructureWith(desired)` also accepts a desired visibility timeout;
changing that object before the next controller `deploy` evaluates the updated
stack and drives queue reconciliation. `test/infra.test.ts` exercises create,
registered provider read (present and absent), repeated no-op deployment,
visibility update from 1 to 3 seconds observed through the runtime SDK, and
engine-driven destruction. It first confirms that an ordinary HTTPS client
without the generated CA rejects the endpoint, then confirms Alchemy's
explicitly trusted provider transport succeeds. These are real network requests
to the official emulator, not mock lifecycle calls.
