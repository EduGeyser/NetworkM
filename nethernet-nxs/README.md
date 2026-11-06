# netty-nethernet-nxs

Support for NetherNet External Signaling (NXS) on top of
[`transport-nethernet`](../transport-nethernet/README.md). With NXS, a NetherNet
host registers with a signaling provider its operator chose. The provider issues
players short-lived admission tokens and routes them to the host; the host checks
each token itself before it creates a peer. Accounts, token issuance and fleet
administration belong to the provider.

The module is optional. A host without a provider does not need it, and
`transport-nethernet` does not depend on it.

## What it implements

The wire formats follow NXS v1 byte for byte, as the vendored specification and
its fixtures define them. The Java API is NetworkM's own.

- **Provider client** (`ProviderClient`): registration and recovery with a P-384
  machine key, signed heartbeats with the host profile and admission keys,
  outcome reports, key rotation, deregistration, operator actions, protocol
  extensions, and assisted joins over the provider's WebSocket (`control`).
  Everything runs on one lifecycle thread per host without blocking it; the
  registration is saved in a `ProviderStateStore` directory. The client talks to
  the NetherNet endpoint through the `transport.ProviderTransport` interface.
- **Stateless admission** (`admission`): `StatelessAdmissionCodec` reads and
  issues the NXS1 token a player carries in its ICE username,
  `StatelessAdmissionValidator` checks it against the installed admission keys
  with no per-join state, and `AdmissionGate` bounds reservations and keeps the
  records of used tickets against replay.
- **Connectivity** (`connectivity`): `EndpointConnectivityController` keeps STUN
  bindings alive from the gameplay port to learn its public mapping, and
  `MaintainedCandidatePublisher` decides which candidates the host profile
  publishes.
- **Diagnostics** (`diagnostic`): `DiagnosticAdmissionCodec` for the diagnostic
  extension of the admission envelope (network ID 0), and the PING/PONG exchange
  on an admitted connection.

## What is not built yet

The host side on the endpoint's shared ICE port: no class in NetworkM implements
`ProviderTransport` or the connectivity `StunMonitor` against
`NetherNetServerChannel` yet, and nothing checks NXS1 tokens on the shared ICE
port before a peer exists, although the engine already passes such checks on
(`IceEndpoint.onHeldCheck`). Until that lands, the module cannot serve NXS players
on a real endpoint; its tests run it against fakes.

## Tests

`./gradlew :nethernet-nxs:test` also runs the specification's Node.js verifiers
against this module's output (`NodeConformanceTest`). The test fails without
Node.js. `-PnxsNode=<path>` selects the binary; otherwise `node` on the path is
used.

## Specification

The NXS v1 specification, schemas, fixtures and Node verifiers are vendored
unchanged from CloudburstMC/Network (`nethernet` branch, commit `14cede4`) in
[docs/external-signaling](docs/external-signaling/README.md). The fixture hashes
pin their bytes, so the directory is stored without line ending conversion.
Problems with the specification go upstream rather than into a local deviation.
