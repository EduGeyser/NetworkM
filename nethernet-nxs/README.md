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
- **Host side** (`host`): `NxsHost.open` adds NXS to a bound
  `NetherNetServerChannel` as one of its join sources, and its `HostTransport`
  is the `ProviderTransport` the client drives. It takes the checks the shared
  ICE port holds for unknown ufrags, validates each NXS1 token once and turns a
  player's into an authenticated join, which passes the endpoint's throttle and
  capacity budget like every other join. It serves assisted joins, publishes the
  host profile (configured, local, or STUN-maintained candidates) and reports
  ticket outcomes. Every NXS child carries a login binding; the application
  decides it with `TransportIdentityBinding.verifyLogin` before the ticket's
  deadline, or the child is closed. A child's connection ID is its ticket ID.
  Closing the client closes the transport, which removes itself from the
  endpoint: players whose login was accepted stay connected, and every other
  NXS child is closed.
- **DTLS certificate** (`DtlsCertificate`, in transport-nethernet): every NXS
  peer presents one fixed certificate, since stateless clients build the host's
  answer from the fingerprint the profile publishes. `NxsHost.open` takes it.
  `DtlsCertificate.loadOrCreate(path)` creates an ECDSA P-256 certificate signed
  with SHA-256 on the first start, owner-only, and keeps it; a good place is
  `dtls-certificate.pem` next to the server identity key. `DtlsCertificate.load`
  and `DtlsCertificate.of` take a supplied PEM certificate and key. Keep one
  certificate per endpoint: clients that hold the published profile expect it.
- **Stateless admission** (`admission`): `StatelessAdmissionCodec` reads and
  issues the NXS1 token a player carries in its ICE username,
  `StatelessAdmissionValidator` checks it against the installed admission keys
  with no per-join state, and `TicketLedger` keeps the records of used tickets
  and diagnostic attempts against replay. Capacity is the endpoint's.
- **Connectivity** (`connectivity`): `EndpointConnectivityController` keeps STUN
  bindings alive from the gameplay port to learn its public mapping, and
  `MaintainedCandidatePublisher` decides which candidates the host profile
  publishes.
- **Diagnostics** (`diagnostic`, `host`): connectivity checks with network ID 0
  (diagnostic-v1). The host answers a check's PING on a peer of its own from the
  shared port (`SharedIcePort.openPeer`): no child channel and no session, one of
  the endpoint's pending handshakes while the peer lives, at most four checks at
  once, each closed at most a second after its PONG. Direct checks arrive as held
  checks, assisted ones over the provider's WebSocket, and only under the policy
  the last successful heartbeat installed; a new policy replaces the old one and
  stops the checks it no longer covers. `HostTransport.pollDiagnosticResults`
  reports how each ended, oldest first, from a queue of at most 32; results stay
  local. `DiagnosticProbe` is the prober's side, for a provider that runs its
  checks in Java: it offers both data channels from a fixed local endpoint,
  sends one PING and checks the echo on the selected path. It probes public
  destinations only, private and loopback ones too under local development.
- **Settings** (`provider`): `ProviderRuntimeConfiguration` maps a host's NXS
  settings to the client and host configuration. A secret is the value itself or
  `file:<path>`, a path relative to the configuration directory unless absolute;
  a value without the prefix that names an existing file or directory, or starts
  with a Windows drive, is refused rather than sent as the secret.

## Behind PROXY protocol relays

When UDP relays front the gameplay port and the endpoint reads their PROXY
protocol v2 headers (the PROXY protocol list of `NETHER_SERVER_TRUSTED_PROXIES`),
build the settings with `relayed(true)`. The host profile then publishes only
`advertiseAddresses`, which must name the relays: capturing the profile fails
when there are none or one is this host's own gameplay endpoint. STUN discovery
and assisted joins stay off whatever their setting, since both reach around the
relays. An NXS child's `signaledAddress()` (`AddressSource.NXS_ADMISSION`) is the
client address the relay's header declared for the admitted check. From
activation on, `remoteAddress()` is the remote side of the selected pair as the
relay declared it (`AddressSource.ICE_PROXY_PROTOCOL`), or the TURN relay for a
TURN pair, and the outcome report carries that address.

## Tests

`./gradlew :nethernet-nxs:test` also runs the specification's Node.js verifiers
against this module's output (`NodeConformanceTest`). The test fails without
Node.js. `-PnxsNode=<path>` selects the binary; otherwise `node` on the path is
used.

With `-PwebrtcNativePlatform=<platform>` the tests also run stateless joins and
connectivity checks on real libwebrtc over this machine's network addresses
(libwebrtc does not gather on loopback).

## Specification

The NXS v1 specification, schemas, fixtures and Node verifiers are vendored
unchanged from CloudburstMC/Network (`nethernet` branch, commit `14cede4`) in
[docs/external-signaling](docs/external-signaling/README.md). The fixture hashes
pin their bytes, so the directory is stored without line ending conversion.
Problems with the specification go upstream rather than into a local deviation.
