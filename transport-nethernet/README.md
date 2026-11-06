# netty-transport-nethernet

A NetherNet transport for Netty: Bedrock's WebRTC data channel transport, with a
server endpoint that takes joins from HTTP, Xbox RPC and LAN signaling, and a
client channel. In NetworkM this module targets Java 25 and runs on libwebrtc
through the [slim webrtc-java fork](https://github.com/EduGeyser/webrtc-java),
with Maven coordinates `io.github.sendablemetatype.webrtc:webrtc-java` and Java
packages under `io.github.sendablemetatype.webrtc`. Consumers also need matching
platform native libraries.

The module logs through SLF4J 2.0: the identity checks (through jose4j) and
libwebrtc's native log both write to it. Applications that use SLF4J logging must
provide a backend compatible with the 2.0 API. NetworkM leaves backend selection
to the application; its tests use the SLF4J bridge to Java logging.

What real retail and Education clients send and expect, and how vanilla BDS
answers, is recorded per version in [docs/client-behavior.md](docs/client-behavior.md).
Several choices below rest on it.

## Usage

> [!IMPORTANT]
> This library requires the platform-specific WebRTC native libraries at runtime. See the [WebRTC usage guide](https://github.com/EduGeyser/webrtc-java#usage) for instructions on how to include the native libraries in your project.

### Engine

Channels never touch libwebrtc directly. They run on a `NetherNetEngine`
(package `engine`), which creates peers and the shared ICE port. The default
engine is `WebRtcEngine` (package `engine.webrtc`), built with
`WebRtcEngine.builder()`:

| Builder method | Default | What it sets |
| --- | --- | --- |
| `factories(int)` | 1 | How many webrtc-java factories the engine pools. Each has its own network, signaling and control threads; peers go to them in turn. |
| `portAllocatorFlags(int)` | `DEFAULT_PORT_ALLOCATOR_FLAGS` | The `PortAllocatorConfig.PORTALLOCATOR_*` flags of every peer. TCP stays disabled. |
| `fieldTrials(Map)` | none | WebRTC field trials for every factory. |
| `nativeLogThreshold(Severity)` | `WARNING` | The lowest native severity that reaches SLF4J, see [Native log](#native-log). |

No native resources exist until the first peer or endpoint needs them.

An engine has exactly one owner, which closes it. Every channel constructor and
every `NetherNetChannelFactory` method makes the ownership explicit:

- no engine argument: the channel creates the default engine when it binds (or,
  for a client, at its first attempt) and closes it with itself;
- a `NetherNetEngine`: the channel uses it and never closes it, so one engine can
  serve several channels; its owner closes it after the last one;
- a `Supplier<? extends NetherNetEngine>`: the channel creates its own engine
  from the supplier and closes it with itself.

The `engine` package is the whole boundary: no webrtc-java type appears in the
channel, signaling or option APIs, and engine tuning lives only on the engine's
builder. Another engine implements `NetherNetEngine`, `Peer`, `DataLink` and
`IceEndpoint`; the package documentation states the threading, ordering and
teardown contract it must keep.

### Server endpoint

`NetherNetServerChannel` is one NetherNet endpoint. It takes joins from any number
of join sources, admits each through one admission stage, and emits a
`NetherNetChildChannel` per admitted join, like any Netty server channel.

```java
ServerIdentity identity = ServerIdentity.loadOrCreate(Path.of("nethernet-identity.pem"), "My server");

Channel endpoint = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channelFactory(NetherNetChannelFactory.server())
        .option(NetherChannelOption.NETHER_SERVER_IDENTITY, identity)
        .childHandler(new MyInitializer())
        .bind(new InetSocketAddress(19133))
        .sync().channel();

NetherNetHttpSignaling http = NetherNetHttpSignaling.builder(workerGroup).build();
http.bind(new InetSocketAddress(19132)); // TCP; not from the worker group's loops
((NetherNetServerChannel) endpoint).addSource(http);
```

**Identity.** `NETHER_SERVER_IDENTITY` is required; binding fails without one.
One `ServerIdentity` signs the answers of every source. Plain HTTP clients pin
its key and ask the player once to trust it, so a new key prompts every
returning player again
([client behavior](docs/client-behavior.md#what-the-client-requires-of-the-answer)).
Choose one explicitly:

- `ServerIdentity.loadOrCreate(keyFile, domain)` loads a PEM key (SEC1 or
  PKCS#8, with or without a `PUBLIC KEY` block) or creates a secp384r1 key
  there. The file is created owner-only and never replaced once it exists.
- `ServerIdentity.ephemeral(domain)` keeps the key in memory only.

The endpoint logs the key's SHA-256 fingerprint when it binds;
`identity.fingerprint()` returns it for publishing.

**Sources.** `addSource(source)` and `addSource(source, policy)` add a join
source before or after bind. A source starts once the endpoint is bound and its
ICE port is open. `removeSource(source)` stops and closes one; joins it delivered
that have no child yet are refused. Neither touches connected players, since
every join replies through its own `ReplyPath`. A source starts once: to restart
one, remove it and add a new instance. Closing the endpoint closes its sources.
`sources()` lists them.

**ICE port.** By default every peer gathers its host candidates on one UDP port,
the address and port the endpoint binds (`NETHER_SERVER_SHARED_ICE_PORT`, true).
Port 0 picks a free port. A taken port fails the bind. The engine opens the port
after bind returned; a port that fails to open then closes the endpoint. Between
bind and the port opening, joins are refused with `CAPACITY`. Set
`NETHER_SERVER_SHARED_ICE_PORT` to false to give each session ports of its own
from `NETHER_CANDIDATE_POLICY`'s port window, the way BDS allocates them
([client behavior](docs/client-behavior.md#how-vanilla-bds-answers-for-comparison)).
The ICE port is UDP, so it can share a number with HTTP signaling on TCP, but
not with RakNet on UDP.

**Advertised addresses.** `NETHER_SERVER_ADVERTISED_ADDRESSES` lists addresses
answers announce. Read them from configuration with
`AdvertisedAddresses.parse(values, defaultPort)`: IP literals or host names, port
0 for the media port, at most `AdvertisedAddresses.MAX_ENDPOINTS` (32). Names
are looked up once, off the event loop, when the endpoint binds; until they
resolve, answers announce the literals only, and a name that does not resolve
closes the endpoint. An address the host gathered narrows the host candidates to
it; any other is announced as the public side of a NAT. Empty by default: answers
carry what ICE gathered plus the address in the client's `Host` header.

**Candidate options.**

| Option | Default | Meaning |
| --- | --- | --- |
| `NETHER_SERVER_INFER_PEER_CANDIDATES` | true | Checks guessed candidates at the address a client signaled from, when it offers only host candidates and this host gathered no public one. |
| `NETHER_ICE_SERVERS` | none | STUN and TURN servers every peer uses, next to those a source hands out (Xbox TURN). |
| `NETHER_CANDIDATE_POLICY` | `CandidatePolicy.DEFAULT` | IPv6, local addresses and the port window for gathering. |

**Trusted proxies.** `NETHER_SERVER_TRUSTED_PROXIES` takes a `TrustedProxies`
(transport-common, `util.address`): one list of peers whose PROXY protocol
header is read, one of peers whose `X-Forwarded-For` is read. Parse each list
once at startup with `TrustedProxies.parseList`, off any I/O thread: entries are
addresses, CIDR ranges, host names or `https://` URLs listing more entries;
`http://` URLs are refused. On the endpoint, the PROXY protocol list names the
relays in front of the shared ICE port: the port reads their PROXY protocol v2
headers, answers announce only the relays and TURN, and no remote candidate is
applied. This needs the shared ICE port. HTTP signaling reads its own list
(`NetherNetHttpSignaling.Builder.trustedProxies`); pass it the same parsed value.
With `NETHER_SERVER_PREFER_PROXIED_ADDRESS` (false by default), a child keeps
reporting the client address a trusted signaling proxy declared instead of the
selected ICE pair; a TURN pair is reported as the relay either way.

### Admission

Every join, whatever its source, passes the same stage on the endpoint's event
loop:

1. The per-address throttle (`NETHER_SERVER_THROTTLE`). The default
   `DefaultNetherServerThrottle` allows 100 open connections and 30 joins per
   4 s per address, IPv6 counted per /64; a school shares one NAT address. Joins
   from an unknown address (Xbox RPC) are not throttled.
2. One budget shared by all sources: `NETHER_SERVER_MAX_SESSIONS` (unlimited by
   default) and `NETHER_SERVER_MAX_PENDING_JOINS` (256), the joins that have not
   gone active yet.
3. The offer's identity check (`NETHER_SERVER_OFFER_CHECK`, `OfferCheck.minecraft()`
   by default), off the event loop on a bounded executor, with a deadline
   (`NETHER_SERVER_OFFER_CHECK_TIMEOUT_MILLIS`, 10,000). The endpoint fetches the
   trusted issuers' keys when it binds, so the first join does not wait for them.
4. The source's `OfferPolicy` decides on the check's result.

A join holds its share of the budget and its throttle entry until its child's
`termination()` stage completed, not merely until the child closed. A child that
does not go active within `NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS` (30) is
closed.

**Identity check levels.** `OfferCheck` grades the offer's `a=identity` in three
steps and returns one `OfferCheckResult`:

| Level | Meaning |
| --- | --- |
| `NO_ASSERTION` | The offer has no `a=identity`. |
| `MALFORMED` | The assertion, its token or its key cannot be read. |
| `PROOF_FAILED` | The fingerprint signature does not verify under the token's `cpk`. |
| `KEY_PROVEN` | The client holds the token's key and signed this offer's fingerprints. The token is not attested; `reason()` says why. |
| `ATTESTED` | As `KEY_PROVEN`, and a trusted issuer signed the token. |

The `idp` block is not checked. Proving the key needs no network. Attesting
picks the `TrustedIssuer` by the token's exact `iss` before any key lookup, so a
forged token cannot make the server fetch anything. `OfferCheck.minecraft()`
attests retail tokens against Minecraft's authorization service. For other
issuers, build `new OfferCheck(issuers)` with `TrustedIssuer.jwks(...)` or
`TrustedIssuer.fixedKey(...)`.

**Policies.** `OfferPolicy.REQUIRE_KEY_PROVEN` is the default for every source:
it admits `KEY_PROVEN` and `ATTESTED`. Education clients send a self-signed
token that no issuer attests and pass on their proof
([client behavior](docs/client-behavior.md#the-clients-offer)).
`OfferPolicy.REQUIRE_ATTESTED` admits only `ATTESTED`, which refuses every
Education client and every join while the issuer's keys are unreachable. A
custom `OfferPolicy` returns an `IdentityRefusal` to refuse; it runs on the event
loop and must not block. Each refusal is logged at info level with the join's
connection ID, address and the check's diagnostics; logs never contain the token.

The admitted child carries the result in `child.getOfferCheck()` (attribute
`NetherNetChildChannel.OFFER_CHECK`). `xuid()` and `displayName()` read the `xid`
and `xname` claims; they are only as trustworthy as `attested()` says. The child
also carries `NetherNetChildChannel.CONNECTION_ID`, the client's connection ID.

**Refusals.** A refused join creates nothing. Its source tells the client with a
`JoinRefusal`: an HTTP status with an empty body over HTTP, a `CONNECTERROR`
signal with a NetherNet error code over Xbox RPC and LAN. Which message each makes
the client show is not captured yet. Upstream Network's record of the reasons
clients pick for 401, 403 and 503, and where each status here fits it, is in
[docs/client-disconnect-reasons.md](docs/client-disconnect-reasons.md).

| `JoinRefusal` | HTTP | Error code | When |
| --- | --- | --- | --- |
| `IDENTITY_MISSING` | 401 | 16 | The policy refused an offer without an assertion. |
| `IDENTITY_INVALID` | 401 | 16 | Malformed assertion, failed proof, or not attested where the policy requires it. |
| `HOST_REFUSED` | 403 | 16 | Defined for host refusals; nothing in the library produces it yet. |
| `CAPACITY` | 503 | 16 | Throttle, budget, full check queue, endpoint not ready, closing, or the source was removed. |
| `TRUST_SOURCE_UNREACHABLE` | 500 | 35 | Attestation was required and the issuer's keys could not be fetched. |
| `NO_ANSWER_IN_TIME` | 504 | 2 | The identity check, or the answer, missed its deadline. |
| `INTERNAL` | 500 | 11 | An unusable offer, or a failure on the server while answering. |

### Login key binding

NetherNet runs over DTLS and skips the RakNet encryption handshake, so nothing
ties the Bedrock Login chain to the transport by itself. A Login captured from
another player could be replayed over a new connection. Every child carries a
`TransportIdentityBinding` in `TransportIdentityBinding.ATTRIBUTE`, bound to the
key the offer proved, or unbound when it proved none. Real clients sign the Login
with the offer's `cpk`, retail and Education alike
([client behavior](docs/client-behavior.md#the-clients-offer)).

Applications own Login authentication; NetworkM does not parse Login packets.
Once the chain is verified, decide it against the transport, once per Login:

```java
switch (TransportIdentityBinding.verifyLogin(channel, loginChainKey)) {
    case ACCEPTED -> { } // the login comes from the client that opened this transport
    case NOT_NETHERNET -> { } // not NetherNet: that transport's own authentication applies
    case MISMATCH, UNBOUND, SPENT -> channel.close();
}
```

The check finds the binding by attribute, so wrapper channels that delegate
attributes work. A binding is spent by its first decision. An application that
admits a login through its own trusted forwarding path calls
`TransportIdentityBinding.acceptForwardedIdentity(channel)` instead.
`TransportIdentityBinding.state(channel)` reports where a binding stands without
spending it.

### HTTP signaling

`NetherNetHttpSignaling` is the direct connection path of Mojang's onboarding
guide: the client asks `GET /v1/join` before anything else, then sends its offer
as `POST /v1/join/{networkId}` and gets the full answer back. Build it with
`NetherNetHttpSignaling.builder(workerGroup)`, where the group takes NIO channels
and is not owned. `bind(address)` returns once the listener listens; call it
outside the worker group's loops. Then add it to an endpoint. A closed instance
cannot be bound again.

| Builder method | Default | What it sets |
| --- | --- | --- |
| `tls(TlsContextSelector)` | none | TLS next to plain HTTP on the same port. |
| `requiresTls(boolean)` | false | Refuses plain HTTP with `426`, also while no certificate is available. |
| `trustedProxies(TrustedProxies)` | `NONE` | The proxies allowed to declare a client's address. |
| `statusProvider(StatusProvider)` | none | What `GET /v1/join` answers with. Without one, an empty `200`. |
| `statusTimeout(Duration)` | 1 s | How long a status request waits for the provider. |
| `negotiationTimeout(Duration)` | 15 s | How long an offer may wait for admission and its answer, from the end of its request. Past it, `504`. |
| `maxConnections(int)` | 1024 | Open signaling connections. |
| `maxConnectionsPerAddress(int)` | 100 | Open connections per client address (IPv6 per /64, proxied clients by their declared source). |
| `maxRequestBodyBytes(int)` | 32 KiB | The largest request body; larger ones get `413`. |

**TLS.** Each connection's first bytes say whether it is TLS. On a TLS
ClientHello the `TlsContextSelector` is asked with the requested server name
(SNI), or null when the client sent none, as clients do for an IP address. It
returns the `SslContext` to serve, or null to withdraw TLS for that connection,
which closes it. Clients read that as TLS refused and go on with plain HTTP and
the trust prompt, whereas a certificate they reject may end the join
([client behavior](docs/client-behavior.md#what-the-client-requires-of-the-answer)).
Withdraw TLS rather than serve a certificate the client would reject, such as an
IP address certificate to a player who connected by domain name. A new
certificate applies from the next connection. `TlsContexts.fromPem(...)` and
`TlsContexts.fromPkcs12(...)` load contexts from files.

```java
SslContext ipCertificate = TlsContexts.fromPem(chain, key, null);
NetherNetHttpSignaling http = NetherNetHttpSignaling.builder(workerGroup)
        .tls(serverName -> serverName == null ? ipCertificate : null)
        .statusProvider(request -> StatusAnswer.serve(currentStatus))
        .build();
```

With `requiresTls`, plain HTTP is always refused, which turns a client whose TLS
attempt failed into no NetherNet join at all.

**Status.** The `StatusProvider` answers each `GET /v1/join` with
`StatusAnswer.serve(status)` or `StatusAnswer.decline()` (`404`, which a client
that still has RakNet reads as "join over RakNet"). It runs off the I/O threads,
at most 64 calls at once. A call that throws, misses the status deadline or finds
no free slot gets the last document served for the same `Host`, or an empty
`200`, never an error. The endpoint is open to anyone, so keep each call cheap,
for example by serving a cached `NetherNetServerStatus`. The `StatusRequest`
carries the `Host` header, the source address, the client's self-reported
`ClientInfo` and the `id` query parameter; all but the source come from the
client unchecked. `GET` is also answered `404` whenever an offer would certainly
be refused: before `start`, after `close`, or when the endpoint is at capacity or
throttles the address.

`NetherNetServerStatus.builder()` builds the status document. Every member is
optional; unset members are omitted, and the seven vanilla sends reproduce its
response byte for byte. Use one nonce per endpoint for HTTP and LAN alike,
for example `NetherNetServerStatus.randomNonce()`.

**Connections.** A connection carries one request, or a status request and one
more request started within 5 s; each request has 10 s to arrive whole, and the
last response says `Connection: close`. A client that leaves while its offer is
pending abandons the join, and the endpoint closes the child unless it already
went active. Other statuses besides the refusals: `400` for a malformed request
or network ID, `404` for other paths, `503` for an offer before `start` or after
`close`.

**Proxies.** A PROXY protocol header (v1 or v2) from a trusted PROXY protocol peer
wins over `X-Forwarded-For` from a trusted forwarder; nobody else can declare a
source. `X-Forwarded-For` is walked right to left without DNS.

### Xbox RPC signaling

`NetherNetXboxRpcSignaling` takes joins over Xbox Live's JSON-RPC signaling
WebSocket: joins by friends and by connection ID. It keeps its socket up by
itself. Starting it never waits for Xbox, so an endpoint binds and serves other
sources while Xbox is unreachable. The consumer provides a `TokenSupplier`, asked
before every connection attempt; `refresh` is true after the service refused the
previous token (401 or 403 on the upgrade), and the supplier must not return a
cached one then:

```java
NetherNetXboxRpcSignaling xbox = new NetherNetXboxRpcSignaling(networkId,
        refresh -> tokens.mcToken(refresh)); // CompletionStage<String>, "MCToken ..."
xbox.addHealthListener(health -> log.info("Xbox signaling: {}", health));
endpoint.addSource(xbox);
```

The network ID is kept across reconnects, so join codes and connection IDs stay
valid; `new NetherNetXboxRpcSignaling(tokens)` picks a random one.

Failed attempts are retried with backoff (2 s doubling to 2 min, with jitter). A
lost socket is replaced in place: the network ID, routing and connected players
stay. A socket counts as lost when the service closes it, it fails, no frame
arrives for 45 s, two `System_Ping` requests in a row go unanswered, or the
self-addressed route probe shows that joins no longer reach it. `health()`
returns a `SignalingHealth`: the `SignalingState` (`NEW`, `STARTING`,
`CONNECTED`, `RECONNECTING`, `CLOSED`), the failed attempts in a row, the last
failure and when the next attempt starts. `reconnect()` replaces the socket on
request.

TURN credentials come with each socket and are refreshed before they expire;
`iceServers()` returns the current ones, which every admitted Xbox join uses.
Xbox signaling does not say where a client is, so a child's address is a
placeholder until ICE selects a pair. Only the JSON-RPC service is supported;
the older `signal.franchise` WebSocket is gone from current clients
([client behavior](docs/client-behavior.md#signaling-and-fallback)).

### LAN discovery

`NetherNetDiscoverySignaling` answers LAN discovery requests and takes offers
sent as discovery datagrams. For a server, bind it to the discovery port:

```java
NetherNetDiscoverySignaling lan = new NetherNetDiscoverySignaling(networkId,
        new InetSocketAddress(NetherNetConstants.DISCOVERY_PORT)); // UDP 7551
lan.setServerData(new LanServerData.V7("My server", protocol, "1.26.60",
        "My world", playerCount, maxPlayers, gameType, false, false,
        true, false, sharedNonce, 4));
endpoint.addSource(lan);
```

The source binds in the background when it starts and reports `CONNECTED` once
it listens, or `CLOSED` with the failure. Replies go to the address the offer
came from, which is then pinned: the join takes later signals only from that
address and the client's network ID. Anyone on the network can forge a datagram's
source, so joins carry it as `AddressSource.SIGNALING_LAN`.

Discovery responses use binary server data v7, the layout BDS 1.26.50 and later
send. `LanServerData.V7` is the LAN record, separate from the HTTP status
document; the two need not match. The authentication flags describe the
endpoint's admission policy; they enable nothing. `setServerData` encodes the
record once and caches it; a record that does not fit one datagram throws
`IllegalArgumentException` and the previous one stays. `NetherNetServerDataCodec`
and the scanner read v6 and v7. The v7 layout is not yet confirmed against a
current retail capture.

### Network IDs

HTTP and Xbox signaling treat peer network IDs as opaque strings. HTTP callers
must encode the ID as one UTF-8 URL path segment in `/v1/join/{networkId}`.
The listener decodes percent escapes once and preserves literal `+` characters,
leading zeros and numeric-looking IDs outside the uint64 range. Missing IDs,
malformed percent escapes, invalid UTF-8 and unescaped path delimiters receive
HTTP 400.

Generated IDs are decimal strings. LAN discovery uses uint64 IDs in its binary
wire format. The numeric `NetherNetAddress` constructor and
`getNetworkIdAsLong()` remain available for that representation; use
`getNetworkId()` when handling opaque IDs.

### Channels

Server children and client channels are `NetherNetChannel`s and implement
transport-common's `TransportChannel`. Each carries a reliable and an unreliable
data channel, and each message is carried whole: the channel splits and
reassembles segments itself. No framing codec goes in the pipeline.

**Writing.** A plain `ByteBuf` goes on the reliable channel. Sending unreliably is
opt-in: write a `NetherNetPacket(content, false)`. An unreliable message must fit
one segment, as Bedrock never splits those; a larger one fails its write. A
reliable message takes at most 256 segments; `maxMessageSize()` returns the
largest message a write may carry, and a larger write fails before anything is
sent. An empty write sends nothing. A write completes once the engine accepted
every segment; that does not acknowledge receipt by the peer. A rejected send or
a failure preparing one ends the connection with `TRANSPORT_FAILED` and fails the
pending writes, since resending one segment after later ones went out would
corrupt the reliable stream.

**Reading.** A reliable message arrives as a plain `ByteBuf`, an unreliable one as
a `NetherNetPacket` whose `reliable()` is false. The two data channels have no
ordering between them. Vanilla sends game data only on the reliable channel
([client behavior](docs/client-behavior.md#data-channels-and-framing)).
`AUTO_READ` and `read()` are honored: messages wait in a queue of at most 512
messages and twice `NETHER_MAX_REASSEMBLED_BYTES` bytes, and `read()` delivers one
message. With auto-read on, queued messages are delivered in batches of at most
64. The engine cannot pause reception, so an overflowing queue ends the
connection with `QUEUE_TOO_LONG` instead of growing.

**Broken framing.** As on vanilla, a reliable segment of 1 byte or less, or a
countdown header that does not follow the previous segment's, ends the connection
with `BAD_PACKET`. A message larger than `NETHER_MAX_REASSEMBLED_BYTES` (16 MiB)
ends it with `SPLIT_QUEUE_TOO_LONG`. Nothing that arrives after the transport
ended the connection is delivered.

**Outgoing segment size.** The remote SDP's SCTP media sections determine the
outgoing segment size, including the one-byte NetherNet header. An absent
`a=max-message-size` uses 65,536 bytes, as specified by RFC 8841. Zero means
unlimited. A positive limit is respected, with a local ceiling of 262,144 bytes;
the ceiling is local policy, not a NetherNet protocol limit. Malformed attributes
and a one-byte limit fail negotiation. Attributes outside active SCTP application
sections do not count; if several apply, the smallest is used.
`getMaxOutboundMessageSize()` returns the negotiated segment size.

**Pacing and caps.** While a data channel's engine queue holds more than
`NETHER_SEND_PAUSE_BYTES` (2 MiB), the channel stops handing segments to the
engine and turns unwritable; it resumes once the queue drained to a quarter of
that. libwebrtc closes a data channel past 16 MiB queued, so segments wait in
Java instead. Past `NETHER_MAX_QUEUED_BYTES` (64 MiB) waiting in Java, the
connection ends with `QUEUE_TOO_LONG`.

**Disconnect reasons.** When the transport ends a connection, the channel fires
one `TransportDisconnectReason` as a user event on its event loop just before it
closes, and only if `channelActive` fired. Closing from the application fires
nothing. These are the transport's own reasons; the number a client shows on its
disconnect screen is a Bedrock `DisconnectFailReason`, decoded in
[docs/client-disconnect-reasons.md](docs/client-disconnect-reasons.md).

| Reason | Cause |
| --- | --- |
| `CLOSED_BY_REMOTE_PEER` | The reliable data channel closed. |
| `ABORTED_BY_REMOTE_PEER` | The peer aborted SCTP. |
| `CONNECTION_LOST` | ICE failed after the connection was up. |
| `TRANSPORT_FAILED` | DTLS or the transport failed, or a send was rejected. |
| `QUEUE_TOO_LONG` | The inbound queue or the outbound backlog passed its cap. |
| `SPLIT_QUEUE_TOO_LONG` | A message passed `NETHER_MAX_REASSEMBLED_BYTES`. |
| `BAD_PACKET` | Broken framing on the reliable channel. |

`termination()` completes once the channel closed and its peer's teardown
finished.

**Address and ping.** `remoteAddress()` is the address signaling saw, or a unique
placeholder in 240.0.0.0/4 when it saw none (`PlaceholderAddress`), until ICE
selects a pair; from then on it is the remote side of the newest selected pair,
which for a relayed connection is the TURN relay. A server child moves to the
pair when it goes active, and fires a `RemoteAddressChangedEvent` for every later
move. `NetherNetChildChannel.reportedAddress()` returns the address with its
`AddressSource`. `getPing()` returns the ICE round trip time in milliseconds,
sampled every 3 s, or 0 while unknown.

**Metrics.** `NETHER_METRICS` takes a `NetherChannelMetrics` for per-connection
counters and events; `NETHER_SERVER_METRICS` a `NetherServerMetrics` for the
endpoint's accepted, refused and throttled joins. Both are off by default.

### Client

`NetherNetClientChannel` connects over one `NetherNetClientSignaling`, which it
closes with itself; give each client channel its own signaling instance.

| Signaling | Connect to | Notes |
| --- | --- | --- |
| `NetherNetHttpClientSignaling(settings, group)` | `InetSocketAddress` of the server's HTTP signaling | One instance per client channel. |
| `NetherNetXboxRpcSignaling(tokens)` | `NetherNetAddress` of the server's network ID | |
| `NetherNetDiscoverySignaling()` | `InetSocketAddress` of the server's discovery port | Discovers the server within 10 s. |

```java
NetherNetHttpClientSignaling signaling = new NetherNetHttpClientSignaling(HttpClientSettings.DEFAULT, group);
Channel channel = new Bootstrap()
        .group(group)
        .channelFactory(() -> new NetherNetClientChannel(signaling))
        .option(NetherChannelOption.NETHER_CLIENT_IDENTITY, ClientIdentity.of(keyPair, token, idpDomain))
        .handler(new MyInitializer())
        .connect(InetSocketAddress.createUnresolved("play.example.com", 19132))
        .sync().channel();
```

**Identity.** Vanilla BDS and this library's endpoints refuse an offer without an
identity. `NETHER_CLIENT_IDENTITY` takes a `ClientIdentity` built with
`ClientIdentity.of(keyPair, token, idpDomain)`: a secp384r1 key pair and a token
whose `cpk` names it, either a retail multiplayer token or one the caller signs
itself, as Education does. The client signs its offer's fingerprints with the key.
Tokens expire, so set a fresh identity per connection.

**HTTP.** HTTP carries one offer and one answer, so the client waits for ICE
gathering and sends its offer with every candidate in it. `HttpClientSettings`
picks the scheme: `AUTO` (the default) asks over HTTPS first and falls back to
plain HTTP only when the server refused TLS; a certificate the client rejects
ends the attempt, as on the retail client. `HTTPS` and `HTTP` use one scheme only.
The settings also take STUN and TURN servers, the trust store for the server's
certificate, the `ClientInfo` to report, and the answer wait (20 s, longer than
the server's default negotiation deadline). Requests are HTTP/1.1 and follow no
redirects. `NetherNetHttpClientSignaling.probe(address, settings, group, networkId)`
asks for the status alone.

**Handshake.** An attempt that does not open both data channels within
`NETHER_CLIENT_HANDSHAKE_TIMEOUT_MS` (3,000) is retried with a new peer and
connection ID, up to `NETHER_CLIENT_MAX_HANDSHAKE_ATTEMPTS` (3). Over HTTP each
attempt also gets the signaling's answer wait. A refusal, a `2xx` without an SDP,
a missed answer deadline or a signaling transport failure ends the connect
without a retry. Once connected, a failing peer ends the connection as on the
server.

### Native log

The engine routes libwebrtc's native log into SLF4J under the logger
`io.github.sendablemetatype.netty.nethernet.webrtc.native`: errors log as ERROR,
warnings as WARN, INFO as DEBUG and VERBOSE as TRACE. Which of them are written
stays with the host's logging configuration. `nativeLogThreshold` on the engine
builder sets the lowest severity forwarded at all, `WARNING` by default.

webrtc-java fixes its own threshold once, when it loads its native library, from
the system property `io.github.sendablemetatype.webrtc.logging.severity`. If the
host has not set that property, the engine sets it before it first uses
webrtc-java. A lower threshold asked for later, or by another engine after the
library loaded, does not lower it. The native log is process wide, so all engines
share one sink.

### Native integration tests

The default test suite runs on a pure Java fake engine. To also run the engine
conformance suite and the end-to-end tests on real libwebrtc, select the native
classifier for the test JVM, for example:

```shell
./gradlew :transport-nethernet:test -PwebrtcNativePlatform=linux-x86_64
```

CI selects the native classifier for each runner and includes these tests in
the build and release checks.

### Upstream integration examples

These references were inherited from NetworkCompatible. They illustrate
upstream API usage; adapt their imports, dependencies and API calls for NetworkM.

- [Kas-tle/ProxyPass](https://github.com/Kas-tle/ProxyPass): Uses server and client to debug game packets over various connection types.
- [MCXboxBroadcast/Broadcaster](https://github.com/MCXboxBroadcast/Broadcaster): Uses server to allow Bedrock clients to transfer to other Bedrock servers via Xbox Live.
- [ViaVersion/ViaFabricPlus](https://github.com/ViaVersion/ViaFabricPlus): Uses client to connect to LAN games and Realms.
- [ViaVersion/ViaProxy](https://github.com/ViaVersion/ViaProxy): Uses client to connect to LAN games and Realms.

## Packet flow

### Server

1. A join source receives an offer and hands the endpoint a `Join` with its
   `ReplyPath`.
2. The endpoint admits the join (throttle, budget, identity check, policy) or
   refuses it through the reply path.
3. An admitted join becomes a `NetherNetChildChannel`, published at once. The
   child creates its engine peer, answers through the reply path (with the
   server identity added) and applies the client's candidates.
4. The child goes active when the client opened both data channels and the
   reliable one is open. From then on, segments from the engine are reassembled
   into whole messages for the pipeline, and pipeline writes are split into
   segments for the engine.

### Client

1. The client channel connects its signaling and creates an engine peer.
2. It sends the signed offer: at once with trickled candidates (Xbox RPC, LAN),
   or after gathering with every candidate in it (HTTP).
3. It applies the answer, goes active once both data channels are open, and from
   then on moves whole messages as the server child does.
