# How real clients behave

What retail Bedrock and Minecraft Education clients actually send and expect over
NetherNet, and how the vanilla server answers. Several design choices in this
transport rest on these facts, so each one says which client version it was seen
on and where it comes from. Clients change between versions: treat anything here
as true for the versions named, not forever.

Sources:

- **Captured**: logged from real clients against a test build, signatures checked
  offline. The 2026-11-05 captures cover retail 1.26.62 (Windows) over HTTP and
  over Xbox RPC (a friends join), and Education 26.33 over HTTP and over Xbox RPC
  (a join by connection ID).
- **Decompiled**: read in the shipped client and BDS binaries.
- **Measured**: observed on a live vanilla BDS.
- **Guide**: Mojang's NetherNet onboarding guide.
- **Reported**: seen by someone else and passed on; not checked here.

## Signaling and fallback

| Behavior | Versions | Source |
| --- | --- | --- |
| Before a NetherNet join, the client sends `GET /v1/join`. A `2xx` means the server supports NetherNet; any other status means it does not. | all with HTTP signaling | Guide |
| When that check fails, or TLS fails outright, a client that still supports RakNet joins over RakNet instead. This is the only point where it falls back. | Education 26.x; retail until RakNet was dropped | Owner observation |
| The client sends exactly one `POST /v1/join/{networkId}` per attempt. A non-`2xx` status ends the attempt: no retry, no RakNet fallback. A refused offer is a dead end for that player. | all with HTTP signaling | Guide |
| While the server list is open, the client polls `GET /v1/join?version=1.26.62&protocol=2225&platform=8&id=<network id>` about every 30 s. `id` is the client's NetherNet network ID for that run: it changes when the client restarts and equals the network ID of its later POST. `platform` appears to follow the Login `DeviceOS` numbering (8 for Windows). | retail 1.26.62 | Captured |
| At join, the client sends `GET /v1/join` with no query string, then the POST about 1 s later. Education does the same. | retail 1.26.62, Education 26.33 | Captured |
| Every request used a new TCP connection, with `User-Agent: libhttpclient/1.0.0.0` and `Host` set to what the player reached (a domain, or the IP a server listing returned). The server answered `Connection: close`, so whether the client reuses kept-alive connections is unknown. | retail 1.26.62, Education 26.33 | Captured |
| Retail clients always probe `GET /v1/join` before RakNet. A port with no TCP listener, or one that silently drops, adds about 6 s to every join; a fast `404` avoids that. | retail 26.30 and later | Reported (Bedrock Gophers, July 2026) |
| Clients wait 2 to 3 s before the GET and POST. | 2026-05 builds | Reported |
| Xbox signaling uses only the JSON-RPC protocol (Type 7). 26.20 did only Type 7 in rtm516's tests; any Type 3 support left would work differently than in 26.10. | 26.20 | Reported (rtm516, 2026-05-13) |
| The older WebSocket signaling (Type 3, `signal.franchise.minecraft-services.net`) is gone from retail since about May 2026. Education kept it only while it stayed on 1.21.133. | retail 26.20 and later, Education 26.x | Owner observation (2026-11) |
| Education reaches a server through its server listing (MESS), which returns an IP address, or by connection ID over Xbox RPC. It uses the same probe: NetherNet first, RakNet as fallback. | Education 26.x | Owner observation |
| Retail no longer joins over RakNet, probably since 26.60. | retail 26.60 and later | Owner observation (2026-11) |

## The client's offer

Every offer carries exactly one session-level `a=identity`, on HTTP and on Xbox
RPC alike. All offers use `a=setup:actpass`, `a=max-message-size:262144` and
`a=group:BUNDLE 0`. HTTP offers carry host candidates and
`a=ice-options:trickle`; the Xbox RPC offer carries none and trickles every
candidate (Captured).

The `a=identity` value is base64 JSON with an `idp` block and an `assertion`
string; the assertion holds a token and a detached JWS over the SDP's
`a=fingerprint` lines, signed with the key in the token's `cpk` claim (Guide).
Retail and Education fill it differently (Captured, 2026-11-05):

| | Retail 1.26.62 | Education 26.33 |
| --- | --- | --- |
| `idp` | `{"domain":"https://authorization.franchise.minecraft-services.net/","protocol":"default"}` | `{"domain":"","protocol":"default"}` |
| Token | RS256 with `kid`, signed by Minecraft's authorization service | ES384, **self-signed**: header `x5u` equals `cpk`. It is not valid under Minecraft's keys and never can be. |
| Claims | `iss`, `aud` (`api://auth-minecraft-services/multiplayer`), `sub`, `iat`, `exp` (4 h), `xid`, `xname`, PlayFab IDs | `aud` multiplayer, `exp` (about 1 year), `leguuid`, `mid`, empty `xid`, `xname`, `nid` and `pid`; no `iss`, `iat` or `sub` |
| `cpk` | base64 X.509 SPKI string | base64 X.509 SPKI string |
| Fingerprint proof | verifies | verifies |
| Token reuse | per session | the identical token on HTTP and Xbox RPC |

What follows from this:

- The guide describes the `idp` block, but its validation steps never check it,
  and it is not signed. Education sends an empty domain, and retail 1.26.50 did
  too (Reported, go-nethernet). Checking `idp` only rejects real clients.
- A check that requires a Minecraft-signed token refuses every Education client.
  Proving the key (the fingerprint signature) works for both.
- **The Login packet's identity key equals the offer's `cpk` on all four paths**:
  retail and Education, over HTTP and over Xbox RPC (Captured). Comparing the two
  at login is what stops a Login captured from another player from being replayed
  over a new connection, and it covers Education too.
- go-nethernet reported `cpk` as a JWK on some build; the captured clients send
  an SPKI string. Accepting both is the safe choice.

## What the client requires of the answer

- **`a=identity` on every answer.** Since 26.40, clients disconnect with "You're
  not invited to play on this server" when an answer lacks it, over Xbox RPC as
  well as HTTP. Before 26.40 only HTTP signaling required it (Reported by rtm516
  in Kas-tle/NetworkCompatible #12 and verified against a real 26.40 client,
  2026-08). The guide says the same: every answer, HTTPS or not.
- **Trust on first use over plain HTTP.** Since the 26.4x previews (July 2026)
  the client accepts plain HTTP signaling. It pins the server's identity key
  (`cpk` of the answer's token) and asks the player once to trust it. Over HTTPS
  with a valid certificate there is no prompt and no pin check. Rotating the
  identity key re-prompts every returning plain-HTTP player, so the key must be
  kept (Guide; Reported in July 2026 and checked by the owner against a local
  server).
- **TLS certificates.** From about 26.50, a certificate for an IP address is no
  longer accepted when the player connected by a domain that resolves to that IP.
  It still works when the player connects by IP. When TLS fails this way, the
  client goes on with plain HTTP rather than falling back to RakNet; with an
  outright invalid certificate, older builds fell back to RakNet (Owner
  observation; the exact versions and the newest builds' behavior are not
  verified).
- **Education 26.33 needs no RakNet.** Joining a MESS-listed server, Education
  26.33 tries HTTPS first and accepts a Let's Encrypt certificate for the
  server's IP address. When the server refuses the TLS connection, it goes on
  with plain HTTP on the same port and joins with no trust prompt. Both joins
  worked against a server with RakNet off and ICE on one shared UDP port
  (Verified by the owner, 2026-11-06). It does not pin the server's identity
  key over plain HTTP, and it checks an HTTPS certificate against the resolved
  IP even when given a domain, a retail 26.30 bug it inherited; newer bases
  check the domain (Owner).
- **A player quitting aborts the SCTP association.** When an Education 26.33
  player leaves, the server's libwebrtc reports `OnAborted(error=PEER_REPORTED,
  message=User-Initiated Abort, reason=Close called)`, which libwebrtc always
  logs at ERROR (`media/sctp/dcsctp_transport.cc:561`). It is the normal close
  path: libwebrtc ends every association with an ABORT. NetworkM reports it as
  `ABORTED_BY_REMOTE_PEER`, and EduGeyser showed "Bedrock client disconnected"
  (Verified by the owner, 2026-11-06).
- Android builds, seen through MCPELauncher, ignore candidates bundled into an
  SDP answer delivered over WebSocket signaling and only use separate
  `CANDIDATEADD` signals. HTTP answers are not affected, since there the
  candidates have to be in the SDP (Reported; HTTP path checked by the owner,
  2026-07).

## Data channels and framing

- Vanilla opens two data channels, `ReliableDataChannel` and
  `UnreliableDataChannel`, but sends game data only on the reliable one (Measured).
- Vanilla segments a message into at most 255 segments of the negotiated SCTP
  max message size minus 1 (262143 bytes by default), with a one-byte countdown
  header. It drops larger packets on send. On receive it has no size cap and
  accepts up to 256 segments (Decompiled: client 1.26.52 and 1.26.60, BDS
  1.26.52.3 and 1.26.60.28, 2026-09-26).
- On receive, vanilla closes the session for a segment of 1 byte or less (a
  header with no payload, which is what an empty message would be) and for a
  countdown header that is not one less than the previous segment's. Nothing
  else about a message makes it close (Decompiled, same builds).

## Networks

- Clients behind NAT that maps per destination, as on many institutional Wi-Fi
  networks, often end up on a peer reflexive selected pair. libwebrtc hides that
  address in its selected pair event (`redacted-ip.invalid`), which left such
  players without a real address. The webrtc-java fork now reports the selected
  connection's real candidate instead (Captured, 2026-11-05).
- On a home network the retail friends join first selected a relay pair and
  switched to the client's server reflexive address within the same millisecond,
  before the data channels opened (Captured, 2026-11-05).

## How vanilla BDS answers, for comparison

- BDS gives every NetherNet session its own UDP port, counting up from
  `server-port`, and releases it when the session ends. It cannot serve several
  sessions on one port: limited to one port with `server-udp-ports`, only one
  session at a time works (Measured, BDS 1.26.52.3).
- BDS answers with `a=setup:active`, so it is the DTLS client. Its ClientHello
  offers DTLS 1.2 only, with ChaCha20 suites first and no post-quantum groups.
  Its SCTP INIT uses ports 5000/5000, `a_rwnd` 5242880 and 65535 streams each
  way (Measured, BDS 1.26.45.1 and 1.26.60.21).
- BDS rejects an offer without `a=identity` with `200`, `application/sdp`, body
  `37` (`ErrorCodeIdentityNotAllowed`), whatever `online-mode` is set to
  (Measured, BDS 1.26.45.1 and 1.26.60.21).
- BDS answers a LAN discovery request with server data in one layout version:
  v6 from BDS 1.26.45.1, v7 from the 1.26.60.21 preview, which adds the protocol
  and version, drops the transport layer and writes player counts as varints
  (Measured, 2026-09-06). Retail BDS and clients past 1.26.60 are not captured
  yet.
- A 1.26.51 client lists a LAN server that answers only in an older version but
  never sends it an offer (Reported: upstream CloudburstMC/Network,
  `ServerData.java`).
- Client and BDS both link Google's libwebrtc statically, with dcsctp for SCTP
  and OpenSSL 1.1.1n for DTLS (Decompiled: client 1.26.45 and 1.26.6022, BDS
  1.26.45.1 and 1.26.60.21).
