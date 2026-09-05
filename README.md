# NetworkM

NetworkM combines the `develop` branch of Network with the `master` branch of the
EduGeyser NetworkCompatible fork. It provides one RakNet implementation and a
separate NetherNet transport.

| Module | Java target | Packages |
| --- | --- | --- |
| `transport-raknet` | 21 | `org.cloudburstmc.netty` |
| `transport-nethernet` | 21 | `dev.kastle.netty` |

Run `./gradlew build` (or `gradlew.bat build` on Windows) with Java 17 or newer.
Gradle provisions JDK 26 for compilation and JDK 21 for tests. Compilation uses
`--release 21`, so both modules require Java 21 or newer at runtime. CI runs the
same tests on Java 21, 25, and 26; use `-PtestJavaVersion=25` or
`-PtestJavaVersion=26` to select a newer test runtime locally.

Both modules use Netty 4.2.17.Final, aligned through its BOM. Applications that
provide Netty must use compatible 4.2 modules together. RakNet depends on
`netty-codec-base` rather than the aggregate `netty-codec` artifact, so it does
not pull in unrelated codecs.

Artifacts use `dev.sendablemetatype.netty:netty-transport-raknet` and
`dev.sendablemetatype.netty:netty-transport-nethernet`. The group can be changed with
`-PnetworkGroup=your.group`; Java packages do not change. The local version is
`1.7.4-networkm-SNAPSHOT`, separate from the source fork's published releases.
`NETWORK_PUBLISH_VERSION` overrides the version for either publishing backend.

Use only one RakNet artifact at runtime. Network and NetworkCompatible use the
same RakNet Java packages, even though their Maven groups differ.

See [RakNet configuration](transport-raknet/README.md) and
[NetherNet setup](transport-nethernet/README.md). NetherNet still requires the
`dev.kastle.webrtc:webrtc-java:1.0.4-edu.3` fork and matching native libraries.
The custom Maven repository is configured in the root build.

Release and feature-snapshot publication are manual workflows. The generic
Maven deployment workflow remains reusable. Publishing requires the relevant
repository credentials and signing keys; ordinary builds do not publish.
Historical POM source links are retained from the upstream forks until a hosted
NetworkM repository is selected.

`codec-query` and `codec-rcon` source trees are retained for reference. They were
not included in either active Gradle build and are not included here.
