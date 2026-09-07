# NetworkM

NetworkM is a networking library providing RakNet and NetherNet transports for
Netty. It was created by merging EduGeyser's Network and NetworkCompatible forks,
bringing their transport features and fixes into one codebase.

Since the merge, NetworkM has been modernized with Java 21, Netty 4.2, and updated
build tooling and dependencies. Further work has improved transport performance,
connection handling, and protocol compatibility, with fixes covered by automated
tests.

| Module | Java target | Packages |
| --- | --- | --- |
| `transport-raknet` | 21 | `io.github.sendablemetatype.netty` |
| `transport-nethernet` | 21 | `io.github.sendablemetatype.netty` |

Run `./gradlew build` (or `gradlew.bat build` on Windows) with Java 17 or newer.
Gradle provisions JDK 26 for compilation and JDK 21 for tests. Compilation uses
`--release 21`, so both modules require Java 21 or newer at runtime. CI runs the
same tests on Java 21, 25, and 26; use `-PtestJavaVersion=25` or
`-PtestJavaVersion=26` to select a newer test runtime locally.

Both modules use Netty 4.2.17.Final, aligned through its BOM. Applications that
provide Netty must use compatible 4.2 modules together. RakNet depends on
`netty-codec-base` rather than the aggregate `netty-codec` artifact, so it does
not pull in unrelated codecs.

Artifacts use `io.github.sendablemetatype.netty:netty-transport-raknet` and
`io.github.sendablemetatype.netty:netty-transport-nethernet`. The group can be changed with
`-PnetworkGroup=your.group`; Java packages do not change. The local version is
`1.7.4-networkm-SNAPSHOT`, separate from the source fork's published releases.
`NETWORK_PUBLISH_VERSION` overrides the version for either publishing backend.

NetworkM uses its own Java packages. Consumers migrating from Network or
NetworkCompatible must update their imports and rebuild. Libraries that directly
reference transport classes, such as Cloudburst Protocol's connection module,
also need a build targeting NetworkM.

See [RakNet configuration](transport-raknet/README.md) and
[NetherNet setup](transport-nethernet/README.md). NetherNet requires the
[EduGeyser webrtc-java fork](https://github.com/EduGeyser/webrtc-java), published
as `dev.kastle.webrtc:webrtc-java:1.0.4-edu.3`, and matching native libraries.
The fork retains the `dev.kastle.webrtc` Maven group and Java packages.
Its custom Maven repository is configured in the root build.

Release and feature-snapshot publication are manual workflows. The generic
Maven deployment workflow remains reusable. Publishing requires the relevant
repository credentials and signing keys; ordinary builds do not publish.
POM project, source, issue, and CI links use GitHub Actions' `GITHUB_REPOSITORY`.
For local publication, set `-PnetworkRepository=owner/repository`. Local builds
without this setting omit repository links.

`codec-query` and `codec-rcon` source trees are retained for reference. They were
not included in either active Gradle build and are not included here.
