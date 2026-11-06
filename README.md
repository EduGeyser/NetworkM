# NetworkM

NetworkM is a networking library providing RakNet and NetherNet transports for
Netty. It was created by merging EduGeyser's Network and NetworkCompatible forks,
bringing their transport features and fixes into one codebase.

Since the merge, NetworkM has been modernized with Java 25, Netty 4.2, and updated
build tooling and dependencies. Further work has improved transport performance,
connection handling, and protocol compatibility, with fixes covered by automated
tests.

## Usage

NetworkM **1.3.0** is available from Maven Central. Both transports require
**Java 25 or newer**. Add Maven Central to your `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
}
```

Add the transport you use, or both if your application needs both.

For RakNet:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-transport-raknet:1.3.0")
}
```

For NetherNet, also include the WebRTC native library for your runtime platform.
This example targets Linux x86-64:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-transport-nethernet:1.3.0")
    runtimeOnly("io.github.sendablemetatype.webrtc:webrtc-java:0.21.0-sm.1:linux-x86_64")
}
```

The WebRTC Java API is included transitively. Keep the native version matched
to it; NetworkM 1.3.0 uses WebRTC **0.21.0-sm.1**. Replace the classifier in the
example with the one for your platform, or include each platform your
application supports:

| Platform | Native classifier |
| --- | --- |
| Linux x86-64 | `linux-x86_64` |
| Linux ARM64 | `linux-aarch64` |
| Linux ARM32 | `linux-aarch32` |
| Windows x86-64 | `windows-x86_64` |
| Windows ARM64 | `windows-aarch64` |
| macOS x86-64 | `macos-x86_64` |
| macOS ARM64 | `macos-aarch64` |

See [RakNet configuration](transport-raknet/README.md) and
[NetherNet setup](transport-nethernet/README.md) for transport configuration.

The optional `nethernet-nxs` module adds a client for NetherNet External
Signaling (NXS) providers on top of the NetherNet transport; see
[its README](nethernet-nxs/README.md). It is not part of the 1.3.0 release.

## Compatibility

| Module | Java target | Packages |
| --- | --- | --- |
| `transport-common` | 25 | `io.github.sendablemetatype.netty` |
| `transport-raknet` | 25 | `io.github.sendablemetatype.netty` |
| `transport-nethernet` | 25 | `io.github.sendablemetatype.netty` |
| `nethernet-nxs` | 25 | `io.github.sendablemetatype.netty` |

All modules use Netty 4.2.20.Final, aligned through its BOM. Applications
that provide Netty must use compatible 4.2 modules together. RakNet depends on
`netty-codec-base` rather than the aggregate `netty-codec` artifact, so it does
not pull in unrelated codecs.

NetworkM uses its own Java packages. Consumers migrating from Network or
NetworkCompatible must update their imports and rebuild. Libraries that directly
reference transport classes, such as Cloudburst Protocol's connection module,
also need a build targeting NetworkM.

NetherNet runs its WebRTC work through an engine interface. The default engine
uses the [slim webrtc-java fork](https://github.com/EduGeyser/webrtc-java), whose
Java packages use the `io.github.sendablemetatype.webrtc` namespace.

## Building from source

Run `./gradlew build` (or `gradlew.bat build` on Windows) with Java 17 or newer.
Gradle provisions JDK 27 for compilation and tests. Compilation uses
`--release 25`. CI runs the same tests on Java 25 and 27; use
`-PtestJavaVersion=25` to test on the minimum supported version locally.
The `nethernet-nxs` tests run the NXS specification's Node.js verifiers and fail
without Node.js; `-PnxsNode=<path>` selects the binary, `node` on the path
otherwise.

Local builds use the development version declared in [gradle.properties](gradle.properties).
`NETWORK_PUBLISH_VERSION` overrides it when preparing a release. Forks can change
the Maven group with `-PnetworkGroup=your.group`; Java packages do not change.

## Publishing

Releases go to Maven Central only. The manual `Publish release` workflow runs
the full CI build matrix on the selected commit and publishes only if every lane
passes. It needs the Central Portal credentials and the signing key; ordinary
builds do not publish. JitPack builds snapshots of any commit.
POM project, source, issue, and CI links use GitHub Actions' `GITHUB_REPOSITORY`.
For local publication, set `-PnetworkRepository=owner/repository`. Local builds
without this setting omit repository links.
