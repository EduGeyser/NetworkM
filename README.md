# NetworkM

NetworkM is a networking library providing RakNet and NetherNet transports for
Netty. It was created by merging EduGeyser's Network and NetworkCompatible forks,
bringing their transport features and fixes into one codebase.

Since the merge, NetworkM has been modernized with Java 25, Netty 4.2, and updated
build tooling and dependencies. Further work has improved transport performance,
connection handling, and protocol compatibility, with fixes covered by automated
tests.

## Usage

NetworkM **2.0.0** is available from Maven Central. Both transports require
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
    implementation("io.github.sendablemetatype.netty:netty-transport-raknet:2.0.0")
}
```

For NetherNet, also include the WebRTC native library for your runtime platform.
This example targets Linux x86-64:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-transport-nethernet:2.0.0")
    runtimeOnly("io.github.sendablemetatype.webrtc:webrtc-java:1.0.1-sm.1:linux-x86_64")
}
```

The WebRTC Java API is included transitively. Keep the native version matched
to it; NetworkM 2.0.0 uses webrtc-java **1.0.1-sm.1**. Replace the classifier
in the example with the one for your platform, or include each platform your
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

Each classifier holds one library under its own platform name, so several can
sit on one class path; the loader picks the one for the running platform. Linux
builds that use musl, such as Alpine, are not supported yet.

Java 25 to 27 print a warning when the native library loads, and a future Java
release will block the load, unless native access is enabled. Pass
`--enable-native-access=ALL-UNNAMED` when webrtc-java is on the class path, or
`--enable-native-access=io.github.sendablemetatype.webrtc` when it is on the
module path.

See [RakNet configuration](transport-raknet/README.md) and
[NetherNet setup](transport-nethernet/README.md) for transport configuration.
Each module README ends with diagrams of its packet flow:
[RakNet](transport-raknet/README.md#packet-flow) and
[NetherNet](transport-nethernet/README.md#packet-flow).

The optional `nethernet-nxs` module adds NetherNet External Signaling (NXS) to
a NetherNet server: the provider client, the host side that admits NXS players
on the server's shared ICE port, and connectivity diagnostics; see
[its README](nethernet-nxs/README.md). It is on Maven Central since 2.0.0:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-nethernet-nxs:2.0.0")
}
```

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
uses the data channels variant of
[EduGeyser's webrtc-java fork](https://github.com/EduGeyser/webrtc-java), whose
Java packages use the `io.github.sendablemetatype.webrtc` namespace.

## Building from source

Run `./gradlew build` (or `gradlew.bat build` on Windows) with Java 17 or newer.
Gradle provisions the newest JDK, which `newestJavaVersion` in
[gradle.properties](gradle.properties) names, for compilation and tests.
Compilation uses `--release 25`. CI runs the same tests on Java 25 and on the
newest JDK; use `-PtestJavaVersion=25` to test on the minimum supported version
locally.
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
