# NetworkM

NetworkM is a networking library providing RakNet and NetherNet transports for
Netty. It was created by merging EduGeyser's Network and NetworkCompatible forks,
bringing their transport features and fixes into one codebase.

Since the merge, NetworkM has been modernized with Java 21, Netty 4.2, and updated
build tooling and dependencies. Further work has improved transport performance,
connection handling, and protocol compatibility, with fixes covered by automated
tests.

## Usage

NetworkM **1.0.0** is available from Maven Central. Both transports require
**Java 21 or newer**. Add Maven Central to your `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
}
```

Add the transport you use, or both if your application needs both.

For RakNet:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-transport-raknet:1.0.0")
}
```

For NetherNet, also include the WebRTC native library for your runtime platform.
This example targets Linux x86-64:

```kotlin
dependencies {
    implementation("io.github.sendablemetatype.netty:netty-transport-nethernet:1.0.0")
    runtimeOnly("io.github.sendablemetatype.webrtc:webrtc-java:0.17.0-sm.1:linux-x86_64")
}
```

The WebRTC Java API is included transitively. Keep the native version matched
to it; NetworkM 1.0.0 uses WebRTC **0.17.0-sm.1**. Replace the classifier in the
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

## Compatibility

| Module | Java target | Packages |
| --- | --- | --- |
| `transport-raknet` | 21 | `io.github.sendablemetatype.netty` |
| `transport-nethernet` | 21 | `io.github.sendablemetatype.netty` |

Both modules use Netty 4.2.17.Final, aligned through its BOM. Applications that
provide Netty must use compatible 4.2 modules together. RakNet depends on
`netty-codec-base` rather than the aggregate `netty-codec` artifact, so it does
not pull in unrelated codecs.

NetworkM uses its own Java packages. Consumers migrating from Network or
NetworkCompatible must update their imports and rebuild. Libraries that directly
reference transport classes, such as Cloudburst Protocol's connection module,
also need a build targeting NetworkM.

NetherNet uses the [slim webrtc-java fork](https://github.com/EduGeyser/webrtc-java).
Its Java packages use the `io.github.sendablemetatype.webrtc` namespace.

## Building from source

Run `./gradlew build` (or `gradlew.bat build` on Windows) with Java 17 or newer.
Gradle provisions JDK 26 for compilation and JDK 21 for tests. Compilation uses
`--release 21`. CI runs the same tests on Java 21, 25, and 26; use
`-PtestJavaVersion=25` or `-PtestJavaVersion=26` to select a newer test runtime
locally.

Local builds use the development version declared in [gradle.properties](gradle.properties).
`NETWORK_PUBLISH_VERSION` overrides it when preparing a release. Forks can change
the Maven group with `-PnetworkGroup=your.group`; Java packages do not change.

## Publishing

Release and feature-snapshot publication are manual workflows. The generic
Maven deployment workflow remains reusable. Publishing requires the relevant
repository credentials and signing keys; ordinary builds do not publish.
POM project, source, issue, and CI links use GitHub Actions' `GITHUB_REPOSITORY`.
For local publication, set `-PnetworkRepository=owner/repository`. Local builds
without this setting omit repository links.

`codec-query` and `codec-rcon` source trees are retained for reference. They were
not included in either active Gradle build and are not included here.
