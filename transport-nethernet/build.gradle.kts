description = "NetherNet transport for Netty"

val webrtcNativePlatform = providers.gradleProperty("webrtcNativePlatform")

dependencies {
    api(project(":transport-common"))
    api(platform(libs.netty.bom))
    api(libs.bundles.netty)
    api(libs.netty.codec.http)
    // PROXY protocol headers in front of HTTP signaling
    implementation(libs.netty.codec.haproxy)
    api(libs.webrtc.java)

    implementation(libs.gson)
    implementation(libs.jose4j)
    // Reads and writes the server identity key as PEM, which the JDK has no stable API for
    implementation(libs.bouncycastle.pkix)
    // Direct declarations preserve the transitive upgrades for Maven consumers too.
    implementation(libs.slf4j.api)
    implementation(libs.errorprone.annotations)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.junit)
    testImplementation(libs.netty.pkitesting)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.slf4j.jdk14)

    if (webrtcNativePlatform.isPresent) {
        val webrtc = libs.webrtc.java.get()
        testRuntimeOnly("${webrtc.module.group}:${webrtc.module.name}:${webrtc.versionConstraint.requiredVersion}:${webrtcNativePlatform.get()}")
    }

    constraints {
        testImplementation(libs.jspecify)
    }
}

tasks.test {
    systemProperty("webrtc.nativeTests", webrtcNativePlatform.isPresent.toString())
    if (webrtcNativePlatform.isPresent) {
        jvmArgs("--enable-native-access=ALL-UNNAMED")
        // webrtc-java reads its native log threshold once, when the first test loads the library.
        systemProperty("io.github.sendablemetatype.webrtc.logging.severity", "WARNING")
    }
}

tasks.jar {
    manifest.attributes["Automatic-Module-Name"] = "io.github.sendablemetatype.netty.transport.nethernet"
}

// The fake engine and the other test helpers, for the tests of modules built on this one. Never published.
val testJar = tasks.register<Jar>("testJar") {
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}
val testHelpers = configurations.create("testHelpers") {
    isCanBeConsumed = true
    isCanBeResolved = false
}
artifacts {
    add(testHelpers.name, testJar)
}

tasks.register<JavaExec>("runDiscovery") {
    mainClass.set("io.github.sendablemetatype.netty.util.nethernet.NetherNetScanner")
    classpath = sourceSets["main"].runtimeClasspath
}

// A before-and-after measurement of the engine on libwebrtc, kept out of the test task. Settings
// go in --args, for example --args="pings=50000 pairs=1,4".
tasks.register<JavaExec>("echoBenchmark") {
    description = "Measures a NetherNet echo through libwebrtc on this machine; needs -PwebrtcNativePlatform."
    mainClass.set("io.github.sendablemetatype.netty.channel.nethernet.engine.webrtc.EchoBenchmark")
    classpath = sourceSets["test"].runtimeClasspath
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("io.github.sendablemetatype.webrtc.logging.severity", "WARNING")
    systemProperty("echoBenchmark.natives",
        "webrtc-java ${libs.webrtc.java.get().versionConstraint.requiredVersion}, ${webrtcNativePlatform.getOrElse("none")}")
    onlyIf("webrtc-java natives are on the classpath (-PwebrtcNativePlatform)") { webrtcNativePlatform.isPresent }
}
