description = "NetherNet External Signaling (NXS) provider client and stateless admission"

val specification = layout.projectDirectory.dir("docs/external-signaling")
val webrtcNativePlatform = providers.gradleProperty("webrtcNativePlatform")

dependencies {
    api(project(":transport-nethernet"))
    implementation(libs.gson)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.junit)
    testImplementation(libs.netty.pkitesting)
    // The fake engine the host side is tested on
    testImplementation(project(path = ":transport-nethernet", configuration = "testHelpers"))
    testRuntimeOnly(libs.junit.platform.launcher)

    if (webrtcNativePlatform.isPresent) {
        val webrtc = libs.webrtc.java.get()
        testRuntimeOnly("${webrtc.module.group}:${webrtc.module.name}:${webrtc.versionConstraint.requiredVersion}:${webrtcNativePlatform.get()}")
    }
}

tasks.processResources {
    from(specification.file("nxs-v1.schema.json"))
}

tasks.processTestResources {
    from(specification.dir("fixtures")) {
        into("nxs")
    }
}

tasks.test {
    systemProperty("webrtc.nativeTests", webrtcNativePlatform.isPresent.toString())
    if (webrtcNativePlatform.isPresent) {
        jvmArgs("--enable-native-access=ALL-UNNAMED")
        systemProperty("io.github.sendablemetatype.webrtc.logging.severity", "WARNING")
    }
    // The Node verifiers run against a copy of the specification that carries this module's output
    inputs.dir(specification)
    systemProperty("nxs.specification", specification.asFile.absolutePath)
    providers.gradleProperty("nxsNode").orNull?.let { systemProperty("nxs.node", it) }
}

tasks.jar {
    manifest.attributes["Automatic-Module-Name"] = "io.github.sendablemetatype.netty.nethernet.nxs"
}
