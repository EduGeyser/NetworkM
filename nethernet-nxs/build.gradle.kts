plugins {
    id("com.gradleup.nmcp")
}

description = "NetherNet External Signaling (NXS) provider client and stateless admission"

val specification = layout.projectDirectory.dir("docs/external-signaling")

dependencies {
    api(project(":transport-nethernet"))
    implementation(libs.gson)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.junit)
    testImplementation(libs.netty.pkitesting)
    testRuntimeOnly(libs.junit.platform.launcher)
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
    // The Node verifiers run against a copy of the specification that carries this module's output
    inputs.dir(specification)
    systemProperty("nxs.specification", specification.asFile.absolutePath)
    providers.gradleProperty("nxsNode").orNull?.let { systemProperty("nxs.node", it) }
}

tasks.jar {
    manifest.attributes["Automatic-Module-Name"] = "io.github.sendablemetatype.netty.channel.nethernet.nxs"
}
