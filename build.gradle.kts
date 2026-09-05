/*
 * Copyright 2023 CloudburstMC
 *
 * CloudburstMC licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

plugins {
    alias(libs.plugins.nmcp.aggregation)
    `maven-publish`
}

repositories {
    mavenCentral()
}

val networkVersion = System.getenv("NETWORK_PUBLISH_VERSION")?.trim()?.takeIf { it.isNotEmpty() }
        ?: rootProject.property("version") as String
val networkGroup = providers.gradleProperty("networkGroup").getOrElse("dev.sendablemetatype.netty")
val testJavaVersion = providers.gradleProperty("testJavaVersion").map(String::toInt).orElse(21)

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.gradleup.nmcp")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    group = networkGroup
    version = networkVersion

    repositories {
        mavenLocal()
        mavenCentral()
        // SendableMetatype webrtc-java fork builds (sendAsync, ICE selected
        // candidate pair bridge), published as a maven layout git branch.
        maven("https://raw.githubusercontent.com/SendableMetatype/webrtc-java/maven-repo/") {
            content { includeGroup("dev.kastle.webrtc") }
        }
    }

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(26))
        }
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
        withJavadocJar()
        withSourcesJar()
    }

    val javaToolchains = extensions.getByType<JavaToolchainService>()

    configure<PublishingExtension> {
        repositories {
            maven {
                name = "maven-deploy"
                url = uri(System.getenv("MAVEN_DEPLOY_URL") ?: "https://repo.opencollab.dev/maven-snapshots/")
                credentials {
                    username = System.getenv("MAVEN_DEPLOY_USERNAME") ?: "username"
                    password = System.getenv("MAVEN_DEPLOY_PASSWORD") ?: "password"
                }
            }
        }
        publications {
            create<MavenPublication>("maven") {
                artifactId = "netty-${project.name}"

                from(components["java"])

                pom {
                    description.set(providers.provider { project.description })
                    name.set(project.name)
                    url.set("https://github.com/Kas-tle/NetworkCompatible")
                    inceptionYear.set("2018")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            name.set("CloudburstMC Team")
                            organization.set("CloudburstMC")
                            organizationUrl.set("https://github.com/CloudburstMC")
                        }
                        developer {
                            name.set("Kas-tle")
                            organization.set("Kas-tle")
                            organizationUrl.set("https://github.com/Kas-tle")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/Kas-tle/NetworkCompatible.git")
                        developerConnection.set("scm:git:ssh://github.com:Kas-tle/NetworkCompatible.git")
                        url.set("https://github.com/Kas-tle/NetworkCompatible")
                    }
                    ciManagement {
                        system.set("GitHub Actions")
                        url.set("https://github.com/Kas-tle/NetworkCompatible/actions")
                    }
                    issueManagement {
                        system.set("GitHub Issues")
                        url.set("https://github.com/Kas-tle/NetworkCompatible/issues")
                    }
                }
            }
        }
    }

    configure<SigningExtension> {
        if (System.getenv("PGP_SECRET") != null && System.getenv("PGP_PASSPHRASE") != null) {
            useInMemoryPgpKeys(System.getenv("PGP_SECRET"), System.getenv("PGP_PASSPHRASE"))
            sign(project.extensions.getByType(PublishingExtension::class).publications["maven"])
        }
    }

    tasks {
        withType<JavaCompile>().configureEach {
            options.encoding = "UTF-8"
            options.release.set(21)
        }
        withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).apply {
                encoding = "UTF-8"
                addStringOption("-release", "21")
            }
        }
        withType<JavaExec>().configureEach {
            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(21))
            })
        }
        withType<Test>().configureEach {
            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(testJavaVersion.map(JavaLanguageVersion::of))
            })
            minHeapSize = "512m"
            maxHeapSize = "1024m"
            jvmArgs = listOf("-XX:MaxMetaspaceSize=512m")
            useJUnitPlatform()
        }
    }
}

dependencies {
    nmcpAggregation(project(":transport-raknet"))
    nmcpAggregation(project(":transport-nethernet"))
}


nmcpAggregation {
    centralPortal {
        username.set(System.getenv("MAVEN_CENTRAL_USERNAME"))
        password.set(System.getenv("MAVEN_CENTRAL_PASSWORD"))

        publishingType.set("AUTOMATIC")
    }
}
