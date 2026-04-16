plugins {
    kotlin("jvm") version "2.0.0"
    `java-library`
    `maven-publish`
    signing
}

group = "dev.cachly"
version = "0.1.0-beta.1"

repositories {
    mavenCentral()
}

dependencies {
    api("redis.clients:jedis:5.1.0")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.0")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.3.1")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

java {
    withSourcesJar()
    withJavadocJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("cachly-kotlin")
                description.set("Official Kotlin SDK for cachly.dev – Managed Valkey/Redis cache with semantic AI caching")
                url.set("https://cachly.dev")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("cachly-dev")
                        name.set("cachly.dev")
                        email.set("hello@cachly.dev")
                        url.set("https://cachly.dev")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/cachly-dev/sdk-kotlin.git")
                    developerConnection.set("scm:git:ssh://github.com/cachly-dev/sdk-kotlin.git")
                    url.set("https://github.com/cachly-dev/sdk-kotlin")
                }
            }
        }
    }
    repositories {
        maven {
            name = "central"
            url = uri("https://central.sonatype.com/api/v1/publisher/upload")
            credentials {
                username = project.findProperty("centralUsername") as String? ?: System.getenv("CENTRAL_USERNAME") ?: ""
                password = project.findProperty("centralPassword") as String? ?: System.getenv("CENTRAL_PASSWORD") ?: ""
            }
        }
    }
}

signing {
    val signingKey = project.findProperty("signingKey") as String? ?: System.getenv("GPG_SIGNING_KEY") ?: ""
    val signingPassword = project.findProperty("signingPassword") as String? ?: System.getenv("GPG_SIGNING_PASSWORD") ?: ""
    if (signingKey.isNotEmpty()) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
    sign(publishing.publications["maven"])
}

