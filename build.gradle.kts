plugins {
    kotlin("jvm") version "2.0.0"
    `java-library`
    `maven-publish`
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
            }
        }
    }
}

