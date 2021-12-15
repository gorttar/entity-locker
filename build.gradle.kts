plugins {
    kotlin("jvm") version "1.6.0"
    java
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(group = "org.jetbrains.kotlin", name = "kotlin-stdlib", version = "1.6.0")
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = "5.8.2")
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = "5.8.2")
    testImplementation(group = "com.willowtreeapps.assertk", name = "assertk", version = "0.25")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}