plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    //added to make kafka work lol
    maven{
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    implementation("org.apache.beam:beam-runners-direct-java:2.52.0")
    implementation("org.apache.beam:beam-sdks-java-io-kafka:2.52.0")
    implementation("org.apache.beam:beam-sdks-java-core:2.52.0")
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}