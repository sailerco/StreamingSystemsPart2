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
    implementation("com.espertech:esper-common:8.9.0")
    implementation("com.espertech:esperio-kafka:8.9.0")
    implementation("com.espertech:esper-runtime:8.9.0")
    implementation("com.espertech:esper-compiler:8.9.0")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    //todo: https://mvnrepository.com/artifact/com.espertech/esper/7.1.0
    //testImplementation("cglib:cglib-nodep:3.2.6")
    //testImplementation("org.antlr:antlr4-runtime:4.7")
    //testImplementation("org.slf4j:slf4j-api:1.7.25")
    //testImplementation("org.codehaus.janino:janino:3.0.7")
    //testImplementation("org.codehaus.janino:commons-compiler:3.0.7")
}


tasks.test {
    useJUnitPlatform()
}