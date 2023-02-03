plugins {
    id("java")
}

group = "top.sugarspace"
version = "1.0.0"

repositories {
    mavenLocal()
    maven {
        url = uri("https://maven.aliyun.com/repository/public")
    }
    mavenCentral()
}

dependencies {
//    compileOnly("org.apache.hadoop:hadoop-client:3.2.0")
    implementation("org.apache.hadoop:hadoop-client:3.2.0")
    compileOnly("org.slf4j:slf4j-api:2.0.6")
    compileOnly("org.slf4j:slf4j-reload4j:2.0.6")

//    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
//    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}


tasks.getByName<JavaCompile>("compileJava") {
    options.encoding = "UTF-8"
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
}