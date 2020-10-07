FROM gradle:jdk8

COPY settings.gradle.kts .
COPY gradle.properties .
COPY build.gradle.kts .

RUN gradle --no-daemon compileKotlin compileIntegrationKotlin
