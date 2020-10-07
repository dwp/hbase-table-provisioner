FROM dwp-gradle-hbase-table-provisioner:latest

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY src/ ./src
COPY build/ ./build
COPY .gradle/ ./gradle

RUN gradle --no-daemon build compileKotlin compileIntegrationKotlin -x test -x unit -x integration-test
