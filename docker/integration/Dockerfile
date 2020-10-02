FROM dwp-gradle-hbase-table-provisioner:latest

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY src/ ./src
COPY build/ ./build
COPY .gradle/ ./gradle

RUN gradle :unit build integrationClasses -x test -x unit -x htp-integration-test -x trim-htp-integration-test