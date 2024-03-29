FROM openjdk:16-jdk-alpine

ARG APP_VERSION
ENV APP_NAME=hbase-table-provisioner
ENV APP_JAR=$APP_NAME-$APP_VERSION.jar
ENV APP_HOME=/opt/$APP_NAME
ENV USER=htp
ENV GROUP=$USER

ENTRYPOINT ["sh", "-c", "./entrypoint.sh \"$@\"", "--"]

RUN mkdir $APP_HOME
WORKDIR $APP_HOME

RUN addgroup $GROUP
RUN adduser --disabled-password --ingroup $GROUP $USER

COPY entrypoint.sh .
RUN chmod a+x entrypoint.sh
COPY ./build/libs/*.jar ./$APP_NAME.jar
COPY ./application.properties ./test.properties
RUN chown -R $USER.$USER . && chmod +x ./$APP_NAME.jar ./test.properties

USER $USER
