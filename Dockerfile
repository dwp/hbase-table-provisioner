FROM openjdk:16-jdk-alpine

ARG APP_VERSION
ENV APP_NAME=hbase-table-provisioner
ENV APP_JAR=$APP_NAME-$APP_VERSION.jar
ENV APP_HOME=/opt/$APP_NAME
ENV USER=htp
ENV GROUP=$USER

RUN mkdir $APP_HOME
WORKDIR $APP_HOME

COPY entrypoint.sh .
RUN chmod a+x entrypoint.sh

RUN addgroup $GROUP
RUN adduser --disabled-password --ingroup $GROUP $USER
USER $USER

ENTRYPOINT ["sh", "-c", "./entrypoint.sh \"$@\"", "--"]

COPY ./build/libs/*.jar ./$APP_NAME.jar
RUN chown -R $USER.$USER . && chmod a+x ./$APP_NAME.jar
