FROM python:3.7.4-stretch

RUN pip install awscli \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org

ENV USER aws
ENV INSTALL_DIR /opt/${USER}
ENV HOME_DIR /home/${USER}

WORKDIR ${INSTALL_DIR}

COPY ./init.sh ./environment.sh  ./
COPY test-data/  /test-data

RUN mkdir ${HOME_DIR} && \
        useradd ${USER} && \
        chown -R ${USER}.${USER} ${INSTALL_DIR} ${HOME_DIR}

USER $USER

ENTRYPOINT ["./init.sh"]
