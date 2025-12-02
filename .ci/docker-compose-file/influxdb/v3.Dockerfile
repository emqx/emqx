FROM public.ecr.aws/docker/library/influxdb:3.7.0-core

ARG DOCKER_USER

USER root
RUN mkdir -p /var/lib/secret_container && \
    chmod 0777 /var/lib/secret_container

USER "${DOCKER_USER:-root}"
RUN influxdb3 create token --admin \
              --offline --output-file "/var/lib/secret_container/influxv3.json"

USER root
RUN chmod -R 0777 /var/lib/secret_container

USER influxdb3
