ARG BUILD_FROM=ghcr.io/emqx/emqx-builder/5.1-4:1.14.5-25.3.2-2-debian11
ARG RUN_FROM=public.ecr.aws/debian/debian:11-slim
FROM ${BUILD_FROM} AS builder
ARG DEBUG=0

COPY . /emqx

ARG EMQX_NAME=emqx
ARG PKG_VSN

ENV EMQX_RELUP=false
ENV DEBUG=${DEBUG}
ENV EMQX_REL_FORM='docker'

WORKDIR /emqx/

RUN git config --global --add safe.directory '*'

RUN if [ -f rebar-git-cache.tar ]; then \
        mkdir .cache && \
        tar -xf rebar-git-cache.tar -C .cache && \
        export REBAR_GIT_CACHE_DIR='/emqx/.cache' && \
        export REBAR_GIT_CACHE_REF_AUTOFILL=0 ;\
    fi \
    && export PROFILE=${EMQX_NAME%%-elixir} \
    && export EMQX_NAME1="${EMQX_NAME}" \
    && export EMQX_NAME=${PROFILE} \
    && export EMQX_REL_PATH="/emqx/_build/${EMQX_NAME}/rel/emqx" \
    && make ${EMQX_NAME1} \
    && rm -f ${EMQX_REL_PATH}/*.tar.gz \
    && mkdir -p /emqx-rel \
    && mv ${EMQX_REL_PATH} /emqx-rel

FROM $RUN_FROM
ARG EXTRA_DEPS=''

# Elixir complains if runs without UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY deploy/docker/docker-entrypoint.sh /usr/bin/
COPY --from=builder /emqx-rel/emqx /opt/emqx

RUN ln -s /opt/emqx/bin/* /usr/local/bin/

RUN apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates procps $(echo "${EXTRA_DEPS}" | tr ',' ' '); \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/emqx

RUN groupadd -r -g 1000 emqx; \
    useradd -r -m -u 1000 -g emqx emqx; \
    chgrp -Rf emqx /opt/emqx; \
    chmod -Rf g+w /opt/emqx; \
    chown -Rf emqx /opt/emqx

USER emqx

VOLUME ["/opt/emqx/log", "/opt/emqx/data"]

# emqx will occupy these port:
# - 1883 port for MQTT
# - 8083 for WebSocket/HTTP
# - 8084 for WSS/HTTPS
# - 8883 port for MQTT(SSL)
# - 11883 port for internal MQTT/TCP
# - 18083 for dashboard and API
# - 4370 default Erlang distribution port
# - 5369 for backplane gen_rpc
EXPOSE 1883 8083 8084 8883 11883 18083 4370 5369

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["/opt/emqx/bin/emqx", "foreground"]
