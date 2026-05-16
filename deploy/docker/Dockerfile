ARG BUILD_FROM=ghcr.io/emqx/emqx-builder/5.6-3:1.18.3-27.3.4.2-4-debian13
ARG RUN_FROM=debian:13-slim
ARG SOURCE_TYPE=src # tgz

# Fetch a self-contained `curl` binary from stunnel/static-curl. Debian's stock
# curl drags libgnutls30t64 in via librtmp1 (libcurl4t64 -> librtmp1 -> gnutls),
# which is what makes scanners flag the four GnuTLS CVEs even though EMQX
# itself never links GnuTLS. The static binary is built with OpenSSL and no
# RTMP, so we get `curl` back without re-introducing GnuTLS. This stage is
# discarded after the COPY in the runtime stage; only the verified binary
# ships in the final image. Based on BUILD_FROM (the emqx-builder image)
# because it already ships curl + xz + tar; no apt-install needed.
FROM ${BUILD_FROM} AS curl_static
ARG TARGETARCH
ARG CURL_STATIC_VERSION=8.20.0
RUN set -eu; \
    case "${TARGETARCH}" in \
        amd64) curl_arch=x86_64; curl_sha=3cf20eb1bca2726d74a39fcda2b758a08a23a3dce73ad6f3f468d7e4d49d215b ;; \
        arm64) curl_arch=aarch64; curl_sha=061b624119f128038bdfa96f975980d6c35ed502130c4825d59b3143efeafa0e ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH} for stunnel/static-curl" >&2; exit 1 ;; \
    esac; \
    f="curl-linux-${curl_arch}-glibc-${CURL_STATIC_VERSION}.tar.xz"; \
    curl -fsSL -o "/tmp/${f}" "https://github.com/stunnel/static-curl/releases/download/${CURL_STATIC_VERSION}/${f}"; \
    echo "${curl_sha}  /tmp/${f}" | sha256sum -c -; \
    tar -xJf "/tmp/${f}" -C /tmp curl; \
    install -m 0755 /tmp/curl /usr/local/bin/curl; \
    /usr/local/bin/curl --version

FROM ${BUILD_FROM} AS builder_src
ONBUILD COPY . /emqx

FROM ${RUN_FROM} AS builder_tgz
ARG PROFILE=emqx
ARG PKG_VSN
ARG SUFFIX
ARG TARGETARCH
ONBUILD COPY ${PROFILE}-${PKG_VSN}${SUFFIX}-debian13-$TARGETARCH.tar.gz /${PROFILE}.tar.gz

FROM builder_${SOURCE_TYPE} AS builder

ARG PROFILE=emqx
ARG IS_ELIXIR=no
ARG DEBUG

ENV EMQX_RELUP=false
ENV EMQX_REL_FORM='docker'

WORKDIR /emqx/

RUN mkdir -p /emqx-rel/emqx && \
    if [ -f "/${PROFILE}.tar.gz" ]; then \
      tar zxf "/${PROFILE}.tar.gz" -C /emqx-rel/emqx; \
    else \
      if [ -f rebar-git-cache.tar ]; then \
        mkdir .cache && \
        tar -xf rebar-git-cache.tar -C .cache && \
        export REBAR_GIT_CACHE_DIR='/emqx/.cache' && \
        export REBAR_GIT_CACHE_REF_AUTOFILL=0; \
      fi && \
      export EMQX_REL_PATH="/emqx/_build/${PROFILE}/rel/emqx" && \
      git config --global --add safe.directory '*' && \
      make ${PROFILE}-tgz && \
      tar zxf _packages/${PROFILE}/*.tar.gz -C /emqx-rel/emqx; \
    fi && \
    rm -rf /emqx-rel/emqx/dynlibs

FROM $RUN_FROM
ARG EXTRA_DEPS=''

# Elixir complains if runs without UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY deploy/docker/docker-entrypoint.sh /usr/bin/
COPY --from=curl_static /usr/local/bin/curl /usr/bin/curl

RUN set -eu; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    apt-get upgrade -y; \
    apt-get install -y --no-install-recommends ca-certificates procps $(echo "${EXTRA_DEPS}" | tr ',' ' '); \
    if dpkg-query -s libgnutls30t64 >/dev/null 2>&1; then \
        apt-get remove --purge -y libgnutls30t64; \
        apt-get autoremove --purge -y; \
    fi; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd -r -g 1000 emqx; \
    useradd -r -m -u 1000 -g emqx emqx;

COPY --from=builder --chown=emqx:emqx /emqx-rel /opt/

RUN ln -s /opt/emqx/bin/* /usr/local/bin/

WORKDIR /opt/emqx

USER emqx

VOLUME ["/opt/emqx/log", "/opt/emqx/data"]

# emqx will occupy these port:
# - 1883 port for MQTT
# - 8083 for WebSocket/HTTP
# - 8084 for WSS/HTTPS
# - 8883 port for MQTT(SSL)
# - 18083 for dashboard and API
# - 4370 default Erlang distribution port
# - 5369 for backplane gen_rpc
EXPOSE 1883 8083 8084 8883 18083 4370 5369

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["/opt/emqx/bin/emqx", "foreground"]
