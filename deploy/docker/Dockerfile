ARG BUILD_FROM=ghcr.io/emqx/emqx-builder/6.1-8:1.19.1-28.4.1-4-debian13
ARG RUN_FROM=dhi.io/debian-base:trixie
ARG DEPS_FROM=dhi.io/debian-base:trixie-dev
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

FROM ${DEPS_FROM} AS builder_tgz
USER 0:0
ARG PROFILE=emqx-enterprise
ARG PKG_VSN
ARG SUFFIX
ARG TARGETARCH
ONBUILD COPY ${PROFILE}-${PKG_VSN}${SUFFIX}-debian13-$TARGETARCH.tar.gz /${PROFILE}.tar.gz

FROM builder_${SOURCE_TYPE} AS builder

ARG PROFILE=emqx-enterprise
ARG PKG_VSN
ARG DEBUG

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
      export PKG_VSN="${PKG_VSN}" && \
      export EMQX_REL_PATH="/emqx/_build/${PROFILE}/rel/emqx" && \
      make ${PROFILE}-tgz && \
      tar zxf _packages/${PROFILE}/*.tar.gz -C /emqx-rel/emqx; \
    fi && \
    rm -rf /emqx-rel/emqx/dynlibs

FROM ${RUN_FROM} AS runtime_base

# DHI runtime images have no package manager. Install runtime dependencies in
# the matching dev image, then export only their files and package metadata.
FROM ${DEPS_FROM} AS runtime_deps
USER 0:0
ARG EXTRA_DEPS=''
COPY --from=runtime_base /var/lib/dpkg/ /runtime-rootfs/var/lib/dpkg/
COPY deploy/docker/export-runtime-deps.sh /usr/local/bin/
RUN set -eu; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    set -- bsdutils hostname grep sed mawk procps libc-bin libstdc++6 libssl3t64 \
        $(echo "${EXTRA_DEPS}" | tr ',' ' '); \
    apt-get install -y --no-install-recommends "$@"; \
    bash /usr/local/bin/export-runtime-deps.sh /runtime-rootfs "$@"

FROM runtime_base
USER 0:0

# Elixir complains if runs without UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY deploy/docker/docker-entrypoint.sh /usr/bin/
COPY --from=curl_static /usr/local/bin/curl /usr/bin/curl
COPY --from=runtime_deps /runtime-rootfs/ /

# Keep the existing volume ownership contract without installing user-management
# tools. The hardened base has a shell but starts as nonroot (UID 65532).
RUN set -eu; \
    printf 'emqx:x:1000:1000::/home/emqx:/bin/bash\n' >> /etc/passwd; \
    printf 'emqx:x:1000:\n' >> /etc/group; \
    mkdir -p /home/emqx; \
    chown 1000:1000 /home/emqx; \
    ldconfig

COPY --from=builder --chown=emqx:emqx /emqx-rel /opt/

RUN mkdir -p /usr/local/bin && ln -s /opt/emqx/bin/* /usr/local/bin/

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
