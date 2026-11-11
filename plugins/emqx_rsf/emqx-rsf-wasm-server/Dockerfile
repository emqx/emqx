# syntax=docker/dockerfile:1
#
# emqx-rsf-wasm-server: user-defined Python functions in a WebAssembly sandbox.
#
#   docker build -t emqx-rsf-wasm-server .
#   docker run --rm -p 8080:8080 -e RSF_TOKEN=... --read-only \
#     --cap-drop ALL --security-opt no-new-privileges emqx-rsf-wasm-server

ARG PYTHON_HOST_IMAGE=python:3.14-slim
ARG CPYTHON_WASI_VERSION=3.14.7

FROM debian:bookworm-slim AS fetch
ARG TARGETARCH
ARG WASMTIME_VERSION=49.0.3
ARG WASMTIME_SHA256_AMD64=4001225413f208345b980525f31c4080e1873808b38d0ddc7918497340667fbb
ARG WASMTIME_SHA256_ARM64=880fcb026da88ad7fc1713d572b0d03d139758b7da96cd3cb8c9cbf847a521df
ARG CPYTHON_WASI_VERSION
ARG CPYTHON_WASI_SDK=24
ARG CPYTHON_WASI_SHA256=2e064d3fb8172471d39d741348efa722349c40b96301f69968dff714999c584b

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl xz-utils unzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) arch=x86_64;  sha="${WASMTIME_SHA256_AMD64}" ;; \
      arm64) arch=aarch64; sha="${WASMTIME_SHA256_ARM64}" ;; \
      *) echo "unsupported arch: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    pkg="wasmtime-v${WASMTIME_VERSION}-${arch}-linux"; \
    curl -fsSL -o wasmtime.tar.xz \
      "https://github.com/bytecodealliance/wasmtime/releases/download/v${WASMTIME_VERSION}/${pkg}.tar.xz"; \
    echo "${sha}  wasmtime.tar.xz" | sha256sum -c -; \
    tar -xf wasmtime.tar.xz; \
    install -D -m 0755 "${pkg}/wasmtime" /out/bin/wasmtime; \
    install -D -m 0644 "${pkg}/LICENSE" /out/licenses/wasmtime.LICENSE

RUN set -eux; \
    zip="python-${CPYTHON_WASI_VERSION}-wasi_sdk-${CPYTHON_WASI_SDK}.zip"; \
    curl -fsSL -o python.zip \
      "https://github.com/brettcannon/cpython-wasi-build/releases/download/v${CPYTHON_WASI_VERSION}/${zip}"; \
    echo "${CPYTHON_WASI_SHA256}  python.zip" | sha256sum -c -; \
    mkdir -p /out/python; \
    unzip -q python.zip -d /out/python; \
    mv /out/python/LICENSE /out/licenses/cpython.LICENSE; \
    cd /out/python/lib/python3.*; \
    rm -rf test idlelib tkinter turtledemo ensurepip

# Compile python.wasm ahead of time for the baseline CPU of the target
# architecture, so the image runs on any CPU of that architecture and the
# server needs no writable directory.
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) triple=x86_64-unknown-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu ;; \
    esac; \
    /out/bin/wasmtime compile --target "${triple}" \
      /out/python/python.wasm -o /out/python/python.cwasm

# The release ships no bytecode, and the runtime image is read-only. Without
# .pyc files every worker compiles the imported stdlib modules from source.
# Use the guest interpreter, so the bytecode matches its magic number.
RUN /out/bin/wasmtime run --allow-precompiled -C cache=n \
      --dir /out/python/lib::/lib /out/python/python.cwasm \
      -m compileall -q /lib

FROM ${PYTHON_HOST_IMAGE}
ARG CPYTHON_WASI_VERSION

COPY --from=fetch /out /opt/rsf
COPY server.py guest_runner.py /opt/rsf/app/

# Everything under /opt/rsf is owned by root and read-only. The guest sees
# /opt/rsf/python/lib as /lib and must not be able to change it.
RUN chown -R root:root /opt/rsf \
 && chmod -R a-w,a+rX /opt/rsf \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin rsf

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RSF_LISTEN=0.0.0.0:8080 \
    RSF_PYTHON_VERSION=${CPYTHON_WASI_VERSION}

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ["python3", "-I", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2)"]

ENTRYPOINT ["python3", "-I", "/opt/rsf/app/server.py"]
