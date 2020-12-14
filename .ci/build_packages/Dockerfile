ARG BUILD_FROM=emqx/build-env:erl22.3-ubuntu20.04
FROM ${BUILD_FROM}

ARG EMQX_NAME=emqx

COPY . /emqx

WORKDIR /emqx

RUN sed -i "/^export LC_ALL=.*$/d" Makefile && make ${EMQX_NAME}-pkg || cat rebar3.crashdump

RUN /emqx/.ci/build_packages/tests.sh
