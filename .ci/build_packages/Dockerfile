ARG BUILD_FROM=emqx/build-env:erl23.3.4.9-3-ubuntu20.04
FROM ${BUILD_FROM}

ARG EMQX_NAME=emqx

COPY . /emqx

WORKDIR /emqx

RUN rm -rf _build/${EMQX_NAME}/lib _build/${EMQX_NAME}-pkg/lib

RUN make ${EMQX_NAME}-zip || cat rebar3.crashdump

RUN make ${EMQX_NAME}-pkg || cat rebar3.crashdump

RUN /emqx/.ci/build_packages/tests.sh
