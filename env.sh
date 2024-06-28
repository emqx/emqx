# https://github.com/emqx/emqx-builder
export EMQX_BUILDER_VSN=5.3-8
export OTP_VSN=26.2.5-2
export ELIXIR_VSN=1.15.7
export EMQX_BUILDER=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-${OTP_VSN}-ubuntu22.04
export EMQX_DOCKER_BUILD_FROM=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-${OTP_VSN}-debian12
export EMQX_DOCKER_RUN_FROM=public.ecr.aws/debian/debian:stable-20240612-slim
# Dashboard version
# from https://github.com/emqx/emqx-dashboard5
export EMQX_DASHBOARD_VERSION=v1.9.1
export EMQX_EE_DASHBOARD_VERSION=e1.7.1

export QUICER_DOWNLOAD_FROM_RELEASE=1
