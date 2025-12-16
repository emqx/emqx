# https://github.com/emqx/emqx-builder
export EMQX_BUILDER_VSN=6.0-6
export OTP_VSN=28.2-1
export ELIXIR_VSN=1.19.1
export EMQX_BUILDER=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-${OTP_VSN}-ubuntu24.04
export EMQX_DOCKER_BUILD_FROM=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-${OTP_VSN}-debian13
export EMQX_DOCKER_RUN_FROM=debian:13-slim
export QUICER_DOWNLOAD_FROM_RELEASE=1
