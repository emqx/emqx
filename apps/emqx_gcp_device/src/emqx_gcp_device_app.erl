%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_app).

-include("emqx_gcp_device.hrl").

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    emqx_gcp_device:create_table(),
    emqx_authn:register_provider(?AUTHN_TYPE, emqx_gcp_device_authn),
    emqx_gcp_device_sup:start_link().

stop(_State) ->
    ok.
