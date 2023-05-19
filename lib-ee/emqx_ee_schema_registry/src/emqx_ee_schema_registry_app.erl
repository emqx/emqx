%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_schema_registry_app).

-behaviour(application).

-include("emqx_ee_schema_registry.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = mria_rlog:wait_for_shards([?SCHEMA_REGISTRY_SHARD], infinity),
    emqx_conf:add_handler(?CONF_KEY_PATH, emqx_ee_schema_registry),
    emqx_ee_schema_registry_sup:start_link().

stop(_State) ->
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    ok.
