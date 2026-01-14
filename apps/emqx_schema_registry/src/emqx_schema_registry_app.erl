%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_app).

-behaviour(application).

-include("emqx_schema_registry.hrl").

-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
    %% Register rule engine extra functions module so that we can handle decode
    %% and encode functions called from the rule engine SQL like language
    ok = emqx_rule_engine:register_external_functions(emqx_schema_registry_serde),
    ok = mria_rlog:wait_for_shards([?SCHEMA_REGISTRY_SHARD], infinity),
    ok = emqx_schema_registry_config:add_handlers(),
    {ok, Sup} = emqx_schema_registry_sup:start_link(),
    ok = emqx_schema_registry_spb_hookcb:register_hooks(),
    {ok, Sup}.

prep_stop(_State) ->
    ok = emqx_schema_registry_spb_hookcb:unregister_hooks(),
    ok.

stop(_State) ->
    ok = emqx_rule_engine:unregister_external_functions(emqx_schema_registry_serde),
    ok = emqx_schema_registry_config:remove_handlers(),
    ok.
