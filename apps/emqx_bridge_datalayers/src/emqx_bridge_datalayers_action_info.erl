%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-import(emqx_utils_conv, [bin/1]).

-define(SCHEMA_MODULE, emqx_bridge_datalayers).

action_type_name() -> datalayers.

connector_type_name() -> datalayers.

schema_module() -> ?SCHEMA_MODULE.

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).

schema_keys(Name) ->
    [bin(Key) || Key <- proplists:get_keys(?SCHEMA_MODULE:fields(Name))].
