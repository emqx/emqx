%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_config_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    execute_side_effects/1,
    cleanup_managed_ns_configs/2
]).

introduced_in() ->
    "5.9.0".

-spec execute_side_effects([emqx_mt_config:side_effect()]) -> {ok, [_Error :: map()]}.
execute_side_effects(SideEffects) ->
    emqx_cluster_rpc:multicall(emqx_mt_config, execute_side_effects_v1, [SideEffects]).

-spec cleanup_managed_ns_configs(emqx_mt:tns(), [{atom(), map()}]) -> ok.
cleanup_managed_ns_configs(Ns, ConfigList) ->
    emqx_cluster_rpc:multicall(emqx_mt_config, cleanup_managed_ns_configs_v1, [Ns, ConfigList]).
