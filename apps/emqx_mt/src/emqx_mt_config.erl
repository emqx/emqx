%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_config).

-export([
    get_max_sessions/1
]).

%% Internal APIs for tests
-export([
    tmp_set_default_max_sessions/1
]).

%% @doc Get the maximum number of sessions allowed for the given namespace.
%% TODO: support per-ns configs
-spec get_max_sessions(emqx_mt:tns()) -> non_neg_integer() | infinity.
get_max_sessions(_Tns) ->
    emqx_config:get([multi_tenancy, default_max_sessions]).

%% @doc Temporarily set the maximum number of sessions allowed for the given namespace.
-spec tmp_set_default_max_sessions(non_neg_integer() | infinity) -> ok.
tmp_set_default_max_sessions(Max) ->
    emqx_config:put([multi_tenancy, default_max_sessions], Max).
