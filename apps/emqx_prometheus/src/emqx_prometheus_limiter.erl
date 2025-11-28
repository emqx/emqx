%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_limiter).

%% API
-export([
    connect_api/0,

    create_api_limiter_group/0,
    update_api_limiter_group/1,
    delete_api_limiter_group/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

connect_api() ->
    LimiterId = {api_group(), limiter_name()},
    emqx_limiter:connect(LimiterId).

create_api_limiter_group() ->
    LimiterConfigs = get_api_limiter_configs(),
    emqx_limiter:create_group(emqx_limiter_shared, api_group(), LimiterConfigs).

update_api_limiter_group(Config) ->
    ?tp("prometheus_api_limiter_updated", #{}),
    LimiterConfigs = to_api_limiter_opts(Config),
    emqx_limiter:update_group(api_group(), LimiterConfigs).

delete_api_limiter_group() ->
    emqx_limiter:delete_group(api_group()).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

api_group() ->
    {prometheus, api}.

limiter_name() ->
    all_ns.

get_api_limiter_configs() ->
    Config = emqx_config:get([prometheus, namespaced_metrics_limiter], #{rate => {1, 5_000}}),
    to_api_limiter_opts(Config).

to_api_limiter_opts(Config) ->
    #{rate := Rate} = Config,
    [{limiter_name(), emqx_limiter:config_from_rate(Rate)}].
