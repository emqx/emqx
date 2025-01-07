%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_eviction_agent
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    _ = emqx_eviction_agent:disable(foo),

    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_status(_Config) ->
    %% usage
    ok = emqx_eviction_agent_cli:cli(["foobar"]),

    %% status
    ok = emqx_eviction_agent_cli:cli(["status"]),

    ok = emqx_eviction_agent:enable(foo, undefined),

    %% status
    ok = emqx_eviction_agent_cli:cli(["status"]).
