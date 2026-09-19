%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_resource_pool_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("emqx/include/asserts.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [emqx_resource],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    {apps, Apps} = lists:keyfind(apps, 1, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connect(Opts0) ->
    Opts = maps:from_list(Opts0),
    #{
        test_pid := TestPid,
        agent := Agent
    } = Opts,
    Client = spawn_link(fun() ->
        receive
        after infinity -> unreachable
        end
    end),
    Ctx = #{
        worker => self(),
        client => Client,
        opts => Opts
    },
    TestPid ! {client_starting, Ctx},
    case emqx_utils_agent:get(Agent) of
        continue ->
            ct:pal("worker ~p continuing with success", [self()]),
            {ok, Client};
        {return, Return} ->
            ct:pal("worker ~p returning: ~p", [self(), Return]),
            Return
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_client_dead_reason(_TCConfig) ->
    Pool = ?FUNCTION_NAME,
    on_exit(fun() -> emqx_resource_pool:stop(Pool) end),
    Mod = ?MODULE,
    {ok, Agent} = emqx_utils_agent:start_link(continue),
    Opts = [
        {test_pid, self()},
        {agent, Agent},
        {auto_reconnect, 2},
        {pool_size, 1}
    ],
    ?assertEqual(ok, emqx_resource_pool:start(Pool, Mod, Opts)),
    {client_starting, Ctx} = ?assertReceive({client_starting, _}),
    #{
        worker := WorkerPid,
        client := ClientPid
    } = Ctx,
    MRef = monitor(process, ClientPid),
    %% a second reason for further attempts
    emqx_utils_agent:set(Agent, {return, {error, permission_denied}}),
    exit(ClientPid, econnrefused),
    ?assertReceive({'DOWN', MRef, _, _, _}),
    ?assertMatch(
        {?status_disconnected, #{reason := econnrefused, time_since_observed_ms := _}},
        emqx_resource_pool:common_health_check_workers(Pool, #{
            timeout => 1_000,
            check_fn => fun(_) -> ok end,
            run_on => worker
        })
    ),
    ?assertMatch(
        {error, {disconnected, #{reason := econnrefused, time_since_observed_ms := _}}},
        ecpool_worker:client(WorkerPid)
    ),
    %% auto reconnect with second reason
    ?assertReceive({client_starting, _}, 5_000),
    ?retry(
        100,
        5,
        ?assertMatch(
            {?status_disconnected, #{reason := permission_denied, time_since_observed_ms := _}},
            emqx_resource_pool:common_health_check_workers(Pool, #{
                timeout => 1_000,
                check_fn => fun(_) -> ok end,
                run_on => worker
            })
        )
    ),
    ok.
