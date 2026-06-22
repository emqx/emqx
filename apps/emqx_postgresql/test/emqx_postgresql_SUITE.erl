% %%--------------------------------------------------------------------
% %% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
% %%--------------------------------------------------------------------

-module(emqx_postgresql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_postgresql/include/emqx_postgresql.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PGSQL_HOST, "pgsql").
-define(PGSQL_RESOURCE_MOD, emqx_postgresql).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?PGSQL_HOST, ?PGSQL_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start(
                [emqx_conf, emqx_connector],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [{apps, Apps} | Config];
        false ->
            {skip, no_pgsql}
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_postgresql_SUITE">>,
        pgsql_config()
    ).

%% Verify that concurrent raw queries do not produce errors because of race conditions.
t_concurrent_raw_queries_no_errors(_Config) ->
    ResourceId = <<"emqx_postgresql_SUITE_concurrent">>,
    N = 200,
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?PGSQL_RESOURCE_MOD, pgsql_config(#{pool_size => 1})),
    {ok, _} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?PGSQL_RESOURCE_MOD,
        CheckedConfig,
        #{spawn_buffer_workers => false}
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    Self = self(),
    [
        spawn(fun() ->
            Res = emqx_resource:simple_sync_query(
                ResourceId, {query, <<"SELECT $1::integer">>, [I]}
            ),
            Self ! {result, Res}
        end)
     || I <- lists:seq(1, N)
    ],
    Results = [
        receive
            {result, R} -> R
        after 5000 -> timeout
        end
     || _ <- lists:seq(1, N)
    ],
    emqx_resource:remove_local(ResourceId),
    Errors = [R || R <- Results, not match_ok(R)],
    ?assertEqual([], Errors).

%% Verify that concurrent raw batch queries with different parameter counts do not
%% race through the unnamed prepared statement.
t_concurrent_raw_batch_queries_no_errors(_Config) ->
    ResourceId = <<"emqx_postgresql_SUITE_concurrent_batch">>,
    Table = <<"emqx_postgresql_batch_race">>,
    Sql4 =
        <<"INSERT INTO ", Table/binary, "(a, b, c, d) VALUES (${a}, ${b}, ${c}, ${d})">>,
    Sql8 =
        <<"INSERT INTO ", Table/binary,
            "(a, b, c, d, e, f, g, h) VALUES "
            "(${a}, ${b}, ${c}, ${d}, ${e}, ${f}, ${g}, ${h})">>,
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(
            ?PGSQL_RESOURCE_MOD,
            pgsql_config(#{
                pool_size => 1,
                disable_prepared_statements => true
            })
        ),
    {ok, #{state := State0}} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?PGSQL_RESOURCE_MOD,
        CheckedConfig,
        #{spawn_buffer_workers => false}
    ),
    {ok, State1} = emqx_postgresql:on_add_channel(
        ResourceId, State0, <<"stmt4">>, #{parameters => #{sql => Sql4}}
    ),
    {ok, State} = emqx_postgresql:on_add_channel(
        ResourceId, State1, <<"stmt8">>, #{parameters => #{sql => Sql8}}
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    ?assert(
        match_ok(
            emqx_resource:simple_sync_query(
                ResourceId,
                {query, <<"DROP TABLE IF EXISTS ", Table/binary>>, []}
            )
        )
    ),
    ?assert(
        match_ok(
            emqx_resource:simple_sync_query(
                ResourceId,
                {query,
                    <<"CREATE TABLE ", Table/binary,
                        " (a integer, b integer, c integer, d integer, "
                        "e integer, f integer, g integer, h integer)">>,
                    []}
            )
        )
    ),
    Self = self(),
    N = 50,
    [
        spawn(fun() ->
            Res = batch_query_with_timeout(
                ResourceId,
                batch_query(I),
                State
            ),
            Self ! {result, Res}
        end)
     || I <- lists:seq(1, N)
    ],
    Results = [
        receive
            {result, R} -> R
        after 10_000 -> timeout
        end
     || _ <- lists:seq(1, N)
    ],
    _ = emqx_resource:simple_sync_query(ResourceId, {query, <<"DROP TABLE ", Table/binary>>, []}),
    emqx_resource:remove_local(ResourceId),
    Errors = [R || R <- Results, R =/= {ok, 2}],
    ct:pal("concurrent raw batch query errors: ~p", [Errors]),
    ?assertEqual([], Errors).

%% Verify that table-existence checks do not race with raw batch execution on
%% the same connection.
t_raw_batch_not_confused_by_table_check(_Config) ->
    ResourceId = <<"emqx_postgresql_SUITE_batch_status">>,
    Table = <<"emqx_postgresql_batch_status_race">>,
    Sql4 =
        <<"INSERT INTO ", Table/binary, "(a, b, c, d) VALUES (${a}, ${b}, ${c}, ${d})">>,
    Sql18 =
        <<"INSERT INTO ", Table/binary,
            "(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) VALUES "
            "(${a}, ${b}, ${c}, ${d}, ${e}, ${f}, ${g}, ${h}, ${i}, ${j}, ${k}, "
            "${l}, ${m}, ${n}, ${o}, ${p}, ${q}, ${r})">>,
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(
            ?PGSQL_RESOURCE_MOD,
            pgsql_config(#{
                pool_size => 1,
                disable_prepared_statements => true
            })
        ),
    {ok, #{state := State0}} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?PGSQL_RESOURCE_MOD,
        CheckedConfig,
        #{spawn_buffer_workers => false}
    ),
    {ok, State1} = emqx_postgresql:on_add_channel(
        ResourceId, State0, <<"stmt4">>, #{parameters => #{sql => Sql4}}
    ),
    {ok, State} = emqx_postgresql:on_add_channel(
        ResourceId, State1, <<"stmt18">>, #{parameters => #{sql => Sql18}}
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    ?assert(
        match_ok(
            emqx_resource:simple_sync_query(
                ResourceId,
                {query, <<"DROP TABLE IF EXISTS ", Table/binary>>, []}
            )
        )
    ),
    ?assert(
        match_ok(
            emqx_resource:simple_sync_query(
                ResourceId,
                {query,
                    <<"CREATE TABLE ", Table/binary,
                        " (a integer, b integer, c integer, d integer, "
                        "e integer, f integer, g integer, h integer, "
                        "i integer, j integer, k integer, l integer, "
                        "m integer, n integer, o integer, p integer, "
                        "q integer, r integer)">>,
                    []}
            )
        )
    ),
    TestPid = self(),
    meck:new(epgsql, [passthrough, no_link]),
    try
        install_interleaving_meck(TestPid),
        _StatusPid =
            spawn_link(fun() ->
                Res = emqx_postgresql:on_get_channel_status(ResourceId, <<"stmt18">>, State),
                TestPid ! {status_result, Res}
            end),
        StatusWaiterPid =
            receive
                {status_has_conn, Pid1} ->
                    Pid1
            after 5_000 ->
                error(status_check_did_not_reach_parse)
            end,
        ?assert(lists:member(StatusWaiterPid, ecpool_worker_pids(State))),
        BatchPid =
            spawn_link(fun() ->
                receive
                    run_batch_query ->
                        ok
                after 5_000 ->
                    error(batch_query_not_started)
                end,
                Res = emqx_postgresql:on_batch_query(
                    ResourceId,
                    [
                        {<<"stmt4">>, #{a => 1, b => 2, c => 3, d => 4}},
                        {<<"stmt4">>, #{a => 5, b => 6, c => 7, d => 8}}
                    ],
                    State
                ),
                TestPid ! {batch_result, Res}
            end),
        trace_batch_handover(BatchPid),
        try
            BatchPid ! run_batch_query,
            wait_for_batch_handover(BatchPid),
            StatusWaiterPid ! run_status_parse,
            receive
                {status_parsed, _} ->
                    ok
            after 5_000 ->
                error(status_check_did_not_finish_parse)
            end,
            receive
                {batch_parsed, BatchWaiterPid} ->
                    BatchWaiterPid ! run_batch
            after 5_000 ->
                error(batch_query_did_not_reach_parse)
            end
        after
            untrace_batch_handover(BatchPid)
        end,
        ?assertEqual(
            connected,
            receive
                {status_result, StatusRes} -> StatusRes
            after 5_000 -> timeout
            end
        ),
        ?assertEqual(
            {ok, 2},
            receive
                {batch_result, BatchRes} -> BatchRes
            after 5_000 -> timeout
            end
        )
    after
        meck:unload(epgsql),
        _ = emqx_resource:simple_sync_query(
            ResourceId, {query, <<"DROP TABLE ", Table/binary>>, []}
        ),
        emqx_resource:remove_local(ResourceId)
    end.

perform_lifecycle_check(ResourceId, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?PGSQL_RESOURCE_MOD, InitialConfig),
    {ok, #{
        state := #{pool_name := PoolName} = State,
        status := InitialStatus
    }} =
        emqx_resource:create_local(
            ResourceId,
            ?CONNECTOR_RESOURCE_GROUP,
            ?PGSQL_RESOURCE_MOD,
            CheckedConfig,
            #{}
        ),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    % % Perform query as further check that the resource is working as expected
    ?assertMatch({ok, _, [{1}]}, emqx_resource:query(ResourceId, test_query_no_params())),
    ?assertMatch({ok, _, [{1}]}, emqx_resource:query(ResourceId, test_query_with_params())),
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(ResourceId)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    ?assertMatch({ok, _, [{1}]}, emqx_resource:query(ResourceId, test_query_no_params())),
    ?assertMatch({ok, _, [{1}]}, emqx_resource:query(ResourceId, test_query_with_params())),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceId)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceId)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

pgsql_config() ->
    pgsql_config(#{}).

pgsql_config(Overrides) ->
    PoolSize = maps:get(pool_size, Overrides, 8),
    #{
        <<"config">> => #{
            <<"auto_reconnect">> => true,
            <<"database">> => <<"mqtt">>,
            <<"disable_prepared_statements">> => maps:get(
                disable_prepared_statements, Overrides, false
            ),
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => PoolSize,
            <<"server">> => iolist_to_binary([
                ?PGSQL_HOST, ":", integer_to_list(?PGSQL_DEFAULT_PORT)
            ])
        }
    }.

test_query_no_params() ->
    {query, <<"SELECT 1">>}.

test_query_with_params() ->
    {query, <<"SELECT $1::integer">>, [1]}.

batch_query(I) when I rem 2 =:= 0 ->
    [
        {<<"stmt4">>, #{a => I, b => I + 1, c => I + 2, d => I + 3}},
        {<<"stmt4">>, #{a => I, b => I + 1, c => I + 2, d => I + 3}}
    ];
batch_query(I) ->
    [
        {<<"stmt8">>, #{
            a => I,
            b => I + 1,
            c => I + 2,
            d => I + 3,
            e => I + 4,
            f => I + 5,
            g => I + 6,
            h => I + 7
        }},
        {<<"stmt8">>, #{
            a => I,
            b => I + 1,
            c => I + 2,
            d => I + 3,
            e => I + 4,
            f => I + 5,
            g => I + 6,
            h => I + 7
        }}
    ].

batch_query_with_timeout(ResourceId, Batch, State) ->
    try
        emqx_utils:nolink_apply(
            fun() ->
                emqx_postgresql:on_batch_query(ResourceId, Batch, State)
            end,
            3_000
        )
    catch
        exit:timeout ->
            timeout;
        Class:Reason:Stacktrace ->
            {exception, Class, Reason, Stacktrace}
    end.

ecpool_worker_pids(#{pool_name := PoolName}) ->
    [WorkerPid || {_WorkerName, WorkerPid} <- ecpool:workers(PoolName)].

trace_batch_handover(BatchPid) ->
    _ = erlang:trace_pattern({ecpool_worker, exec, 3}, true, [local]),
    _ = erlang:trace(BatchPid, true, [call]),
    ok.

untrace_batch_handover(BatchPid) ->
    _ = erlang:trace(BatchPid, false, [call]),
    _ = erlang:trace_pattern({ecpool_worker, exec, 3}, false, [local]),
    ok.

install_interleaving_meck(TestPid) ->
    meck:expect(epgsql, parse2, fun
        (Conn, "", SQL, []) ->
            TestPid ! {status_has_conn, self()},
            receive
                run_status_parse ->
                    ok
            after 5_000 ->
                error(status_parse_timeout)
            end,
            Res = meck:passthrough([Conn, "", SQL, []]),
            TestPid ! {status_parsed, self()},
            Res;
        (Conn, Name, SQL, Types) ->
            meck:passthrough([Conn, Name, SQL, Types])
    end),
    meck:expect(epgsql, execute_batch, fun
        (Conn, #statement{} = Statement, Batch) ->
            meck:passthrough([Conn, Statement, Batch]);
        (Conn, SQL, Batch) ->
            case epgsql:parse(Conn, SQL) of
                {ok, #statement{} = Statement} ->
                    TestPid ! {batch_parsed, self()},
                    receive
                        run_batch ->
                            ok
                    after 5_000 ->
                        error(batch_parse_timeout)
                    end,
                    epgsql:execute_batch(Conn, Statement, Batch);
                Error ->
                    Error
            end
    end).

wait_for_batch_handover(BatchPid) ->
    receive
        {trace, BatchPid, call,
            {ecpool_worker, exec, [_WorkerPid, {emqx_postgresql, execute_batch, _Args}, _Timeout]}} ->
            ok
    after 5_000 ->
        error(batch_query_did_not_reach_handover)
    end.

match_ok({ok, _, _}) -> true;
match_ok(_) -> false.
