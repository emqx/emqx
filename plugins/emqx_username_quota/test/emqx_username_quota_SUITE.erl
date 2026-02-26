%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([mria], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    case whereis(emqx_username_quota_snapshot) of
        undefined -> ok;
        Pid -> exit(Pid, shutdown)
    end,
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    case is_cluster_watch_case(_Case) of
        true ->
            Config;
        false ->
            ok = ensure_snapshot_started(),
            ok = emqx_username_quota:reset(),
            ok = emqx_username_quota_config:update(#{
                <<"snapshot_request_timeout_ms">> => 60000
            }),
            Config
    end.

is_cluster_watch_case({testcase, Case}) ->
    is_cluster_watch_case(Case);
is_cluster_watch_case({testcase, Case, _Opts}) ->
    is_cluster_watch_case(Case);
is_cluster_watch_case(Case) when is_atom(Case) ->
    lists:prefix("t_cluster_watch_", atom_to_list(Case));
is_cluster_watch_case(_Case) ->
    false.

end_per_testcase(_Case, Config) ->
    meck:unload(),
    Config.

ensure_snapshot_started() ->
    case whereis(emqx_username_quota_snapshot) of
        undefined ->
            {ok, Pid} = emqx_username_quota_snapshot:start_link(),
            true = unlink(Pid),
            ok;
        _Pid ->
            ok
    end.

t_register_unregister_counter(_Config) ->
    User = <<"alice">>,
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    ?assertEqual(1, emqx_username_quota:session_count(User)),
    ok = emqx_username_quota:register_session(User, <<"c2">>),
    ?assertEqual(2, emqx_username_quota:session_count(User)),
    ok = emqx_username_quota:unregister_session(User, <<"c1">>),
    ?assertEqual(1, emqx_username_quota:session_count(User)),
    ok = emqx_username_quota:unregister_session(User, <<"c1">>),
    ?assertEqual(1, emqx_username_quota:session_count(User)).

t_authenticate_quota_enforced(_Config) ->
    User = <<"alice">>,
    [
        ok = emqx_username_quota:register_session(User, list_to_binary(io_lib:format("c~p", [N])))
     || N <- lists:seq(1, 100)
    ],
    ?assertEqual(
        {stop, {error, quota_exceeded}},
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"new-client">>},
            ignore
        )
    ),
    ?assertEqual(
        ignore,
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c1">>},
            ignore
        )
    ).

t_config_reject_invalid_max_sessions(_Config) ->
    ?assertMatch(
        {error, {invalid_max_sessions_per_username, 0}},
        emqx_username_quota_config:update(#{<<"max_sessions_per_username">> => 0})
    ),
    ?assertMatch(
        {error, {invalid_max_sessions_per_username, -5}},
        emqx_username_quota_config:update(#{<<"max_sessions_per_username">> => -5})
    ),
    ?assertMatch(
        {error, {invalid_max_sessions_per_username, <<"abc">>}},
        emqx_username_quota_config:update(#{<<"max_sessions_per_username">> => <<"abc">>})
    ),
    %% Settings unchanged after rejected update
    ?assertEqual(100, emqx_username_quota_config:max_sessions_per_username()).

t_override_quota_enforcement(_Config) ->
    User = <<"custom-user">>,
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 100
    }),
    %% Set a custom override of 2 sessions
    {ok, 1} = emqx_username_quota_state:set_overrides([
        #{<<"username">> => User, <<"quota">> => 2}
    ]),
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    ok = emqx_username_quota:register_session(User, <<"c2">>),
    %% Third connection should be rejected (custom limit = 2)
    ?assertEqual(
        {stop, {error, quota_exceeded}},
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c3">>},
            ignore
        )
    ),
    %% Existing client should still be allowed
    ?assertEqual(
        ignore,
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c1">>},
            ignore
        )
    ).

t_override_nolimit(_Config) ->
    User = <<"vip">>,
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 1
    }),
    {ok, 1} = emqx_username_quota_state:set_overrides([
        #{<<"username">> => User, <<"quota">> => <<"nolimit">>}
    ]),
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    %% Even though global limit is 1, nolimit override allows unlimited sessions
    ?assertEqual(
        ignore,
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c2">>},
            ignore
        )
    ),
    ok = emqx_username_quota:register_session(User, <<"c2">>),
    ?assertEqual(
        ignore,
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c3">>},
            ignore
        )
    ).

t_override_blacklist(_Config) ->
    User = <<"blocked-user">>,
    {ok, 1} = emqx_username_quota_state:set_overrides([
        #{<<"username">> => User, <<"quota">> => 0}
    ]),
    %% quota=0 means reject all new connections
    ?assertEqual(
        {stop, {error, quota_exceeded}},
        emqx_username_quota:on_client_authenticate(
            #{username => User, clientid => <<"c1">>},
            ignore
        )
    ).

t_api_overrides_crud(_Config) ->
    %% POST overrides
    {ok, 200, _, #{set := 2}} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"usernames">>],
        #{
            body => [
                #{<<"username">> => <<"u1">>, <<"quota">> => 50},
                #{<<"username">> => <<"u2">>, <<"quota">> => <<"nolimit">>}
            ]
        }
    ),
    %% GET overrides
    {ok, 200, _, #{data := Overrides}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"overrides">>],
        #{}
    ),
    ?assertEqual(2, length(Overrides)),
    %% Verify override values
    OverrideMap = maps:from_list([{maps:get(username, O), maps:get(quota, O)} || O <- Overrides]),
    ?assertEqual(50, maps:get(<<"u1">>, OverrideMap)),
    ?assertEqual(nolimit, maps:get(<<"u2">>, OverrideMap)),
    %% DELETE overrides
    {ok, 200, _, #{deleted := 1}} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"usernames">>],
        #{body => [<<"u1">>]}
    ),
    %% Verify only u2 remains
    {ok, 200, _, #{data := Remaining}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"overrides">>],
        #{}
    ),
    ?assertEqual(1, length(Remaining)),
    ?assertMatch([#{username := <<"u2">>, quota := nolimit}], Remaining).

t_api_overrides_validation(_Config) ->
    %% Invalid: missing quota
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"usernames">>],
        #{body => [#{<<"username">> => <<"u1">>}]}
    ),
    %% Invalid: empty username
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"usernames">>],
        #{body => [#{<<"username">> => <<>>, <<"quota">> => 10}]}
    ),
    %% Invalid: negative quota
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"usernames">>],
        #{body => [#{<<"username">> => <<"u1">>, <<"quota">> => -1}]}
    ),
    %% Invalid: not a list
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"usernames">>],
        #{body => <<"not a list">>}
    ),
    %% Invalid delete: not strings
    {error, 400, _, _} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"usernames">>],
        #{body => [123]}
    ).

t_api_list_get_kick(_Config) ->
    User = <<"api-user">>,
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    ok = emqx_username_quota:register_session(User, <<"c2">>),
    {ok, 200, _Headers, ListBody} = emqx_username_quota_api:handle(
        get,
        [
            <<"quota">>, <<"usernames">>
        ],
        #{
            query_string => #{<<"page">> => <<"1">>, <<"limit">> => <<"10">>}
        }
    ),
    ?assertMatch(#{data := [_ | _], meta := #{}}, ListBody),
    %% Verify list items include limit field
    [FirstItem | _] = maps:get(data, ListBody),
    ?assert(maps:is_key(limit, FirstItem)),
    {ok, 200, _Headers2, OneBody} = emqx_username_quota_api:handle(
        get,
        [
            <<"quota">>, <<"usernames">>, User
        ],
        #{}
    ),
    ?assertMatch(#{username := User, used := 2, limit := _}, OneBody),
    ok = meck:new(emqx_cm, [non_strict, passthrough]),
    ok = meck:expect(emqx_cm, kick_session, fun(_ClientId) -> ok end),
    {ok, 200, _Headers3, #{kicked := 2}} = emqx_username_quota_api:handle(
        post,
        [
            <<"kick">>, User
        ],
        #{}
    ),
    ok = meck:unload(emqx_cm).

t_api_list_busy_with_retry_cursor(_Config) ->
    RetryCursor = <<"cursor-busy">>,
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(
        emqx_username_quota_state,
        list_usernames,
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit) ->
            {error, {busy, RetryCursor}}
        end
    ),
    {error, 503, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{}}
    ),
    ?assertMatch(
        #{
            code := <<"SERVICE_UNAVAILABLE">>,
            message := <<"Snapshot owner is busy handling another request">>,
            retry_cursor := RetryCursor
        },
        Body
    ),
    ok = meck:unload(emqx_username_quota_state).

t_api_list_rebuilding_with_retry_cursor(_Config) ->
    RetryCursor = <<"cursor-rebuilding">>,
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(
        emqx_username_quota_state,
        list_usernames,
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit) ->
            {error, {rebuilding_snapshot, RetryCursor}}
        end
    ),
    {error, 503, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{}}
    ),
    ?assertMatch(
        #{
            code := <<"SERVICE_UNAVAILABLE">>,
            message := <<"Snapshot owner is rebuilding snapshot">>,
            retry_cursor := RetryCursor
        },
        Body
    ),
    ok = meck:unload(emqx_username_quota_state).

t_cluster_watch_init_terminate(_Config) ->
    ok = meck:new(ekka, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    {ok, State} = emqx_username_quota_cluster_watch:init([]),
    ?assertEqual(#{}, State),
    ok = emqx_username_quota_cluster_watch:terminate(normal, State).

t_cluster_watch_clear_for_node(_Config) ->
    ok = meck:new(emqx, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    Node = 'node1@127.0.0.1',
    ok = meck:expect(emqx, running_nodes, fun() -> [Node] end),
    ok = meck:expect(emqx_username_quota_state, clear_for_node, fun(_N) -> ok end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({clear_for_node, Node}, #{})
    ),
    ?assertNot(meck:called(emqx_username_quota_state, clear_for_node, [Node])),
    ok = meck:expect(emqx, running_nodes, fun() -> [] end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({clear_for_node, Node}, #{})
    ),
    ?assert(meck:called(emqx_username_quota_state, clear_for_node, [Node])).

t_cluster_watch_nodedown_core_and_replicant(_Config) ->
    ok = meck:new(mria_rlog, [non_strict, passthrough]),
    Node = 'node2@127.0.0.1',
    ok = meck:expect(mria_rlog, role, fun() -> core end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({nodedown, Node}, #{})
    ),
    ok = meck:expect(mria_rlog, role, fun() -> replicant end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({nodedown, Node}, #{})
    ).

t_cluster_watch_membership_events(_Config) ->
    ok = meck:new(mria_rlog, [non_strict, passthrough]),
    ok = meck:expect(mria_rlog, role, fun() -> replicant end),
    Node = 'node3@127.0.0.1',
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, {mnesia, down, Node}}, #{})
    ),
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, {node, down, Node}}, #{})
    ),
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, up}, #{})
    ),
    ?assertEqual({noreply, #{}}, emqx_username_quota_cluster_watch:handle_info(unknown, #{})).

t_cluster_watch_call_cast_code_change(_Config) ->
    State = #{key => value},
    ?assertEqual(
        {reply, ignored, State},
        emqx_username_quota_cluster_watch:handle_call(req, {self(), make_ref()}, State)
    ),
    ?assertEqual({noreply, State}, emqx_username_quota_cluster_watch:handle_cast(msg, State)),
    ?assertEqual({ok, State}, emqx_username_quota_cluster_watch:code_change(old, State, extra)).

t_cluster_watch_immediate_node_clear(_Config) ->
    ok = meck:new(ekka, [non_strict, passthrough]),
    ok = meck:new(emqx, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    ok = meck:expect(emqx, running_nodes, fun() -> [] end),
    ok = meck:expect(emqx_username_quota_state, clear_for_node, fun(_N) -> ok end),
    {ok, Pid} = emqx_username_quota_cluster_watch:start_link(),
    MRef = erlang:monitor(process, Pid),
    true = unlink(Pid),
    Node = 'node4@127.0.0.1',
    ?assertEqual(async, emqx_username_quota_cluster_watch:immediate_node_clear(Node)),
    ok = wait_until(fun() -> meck:called(emqx_username_quota_state, clear_for_node, [Node]) end),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, shutdown} -> ok
    after 1000 ->
        ?assert(false)
    end.

wait_until(Pred) ->
    wait_until(Pred, 40).

wait_until(Pred, 0) ->
    ?assert(Pred());
wait_until(Pred, N) ->
    case Pred() of
        true ->
            ok;
        false ->
            timer:sleep(25),
            wait_until(Pred, N - 1)
    end.
