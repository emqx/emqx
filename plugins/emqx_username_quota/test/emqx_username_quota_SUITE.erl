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
        [<<"quota">>, <<"overrides">>],
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
        [<<"quota">>, <<"overrides">>],
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

t_list_overrides_ordered(_Config) ->
    {ok, 3} = emqx_username_quota_state:set_overrides([
        #{<<"username">> => <<"charlie">>, <<"quota">> => 10},
        #{<<"username">> => <<"alice">>, <<"quota">> => 20},
        #{<<"username">> => <<"bob">>, <<"quota">> => <<"nolimit">>}
    ]),
    Result = emqx_username_quota_state:list_overrides(),
    ?assertEqual(3, length(Result)),
    %% ordered_set guarantees username-sorted order
    ?assertMatch(
        [
            #{username := <<"alice">>, quota := 20},
            #{username := <<"bob">>, quota := nolimit},
            #{username := <<"charlie">>, quota := 10}
        ],
        Result
    ),
    %% Clean up
    {ok, 3} = emqx_username_quota_state:delete_overrides(
        [<<"alice">>, <<"bob">>, <<"charlie">>]
    ),
    ?assertEqual([], emqx_username_quota_state:list_overrides()).

t_api_overrides_validation(_Config) ->
    %% Invalid: missing quota
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => [#{<<"username">> => <<"u1">>}]}
    ),
    %% Invalid: empty username
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => [#{<<"username">> => <<>>, <<"quota">> => 10}]}
    ),
    %% Invalid: negative quota
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => [#{<<"username">> => <<"u1">>, <<"quota">> => -1}]}
    ),
    %% Invalid: not a list
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => <<"not a list">>}
    ),
    %% Invalid delete: not strings
    {error, 400, _, _} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"overrides">>],
        #{body => [123]}
    ).

t_api_list_get_kick(_Config) ->
    User = <<"api-user">>,
    ok = emqx_username_quota:register_session(User, <<"c1">>),
    ok = emqx_username_quota:register_session(User, <<"c2">>),
    %% Trigger and wait for async snapshot build to complete
    {ok, 200, _Headers, ListBody} = await_list_usernames(
        #{
            <<"page">> => <<"1">>,
            <<"limit">> => <<"10">>,
            <<"used_gte">> => <<"1">>
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
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit, _UsedGte) ->
            {error, {busy, RetryCursor}}
        end
    ),
    {error, 503, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"1">>}}
    ),
    ?assertMatch(
        #{
            code := <<"SERVICE_UNAVAILABLE">>,
            message := <<"Server is busy, please retry">>,
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
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit, _UsedGte) ->
            {error, {rebuilding_snapshot, RetryCursor}}
        end
    ),
    {error, 503, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"1">>}}
    ),
    ?assertMatch(
        #{
            code := <<"SERVICE_UNAVAILABLE">>,
            message := <<"Server is busy building snapshot, please retry">>,
            snapshot_build_in_progress := true,
            retry_cursor := RetryCursor
        },
        Body
    ),
    ok = meck:unload(emqx_username_quota_state).

t_api_list_missing_used_gte(_Config) ->
    {error, 400, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"limit">> => <<"10">>}}
    ),
    ?assertMatch(
        #{
            code := <<"BAD_REQUEST">>,
            message := <<"'used_gte' query parameter is required when no cursor is provided">>
        },
        Body
    ).

t_api_list_used_gte_with_cursor_conflict(_Config) ->
    {error, 400, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"1">>, <<"cursor">> => <<"some-cursor">>}}
    ),
    ?assertMatch(
        #{
            code := <<"BAD_REQUEST">>,
            message := <<"'used_gte' must not be provided together with 'cursor'">>
        },
        Body
    ).

t_api_delete_snapshot(_Config) ->
    {ok, 200, _Headers, Body} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"snapshot">>],
        #{}
    ),
    ?assertMatch(#{status := <<"ok">>}, Body).

t_api_list_invalid_cursor(_Config) ->
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(
        emqx_username_quota_state,
        list_usernames,
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit, _UsedGte) ->
            {error, invalid_cursor}
        end
    ),
    {error, 400, _Headers, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"cursor">> => <<"bad-cursor">>}}
    ),
    ?assertMatch(
        #{
            code := <<"INVALID_CURSOR">>,
            message := <<"Cursor is invalid or references an unavailable node">>
        },
        Body
    ),
    ok = meck:unload(emqx_username_quota_state).

t_api_list_invalid_used_gte(_Config) ->
    %% Non-numeric string: treated as missing used_gte
    {error, 400, _, #{code := <<"BAD_REQUEST">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"abc">>}}
    ),
    %% Zero: rejected (must be >= 1)
    {error, 400, _, #{code := <<"BAD_REQUEST">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"0">>}}
    ),
    %% Negative: rejected
    {error, 400, _, #{code := <<"BAD_REQUEST">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"-5">>}}
    ).

t_api_list_not_core_node(_Config) ->
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(
        emqx_username_quota_state,
        list_usernames,
        fun(_RequesterPid, _DeadlineMs, _Cursor, _Limit, _UsedGte) ->
            {error, not_core_node}
        end
    ),
    {error, 404, _, Body} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"used_gte">> => <<"1">>}}
    ),
    ?assertMatch(
        #{
            code := <<"NOT_AVAILABLE">>,
            message := <<"Snapshot is only available on core nodes">>
        },
        Body
    ),
    ok = meck:unload(emqx_username_quota_state).

t_api_list_malformed_cursor(_Config) ->
    %% Garbage base64: triggers invalid_cursor
    {error, 400, _, #{code := <<"INVALID_CURSOR">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"cursor">> => <<"not-valid-base64!@#">>}}
    ),
    %% Valid base64 but wrong Erlang term structure inside
    BadCursor = base64:encode(term_to_binary({wrong, structure}), #{
        mode => urlsafe, padding => false
    }),
    {error, 400, _, #{code := <<"INVALID_CURSOR">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"cursor">> => BadCursor}}
    ),
    %% Valid cursor format but references a different node
    RemoteCursor = base64:encode(
        term_to_binary({'other@host', 1, 1, {0, <<"u">>}}),
        #{mode => urlsafe, padding => false}
    ),
    {error, 400, _, #{code := <<"INVALID_CURSOR">>}} = emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => #{<<"cursor">> => RemoteCursor}}
    ).

t_api_unknown_route(_Config) ->
    ?assertEqual(
        {error, not_found},
        emqx_username_quota_api:handle(get, [<<"unknown">>], #{})
    ),
    ?assertEqual(
        {error, not_found},
        emqx_username_quota_api:handle(put, [<<"quota">>, <<"usernames">>], #{})
    ).

t_api_overrides_validation_edge_cases(_Config) ->
    %% Float quota
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => [#{<<"username">> => <<"u1">>, <<"quota">> => 10.5}]}
    ),
    %% String quota (not "nolimit")
    {error, 400, _, _} = emqx_username_quota_api:handle(
        post,
        [<<"quota">>, <<"overrides">>],
        #{body => [#{<<"username">> => <<"u1">>, <<"quota">> => <<"100">>}]}
    ),
    %% Delete with empty string in list
    {error, 400, _, _} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"overrides">>],
        #{body => [<<"u1">>, <<>>]}
    ),
    %% Delete with non-list body
    {error, 400, _, _} = emqx_username_quota_api:handle(
        delete,
        [<<"quota">>, <<"overrides">>],
        #{body => #{<<"username">> => <<"u1">>}}
    ).

t_cluster_watch_init_terminate(_Config) ->
    ok = meck:new(ekka, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    {ok, State} = emqx_username_quota_cluster_watch:init([]),
    ?assertEqual(#{}, State),
    %% Drain the bootstrap message sent by init to self()
    receive
        bootstrap -> ok
    after 0 -> ok
    end,
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
    ok = meck:new(emqx_cm, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    ok = meck:expect(emqx, running_nodes, fun() -> [] end),
    ok = meck:expect(emqx_cm, all_channels_stream, fun(_) -> [] end),
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

%% @doc Retry list_usernames API call until snapshot is ready (async build may be in progress).
await_list_usernames(QueryString) ->
    await_list_usernames(QueryString, 40).

await_list_usernames(QueryString, 0) ->
    emqx_username_quota_api:handle(
        get,
        [<<"quota">>, <<"usernames">>],
        #{query_string => QueryString}
    );
await_list_usernames(QueryString, N) ->
    case
        emqx_username_quota_api:handle(
            get,
            [<<"quota">>, <<"usernames">>],
            #{query_string => QueryString}
        )
    of
        {ok, 200, Headers, Body} ->
            {ok, 200, Headers, Body};
        {error, 503, _, _} ->
            timer:sleep(25),
            await_list_usernames(QueryString, N - 1)
    end.
