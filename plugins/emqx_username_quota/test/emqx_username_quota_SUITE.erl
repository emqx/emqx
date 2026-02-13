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
    ok = ensure_snapshot_started(),
    ok = emqx_username_quota:reset(),
    ok = emqx_username_quota_config:update(#{
        <<"snapshot_request_timeout_ms">> => 60000
    }),
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

t_whitelist_bypass(_Config) ->
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 1,
        <<"username_white_list">> => [#{<<"username">> => <<"vip">>}]
    }),
    ok = emqx_username_quota:register_session(<<"vip">>, <<"c1">>),
    ?assertEqual(
        ignore,
        emqx_username_quota:on_client_authenticate(
            #{username => <<"vip">>, clientid => <<"c2">>},
            ignore
        )
    ).

t_api_list_get_delete(_Config) ->
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
    {ok, 200, _Headers2, OneBody} = emqx_username_quota_api:handle(
        get,
        [
            <<"quota">>, <<"usernames">>, User
        ],
        #{}
    ),
    ?assertMatch(#{username := User, used := 2}, OneBody),
    ok = meck:new(emqx_cm, [non_strict, passthrough]),
    ok = meck:expect(emqx_cm, kick_session, fun(_ClientId) -> ok end),
    {ok, 200, _Headers3, #{kicked := 2}} = emqx_username_quota_api:handle(
        delete,
        [
            <<"quota">>, <<"usernames">>, User
        ],
        #{}
    ),
    ok = meck:unload(emqx_cm).
