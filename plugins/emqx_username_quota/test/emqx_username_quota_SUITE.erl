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

init_per_testcase(_Case, Config) ->
    ok = emqx_username_quota:reset(),
    Config.

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
