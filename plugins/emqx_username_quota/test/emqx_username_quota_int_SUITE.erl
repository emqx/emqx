%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_int_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    EmqxConfig = #{
        listeners => #{
            tcp => #{default => #{bind => 0}},
            ssl => #{default => #{bind => 0}},
            ws => #{default => #{bind => 0}},
            wss => #{default => #{bind => 0}}
        }
    },
    Apps = emqx_cth_suite:start(
        [emqx_conf, {emqx, #{config => EmqxConfig}}, emqx_username_quota], #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    {ok, _} = application:ensure_all_started(emqx_username_quota),
    true = is_authn_hook_registered(),
    ok = emqx_username_quota_config:update(#{<<"max_sessions_per_username">> => 100}),
    ok = emqx_username_quota:reset(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

t_use_emqx_api_without_mocks(_Config) ->
    User = <<"alice">>,
    ?assertEqual(true, emqx:is_running()),
    ?assert(is_integer(emqx:get_config([mqtt, max_mqueue_len], 0))),
    [
        ok = emqx_hooks:run(
            'session.created',
            [#{username => User, clientid => list_to_binary(io_lib:format("c~p", [N]))}, #{}]
        )
     || N <- lists:seq(1, 100)
    ],
    ok = wait_count(User, 100, 400),
    ?assertEqual(
        {error, quota_exceeded},
        emqx_hooks:run_fold(
            'client.authenticate',
            [#{username => User, clientid => <<"new-client">>}],
            ignore
        )
    ),
    ok = emqx_hooks:run('session.terminated', [
        #{username => User, clientid => <<"c1">>}, normal, #{}
    ]),
    ok = wait_count(User, 99, 400),
    ?assertEqual(
        ignore,
        emqx_hooks:run_fold(
            'client.authenticate',
            [#{username => User, clientid => <<"new-client">>}],
            ignore
        )
    ),
    ok.

is_authn_hook_registered() ->
    lists:any(
        fun(Callback) ->
            emqx_hooks:callback_action(Callback) =:=
                {emqx_username_quota, on_client_authenticate, []}
        end,
        emqx_hooks:lookup('client.authenticate')
    ).

wait_count(User, Expected, Retries) when Retries > 0 ->
    case emqx_username_quota:session_count(User) of
        Expected ->
            ok;
        _ ->
            timer:sleep(25),
            wait_count(User, Expected, Retries - 1)
    end;
wait_count(User, Expected, 0) ->
    ?assertEqual(Expected, emqx_username_quota:session_count(User)).
