%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_olp_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("lc/include/lc.hrl").

-define(TCP_PORT, 1883).
-define(TCP_SOCKET_PORT, 20841).
-define(WS_PORT, 20842).
-define(QUIC_PORT, 20843).

-define(TRANSPORTS, [tcp, tcp_socket, ws, quic]).

all() -> emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() -> emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = emqx_common_test_helpers:gen_ca(PrivDir, "ca"),
    _ = emqx_common_test_helpers:gen_host_cert("server", "ca", PrivDir, #{}),
    ListenerConf = io_lib:format(
        "listeners.tcp.sock.bind = ~b\n"
        "listeners.tcp.sock.tcp_backend = socket\n"
        "listeners.ws.default.bind = ~b\n"
        "listeners.quic.default {\n"
        "  enable = true\n"
        "  bind = ~b\n"
        "  ssl_options {\n"
        "    cacertfile = \"~ts\"\n"
        "    certfile = \"~ts\"\n"
        "    keyfile = \"~ts\"\n"
        "  }\n"
        "}",
        [
            ?TCP_SOCKET_PORT,
            ?WS_PORT,
            ?QUIC_PORT,
            filename:join(PrivDir, "ca.pem"),
            filename:join(PrivDir, "server.pem"),
            filename:join(PrivDir, "server.key")
        ]
    ),
    Apps = emqx_cth_suite:start(
        [quicer, {emqx, ListenerConf}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    OldSch = erlang:system_flag(schedulers_online, 1),
    [{apps, Apps}, {old_sch, OldSch} | Config].

end_per_suite(Config) ->
    erlang:system_flag(schedulers_online, ?config(old_sch, Config)),
    emqx_cth_suite:stop(?config(apps, Config)).

end_per_testcase(_, _Config) ->
    meck:unload(),
    emqx_config:put([overload_protection, enable], false),
    emqx_config:put([overload_protection, backoff_new_conn], true).

init_per_testcase(_, Config) ->
    emqx_olp:enable(),
    case wait_for(fun() -> lc_sup:whereis_runq_flagman() end, 10) of
        true -> ok;
        false -> ct:fail("runq_flagman is not up")
    end,
    LCConf = load_ctl:get_config(),
    ok = load_ctl:put_config(LCConf#{
        ?RUNQ_MON_F0 => true,
        ?RUNQ_MON_F1 => 5,
        ?RUNQ_MON_F2 => 1,
        ?RUNQ_MON_T1 => 200,
        ?RUNQ_MON_T2 => 50,
        ?RUNQ_MON_C1 => 2,
        ?RUNQ_MON_F5 => -1
    }),
    Config.

%% Test that olp could be enabled/disabled globally
t_disable_enable(_Config) ->
    Old = load_ctl:whereis_runq_flagman(),
    ok = emqx_olp:disable(),
    ?assert(not is_process_alive(Old)),
    {ok, Pid} = emqx_olp:enable(),
    ?assert(is_process_alive(Pid)).

%% Test that overload detection works
t_is_overloaded(_Config) ->
    meck:new(load_ctl, [passthrough]),
    meck:expect(load_ctl, is_overloaded, fun() -> true end),
    ?assert(emqx_olp:is_overloaded()),
    meck:expect(load_ctl, is_overloaded, fun() -> false end),
    ?assert(not emqx_olp:is_overloaded()),
    meck:unload(load_ctl).

%% Test that new conn is rejected when olp is enabled
t_overloaded_conn(_Config) ->
    process_flag(trap_exit, true),
    ?assert(erlang:is_process_alive(load_ctl:whereis_runq_flagman())),
    emqx_config:put([overload_protection, enable], true),
    meck:new(load_ctl, [passthrough]),
    meck:expect(load_ctl, is_overloaded, fun() -> true end),
    ?assert(emqx_olp:is_overloaded()),
    true = emqx:is_running(node()),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, "myclient"}]),
    ?assertNotMatch({ok, _Pid}, emqtt:connect(C)),
    meck:unload(load_ctl).

%% Test that new conn is rejected when olp is enabled
t_overload_cooldown_conn(Config) ->
    t_overloaded_conn(Config),
    meck:new(load_ctl, [passthrough]),
    meck:expect(load_ctl, is_overloaded, fun() -> false end),
    ?assert(not emqx_olp:is_overloaded()),
    true = emqx:is_running(node()),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, "myclient"}]),
    ?assertMatch({ok, _Pid}, emqtt:connect(C)),
    emqtt:stop(C),
    meck:unload(load_ctl).

t_backoff_new_conn_disabled() ->
    [{matrix, true}].

-doc "A new connection is accepted while overloaded when backoff_new_conn is false.".
t_backoff_new_conn_disabled(matrix) ->
    [[T] || T <- ?TRANSPORTS];
t_backoff_new_conn_disabled(TCConfig) when is_list(TCConfig) ->
    ok = mock_runq_overloaded(true),
    emqx_config:put([overload_protection, enable], true),
    emqx_config:put([overload_protection, backoff_new_conn], false),
    ?assert(emqx_olp:is_overloaded()),
    ?assertEqual(ok, try_connect(TCConfig)).

try_connect(TCConfig) ->
    Transport = emqx_common_test_helpers:get_matrix_prop(TCConfig, ?TRANSPORTS, tcp),
    {ConnFun, Opts0} = connect_opts(Transport),
    Opts = maps:merge(#{host => "127.0.0.1", connect_timeout => 5}, Opts0),
    {ok, Client} = emqtt:start_link(Opts),
    true = erlang:unlink(Client),
    case ConnFun(Client) of
        {ok, _} ->
            ok = emqtt:disconnect(Client);
        {error, Reason} ->
            %% The client may have exited already.
            try
                emqtt:stop(Client)
            catch
                exit:_ -> ok
            end,
            {error, Reason}
    end.

connect_opts(tcp) ->
    {fun emqtt:connect/1, #{port => ?TCP_PORT}};
connect_opts(tcp_socket) ->
    {fun emqtt:connect/1, #{port => ?TCP_SOCKET_PORT}};
connect_opts(ws) ->
    {fun emqtt:ws_connect/1, #{port => ?WS_PORT}};
connect_opts(quic) ->
    {fun emqtt:quic_connect/1, #{
        port => ?QUIC_PORT, ssl => true, ssl_opts => [{verify, verify_none}]
    }}.

mock_runq_overloaded(IsOverloaded) ->
    ok = meck:new(load_ctl, [passthrough, no_history]),
    ok = meck:expect(load_ctl, is_overloaded, fun() -> IsOverloaded end).

wait_for(_Fun, 0) ->
    false;
wait_for(Fun, Retry) ->
    case is_pid(Fun()) of
        true ->
            true;
        false ->
            timer:sleep(10),
            wait_for(Fun, Retry - 1)
    end.
