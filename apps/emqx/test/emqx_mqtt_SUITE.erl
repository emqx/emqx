%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(STATS_KYES, [
    recv_pkt,
    recv_msg,
    send_pkt,
    send_msg,
    recv_oct,
    recv_cnt,
    send_oct,
    send_cnt,
    send_pend
]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        false -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

t_conn_stats(_) ->
    with_client(
        fun(CPid) ->
            Stats = emqx_connection:stats(CPid),
            ct:pal("==== stats: ~p", [Stats]),
            [?assert(proplists:get_value(Key, Stats) >= 0) || Key <- ?STATS_KYES]
        end,
        []
    ).

t_tcp_sock_passive(_) ->
    with_client(fun(CPid) -> CPid ! {tcp_passive, sock} end, []).

with_client(TestFun, _Options) ->
    ClientId = <<"t_conn">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(50),
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ct:fail({client_not_started, ClientId});
        [ChanPid] ->
            TestFun(ChanPid),
            emqtt:stop(C)
    end.

t_async_set_keepalive(init, Config) ->
    ok = snabbkaffe:start_trace(),
    Config;
t_async_set_keepalive('end', _Config) ->
    snabbkaffe:stop(),
    ok.

t_async_set_keepalive(_) ->
    case os:type() of
        {unix, darwin} ->
            do_async_set_keepalive(16#10, 16#101, 16#102);
        {unix, linux} ->
            do_async_set_keepalive(4, 5, 6);
        _ ->
            %% don't support the feature on other OS
            ok
    end.

do_async_set_keepalive(OptKeepIdle, OptKeepInterval, OptKeepCount) ->
    ClientID = <<"client-tcp-keepalive">>,
    {ok, Client} = emqtt:start_link([
        {host, "localhost"},
        {proto_ver, v5},
        {clientid, ClientID},
        {clean_start, false}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _} = ?block_until(
        #{
            ?snk_kind := insert_channel_info,
            clientid := ClientID
        },
        2000,
        100
    ),
    [Pid] = emqx_cm:lookup_channels(ClientID),
    State = emqx_connection:get_state(Pid),
    Transport = maps:get(transport, State),
    Socket = maps:get(socket, State),
    ?assert(is_port(Socket)),
    Opts = [{raw, 6, OptKeepIdle, 4}, {raw, 6, OptKeepInterval, 4}, {raw, 6, OptKeepCount, 4}],
    {ok, [
        {raw, 6, OptKeepIdle, <<Idle:32/native>>},
        {raw, 6, OptKeepInterval, <<Interval:32/native>>},
        {raw, 6, OptKeepCount, <<Probes:32/native>>}
    ]} = Transport:getopts(Socket, Opts),
    ct:pal("Idle=~p, Interval=~p, Probes=~p", [Idle, Interval, Probes]),
    emqx_connection:async_set_keepalive(os:type(), Pid, Idle + 1, Interval + 1, Probes + 1),
    {ok, _} = ?block_until(#{?snk_kind := "custom_socket_options_successfully"}, 1000),
    {ok, [
        {raw, 6, OptKeepIdle, <<NewIdle:32/native>>},
        {raw, 6, OptKeepInterval, <<NewInterval:32/native>>},
        {raw, 6, OptKeepCount, <<NewProbes:32/native>>}
    ]} = Transport:getopts(Socket, Opts),
    ?assertEqual(NewIdle, Idle + 1),
    ?assertEqual(NewInterval, Interval + 1),
    ?assertEqual(NewProbes, Probes + 1),
    emqtt:stop(Client),
    ok.
