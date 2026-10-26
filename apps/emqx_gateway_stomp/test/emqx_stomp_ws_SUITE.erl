%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_stomp_ws_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_stomp.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [t_heartbeat_timeout, t_traffic_keepalive].

init_per_suite(Config) ->
    application:load(emqx_gateway_stomp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, "gateway.stomp {}"},
            emqx_gateway,
            emqx_auth,
            gun
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    %% Use STOMP's packet-based heartbeat handling to exercise the generic WS connection.
    Opts = #{
        frame_mod => emqx_stomp_frame,
        chann_mod => emqx_stomp_channel,
        ctx => #{gwname => stomp},
        enable_stats => false,
        websocket => #{
            compress => false,
            check_origin_enable => false,
            proxy_address_header => <<"x-forwarded-for">>,
            proxy_port_header => <<"x-forwarded-port">>,
            supported_subprotocols => ["stomp"],
            fail_if_no_subprotocol => true
        }
    },
    Dispatch = cowboy_router:compile([
        {'_', [{"/stomp", emqx_gateway_conn_ws, Opts}]}
    ]),
    {ok, _} = cowboy:start_clear(?MODULE, [{port, 61614}], #{env => #{dispatch => Dispatch}}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = cowboy:stop_listener(?MODULE),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

-doc "Client heartbeats keep the connection alive; stopping them causes a timely disconnect.".
t_heartbeat_timeout(_) ->
    with_connection(fun(ConnPid, StreamRef) ->
        lists:foreach(
            fun(_) ->
                ?assertEqual({binary, <<$\n>>}, recv(ConnPid, StreamRef, 2000)),
                ok = gun:ws_send(ConnPid, StreamRef, {binary, <<$\r, $\n>>})
            end,
            lists:seq(1, 3)
        ),
        %% The next check permits one idle interval; the following check must close.
        Deadline = erlang:monotonic_time(millisecond) + 2500,
        await_close(ConnPid, StreamRef, Deadline)
    end).

-doc "Complete STOMP messages keep the connection alive without client heartbeats.".
t_traffic_keepalive(_) ->
    with_connection(fun(ConnPid, StreamRef) ->
        lists:foreach(
            fun(N) ->
                ?assertEqual({binary, <<$\n>>}, recv(ConnPid, StreamRef, 2000)),
                Receipt = integer_to_binary(N),
                Frame = emqx_stomp_frame:make(
                    <<"SEND">>,
                    [{<<"destination">>, <<"/ws/keepalive">>}, {<<"receipt">>, Receipt}],
                    <<"hello">>
                ),
                ok = send(ConnPid, StreamRef, Frame),
                {binary, Data} = recv(ConnPid, StreamRef, 1000),
                {ok, Reply, <<>>, _} = emqx_stomp_frame:parse(
                    Data, emqx_stomp_frame:initial_parse_state(#{})
                ),
                ?assertEqual(<<"RECEIPT">>, Reply#stomp_frame.command),
                ?assertEqual(
                    Receipt, proplists:get_value(<<"receipt-id">>, Reply#stomp_frame.headers)
                )
            end,
            lists:seq(1, 4)
        )
    end).

with_connection(Fun) ->
    {ok, ConnPid} = gun:open("127.0.0.1", 61614, #{retry => 0, protocols => [http]}),
    try
        {ok, http} = gun:await_up(ConnPid),
        StreamRef = gun:ws_upgrade(ConnPid, "/stomp", [], #{protocols => [{<<"stomp">>, gun_ws_h}]}),
        receive
            {gun_upgrade, ConnPid, StreamRef, [<<"websocket">>], _} ->
                ok;
            {gun_response, ConnPid, StreamRef, _, Status, Headers} ->
                ct:fail({websocket_upgrade_failed, Status, Headers})
        after 5000 ->
            ct:fail(websocket_upgrade_timeout)
        end,
        Connect = emqx_stomp_frame:make(<<"CONNECT">>, [
            {<<"accept-version">>, <<"1.2">>},
            {<<"host">>, <<"localhost">>},
            {<<"heart-beat">>, <<"1000,1000">>}
        ]),
        ok = send(ConnPid, StreamRef, Connect),
        {binary, Data} = recv(ConnPid, StreamRef, 5000),
        {ok, Reply, <<>>, _} = emqx_stomp_frame:parse(
            Data, emqx_stomp_frame:initial_parse_state(#{})
        ),
        ?assertEqual(<<"CONNECTED">>, Reply#stomp_frame.command),
        Fun(ConnPid, StreamRef)
    after
        gun:close(ConnPid)
    end.

send(ConnPid, StreamRef, Frame) ->
    Data = emqx_stomp_frame:serialize_pkt(Frame, #{}),
    gun:ws_send(ConnPid, StreamRef, {binary, Data}).

recv(ConnPid, StreamRef, Timeout) ->
    receive
        {gun_ws, ConnPid, StreamRef, Frame} -> Frame;
        {gun_down, ConnPid, _, Reason, _} -> {closed, Reason}
    after Timeout ->
        ct:fail(websocket_receive_timeout)
    end.

await_close(ConnPid, StreamRef, Deadline) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    case recv(ConnPid, StreamRef, Timeout) of
        close -> ok;
        {close, _, _} -> ok;
        {closed, _} -> ok;
        {binary, <<$\n>>} -> await_close(ConnPid, StreamRef, Deadline)
    end.
