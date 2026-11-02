%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_gateway_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-undef(PACKET).
-include_lib("emqx_gateway_stomp/include/emqx_stomp.hrl").

all() ->
    [{group, Reason} || Reason <- [no_license, quota_exceeded, expired, max_uptime_reached]].

groups() ->
    [{Reason, [], [t_stomp]} || {group, Reason} <- all()].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_gateway,
            emqx_gateway_stomp,
            {emqx_license, #{config => #{license => #{key => default}}}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(Reason, Config) ->
    [{rejection, Reason} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    {ok, _} = emqx_gateway_conf:load_gateway(stomp, #{
        <<"listeners">> => [
            #{<<"type">> => <<"tcp">>, <<"name">> => <<"default">>, <<"bind">> => Port}
        ]
    }),
    [{port, Port} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_gateway_conf:unload_gateway(stomp).

-doc "STOMP sends a readable ERROR frame on license rejection and connects after recovery.".
t_stomp(Config) ->
    Port = ?config(port, Config),
    Checker = whereis(emqx_license_checker),
    Resources = whereis(emqx_license_resources),
    stomp_connect(Port, accepted),
    with_rejection(?config(rejection, Config), fun() -> stomp_connect(Port, rejected) end),
    check_shutdown_count(Port),
    ?assertEqual(Checker, whereis(emqx_license_checker)),
    ?assertEqual(Resources, whereis(emqx_license_resources)),
    stomp_connect(Port, accepted).

check_shutdown_count(Port) ->
    ListenerId = emqx_gateway_utils:listener_id(stomp, tcp, default),
    ?retry(
        20,
        100,
        ?assertEqual(
            1,
            proplists:get_value(quota_exceeded, esockd:get_shutdown_count({ListenerId, Port}))
        )
    ).

with_rejection(Reason, Fun) ->
    {ok, Limits} = emqx_license_checker:limits(),
    Checker = whereis(emqx_license_checker),
    %% Keep the periodic check from restoring the limits during the connection attempt.
    ok = sys:suspend(Checker),
    try
        _ = sys:replace_state(Checker, fun(State) ->
            set_limits(Reason, Limits),
            State
        end),
        ?assertEqual(
            {stop, {error, ?RC_QUOTA_EXCEEDED}},
            emqx_license:check(#{clientid => clientid()}, #{})
        ),
        Fun()
    after
        _ = sys:replace_state(Checker, fun(State) ->
            true = ets:insert(emqx_license, {limits, Limits}),
            State
        end),
        ok = sys:resume(Checker)
    end,
    ?assertEqual({ok, Limits}, emqx_license_checker:limits()).

set_limits(no_license, _Limits) ->
    true = ets:delete(emqx_license, limits),
    ?assertEqual({error, no_license}, emqx_license_checker:limits());
set_limits(Reason, Limits) ->
    MaxSessions =
        case Reason of
            quota_exceeded -> 0;
            _ -> Reason
        end,
    true = ets:insert(emqx_license, {limits, Limits#{max_sessions := MaxSessions}}).

clientid() ->
    integer_to_binary(erlang:unique_integer([positive, monotonic])).

with_tcp(Port, Fun) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 5000),
    try
        Fun(Socket)
    after
        gen_tcp:close(Socket)
    end.

stomp_connect(Port, rejected) ->
    stomp_connect(Port, {rejected, <<"Login Failed: Quota exceeded">>});
stomp_connect(Port, Expected) ->
    with_tcp(Port, fun(Socket) ->
        Frame = #stomp_frame{
            command = <<"CONNECT">>,
            headers = [{<<"accept-version">>, <<"1.2">>}, {<<"host">>, <<"localhost">>}]
        },
        ok = gen_tcp:send(Socket, emqx_stomp_frame:serialize_pkt(Frame, #{})),
        {ok, Reply} = recv_stomp(Socket, emqx_stomp_frame:initial_parse_state(#{})),
        case Expected of
            accepted ->
                ?assertEqual(<<"CONNECTED">>, Reply#stomp_frame.command);
            {rejected, Text} ->
                ?assertEqual(<<"ERROR">>, Reply#stomp_frame.command),
                ?assertEqual(Text, Reply#stomp_frame.body),
                ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 5000))
        end
    end).

recv_stomp(Socket, State) ->
    {ok, Data} = gen_tcp:recv(Socket, 0, 5000),
    case emqx_stomp_frame:parse(Data, State) of
        {ok, Frame, _Rest, _Next} -> {ok, Frame};
        {more, Next} -> recv_stomp(Socket, Next)
    end.
