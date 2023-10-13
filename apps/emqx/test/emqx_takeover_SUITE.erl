%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPIC, <<"t">>).
-define(CNT, 100).
-define(SLEEP, 10).

%%--------------------------------------------------------------------
%% Initial funcs

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_takeover_cluster = TC, Config) ->
    ct:timetrap({seconds, 30}),
    Nodes = emqx_cth_cluster:start(
        [
            {emqx_takeover_SUITE1, #{
                apps => [mk_emqx_appspec()]
            }},
            {emqx_takeover_SUITE2, #{
                apps => [mk_emqx_appspec()]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    [{cluster, Nodes} | Config];
init_per_testcase(_TC, Config) ->
    ct:timetrap({seconds, 30}),
    Config.

mk_emqx_appspec() ->
    {emqx, #{
        after_start => fun() ->
            % NOTE
            % This one is actually defined on `emqx_conf_schema` level, but used
            % in `emqx_broker`. Thus we have to resort to this ugly hack.
            emqx_config:force_put([rpc, mode], async)
        end
    }}.

end_per_testcase(t_takeover_cluster, Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config));
end_per_testcase(_TC, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Testcases

t_takeover(_) ->
    process_flag(trap_exit, true),
    SubClientId = <<"clientid">>,
    PubClientId = <<"clientid:pub">>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(0, Middle),
    Client2Msgs = messages(Middle, ?CNT div 2),
    Published = Client1Msgs ++ Client2Msgs,

    meck:new(emqx_cm, [non_strict, passthrough]),
    meck:expect(emqx_cm, takeover_session_end, fun(Arg) ->
        ok = timer:sleep(?SLEEP * 3),
        meck:passthrough([Arg])
    end),

    Commands = [
        {fun start_publisher/2, [PubClientId]},
        {fun start_subscriber/2, [SubClientId]},
        {fun connect_subscriber/1, []},
        {fun subscribe/3, [<<"t">>, ?QOS_1]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        {fun start_subscriber/2, [SubClientId]},
        {fun async/3, [fun connect_subscriber/1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
        {fun stop_subscriber/1, []}
    ],

    #{subscribers := [CPid2, CPid1]} = run_command_sequence(Commands, #{}),

    ?assertReceive({'EXIT', CPid1, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}),
    ?assertReceive({'EXIT', CPid2, normal}),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("middle: ~p", [Middle]),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_no_missing_messages(Published, Received),
    assert_message_order_preserved(Published, Received),

    meck:unload(emqx_cm),
    ok.

t_takeover_cluster(Config) ->
    process_flag(trap_exit, true),
    SubClientId = atom_to_binary(?FUNCTION_NAME),
    PubClientId = <<SubClientId/binary, ":pub">>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(0, Middle),
    Client2Msgs = messages(Middle, ?CNT div 2),
    Published = Client1Msgs ++ Client2Msgs,

    [Node1, Node2] = ?config(cluster, Config),
    Node1Port = get_mqtt_tcp_port(Node1),
    Node2Port = get_mqtt_tcp_port(Node2),

    Commands = [
        {fun start_publisher/3, [Node2Port, PubClientId]},
        {fun start_subscriber/3, [Node1Port, SubClientId]},
        {fun connect_subscriber/1, []},
        {fun subscribe/3, [<<"t">>, ?QOS_1]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        {fun start_subscriber/3, [Node2Port, SubClientId]},
        {fun async/3, [fun connect_subscriber/1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
        {fun stop_subscriber/1, []}
    ],

    #{subscribers := [CPid2, CPid1]} = run_command_sequence(Commands, #{}),

    ?assertReceive({'EXIT', CPid1, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}),
    ?assertReceive({'EXIT', CPid2, normal}),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    assert_no_missing_messages(Published, Received),
    assert_message_order_preserved(Published, Received),

    ok.

%%--------------------------------------------------------------------
%% Commands

run_command_sequence(Commands, CtxIn) ->
    lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        CtxIn,
        lists:flatten(Commands)
    ).

start_publisher(Ctx, ClientId) ->
    start_publisher(Ctx, get_mqtt_tcp_port(node()), ClientId).

start_publisher(Ctx, Port, ClientId) ->
    CPid = start_client(Port, ClientId),
    {ok, _} = emqtt:connect(CPid),
    _ = ct:pal("PUBLISHER ~p: connected", [CPid]),
    Ctx#{publisher => CPid}.

start_subscriber(Ctx, ClientId) ->
    start_subscriber(Ctx, get_mqtt_tcp_port(node()), ClientId).

start_subscriber(Ctx, Port, ClientId) ->
    CPid = start_client(Port, ClientId),
    Ctx#{subscribers => [CPid | maps:get(subscribers, Ctx, [])]}.

start_client(Port, ClientId) ->
    {ok, CPid} = emqtt:start_link([
        {port, Port},
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false}
    ]),
    CPid.

connect_subscriber(Ctx = #{subscribers := [CPid | _]}) ->
    {ok, _} = emqtt:connect(CPid),
    _ = ct:pal("SUBSCRIBER ~p: connected", [CPid]),
    Ctx.

subscribe(Ctx = #{subscribers := [CPid | _]}, Topic, Qos) ->
    {ok, Props, [Qos]} = emqtt:subscribe(CPid, Topic, Qos),
    _ = ct:pal("SUBSCRIBER ~p: subscribed: ~p", [CPid, Props]),
    Ctx.

async(Ctx, Fun, Args) ->
    _ = erlang:spawn_link(fun() -> apply(Fun, [Ctx | Args]) end),
    Ctx.

publish_msg(Ctx = #{publisher := CPid}, Msg) ->
    ok = timer:sleep(rand:uniform(?SLEEP)),
    ok = emqtt:publish_async(
        CPid,
        emqx_message:topic(Msg),
        emqx_message:payload(Msg),
        emqx_message:qos(Msg),
        fun(_) -> ok end
    ),
    Ctx.

stop_subscriber(Ctx = #{subscribers := [CPid | _]}) ->
    ok = timer:sleep(?SLEEP),
    ok = emqtt:stop(CPid),
    Ctx.

get_mqtt_tcp_port(Node) ->
    {_, Port} = rpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

%%--------------------------------------------------------------------
%% Helpers

assert_no_missing_messages(MsgsRef, Msgs) ->
    Missing = lists:filtermap(
        fun(MsgRef) ->
            PayloadRef = emqx_message:payload(MsgRef),
            case lists:any(fun(#{payload := Payload}) -> Payload == PayloadRef end, Msgs) of
                true -> false;
                false -> {true, MsgRef}
            end
        end,
        MsgsRef
    ),
    case Missing of
        [] ->
            ok;
        _ ->
            ct:fail("Missing messages: ~p", [Missing])
    end.

assert_message_order_preserved([], []) ->
    ok;
assert_message_order_preserved([MsgRef | RestRef], [#{payload := Payload} | Rest]) ->
    case emqx_message:payload(MsgRef) == Payload of
        false ->
            ct:fail("Message order is not correct, expected: ~p, received: ~p", [
                emqx_message:payload(MsgRef), Payload
            ]);
        true ->
            assert_message_order_preserved(RestRef, Rest)
    end.

messages(Offset, Cnt) ->
    [emqx_message:make(ct, ?QOS_1, ?TOPIC, payload(Offset + I)) || I <- lists:seq(1, Cnt)].

payload(I) ->
    % NOTE
    % Introduce randomness so that natural order is not the same as arrival order.
    iolist_to_binary(
        io_lib:format("~4.16.0B [~B]", [rand:uniform(16#10000) - 1, I])
    ).
