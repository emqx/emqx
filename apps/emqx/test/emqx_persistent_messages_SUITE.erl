%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_messages_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(NOW,
    (calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]))
).
-define(DS_SHARD, <<"local">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% avoid inter-suite flakiness...
    application:stop(emqx),
    application:stop(emqx_durable_storage),
    TCApps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{tc_apps, TCApps} | Config].

end_per_suite(Config) ->
    TCApps = ?config(tc_apps, Config),
    emqx_cth_suite:stop(TCApps),
    ok.

init_per_testcase(t_session_subscription_iterators, Config) ->
    Cluster = cluster(),
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => ?config(priv_dir, Config)}),
    [{nodes, Nodes} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_session_subscription_iterators, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_messages_persisted(_Config) ->
    C1 = connect(<<?MODULE_STRING "1">>, true, 30),
    C2 = connect(<<?MODULE_STRING "2">>, false, 60),
    C3 = connect(<<?MODULE_STRING "3">>, false, undefined),
    C4 = connect(<<?MODULE_STRING "4">>, false, 0),

    CP = connect(<<?MODULE_STRING "-pub">>, true, undefined),

    {ok, _, [1]} = emqtt:subscribe(C1, <<"client/+/topic">>, qos1),
    {ok, _, [0]} = emqtt:subscribe(C2, <<"client/+/topic">>, qos0),
    {ok, _, [1]} = emqtt:subscribe(C2, <<"random/+">>, qos1),
    {ok, _, [2]} = emqtt:subscribe(C3, <<"client/#">>, qos2),
    {ok, _, [0]} = emqtt:subscribe(C4, <<"random/#">>, qos0),

    Messages = [
        M1 = {<<"client/1/topic">>, <<"1">>},
        M2 = {<<"client/2/topic">>, <<"2">>},
        M3 = {<<"client/3/topic/sub">>, <<"3">>},
        M4 = {<<"client/4">>, <<"4">>},
        M5 = {<<"random/5">>, <<"5">>},
        M6 = {<<"random/6/topic">>, <<"6">>},
        M7 = {<<"client/7/topic">>, <<"7">>},
        M8 = {<<"client/8/topic/sub">>, <<"8">>},
        M9 = {<<"random/9">>, <<"9">>},
        M10 = {<<"random/10">>, <<"10">>}
    ],

    Results = [emqtt:publish(CP, Topic, Payload, 1) || {Topic, Payload} <- Messages],

    ct:pal("Results = ~p", [Results]),

    Persisted = consume(?DS_SHARD, {['#'], 0}),

    ct:pal("Persisted = ~p", [Persisted]),

    ?assertEqual(
        % [M1, M2, M5, M7, M9, M10],
        [M1, M2, M3, M4, M5, M6, M7, M8, M9, M10],
        [{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted]
    ),

    ok.

%% TODO: test quic and ws too
t_session_subscription_iterators(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    Topic = <<"t/topic">>,
    SubTopicFilter = <<"t/+">>,
    AnotherTopic = <<"u/another-topic">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        begin
            [
                Payload1,
                Payload2,
                Payload3,
                Payload4
            ] = lists:map(
                fun(N) -> <<"hello", (integer_to_binary(N))/binary>> end,
                lists:seq(1, 4)
            ),
            ct:pal("starting"),
            {ok, Client} = emqtt:start_link([
                {port, Port},
                {clientid, ClientId},
                {proto_ver, v5}
            ]),
            {ok, _} = emqtt:connect(Client),
            ct:pal("publishing 1"),
            Message1 = emqx_message:make(Topic, Payload1),
            publish(Node1, Message1),
            receive_messages(1),
            ct:pal("subscribing 1"),
            {ok, _, [2]} = emqtt:subscribe(Client, SubTopicFilter, qos2),
            ct:pal("publishing 2"),
            Message2 = emqx_message:make(Topic, Payload2),
            publish(Node1, Message2),
            receive_messages(1),
            ct:pal("subscribing 2"),
            {ok, _, [1]} = emqtt:subscribe(Client, SubTopicFilter, qos1),
            ct:pal("publishing 3"),
            Message3 = emqx_message:make(Topic, Payload3),
            publish(Node1, Message3),
            receive_messages(1),
            ct:pal("publishing 4"),
            Message4 = emqx_message:make(AnotherTopic, Payload4),
            publish(Node1, Message4),
            IteratorIds = get_iterator_ids(Node1, ClientId),
            emqtt:stop(Client),
            #{
                messages => [Message1, Message2, Message3, Message4],
                iterator_ids => IteratorIds
            }
        end,
        fun(Results, Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            #{
                messages := [_Message1, Message2, Message3 | _],
                iterator_ids := IteratorIds
            } = Results,
            case ?of_kind(ds_session_subscription_added, Trace) of
                [] ->
                    %% Since `emqx_durable_storage' is a dependency of `emqx', it gets
                    %% compiled in "prod" mode when running emqx standalone tests.
                    ok;
                [_ | _] ->
                    ?assertMatch(
                        [
                            #{?snk_kind := ds_session_subscription_added},
                            #{?snk_kind := ds_session_subscription_present}
                        ],
                        ?of_kind(
                            [
                                ds_session_subscription_added,
                                ds_session_subscription_present
                            ],
                            Trace
                        )
                    ),
                    ok
            end,
            ?assertMatch([_], IteratorIds),
            ?assertMatch({ok, [_]}, get_all_iterator_ids(Node1)),
            ?assertMatch({ok, [_]}, get_all_iterator_ids(Node2)),
            [IteratorId] = IteratorIds,
            ReplayMessages1 = erpc:call(Node1, fun() -> consume(?DS_SHARD, IteratorId) end),
            ExpectedMessages = [Message2, Message3],
            ?assertEqual(ExpectedMessages, ReplayMessages1),
            %% Different DS shard
            ReplayMessages2 = erpc:call(Node2, fun() -> consume(?DS_SHARD, IteratorId) end),
            ?assertEqual([], ReplayMessages2),
            ok
        end
    ),
    ok.

%%

connect(ClientId, CleanStart, EI) ->
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, CleanStart},
        {properties,
            maps:from_list(
                [{'Session-Expiry-Interval', EI} || is_integer(EI)]
            )}
    ]),
    {ok, _} = emqtt:connect(Client),
    Client.

consume(Shard, Replay = {_TopicFiler, _StartMS}) ->
    {ok, It} = emqx_ds_storage_layer:make_iterator(Shard, Replay),
    consume(It);
consume(Shard, IteratorId) when is_binary(IteratorId) ->
    {ok, It} = emqx_ds_storage_layer:restore_iterator(Shard, IteratorId),
    consume(It).

consume(It) ->
    case emqx_ds_storage_layer:next(It) of
        {value, Msg, NIt} ->
            [emqx_persistent_session_ds:deserialize_message(Msg) | consume(NIt)];
        none ->
            []
    end.

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        {deliver, _Topic, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 5000 ->
        Msgs
    end.

publish(Node, Message) ->
    erpc:call(Node, emqx, publish, [Message]).

get_iterator_ids(Node, ClientId) ->
    Channel = erpc:call(Node, fun() ->
        [ConnPid] = emqx_cm:lookup_channels(ClientId),
        sys:get_state(ConnPid)
    end),
    emqx_connection:info({channel, {session, iterators}}, Channel).

app_specs() ->
    [
        emqx_durable_storage,
        {emqx, #{
            before_start => fun() ->
                emqx_app:set_config_loader(?MODULE)
            end,
            config => #{persistent_session_store => #{ds => true}},
            override_env => [{boot_modules, [broker, listeners]}]
        }}
    ].

cluster() ->
    Node1 = persistent_messages_SUITE1,
    Spec = #{
        role => core,
        join_to => emqx_cth_cluster:node_name(Node1),
        listeners => true,
        apps => app_specs()
    },
    [
        {Node1, Spec},
        {persistent_messages_SUITE2, Spec}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

get_all_iterator_ids(Node) ->
    Fn = fun(K, _V, Acc) -> [K | Acc] end,
    erpc:call(Node, fun() ->
        emqx_ds_storage_layer:foldl_iterator_prefix(?DS_SHARD, <<>>, Fn, [])
    end).
