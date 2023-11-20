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
-include_lib("emqx/include/emqx_mqtt.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-define(PERSISTENT_MESSAGE_DB, emqx_persistent_message).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% avoid inter-suite flakiness...
    %% TODO: remove after other suites start to use `emx_cth_suite'
    application:stop(emqx),
    application:stop(emqx_durable_storage),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_session_subscription_iterators = TestCase, Config) ->
    Cluster = cluster(),
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}),
    [{nodes, Nodes} | Config];
init_per_testcase(TestCase, Config) ->
    ok = emqx_ds:drop_db(?PERSISTENT_MESSAGE_DB),
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{apps, Apps} | Config].

end_per_testcase(t_session_subscription_iterators, Config) ->
    Nodes = ?config(nodes, Config),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = emqx_cth_cluster:stop(Nodes),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, Config) ->
    Apps = proplists:get_value(apps, Config, []),
    emqx_common_test_helpers:call_janitor(60_000),
    clear_db(),
    emqx_cth_suite:stop(Apps),
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
        _M3 = {<<"client/3/topic/sub">>, <<"3">>},
        _M4 = {<<"client/4">>, <<"4">>},
        M5 = {<<"random/5">>, <<"5">>},
        _M6 = {<<"random/6/topic">>, <<"6">>},
        M7 = {<<"client/7/topic">>, <<"7">>},
        _M8 = {<<"client/8/topic/sub">>, <<"8">>},
        M9 = {<<"random/9">>, <<"9">>},
        M10 = {<<"random/10">>, <<"10">>}
    ],

    Results = [emqtt:publish(CP, Topic, Payload, 1) || {Topic, Payload} <- Messages],

    ct:pal("Results = ~p", [Results]),
    timer:sleep(2000),

    Persisted = consume(['#'], 0),

    ct:pal("Persisted = ~p", [Persisted]),

    ?assertEqual(
        lists:sort([M1, M2, M5, M7, M9, M10]),
        lists:sort([{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted])
    ),

    ok.

t_messages_persisted_2(_Config) ->
    Prefix = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(<<Prefix/binary, "1">>, _CleanStart0 = true, _EI0 = 30),
    CP = connect(<<Prefix/binary, "-pub">>, _CleanStart1 = true, _EI1 = undefined),
    T = fun(T0) -> <<Prefix/binary, T0/binary>> end,

    %% won't be persisted
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"0">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"1">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"2">>, 1),

    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C1, T(<<"client/+/topic">>), qos1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"3">>, 1),
    %% will be persisted
    {ok, #{reason_code := ?RC_SUCCESS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"4">>, 1),
    {ok, #{reason_code := ?RC_SUCCESS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"5">>, 1),

    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C1, T(<<"client/+/topic">>)),
    %% won't be persisted
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"6">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"7">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"8">>, 1),

    timer:sleep(2000),

    Persisted = consume(['#'], 0),

    ct:pal("Persisted = ~p", [Persisted]),

    ?assertEqual(
        lists:sort([
            {T(<<"client/1/topic">>), <<"4">>},
            {T(<<"client/2/topic">>), <<"5">>}
        ]),
        lists:sort([{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted])
    ),

    ok.

%% TODO: test quic and ws too
t_session_subscription_iterators(Config) ->
    [Node1, _Node2] = ?config(nodes, Config),
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
            Client = connect(#{
                clientid => ClientId,
                port => Port,
                properties => #{'Session-Expiry-Interval' => 300}
            }),
            ct:pal("publishing 1"),
            Message1 = emqx_message:make(Topic, Payload1),
            publish(Node1, Message1),
            ct:pal("subscribing 1"),
            {ok, _, [2]} = emqtt:subscribe(Client, SubTopicFilter, qos2),
            ct:pal("publishing 2"),
            Message2 = emqx_message:make(Topic, Payload2),
            publish(Node1, Message2),
            % TODO: no incoming publishes at the moment
            % [_] = receive_messages(1),
            ct:pal("subscribing 2"),
            {ok, _, [1]} = emqtt:subscribe(Client, SubTopicFilter, qos1),
            ct:pal("publishing 3"),
            Message3 = emqx_message:make(Topic, Payload3),
            publish(Node1, Message3),
            % [_] = receive_messages(1),
            ct:pal("publishing 4"),
            Message4 = emqx_message:make(AnotherTopic, Payload4),
            publish(Node1, Message4),
            emqtt:stop(Client),
            #{
                messages => [Message1, Message2, Message3, Message4]
            }
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
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
            ok
        end
    ),
    ok.

%%

connect(ClientId, CleanStart, EI) ->
    connect(#{
        clientid => ClientId,
        clean_start => CleanStart,
        properties => maps:from_list(
            [{'Session-Expiry-Interval', EI} || is_integer(EI)]
        )
    }).

connect(Opts0 = #{}) ->
    Defaults = #{proto_ver => v5},
    Opts = maps:to_list(emqx_utils_maps:deep_merge(Defaults, Opts0)),
    {ok, Client} = emqtt:start_link(Opts),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    {ok, _} = emqtt:connect(Client),
    Client.

consume(TopicFilter, StartMS) ->
    Streams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartMS),
    lists:flatmap(
        fun({_Rank, Stream}) ->
            {ok, It} = emqx_ds:make_iterator(?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartMS),
            consume(It)
        end,
        Streams
    ).

consume(It) ->
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, It, 100) of
        {ok, _NIt, _Msgs = []} ->
            [];
        {ok, NIt, Msgs} ->
            Msgs ++ consume(NIt);
        {ok, end_of_stream} ->
            []
    end.

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs])
    after 5_000 ->
        Msgs
    end.

publish(Node, Message) ->
    erpc:call(Node, emqx, publish, [Message]).

app_specs() ->
    [
        emqx_durable_storage,
        {emqx, "session_persistence {enable = true}"}
    ].

cluster() ->
    Spec = #{role => core, apps => app_specs()},
    [
        {persistent_messages_SUITE1, Spec},
        {persistent_messages_SUITE2, Spec}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

clear_db() ->
    ok = emqx_ds:drop_db(?PERSISTENT_MESSAGE_DB),
    ok.
