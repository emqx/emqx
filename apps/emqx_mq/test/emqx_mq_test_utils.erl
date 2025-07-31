%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_test_utils).

-export([
    emqtt_connect/1,
    emqtt_pub_mq/4,
    emqtt_pub_mq/3,
    emqtt_sub_mq/2,
    emqtt_drain/0,
    emqtt_drain/1,
    emqtt_drain/2
]).

-export([create_mq/1, create_mq/2]).

-export([populate/2, populate_compacted/2]).

-include_lib("../src/emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

emqtt_connect(Opts) ->
    BaseOpts = [{proto_ver, v5}],
    {ok, C} = emqtt:start_link(BaseOpts ++ Opts),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_pub_mq(Client, Topic, Payload, CompactionKey) ->
    PubOpts = [{qos, 1}],
    Properties = #{'User-Property' => [{?MQ_COMPACTION_KEY_USER_PROPERTY, CompactionKey}]},
    emqtt:publish(Client, Topic, Properties, Payload, PubOpts).

emqtt_pub_mq(Client, Topic, Payload) ->
    PubOpts = [{qos, 1}],
    Properties = #{},
    emqtt:publish(Client, Topic, Properties, Payload, PubOpts).

emqtt_sub_mq(Client, Topic) ->
    FullTopic = <<"$q/", Topic/binary>>,
    {ok, _, _} = emqtt:subscribe(Client, {FullTopic, 1}),
    ok.

emqtt_drain() ->
    emqtt_drain(0, 0).

emqtt_drain(MinMsg) when is_integer(MinMsg) ->
    emqtt_drain(MinMsg, 0).

emqtt_drain(MinMsg, Timeout) when is_integer(MinMsg) andalso is_integer(Timeout) ->
    emqtt_drain(MinMsg, Timeout, [], 0).

emqtt_drain(MinMsg, Timeout, AccMsgs, AccNReceived) ->
    receive
        {publish, Msg} ->
            emqtt_drain(MinMsg, Timeout, [Msg | AccMsgs], AccNReceived + 1)
    after Timeout ->
        case AccNReceived >= MinMsg of
            true ->
                {ok, lists:reverse(AccMsgs)};
            false ->
                {error, {not_enough_messages, {received, AccNReceived}, {min, MinMsg}}}
        end
    end.

create_mq(Topic) ->
    create_mq(Topic, true).

create_mq(Topic, IsCompacted) ->
    SampleTopic0 = string:replace(Topic, "#", "x", all),
    SampleTopic1 = string:replace(SampleTopic0, "+", "x", all),
    SampleTopic = iolist_to_binary(SampleTopic1),
    ok = emqx_mq_registry:create(Topic, IsCompacted),
    ?retry(
        5,
        100,
        [#{topic_filter := Topic}] = emqx_mq_registry:find(SampleTopic)
    ),
    ok.

populate(N, Fun) ->
    C = emqx_mq_test_utils:emqtt_connect([]),
    lists:foreach(
        fun(I) ->
            {Topic, Payload} = Fun(I),
            emqx_mq_test_utils:emqtt_pub_mq(C, Topic, Payload)
        end,
        lists:seq(0, N - 1)
    ),
    ok = emqtt:disconnect(C).

populate_compacted(N, Fun) ->
    C = emqx_mq_test_utils:emqtt_connect([]),
    lists:foreach(
        fun(I) ->
            {Topic, Payload, CompactionKey} = Fun(I),
            emqx_mq_test_utils:emqtt_pub_mq(C, Topic, Payload, CompactionKey)
        end,
        lists:seq(0, N - 1)
    ),
    ok = emqtt:disconnect(C).
