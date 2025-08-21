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

-export([create_mq/1]).

-export([populate/2, populate_compacted/2]).

-export([cleanup_mqs/0, stop_all_consumers/0]).

-export([compile_variform/1]).

-export([config/0]).

-include_lib("../src/emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").

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

create_mq(Topic) when is_binary(Topic) ->
    create_mq(#{topic_filter => Topic});
create_mq(#{topic_filter := TopicFilter} = MQ0) ->
    Default = #{
        is_compacted => false,
        consumer_max_inactive => 1000,
        ping_interval => 5000,
        redispatch_interval => 100,
        dispatch_strategy => random,
        local_max_inflight => 4,
        busy_session_retry_interval => 100,
        stream_max_buffer_size => 10,
        stream_max_unacked => 5,
        consumer_persistence_interval => 1000,
        data_retention_period => 3600_000
    },
    MQ1 = maps:merge(Default, MQ0),

    SampleTopic0 = string:replace(TopicFilter, "#", "x", all),
    SampleTopic1 = string:replace(SampleTopic0, "+", "x", all),
    SampleTopic = iolist_to_binary(SampleTopic1),
    {ok, MQ} = emqx_mq_registry:create(MQ1),
    ?retry(
        5,
        100,
        ?assert(
            lists:any(
                fun(#{topic_filter := TF}) ->
                    TopicFilter =:= TF
                end,
                emqx_mq_registry:match(SampleTopic)
            )
        )
    ),
    MQ.

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

cleanup_mqs() ->
    ok = stop_all_consumers(),
    ok = emqx_mq_registry:delete_all(),
    ok = emqx_mq_message_db:delete_all(),
    ok = emqx_mq_consumer_db:delete_all().

stop_all_consumers() ->
    ConsumerPids = [Pid || {_, Pid, _, _} <- supervisor:which_children(emqx_mq_consumer_sup)],
    ok = lists:foreach(
        fun(Pid) ->
            ok = emqx_mq_consumer:stop(Pid)
        end,
        ConsumerPids
    ).

compile_variform(Expression) ->
    {ok, Compiled} = emqx_variform:compile(Expression),
    Compiled.

config() ->
    <<"mq.gc_interval = 1h">>.
