%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_test_utils).

-export([
    emqtt_connect/1,
    emqtt_pub_mq/3,
    emqtt_pub_mq/4,
    emqtt_sub_mq/3,
    emqtt_sub_mq/2,
    emqtt_drain/0,
    emqtt_drain/1,
    emqtt_drain/2,
    emqtt_ack/1
]).

-export([create_mq/1, ensure_mq_created/1, fill_mq_defaults/1]).

-export([populate/2, populate_lastvalue/2]).

-export([cleanup_mqs/0, stop_all_consumers/0, all_consumers/0]).

-export([cth_config/1, cth_config/2]).

-export([reset_config/0]).

-include_lib("../src/emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

emqtt_connect(Opts) ->
    BaseOpts = [{proto_ver, v5}],
    {ok, C} = emqtt:start_link(BaseOpts ++ Opts),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_pub_mq(Client, Topic, Payload) ->
    emqtt_pub_mq(Client, Topic, Payload, #{}).

emqtt_pub_mq(Client, Topic, Payload, Opts) ->
    Properties =
        case Opts of
            #{key := Key} -> #{'User-Property' => [{?MQ_KEY_USER_PROPERTY, Key}]};
            _ -> #{}
        end,
    Qos = maps:get(qos, Opts, 1),
    PubOpts = [{qos, Qos}],
    emqtt:publish(Client, Topic, Properties, Payload, PubOpts).

emqtt_sub_mq(Client, Name, Topic) ->
    FullTopic = <<"$queue/", Name/binary, "/", Topic/binary>>,
    {ok, _, [?QOS_1]} = emqtt:subscribe(Client, {FullTopic, ?QOS_1}),
    ok.

emqtt_sub_mq(Client, Name) ->
    FullTopic = <<"$queue/", Name/binary>>,
    {ok, _, [?QOS_1]} = emqtt:subscribe(Client, {FullTopic, ?QOS_1}),
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

emqtt_ack(Msgs) ->
    ok = lists:foreach(
        fun(#{client_pid := Pid, packet_id := PacketId}) ->
            emqtt:puback(Pid, PacketId)
        end,
        Msgs
    ).

create_mq(MQ0) ->
    MQ = fill_mq_defaults(MQ0),
    emqx_mq_registry:create(MQ).

ensure_mq_created(#{topic_filter := TopicFilter} = MQ0) ->
    {ok, MQ} = ?retry(50, 100, {ok, _} = create_mq(MQ0)),
    SampleTopic0 = string:replace(TopicFilter, "#", "x", all),
    SampleTopic1 = string:replace(SampleTopic0, "+", "x", all),
    SampleTopic = iolist_to_binary(SampleTopic1),
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

fill_mq_defaults(#{topic_filter := _TopicFilter} = MQ0) ->
    Default = #{
        name => <<"test-mq">>,
        is_lastvalue => false,
        consumer_max_inactive => 1000,
        ping_interval => 5000,
        redispatch_interval => 100,
        dispatch_strategy => random,
        local_max_inflight => 4,
        busy_session_retry_interval => 100,
        stream_max_buffer_size => 10,
        stream_max_unacked => 5,
        consumer_persistence_interval => 1000,
        data_retention_period => 3600_000,
        limits => #{
            max_shard_message_count => infinity,
            max_shard_message_bytes => infinity
        }
    },
    LastVelueDefault = #{
        key_expression =>
            compile_key_expression(<<"message.headers.properties.User-Property.mq-key">>)
    },
    MQ1 = maps:merge(Default, MQ0),
    case MQ1 of
        #{is_lastvalue := true} ->
            MQ = maps:merge(LastVelueDefault, MQ1),
            KeyExpression = maps:get(key_expression, MQ),
            MQ#{key_expression => compile_key_expression(KeyExpression)};
        _ ->
            MQ1
    end.

populate(N, #{topic_prefix := TopicPrefix} = Opts) ->
    PayloadPrefix = maps:get(payload_prefix, Opts, <<"payload-">>),
    NeedDifferentClients = maps:get(different_clients, Opts, false),
    C0 = emqx_mq_test_utils:emqtt_connect([]),
    C = lists:foldl(
        fun(I, Conn0) ->
            IBin = integer_to_binary(I),
            Topic = <<TopicPrefix/binary, IBin/binary>>,
            Payload = <<PayloadPrefix/binary, IBin/binary>>,
            emqx_mq_test_utils:emqtt_pub_mq(Conn0, Topic, Payload, pub_opts(Opts, #{})),
            case NeedDifferentClients of
                true ->
                    emqtt:disconnect(Conn0),
                    emqx_mq_test_utils:emqtt_connect([]);
                false ->
                    Conn0
            end
        end,
        C0,
        lists:seq(0, N - 1)
    ),
    ok = emqtt:disconnect(C).

populate_lastvalue(N, #{topic_prefix := TopicPrefix} = Opts) ->
    PayloadPrefix = maps:get(payload_prefix, Opts, <<"payload-">>),
    NeedDifferentClients = maps:get(different_clients, Opts, false),
    NKeys = maps:get(n_keys, Opts, N),
    C0 = emqx_mq_test_utils:emqtt_connect([]),
    C = lists:foldl(
        fun(I, Conn0) ->
            IBin = integer_to_binary(I),
            Topic = <<TopicPrefix/binary, IBin/binary>>,
            Payload = <<PayloadPrefix/binary, IBin/binary>>,
            Key = <<"k-", (integer_to_binary(I rem NKeys))/binary>>,
            emqx_mq_test_utils:emqtt_pub_mq(Conn0, Topic, Payload, pub_opts(Opts, #{key => Key})),
            case NeedDifferentClients of
                true ->
                    emqtt:disconnect(Conn0),
                    emqx_mq_test_utils:emqtt_connect([]);
                false ->
                    Conn0
            end
        end,
        C0,
        lists:seq(0, N - 1)
    ),
    ok = emqtt:disconnect(C).

pub_opts(PopulateOpts, PubOpts) ->
    maps:merge(maps:with([qos], PopulateOpts), PubOpts).

cleanup_mqs() ->
    ok = stop_all_consumers(),
    ok = emqx_mq_registry:delete_all(),
    ok = emqx_mq_message_db:delete_all(),
    ok = emqx_mq_state_storage:delete_all().

stop_all_consumers() ->
    ok = lists:foreach(
        fun(Pid) ->
            ok = emqx_mq_consumer:stop(Pid)
        end,
        all_consumers()
    ).

all_consumers() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(emqx_mq_consumer_sup), is_pid(Pid)].

cth_config(App) ->
    cth_config(App, #{}).

cth_config(emqx_mq, ConfigOverrides) ->
    DefaultConfig = #{<<"mq">> => default_mq_config()},
    Config = emqx_utils_maps:deep_merge(DefaultConfig, ConfigOverrides),
    #{
        config => Config,
        after_start => fun() -> started = emqx_mq_controller:wait_status(15_000) end
    };
cth_config(emqx, ConfigOverrides) ->
    DefaultConfig = #{
        <<"durable_storage">> => #{
            <<"mq_messages">> => #{
                <<"transaction">> => #{
                    <<"flush_interval">> => 100,
                    <<"idle_flush_interval">> => 20,
                    <<"conflict_window">> => 5000
                },
                <<"subscriptions">> => #{
                    <<"batch_size">> => 1
                }
            }
        }
    },
    Config = emqx_utils_maps:deep_merge(DefaultConfig, ConfigOverrides),
    #{
        config => Config
    }.

compile_key_expression(KeyExpression) ->
    {ok, KeyExpressionCompiled} = emqx_variform:compile(KeyExpression),
    KeyExpressionCompiled.

reset_config() ->
    {ok, _} = emqx:update_config([mq], default_mq_config()),
    ok.

default_mq_config() ->
    #{
        <<"gc_interval">> => <<"1h">>,
        <<"auto_create">> => #{
            <<"regular">> => false,
            <<"lastvalue">> => false
        },
        <<"enable">> => true
    }.
