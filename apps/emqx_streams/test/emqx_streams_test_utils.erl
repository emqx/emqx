%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_test_utils).

-export([
    emqtt_connect/1,
    emqtt_pub_stream/3,
    emqtt_pub_stream/4,
    emqtt_sub_stream/2,
    emqtt_drain/0,
    emqtt_drain/1,
    emqtt_drain/2,
    emqtt_ack/1
]).

-export([create_stream/1, fill_stream_defaults/1]).

-export([populate/2, populate_lastvalue/2]).

-export([cleanup_streams/0]).

-export([cth_config/1, cth_config/2, reset_config/0]).

-include_lib("../src/emqx_streams_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STREAM_KEY_USER_PROPERTY, <<"stream-key">>).

emqtt_connect(Opts) ->
    BaseOpts = [{proto_ver, v5}],
    {ok, C} = emqtt:start_link(BaseOpts ++ Opts),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_pub_stream(Client, Topic, Payload) ->
    emqtt_pub_stream(Client, Topic, Payload, #{}).

emqtt_pub_stream(Client, Topic, Payload, Opts) ->
    Properties =
        case Opts of
            #{key := Key} -> #{'User-Property' => [{?STREAM_KEY_USER_PROPERTY, Key}]};
            _ -> #{}
        end,
    Qos = maps:get(qos, Opts, 1),
    PubOpts = [{qos, Qos}],
    emqtt:publish(Client, Topic, Properties, Payload, PubOpts).

emqtt_sub_stream(Client, Topic) ->
    FullTopic = <<"$s/", Topic/binary>>,
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

emqtt_ack(Msgs) ->
    ok = lists:foreach(
        fun(#{client_pid := Pid, packet_id := PacketId}) ->
            emqtt:puback(Pid, PacketId)
        end,
        Msgs
    ).

create_stream(#{topic_filter := TopicFilter} = Stream0) ->
    Stream1 = fill_stream_defaults(Stream0),
    SampleTopic0 = string:replace(TopicFilter, "#", "x", all),
    SampleTopic1 = string:replace(SampleTopic0, "+", "x", all),
    SampleTopic = iolist_to_binary(SampleTopic1),
    {ok, Stream} = ?retry(50, 100, {ok, _} = emqx_streams_registry:create(Stream1)),
    ?retry(
        5,
        100,
        ?assert(
            lists:any(
                fun(#{topic_filter := TF}) ->
                    TopicFilter =:= TF
                end,
                emqx_streams_registry:match(SampleTopic)
            )
        )
    ),
    Stream.

fill_stream_defaults(#{topic_filter := _TopicFilter} = Stream0) ->
    Default = #{
        is_lastvalue => false,
        limits => #{
            max_shard_message_count => infinity,
            max_shard_message_bytes => infinity
        }
    },
    LastVelueDefault = #{
        key_expression =>
            compile_key_expression(<<"message.headers.properties.User-Property.stream-key">>)
    },
    Stream1 = maps:merge(Default, Stream0),
    case Stream1 of
        #{is_lastvalue := true} ->
            Stream = maps:merge(LastVelueDefault, Stream1),
            KeyExpression = maps:get(key_expression, Stream),
            Stream#{key_expression => compile_key_expression(KeyExpression)};
        _ ->
            Stream1
    end.

populate(N, #{topic_prefix := TopicPrefix} = Opts) ->
    PayloadPrefix = maps:get(payload_prefix, Opts, <<"payload-">>),
    NeedDifferentClients = maps:get(different_clients, Opts, false),
    C0 = emqtt_connect([]),
    C = lists:foldl(
        fun(I, Conn0) ->
            IBin = integer_to_binary(I),
            Topic = <<TopicPrefix/binary, IBin/binary>>,
            Payload = <<PayloadPrefix/binary, IBin/binary>>,
            emqtt_pub_stream(Conn0, Topic, Payload, pub_opts(Opts, #{})),
            case NeedDifferentClients of
                true ->
                    emqtt:disconnect(Conn0),
                    emqtt_connect([]);
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
    C0 = emqtt_connect([]),
    C = lists:foldl(
        fun(I, Conn0) ->
            IBin = integer_to_binary(I),
            Topic = <<TopicPrefix/binary, IBin/binary>>,
            Payload = <<PayloadPrefix/binary, IBin/binary>>,
            Key = <<"k-", (integer_to_binary(I rem NKeys))/binary>>,
            emqtt_pub_stream(Conn0, Topic, Payload, pub_opts(Opts, #{key => Key})),
            case NeedDifferentClients of
                true ->
                    emqtt:disconnect(Conn0),
                    emqtt_connect([]);
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

cleanup_streams() ->
    ok = emqx_streams_registry:delete_all().

cth_config(App) ->
    cth_config(App, #{}).

cth_config(emqx_streams, ConfigOverrides) ->
    DefaultConfig = #{<<"streams">> => default_streams_config()},
    Config = emqx_utils_maps:deep_merge(DefaultConfig, ConfigOverrides),
    #{
        config => Config,
        after_start => fun() -> ok = emqx_streams_app:wait_readiness(15_000) end
    };
cth_config(emqx, ConfigOverrides) ->
    DefaultConfig = #{
        <<"durable_storage">> => #{
            <<"streams_messages">> => #{
                <<"n_shards">> => 2,
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
    {ok, _} = emqx:update_config([streams], default_streams_config()),
    ok.

default_streams_config() ->
    #{
        <<"max_stream_count">> => 1000,
        <<"enable">> => true
    }.
