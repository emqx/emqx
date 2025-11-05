%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_test_utils).

-export([create_stream/1, fill_stream_defaults/1]).

-export([cleanup_streams/0]).

-export([cth_config/1, cth_config/2, reset_config/0]).

-include_lib("../src/emqx_streams_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").

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
        %% TODO
        %% Configure Streams DS config
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
