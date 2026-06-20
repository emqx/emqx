%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_stream).

-moduledoc """
Stream read/write/delete tools backed by EMQX Streams message storage.
""".

-behaviour(emqx_agent_tool).

-include_lib("emqx/include/emqx_mqtt.hrl").

-define(WRITE_TYPE, <<"stream_write">>).
-define(READ_TYPE, <<"stream_read">>).
-define(DEL_TYPE, <<"stream_del">>).

-define(WRITE_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"key">> => #{<<"type">> => <<"string">>},
        <<"data">> => #{<<"description">> => <<"JSON value to write">>}
    },
    <<"required">> => [<<"key">>, <<"data">>],
    <<"additionalProperties">> => false
}).

-define(READ_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"key">> => #{<<"type">> => <<"string">>},
        <<"from">> => #{
            <<"type">> => <<"integer">>,
            <<"description">> => <<"Read messages from this Unix timestamp in seconds">>
        },
        <<"from_ago">> => #{
            <<"type">> => <<"integer">>,
            <<"description">> => <<"Read messages from this many seconds ago">>
        }
    },
    <<"additionalProperties">> => false
}).

-define(DEL_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"key">> => #{<<"type">> => <<"string">>}
    },
    <<"additionalProperties">> => false
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).
-export([write/2, read/2, delete/2]).

init() ->
    ok = emqx_agent_tool_registry:register_type(?WRITE_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?READ_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?DEL_TYPE, ?MODULE).

deinit() ->
    ok = emqx_agent_tool_registry:unregister_type(?WRITE_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?READ_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?DEL_TYPE).

create(#{<<"type">> := Type, <<"id">> := ToolId, <<"desc">> := Desc, <<"stream">> := Stream}) when
    Type =:= ?WRITE_TYPE; Type =:= ?READ_TYPE; Type =:= ?DEL_TYPE
->
    {ok, #{
        tool_id => ToolId,
        type => Type,
        module => ?MODULE,
        display_name => display_name(Desc, Type),
        description => description(Stream, Type),
        context => #{<<"type">> => Type, <<"stream">> => Stream},
        input_schema => input_schema(Type)
    }}.

destroy(_Tool) ->
    ok.

to_map(#{tool_id := Id, type := Type, description := Desc, context := #{<<"stream">> := Stream}}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => Type,
        <<"description">> => Desc,
        <<"stream">> => Stream,
        <<"input_schema">> => input_schema(Type)
    }.

handle_invoke(#{<<"type">> := ?WRITE_TYPE, <<"stream">> := StreamName}, Request) ->
    write(StreamName, maps:get(<<"args">>, Request, #{}));
handle_invoke(#{<<"type">> := ?READ_TYPE, <<"stream">> := StreamName}, Request) ->
    read(StreamName, maps:get(<<"args">>, Request, #{}));
handle_invoke(#{<<"type">> := ?DEL_TYPE, <<"stream">> := StreamName}, Request) ->
    delete(StreamName, maps:get(<<"args">>, Request, #{})).

write(StreamName, Args) ->
    with_stream(StreamName, fun(Stream) ->
        Key = maps:get(<<"key">>, Args),
        Data = maps:get(<<"data">>, Args),
        Message = emqx_message:make(
            <<"emqx_agent">>, ?QOS_1, stream_topic(Stream), encode_data(Data)
        ),
        case emqx_streams_message_db:insert(Stream, Key, Message) of
            ok -> {ok, #{<<"key">> => Key}};
            {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
        end
    end).

read(StreamName, Args) ->
    case read_limits(Args) of
        {ok, Limits} ->
            with_stream(StreamName, fun(Stream) ->
                Records =
                    case maps:get(<<"key">>, Args, undefined) of
                        undefined -> emqx_streams_message_db:dirty_read_all(Stream, Limits);
                        Key -> emqx_streams_message_db:dirty_read_key(Stream, Key, Limits)
                    end,
                {ok, [record_to_result(Record) || Record <- lists:sort(Records)]}
            end);
        {error, Reason} ->
            {error, Reason}
    end.

delete(StreamName, Args) ->
    with_stream(StreamName, fun(Stream) ->
        Result =
            case maps:get(<<"key">>, Args, undefined) of
                undefined -> emqx_streams_message_db:delete_all(Stream);
                Key -> emqx_streams_message_db:delete_key(Stream, Key)
            end,
        case Result of
            ok -> {ok, #{}};
            {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
        end
    end).

with_stream(StreamName, Fun) ->
    case emqx_streams_registry:find(StreamName) of
        {ok, Stream} -> Fun(Stream);
        not_found -> {error, <<"stream not found">>}
    end.

read_limits(Args) ->
    case {maps:is_key(<<"from">>, Args), maps:is_key(<<"from_ago">>, Args)} of
        {true, true} ->
            {error, <<"from conflicts with from_ago">>};
        {true, false} ->
            {ok, #{start_time => maps:get(<<"from">>, Args) * 1_000_000}};
        {false, true} ->
            FromAgo = maps:get(<<"from_ago">>, Args),
            {ok, #{start_time => (erlang:system_time(second) - FromAgo) * 1_000_000}};
        {false, false} ->
            {ok, #{}}
    end.

record_to_result({Topic, _Time, MessageBin}) ->
    #{
        <<"key">> => key_from_topic(Topic),
        <<"data">> => decode_data(
            emqx_message:payload(emqx_streams_message_db:decode_message(MessageBin))
        )
    }.

key_from_topic([<<"topic">>, _TopicFilter, _StreamId, <<"key">>, Key]) ->
    Key.

encode_data(Data) ->
    emqx_utils_json:encode(Data).

decode_data(Payload) ->
    try emqx_utils_json:decode(Payload) of
        Data -> Data
    catch
        _:_ -> Payload
    end.

stream_topic(#{topic_filter := TopicFilter}) ->
    concrete_topic(TopicFilter).

concrete_topic(TopicFilter) ->
    Topic1 = binary:replace(TopicFilter, <<"#">>, <<"agent">>, [global]),
    binary:replace(Topic1, <<"+">>, <<"agent">>, [global]).

display_name(Desc, ?WRITE_TYPE) -> <<Desc/binary, " — Stream Write">>;
display_name(Desc, ?READ_TYPE) -> <<Desc/binary, " — Stream Read">>;
display_name(Desc, ?DEL_TYPE) -> <<Desc/binary, " — Stream Delete">>.

description(Stream, ?WRITE_TYPE) -> <<"Write data to stream: ", Stream/binary>>;
description(Stream, ?READ_TYPE) -> <<"Read data from stream: ", Stream/binary>>;
description(Stream, ?DEL_TYPE) -> <<"Delete data from stream: ", Stream/binary>>.

input_schema(?WRITE_TYPE) -> ?WRITE_SCHEMA;
input_schema(?READ_TYPE) -> ?READ_SCHEMA;
input_schema(?DEL_TYPE) -> ?DEL_SCHEMA.
