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
    <<"required">> => [<<"key">>],
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
-export([write/2, write/3, read/2, read/3, read_all/2, delete/2]).

init() ->
    ok = emqx_agent_tool_registry:register_type(?WRITE_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?READ_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?DEL_TYPE, ?MODULE).

deinit() ->
    ok = emqx_agent_tool_registry:unregister_type(?WRITE_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?READ_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?DEL_TYPE).

create(
    #{<<"type">> := Type, <<"id">> := ToolId, <<"desc">> := Desc, <<"stream">> := Stream} =
        CreateContext
) when
    Type =:= ?WRITE_TYPE; Type =:= ?READ_TYPE; Type =:= ?DEL_TYPE
->
    case tool_format(CreateContext) of
        {ok, Format} ->
            Context = context(Type, Stream, Format),
            {ok, #{
                tool_id => ToolId,
                type => Type,
                module => ?MODULE,
                display_name => display_name(Desc, Type),
                description => description(Stream, Type),
                context => Context,
                input_schema => input_schema(Context)
            }};
        {error, _} = Error ->
            Error
    end.

destroy(_Tool) ->
    ok.

to_map(#{
    tool_id := Id, type := Type, description := Desc, context := #{<<"stream">> := Stream} = Context
}) ->
    Base = #{
        <<"tool_id">> => Id,
        <<"type">> => Type,
        <<"description">> => Desc,
        <<"stream">> => Stream,
        <<"input_schema">> => input_schema(Context)
    },
    maps:merge(Base, maps:with([<<"format">>], Context)).

handle_invoke(
    #{<<"type">> := ?WRITE_TYPE, <<"stream">> := StreamName, <<"format">> := Format}, Request
) ->
    write(StreamName, maps:get(<<"args">>, Request, #{}), Format);
handle_invoke(
    #{<<"type">> := ?READ_TYPE, <<"stream">> := StreamName, <<"format">> := Format}, Request
) ->
    read(StreamName, maps:get(<<"args">>, Request, #{}), Format);
handle_invoke(#{<<"type">> := ?DEL_TYPE, <<"stream">> := StreamName}, Request) ->
    delete(StreamName, maps:get(<<"args">>, Request, #{})).

write(StreamName, Args) ->
    write(StreamName, Args, <<"json">>).

write(StreamName, Args, Format) ->
    maybe
        {ok, Stream} ?= find_stream(StreamName),
        Key = maps:get(<<"key">>, Args),
        Data = maps:get(<<"data">>, Args),
        {ok, Payload} ?= encode_data(Format, Data),
        Message = emqx_message:make(
            <<"emqx_agent">>, ?QOS_1, stream_topic(Stream), Payload
        ),
        ok ?= insert_record(Stream, Key, Message),
        {ok, #{<<"key">> => Key}}
    else
        {error, Reason} -> {error, Reason}
    end.

read(StreamName, Args) ->
    read(StreamName, Args, <<"json">>).

read(StreamName, Args, Format) ->
    maybe
        {ok, Limits} ?= read_limits(Args),
        {ok, Stream} ?= find_stream(StreamName),
        {ok, Records} ?= read_records(Stream, Args, Limits),
        {ok, [record_to_result(Record, Format) || Record <- Records]}
    else
        {error, Reason} ->
            {error, Reason}
    end.

read_all(StreamName, Format) ->
    maybe
        {ok, Stream} ?= find_stream(StreamName),
        Records = emqx_streams_message_db:dirty_read_all(Stream, #{}),
        {ok, [record_to_result(Record, Format) || Record <- Records]}
    else
        {error, Reason} ->
            {error, Reason}
    end.

delete(StreamName, Args) ->
    maybe
        {ok, Stream} ?= find_stream(StreamName),
        ok ?= delete_records(Stream, Args),
        {ok, #{}}
    else
        {error, Reason} -> {error, Reason}
    end.

find_stream(StreamName) ->
    case emqx_streams_registry:find(StreamName) of
        {ok, Stream} -> {ok, Stream};
        not_found -> {error, <<"stream not found">>}
    end.

insert_record(Stream, Key, Message) ->
    case emqx_streams_message_db:insert(Stream, Key, Message) of
        ok -> ok;
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

read_records(Stream, #{<<"key">> := Key}, Limits) when is_binary(Key) ->
    {ok, emqx_streams_message_db:dirty_read_key(Stream, Key, Limits)};
read_records(_Stream, #{<<"key">> := _Key}, _Limits) ->
    {error, <<"key must be a binary">>};
read_records(_Stream, _Args, _Limits) ->
    {error, <<"missing required field: key">>}.

delete_records(Stream, Args) ->
    Result =
        case maps:get(<<"key">>, Args, undefined) of
            undefined -> emqx_streams_message_db:delete_all(Stream);
            Key -> emqx_streams_message_db:delete_key(Stream, Key)
        end,
    case Result of
        ok -> ok;
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

read_limits(#{<<"from">> := _From, <<"from_ago">> := _FromAgo}) ->
    {error, <<"from conflicts with from_ago">>};
read_limits(#{<<"from">> := From}) ->
    {ok, #{start_time => From * 1_000_000}};
read_limits(#{<<"from_ago">> := FromAgo}) ->
    {ok, #{start_time => (erlang:system_time(second) - FromAgo) * 1_000_000}};
read_limits(_Args) ->
    {ok, #{}}.

record_to_result({Topic, Time, MessageBin}, Format) ->
    #{
        <<"key">> => key_from_topic(Topic),
        <<"time">> => Time,
        <<"data">> => decode_data(
            Format,
            emqx_message:payload(emqx_streams_message_db:decode_message(MessageBin))
        )
    }.

key_from_topic([<<"topic">>, _TopicFilter, _StreamId, <<"key">>, Key]) ->
    Key.

encode_data(<<"json">>, Data) ->
    case emqx_utils_json:safe_encode(Data) of
        {ok, Payload} -> {ok, Payload};
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end;
encode_data(<<"binary">>, Data) ->
    {ok, Data}.

decode_data(<<"json">>, Payload) ->
    case emqx_utils_json:safe_decode(Payload) of
        {ok, Data} -> Data;
        {error, _Reason} -> Payload
    end;
decode_data(<<"binary">>, Payload) ->
    Payload.

context(?WRITE_TYPE, Stream, Format) ->
    #{<<"type">> => ?WRITE_TYPE, <<"stream">> => Stream, <<"format">> => Format};
context(?READ_TYPE, Stream, Format) ->
    #{<<"type">> => ?READ_TYPE, <<"stream">> => Stream, <<"format">> => Format};
context(?DEL_TYPE, Stream, _Format) ->
    #{<<"type">> => ?DEL_TYPE, <<"stream">> => Stream}.

tool_format(#{<<"type">> := ?DEL_TYPE}) ->
    {ok, undefined};
tool_format(#{<<"type">> := Type, <<"format">> := Format}) when
    (Type =:= ?WRITE_TYPE orelse Type =:= ?READ_TYPE) andalso
        (Format =:= <<"json">> orelse Format =:= <<"binary">>)
->
    {ok, Format};
tool_format(#{<<"type">> := Type}) when Type =:= ?WRITE_TYPE orelse Type =:= ?READ_TYPE ->
    {ok, <<"json">>};
tool_format(#{<<"format">> := Format}) ->
    {error, {invalid_format, Format}}.

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

input_schema(#{<<"type">> := ?WRITE_TYPE, <<"format">> := Format}) -> write_schema(Format);
input_schema(#{<<"type">> := ?READ_TYPE}) -> ?READ_SCHEMA;
input_schema(#{<<"type">> := ?DEL_TYPE}) -> ?DEL_SCHEMA.

write_schema(Format) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"key">> => #{<<"type">> => <<"string">>},
            <<"data">> => write_data_schema(Format)
        },
        <<"required">> => [<<"key">>, <<"data">>],
        <<"additionalProperties">> => false
    }.

write_data_schema(<<"binary">>) ->
    #{<<"type">> => <<"string">>, <<"description">> => <<"Binary data to write">>};
write_data_schema(_Format) ->
    #{<<"type">> => <<"object">>, <<"description">> => <<"JSON object to write">>}.
