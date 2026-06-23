%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_kv).

-moduledoc """
Key-value tools backed by last-value EMQX streams.
""".

-behaviour(emqx_agent_tool).

-define(WRITE_TYPE, <<"kv_write">>).
-define(READ_TYPE, <<"kv_read">>).
-define(READ_ALL_TYPE, <<"kv_read_all">>).
-define(DEL_TYPE, <<"kv_del">>).
-define(CLEAR_TYPE, <<"kv_clear">>).

-define(KEY_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"key">> => #{<<"type">> => <<"string">>}
    },
    <<"required">> => [<<"key">>],
    <<"additionalProperties">> => false
}).

-define(EMPTY_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{},
    <<"additionalProperties">> => false
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

init() ->
    ok = emqx_agent_tool_registry:register_type(?WRITE_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?READ_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?READ_ALL_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?DEL_TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:register_type(?CLEAR_TYPE, ?MODULE).

deinit() ->
    ok = emqx_agent_tool_registry:unregister_type(?WRITE_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?READ_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?READ_ALL_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?DEL_TYPE),
    ok = emqx_agent_tool_registry:unregister_type(?CLEAR_TYPE).

create(
    #{<<"type">> := Type, <<"id">> := ToolId, <<"desc">> := Desc, <<"stream">> := Stream} =
        CreateContext
) when
    Type =:= ?WRITE_TYPE;
    Type =:= ?READ_TYPE;
    Type =:= ?READ_ALL_TYPE;
    Type =:= ?DEL_TYPE;
    Type =:= ?CLEAR_TYPE
->
    maybe
        ok ?= validate_lastvalue_stream(Stream),
        {ok, Format} ?= tool_format(CreateContext),
        Context = context(Type, Stream, Format),
        {ok, #{
            tool_id => ToolId,
            type => Type,
            module => ?MODULE,
            display_name => display_name(Desc, Type),
            description => description(Stream, Type),
            context => Context,
            input_schema => input_schema(Context)
        }}
    else
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
    #{<<"type">> := ?WRITE_TYPE, <<"stream">> := Stream, <<"format">> := Format}, Request
) ->
    Args = maps:get(<<"args">>, Request, #{}),
    emqx_agent_tool_stream:write(
        Stream,
        #{
            <<"key">> => maps:get(<<"key">>, Args),
            <<"data">> => maps:get(<<"payload">>, Args)
        },
        Format
    );
handle_invoke(#{<<"type">> := ?READ_TYPE, <<"stream">> := Stream, <<"format">> := Format}, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    case
        emqx_agent_tool_stream:read(
            Stream,
            #{<<"key">> => maps:get(<<"key">>, Args)},
            Format
        )
    of
        {ok, Items} -> {ok, [maps:get(<<"data">>, Item) || Item <- Items]};
        {error, _} = Error -> Error
    end;
handle_invoke(
    #{<<"type">> := ?READ_ALL_TYPE, <<"stream">> := Stream, <<"format">> := Format}, _Request
) ->
    case emqx_agent_tool_stream:read_all(Stream, Format) of
        {ok, Items} ->
            {ok, [
                #{<<"key">> => Key, <<"value">> => Data}
             || #{<<"key">> := Key, <<"data">> := Data} <- Items
            ]};
        {error, _} = Error ->
            Error
    end;
handle_invoke(#{<<"type">> := ?DEL_TYPE, <<"stream">> := Stream}, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    emqx_agent_tool_stream:delete(Stream, #{<<"key">> => maps:get(<<"key">>, Args)});
handle_invoke(#{<<"type">> := ?CLEAR_TYPE, <<"stream">> := Stream}, _Request) ->
    emqx_agent_tool_stream:delete(Stream, #{}).

validate_lastvalue_stream(StreamName) ->
    case emqx_streams_registry:find(StreamName) of
        {ok, Stream} ->
            case emqx_streams_prop:is_lastvalue(Stream) of
                true -> ok;
                false -> {error, stream_is_not_lastvalue}
            end;
        not_found ->
            {error, stream_not_found}
    end.

context(?WRITE_TYPE, Stream, Format) ->
    #{<<"type">> => ?WRITE_TYPE, <<"stream">> => Stream, <<"format">> => Format};
context(?READ_TYPE, Stream, Format) ->
    #{<<"type">> => ?READ_TYPE, <<"stream">> => Stream, <<"format">> => Format};
context(?READ_ALL_TYPE, Stream, Format) ->
    #{<<"type">> => ?READ_ALL_TYPE, <<"stream">> => Stream, <<"format">> => Format};
context(?DEL_TYPE, Stream, _Format) ->
    #{<<"type">> => ?DEL_TYPE, <<"stream">> => Stream};
context(?CLEAR_TYPE, Stream, _Format) ->
    #{<<"type">> => ?CLEAR_TYPE, <<"stream">> => Stream}.

tool_format(#{<<"type">> := Type, <<"format">> := Format}) when
    (Type =:= ?WRITE_TYPE orelse Type =:= ?READ_TYPE orelse Type =:= ?READ_ALL_TYPE) andalso
        (Format =:= <<"json">> orelse Format =:= <<"binary">>)
->
    {ok, Format};
tool_format(#{<<"type">> := Type}) when
    Type =:= ?WRITE_TYPE; Type =:= ?READ_TYPE; Type =:= ?READ_ALL_TYPE
->
    {ok, <<"json">>};
tool_format(#{<<"type">> := Type}) when Type =:= ?DEL_TYPE; Type =:= ?CLEAR_TYPE ->
    {ok, undefined};
tool_format(#{<<"format">> := Format}) ->
    {error, {invalid_format, Format}}.

display_name(Desc, ?WRITE_TYPE) -> <<Desc/binary, " — KV Write">>;
display_name(Desc, ?READ_TYPE) -> <<Desc/binary, " — KV Read">>;
display_name(Desc, ?READ_ALL_TYPE) -> <<Desc/binary, " — KV Read All">>;
display_name(Desc, ?DEL_TYPE) -> <<Desc/binary, " — KV Delete">>;
display_name(Desc, ?CLEAR_TYPE) -> <<Desc/binary, " — KV Clear">>.

description(Stream, ?WRITE_TYPE) ->
    <<"Write a key-value entry to stream: ", Stream/binary>>;
description(Stream, ?READ_TYPE) ->
    <<"Read a key-value entry from stream: ", Stream/binary>>;
description(Stream, ?READ_ALL_TYPE) ->
    <<"Read all key-value entries from stream: ", Stream/binary>>;
description(Stream, ?DEL_TYPE) ->
    <<"Delete a key-value entry from stream: ", Stream/binary>>;
description(Stream, ?CLEAR_TYPE) ->
    <<"Clear all key-value entries from stream: ", Stream/binary>>.

input_schema(#{<<"type">> := ?WRITE_TYPE, <<"format">> := Format}) -> write_schema(Format);
input_schema(#{<<"type">> := ?READ_TYPE}) -> ?KEY_SCHEMA;
input_schema(#{<<"type">> := ?READ_ALL_TYPE}) -> ?EMPTY_SCHEMA;
input_schema(#{<<"type">> := ?DEL_TYPE}) -> ?KEY_SCHEMA;
input_schema(#{<<"type">> := ?CLEAR_TYPE}) -> ?EMPTY_SCHEMA.

write_schema(Format) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"key">> => #{<<"type">> => <<"string">>},
            <<"payload">> => write_payload_schema(Format)
        },
        <<"required">> => [<<"key">>, <<"payload">>],
        <<"additionalProperties">> => false
    }.

write_payload_schema(<<"binary">>) ->
    #{<<"type">> => <<"string">>, <<"description">> => <<"Binary value to write">>};
write_payload_schema(_Format) ->
    #{<<"type">> => <<"object">>, <<"description">> => <<"JSON object to write">>}.
