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

-define(WRITE_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"key">> => #{<<"type">> => <<"string">>},
        <<"payload">> => #{<<"description">> => <<"JSON value to write">>}
    },
    <<"required">> => [<<"key">>, <<"payload">>],
    <<"additionalProperties">> => false
}).

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

create(#{<<"type">> := Type, <<"id">> := ToolId, <<"desc">> := Desc, <<"stream">> := Stream}) when
    Type =:= ?WRITE_TYPE;
    Type =:= ?READ_TYPE;
    Type =:= ?READ_ALL_TYPE;
    Type =:= ?DEL_TYPE;
    Type =:= ?CLEAR_TYPE
->
    case validate_lastvalue_stream(Stream) of
        ok ->
            {ok, #{
                tool_id => ToolId,
                type => Type,
                module => ?MODULE,
                display_name => display_name(Desc, Type),
                description => description(Stream, Type),
                context => #{<<"type">> => Type, <<"stream">> => Stream},
                input_schema => input_schema(Type)
            }};
        {error, _} = Error ->
            Error
    end.

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

handle_invoke(#{<<"type">> := ?WRITE_TYPE, <<"stream">> := Stream}, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    emqx_agent_tool_stream:write(Stream, #{
        <<"key">> => maps:get(<<"key">>, Args),
        <<"data">> => maps:get(<<"payload">>, Args)
    });
handle_invoke(#{<<"type">> := ?READ_TYPE, <<"stream">> := Stream}, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    case emqx_agent_tool_stream:read(Stream, #{<<"key">> => maps:get(<<"key">>, Args)}) of
        {ok, Items} -> {ok, [maps:get(<<"data">>, Item) || Item <- Items]};
        {error, _} = Error -> Error
    end;
handle_invoke(#{<<"type">> := ?READ_ALL_TYPE, <<"stream">> := Stream}, _Request) ->
    case emqx_agent_tool_stream:read(Stream, #{}) of
        {ok, Items} ->
            {ok, [
                #{<<"key">> => maps:get(<<"key">>, Item), <<"value">> => maps:get(<<"data">>, Item)}
             || Item <- Items
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

input_schema(?WRITE_TYPE) -> ?WRITE_SCHEMA;
input_schema(?READ_TYPE) -> ?KEY_SCHEMA;
input_schema(?READ_ALL_TYPE) -> ?EMPTY_SCHEMA;
input_schema(?DEL_TYPE) -> ?KEY_SCHEMA;
input_schema(?CLEAR_TYPE) -> ?EMPTY_SCHEMA.
