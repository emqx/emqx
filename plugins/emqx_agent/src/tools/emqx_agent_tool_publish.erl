%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_publish).

-moduledoc """
MQTT message-publish tool.

Allows an LLM agent to publish MQTT messages to topics rooted under a
configured prefix.  The prefix is fixed at creation time so that the
agent cannot publish outside its authorised namespace.

Invoke topic:  cap/message__publish/<tool_id>/request/<req_id>
Reply  topic:  cap/message__publish/<tool_id>/response/<req_id>

Context keys:
  <<"id">>          => binary()  — unique instance identifier
  <<"desc">>        => binary()  — human-readable description
  <<"topic_prefix">> => binary()  — prepended to the agent-supplied topic
                                    (e.g. <<"devices/room1/">>)


Input args (fixed wrapper + configurable payload schema):
  topic   => binary()  — topic suffix; combined with topic_prefix
  payload => json()    — payload value validated by payload_schema
  from    => binary()  — publisher identity (optional, defaults to id)
  qos     => 0 | 1 | 2  — QoS level (optional, default 0)

Lifecycle:
  init()        — register the tool type
  create(Ctx)   — build a runtime tool instance
  destroy(Tool) — clean up runtime resources owned by the tool
  deinit()      — unregister the tool type
""".

-behaviour(emqx_agent_tool).

-include_lib("emqx/include/logger.hrl").

-define(TOOL_TYPE, <<"message__publish">>).

-define(DEFAULT_PAYLOAD_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"message">> => #{<<"type">> => <<"string">>}
    },
    <<"required">> => [<<"message">>],
    <<"additionalProperties">> => false
}).

-define(INPUT_SCHEMA(PayloadSchema), #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"topic">> => #{
            <<"type">> => <<"string">>,
            <<"description">> =>
                <<
                    "Topic suffix only — do NOT include the prefix. "
                    "The full publish topic is: prefix + this value. "
                    "E.g. if prefix is 'box/alert/' and you want to publish to 'box/alert/box-123', "
                    "pass topic='box-123', not topic='box/alert/box-123'."
                >>
        },
        <<"payload">> => PayloadSchema,
        <<"from">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Publisher identity (optional)">>
        },
        <<"qos">> => #{
            <<"type">> => <<"integer">>,
            <<"enum">> => [0, 1, 2],
            <<"description">> => <<"QoS level (optional, default 0)">>
        }
    },
    <<"required">> => [<<"topic">>, <<"payload">>]
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    emqx_agent_tool_registry:register_type(?TOOL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_tool_registry:unregister_type(?TOOL_TYPE).

%% Context keys:
%%   <<"id">>          => binary()
%%   <<"desc">>        => binary()
%%   <<"topic_prefix">> => binary()
-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(#{
    <<"id">> := ToolId,
    <<"desc">> := Desc,
    <<"topic_prefix">> := TopicPrefix,
    <<"payload_schema">> := PayloadSchema0
}) ->
    case decode_schema(PayloadSchema0) of
        {ok, PayloadSchema} ->
            create_with_payload_schema(ToolId, Desc, TopicPrefix, PayloadSchema);
        {error, Reason} ->
            {error, {invalid_payload_schema, Reason}}
    end;
create(#{<<"id">> := _, <<"desc">> := _, <<"topic_prefix">> := _}) ->
    {error, missing_payload_schema}.

create_with_payload_schema(_ToolId, _Desc, _TopicPrefix, undefined) ->
    {error, missing_payload_schema};
create_with_payload_schema(ToolId, Desc, TopicPrefix, PayloadSchema) ->
    case emqx_agent_oai_tool_schema:validate_oai_schema_field(PayloadSchema) of
        ok ->
            {ok, #{
                tool_id => ToolId,
                type => ?TOOL_TYPE,
                module => ?MODULE,
                display_name => <<Desc/binary, " — Publish">>,
                description =>
                    <<"Publish an MQTT message to a topic under the prefix: ", TopicPrefix/binary>>,
                context => #{
                    <<"id">> => ToolId,
                    <<"topic_prefix">> => TopicPrefix,
                    <<"payload_schema">> => PayloadSchema
                },
                input_schema => ?INPUT_SCHEMA(PayloadSchema)
            }};
        {error, Reason} ->
            {error, {invalid_payload_schema, Reason}}
    end.

-spec destroy(map()) -> ok.
destroy(_Tool) ->
    ok.

-spec to_map(map()) -> map().
to_map(
    #{
        tool_id := Id,
        description := Desc,
        context := #{<<"topic_prefix">> := TopicPrefix, <<"payload_schema">> := PayloadSchema},
        input_schema := InputSchema
    }
) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"payload_schema">> => PayloadSchema,
        <<"input_schema">> => InputSchema
    };
to_map(
    #{
        tool_id := Id,
        description := Desc,
        context := #{<<"topic_prefix">> := TopicPrefix},
        input_schema := InputSchema
    }
) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"payload_schema">> => maps:get(
            <<"payload">>, maps:get(<<"properties">>, InputSchema, #{}), ?DEFAULT_PAYLOAD_SCHEMA
        ),
        <<"input_schema">> => InputSchema
    }.

handle_invoke(#{<<"topic_prefix">> := TopicPrefix, <<"payload_schema">> := PayloadSchema}, Request) ->
    do_publish(TopicPrefix, PayloadSchema, Request);
handle_invoke(_Context, _Request) ->
    {error, missing_payload_schema}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_publish(TopicPrefix, PayloadSchema, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    TopicSuffix = maps:get(<<"topic">>, Args),
    Payload = maps:get(<<"payload">>, Args),
    From = maps:get(<<"from">>, Args, <<>>),
    Qos = maps:get(<<"qos">>, Args, 0),

    FullTopic = <<TopicPrefix/binary, TopicSuffix/binary>>,

    case emqx_agent_oai_tool_value:validate_value(PayloadSchema, Payload) of
        ok ->
            try
                MsgPayload = normalize_payload(Payload),
                Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
                _ = emqx_broker:publish(Msg),
                {ok, #{<<"topic">> => FullTopic}}
            catch
                Class:Reason ->
                    ?SLOG(error, #{
                        msg => "tool_publish_failed",
                        topic => FullTopic,
                        error => Class,
                        reason => Reason
                    }),
                    {error, iolist_to_binary(io_lib:format("~p", [Reason]))}
            end;
        {error, Errors} ->
            {error, format_payload_errors(Errors)}
    end.

format_payload_errors(Errors) ->
    iolist_to_binary(io_lib:format("invalid payload: ~p", [Errors])).

normalize_payload(Payload) when is_map(Payload) ->
    emqx_utils_json:encode(Payload);
normalize_payload(Payload) ->
    Payload.

decode_schema(<<>>) ->
    {ok, undefined};
decode_schema(V) when is_binary(V) ->
    try emqx_utils_json:decode(V) of
        Decoded -> {ok, Decoded}
    catch
        _:Reason -> {error, {invalid_json, Reason}}
    end;
decode_schema(V) ->
    {ok, V}.
