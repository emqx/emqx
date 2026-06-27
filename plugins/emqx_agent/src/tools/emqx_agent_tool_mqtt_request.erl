%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_mqtt_request).

-moduledoc """
MQTT request/reply tool (MQTT 5 request pattern).

Publishes a request to a topic rooted under a configured prefix and
waits synchronously for a response.  The response topic is embedded
in the outbound message as an MQTT 5 `Response-Topic` property so
that any MQTT 5-aware responder can reply without out-of-band
coordination.

Invoke topic:  $cap/message__request/<tool_id>/request/<req_id>
Reply  topic:  $cap/message__request/<tool_id>/response/<req_id>

Context keys:
  <<"id">>          => binary()  — unique instance identifier
  <<"desc">>        => binary()  — human-readable description
  <<"topic_prefix">> => binary()  — prepended to the agent-supplied topic

Input args:
  topic       => binary()   — topic suffix; combined with topic_prefix
  payload     => json()     — request payload
  from        => binary()   — publisher identity (optional)
  qos         => 0|1|2      — QoS level (optional, default 0)
  timeout_ms  => integer()  — response wait limit in ms (optional, default 5000)

Tool output (in `data`):
  status  => <<"ok">>    — response arrived in time
    payload => json() | binary()  — decoded/sanitized response payload
    attachments => [map()]        — extracted attachments, when present
  status  => <<"error">>
    reason  => <<"timeout">> | binary()
""".

-behaviour(emqx_agent_tool).

-include_lib("emqx/include/emqx.hrl").

-define(TOOL_TYPE, <<"message__request">>).
-define(RESPONSE_TOPIC_PREFIX, <<"$cap/tmp/response/">>).
-define(DEFAULT_TIMEOUT_MS, 5000).

-define(DEFAULT_REQUEST_PAYLOAD_SCHEMA, #{
    <<"description">> => <<"Request payload (any JSON value or string)">>
}).

-define(INPUT_SCHEMA(RequestPayloadSchema), #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"topic">> => #{
            <<"type">> => <<"string">>,
            <<"description">> =>
                <<
                    "Topic suffix only — do NOT include the prefix. "
                    "The full request topic is: prefix + this value. "
                    "E.g. if prefix is 'box/shot/' and you want to request 'box/shot/box-123', "
                    "pass topic='box-123', not topic='box/shot/box-123'."
                >>
        },
        <<"payload">> => RequestPayloadSchema,
        <<"from">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Publisher identity (optional)">>
        },
        <<"qos">> => #{
            <<"type">> => <<"integer">>,
            <<"enum">> => [0, 1, 2],
            <<"description">> => <<"QoS level (optional, default 0)">>
        },
        <<"timeout_ms">> => #{
            <<"type">> => <<"integer">>,
            <<"description">> => <<"Response wait limit in milliseconds (optional, default 5000)">>
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

-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(#{<<"id">> := ToolId, <<"desc">> := Desc, <<"topic_prefix">> := TopicPrefix} = Context) ->
    case request_payload_schema(Context) of
        {ok, RequestPayloadSchema} ->
            create_with_request_payload_schema(
                ToolId, Desc, TopicPrefix, RequestPayloadSchema, Context
            );
        {error, Reason} ->
            {error, {invalid_request_payload_schema, Reason}}
    end.

create_with_request_payload_schema(ToolId, Desc, TopicPrefix, RequestPayloadSchema, Context) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<Desc/binary, " — Request">>,
        description =>
            <<"Send an MQTT request to a topic under the prefix: ", TopicPrefix/binary,
                " and wait for a response. If the response contains image attachments, "
                "the result payload uses Attachment <id> placeholders and the actual images "
                "are provided to the model after the tool result.">>,
        context => maps:merge(
            #{
                <<"payload_type">> => <<"json">>,
                <<"autodiscover_images">> => true,
                <<"images">> => []
            },
            Context#{<<"request_payload_schema">> => RequestPayloadSchema}
        ),
        input_schema => ?INPUT_SCHEMA(RequestPayloadSchema)
    }}.

-spec destroy(map()) -> ok.
destroy(_Tool) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{
    tool_id := Id,
    description := Desc,
    context := #{<<"topic_prefix">> := TopicPrefix} = Ctx,
    input_schema := InputSchema
}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"request_payload_schema">> => maps:get(
            <<"request_payload_schema">>, Ctx, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA
        ),
        <<"payload_type">> => maps:get(<<"payload_type">>, Ctx, <<"json">>),
        <<"autodiscover_images">> => maps:get(<<"autodiscover_images">>, Ctx, true),
        <<"images">> => maps:get(<<"images">>, Ctx, []),
        <<"input_schema">> => InputSchema
    }.

handle_invoke(#{<<"topic_prefix">> := TopicPrefix} = Context, Request) ->
    do_request(TopicPrefix, Context, Request).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_request(TopicPrefix, Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    TopicSuffix = maps:get(<<"topic">>, Args),
    MsgPayload = normalize_payload(maps:get(<<"payload">>, Args, <<>>)),
    From = maps:get(<<"from">>, Args, <<>>),
    Qos = maps:get(<<"qos">>, Args, 0),
    TimeoutMs = maps:get(<<"timeout_ms">>, Args, ?DEFAULT_TIMEOUT_MS),

    FullTopic = <<TopicPrefix/binary, TopicSuffix/binary>>,

    %% Unique response topic — scoped to this request.
    Unique = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    ResponseTopic = <<?RESPONSE_TOPIC_PREFIX/binary, Unique/binary>>,

    do_round_trip(FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs, Context).

do_round_trip(FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs, Context) ->
    %% Subscribe before publishing so we cannot miss the response.
    ok = emqx:subscribe(ResponseTopic),

    Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
    Msg2 = emqx_message:set_header(properties, #{'Response-Topic' => ResponseTopic}, Msg),
    _ = emqx_broker:publish(Msg2),

    Result =
        receive
            #deliver{topic = ResponseTopic, message = #message{payload = RespPayload}} ->
                decode_response(RespPayload, Context)
        after TimeoutMs ->
            {error, <<"timeout">>}
        end,

    ok = emqx:unsubscribe(ResponseTopic),

    Result.

normalize_payload(Payload) when is_map(Payload) ->
    emqx_utils_json:encode(Payload);
normalize_payload(Payload) ->
    Payload.

decode_response(RespPayload, Context) ->
    case decode_payload(RespPayload, Context) of
        {ok, Payload0} ->
            {ok, Payload, Attachments} = emqx_agent_tool_attachments:process(
                Payload0,
                attachment_opts(Context)
            ),
            case Attachments of
                [] -> {ok, #{<<"payload">> => Payload}};
                [_ | _] -> {ok, #{<<"payload">> => Payload}, Attachments}
            end;
        {error, Reason} ->
            {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

decode_payload(RespPayload, #{<<"payload_type">> := <<"json">>}) ->
    case emqx_utils_json:safe_decode(RespPayload) of
        {ok, Payload} -> {ok, Payload};
        {error, Reason} -> {error, {invalid_json_response, Reason}}
    end;
decode_payload(RespPayload, #{<<"payload_type">> := <<"binary">>}) ->
    {ok, RespPayload}.

attachment_opts(#{
    <<"autodiscover_images">> := AutodiscoverImages,
    <<"images">> := Images
}) ->
    #{
        autodiscover_images => AutodiscoverImages,
        images => Images,
        content_type => undefined
    }.

request_payload_schema(Context) ->
    case maps:get(<<"request_payload_schema">>, Context, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA) of
        undefined -> {ok, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA};
        Schema -> decode_schema(Schema)
    end.

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
