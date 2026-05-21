%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_mqtt_request).

-moduledoc """
MQTT request/reply skill (MQTT 5 request pattern).

Publishes a request to a topic rooted under a configured prefix and
waits synchronously for a response.  The response topic is embedded
in the outbound message as an MQTT 5 `Response-Topic` property so
that any MQTT 5-aware responder can reply without out-of-band
coordination.

Invoke topic:  $cap/message__request/<skill_id>/request/<req_id>
Reply  topic:  $cap/message__request/<skill_id>/response/<req_id>

Context keys:
  skill_id     => binary()  — unique instance identifier
  desc         => binary()  — human-readable description
  topic_prefix => binary()  — prepended to the agent-supplied topic

Input args:
  topic       => binary()   — topic suffix; combined with topic_prefix
  payload     => json()     — request payload
  from        => binary()   — publisher identity (optional)
  qos         => 0|1|2      — QoS level (optional, default 0)
  timeout_ms  => integer()  — response wait limit in ms (optional, default 5000)

Skill output (in `data`):
  status  => <<"ok">>    — response arrived in time
    payload => binary()  — raw response payload
  status  => <<"error">>
    reason  => <<"timeout">> | binary()
""".

-behaviour(emqx_agent_skill).

-include_lib("emqx/include/emqx.hrl").

-define(SKILL_TYPE, <<"message__request">>).
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
    emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_skill_registry:unregister_type(?SKILL_TYPE).

-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(#{skill_id := SkillId, desc := Desc, topic_prefix := TopicPrefix} = Context) ->
    RequestPayloadSchema =
        case maps:get(request_payload_schema, Context, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA) of
            undefined -> ?DEFAULT_REQUEST_PAYLOAD_SCHEMA;
            Schema -> Schema
        end,
    {ok, #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<Desc/binary, " — Request">>,
        description =>
            <<"Send an MQTT request to a topic under the prefix: ", TopicPrefix/binary,
                " and wait for a response">>,
        context => #{
            skill_id => SkillId,
            topic_prefix => TopicPrefix,
            request_payload_schema => RequestPayloadSchema
        },
        input_schema => ?INPUT_SCHEMA(RequestPayloadSchema)
    }}.

-spec destroy(map()) -> ok.
destroy(_Skill) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id,
    description := Desc,
    context := #{topic_prefix := TopicPrefix} = Ctx,
    input_schema := InputSchema
}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"request_payload_schema">> => maps:get(
            request_payload_schema, Ctx, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA
        ),
        <<"input_schema">> => InputSchema
    }.

handle_invoke(#{topic_prefix := TopicPrefix}, Request) ->
    do_request(TopicPrefix, Request).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_request(TopicPrefix, Request) ->
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

    do_round_trip(FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs).

do_round_trip(FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs) ->
    %% Subscribe before publishing so we cannot miss the response.
    ok = emqx:subscribe(ResponseTopic),

    Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
    Msg2 = emqx_message:set_header(properties, #{'Response-Topic' => ResponseTopic}, Msg),
    _ = emqx_broker:publish(Msg2),

    Result =
        receive
            #deliver{topic = ResponseTopic, message = #message{payload = RespPayload}} ->
                {ok, #{<<"payload">> => RespPayload}}
        after TimeoutMs ->
            {error, <<"timeout">>}
        end,

    ok = emqx:unsubscribe(ResponseTopic),

    Result.

normalize_payload(Payload) when is_map(Payload) ->
    emqx_utils_json:encode(Payload);
normalize_payload(Payload) ->
    Payload.
