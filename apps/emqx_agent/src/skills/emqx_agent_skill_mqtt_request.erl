%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT request/reply skill (MQTT 5 request pattern).
%%
%% Publishes a request to a topic rooted under a configured prefix and
%% waits synchronously for a response.  The response topic is embedded
%% in the outbound message as an MQTT 5 `Response-Topic` property so
%% that any MQTT 5-aware responder can reply without out-of-band
%% coordination.
%%
%% Invoke topic:  cap/invoke/message.request/<skill_id>
%% Reply  topic:  cap/reply/<req_id>
%%
%% Context keys:
%%   skill_id     => binary()  — unique instance identifier
%%   desc         => binary()  — human-readable description
%%   topic_prefix => binary()  — prepended to the agent-supplied topic
%%
%% Input args:
%%   topic       => binary()   — topic suffix; combined with topic_prefix
%%   payload     => json()     — request payload
%%   from        => binary()   — publisher identity (optional)
%%   qos         => 0|1|2      — QoS level (optional, default 0)
%%   timeout_ms  => integer()  — response wait limit in ms (optional, default 5000)
%%
%% Skill output (in `data`):
%%   status  => <<"ok">>    — response arrived in time
%%     payload => binary()  — raw response payload
%%   status  => <<"error">>
%%     reason  => <<"timeout">> | binary()

-module(emqx_agent_skill_mqtt_request).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-define(SKILL_TYPE, <<"message.request">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).
-define(RESPONSE_TOPIC_PREFIX, <<"cap/tmp/response/">>).
-define(DEFAULT_TIMEOUT_MS, 5000).

-define(DEFAULT_REQUEST_PAYLOAD_SCHEMA, #{
    <<"description">> => <<"Request payload (any JSON value or string)">>
}).

-define(DEFAULT_RESPONSE_SCHEMA, #{
    <<"description">> => <<"Response payload (present when status = ok)">>
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

-define(OUTPUT_SCHEMA(ResponseSchema), #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{
            <<"type">> => <<"string">>,
            <<"enum">> => [<<"ok">>, <<"error">>]
        },
        <<"payload">> => ResponseSchema,
        <<"reason">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Error reason (present when status = error)">>
        }
    },
    <<"required">> => [<<"status">>]
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1]).
-export([on_message_publish/1]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

-spec create(Context :: map()) -> ok.
create(#{skill_id := SkillId, desc := Desc, topic_prefix := TopicPrefix} = Context) ->
    RequestPayloadSchema = maps:get(
        request_payload_schema, Context, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA
    ),
    ResponseSchema = maps:get(response_schema, Context, ?DEFAULT_RESPONSE_SCHEMA),
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<Desc/binary, " — Request">>,
        description =>
            <<"Send an MQTT request to a topic under the prefix: ", TopicPrefix/binary,
                " and wait for a response">>,
        context => #{
            skill_id => SkillId,
            topic_prefix => TopicPrefix,
            request_payload_schema => RequestPayloadSchema,
            response_schema => ResponseSchema
        },
        input_schema => ?INPUT_SCHEMA(RequestPayloadSchema),
        output_schema => ?OUTPUT_SCHEMA(ResponseSchema)
    }).

-spec destroy(binary()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id,
    description := Desc,
    context := #{topic_prefix := TopicPrefix} = Ctx,
    input_schema := InputSchema,
    output_schema := OutputSchema
}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"request_payload_schema">> => maps:get(
            request_payload_schema, Ctx, ?DEFAULT_REQUEST_PAYLOAD_SCHEMA
        ),
        <<"response_schema">> => maps:get(response_schema, Ctx, ?DEFAULT_RESPONSE_SCHEMA),
        <<"input_schema">> => InputSchema,
        <<"output_schema">> => OutputSchema
    }.

%%--------------------------------------------------------------------
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(
    #message{
        topic = <<"cap/invoke/message.request/", SkillId/binary>>, payload = Payload
    } = Message
) ->
    handle_invoke(SkillId, Payload),
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    case emqx_agent_skill_registry:lookup(?SKILL_TYPE, SkillId) of
        {error, not_found} ->
            ok;
        {ok, #{context := #{topic_prefix := TopicPrefix}}} ->
            Request = emqx_utils_json:decode(Payload),
            do_request(SkillId, TopicPrefix, Request)
    end.

do_request(SkillId, TopicPrefix, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    TopicSuffix = maps:get(<<"topic">>, Args),
    MsgPayload = normalize_payload(maps:get(<<"payload">>, Args, <<>>)),
    From = maps:get(<<"from">>, Args, SkillId),
    Qos = maps:get(<<"qos">>, Args, 0),
    TimeoutMs = maps:get(<<"timeout_ms">>, Args, ?DEFAULT_TIMEOUT_MS),

    FullTopic = <<TopicPrefix/binary, TopicSuffix/binary>>,
    ReqId = maps:get(<<"req_id">>, Request),

    %% Unique response topic — scoped to this skill instance and request.
    Unique = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    ResponseTopic = <<?RESPONSE_TOPIC_PREFIX/binary, SkillId/binary, "/", Unique/binary>>,

    _Pid = spawn(fun() ->
        do_round_trip(
            SkillId, Request, ReqId, FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs
        )
    end),
    ok.

do_round_trip(SkillId, Request, ReqId, FullTopic, MsgPayload, From, Qos, ResponseTopic, TimeoutMs) ->
    %% Subscribe before publishing so we cannot miss the response.
    ok = emqx:subscribe(ResponseTopic),

    Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
    Msg2 = emqx_message:set_header(properties, #{'Response-Topic' => ResponseTopic}, Msg),
    _ = emqx_broker:publish(Msg2),

    Result =
        receive
            #deliver{topic = ResponseTopic, message = #message{payload = RespPayload}} ->
                #{<<"status">> => <<"ok">>, <<"payload">> => RespPayload}
        after TimeoutMs ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"timeout">>}
        end,

    ok = emqx:unsubscribe(ResponseTopic),

    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => ?SKILL_TYPE, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Result
    }),
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    ReplyMsg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(ReplyMsg),
    ok.

normalize_payload(Payload) when is_map(Payload) ->
    emqx_utils_json:encode(Payload);
normalize_payload(Payload) ->
    Payload.
