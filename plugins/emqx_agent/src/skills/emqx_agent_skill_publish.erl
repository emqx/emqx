%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT message-publish skill.
%%
%% Allows an LLM agent to publish MQTT messages to topics rooted under a
%% configured prefix.  The prefix is fixed at creation time so that the
%% agent cannot publish outside its authorised namespace.
%%
%% Invoke topic:  cap/message__publish/<skill_id>/request/<req_id>
%% Reply  topic:  cap/message__publish/<skill_id>/response/<req_id>
%%
%% Context keys:
%%   skill_id     => binary()  — unique instance identifier
%%   desc         => binary()  — human-readable description
%%   topic_prefix => binary()  — prepended to the agent-supplied topic
%%                               (e.g. <<"devices/room1/">>)
%%
%% Input args (fixed wrapper + configurable payload schema):
%%   topic   => binary()  — topic suffix; combined with topic_prefix
%%   payload => json()    — payload value validated by payload_schema
%%   from    => binary()  — publisher identity (optional, defaults to skill_id)
%%   qos     => 0 | 1 | 2  — QoS level (optional, default 0)
%%
%% Lifecycle:
%%   init()        — register the skill type
%%   create(Ctx)   — build a runtime skill instance
%%   destroy(Skill) — clean up runtime resources owned by the skill
%%   deinit()      — unregister the skill type

-module(emqx_agent_skill_publish).

-include_lib("emqx/include/logger.hrl").

-define(SKILL_TYPE, <<"message__publish">>).

-define(DEFAULT_PAYLOAD_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"message">> => #{<<"type">> => <<"string">>}
    },
    <<"required">> => [<<"message">>]
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
    emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_skill_registry:unregister_type(?SKILL_TYPE).

%% Context keys:
%%   skill_id     => binary()
%%   desc         => binary()
%%   topic_prefix => binary()
-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(#{
    skill_id := SkillId, desc := Desc, topic_prefix := TopicPrefix, input_schema := InputSchema
}) ->
    create(#{
        skill_id => SkillId,
        desc => Desc,
        topic_prefix => TopicPrefix,
        payload_schema => InputSchema
    });
create(#{
    skill_id := SkillId,
    desc := Desc,
    topic_prefix := TopicPrefix,
    payload_schema := PayloadSchema
}) ->
    create_with_payload_schema(SkillId, Desc, TopicPrefix, PayloadSchema);
create(#{skill_id := SkillId, desc := Desc, topic_prefix := TopicPrefix}) ->
    create_with_payload_schema(SkillId, Desc, TopicPrefix, ?DEFAULT_PAYLOAD_SCHEMA).

create_with_payload_schema(SkillId, Desc, TopicPrefix, undefined) ->
    create_with_payload_schema(SkillId, Desc, TopicPrefix, ?DEFAULT_PAYLOAD_SCHEMA);
create_with_payload_schema(SkillId, Desc, TopicPrefix, PayloadSchema) ->
    {ok, #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<Desc/binary, " — Publish">>,
        description =>
            <<"Publish an MQTT message to a topic under the prefix: ", TopicPrefix/binary>>,
        context => #{
            skill_id => SkillId,
            topic_prefix => TopicPrefix,
            payload_schema => PayloadSchema
        },
        input_schema => ?INPUT_SCHEMA(PayloadSchema)
    }}.

-spec destroy(map()) -> ok.
destroy(_Skill) ->
    ok.

-spec to_map(map()) -> map().
to_map(
    #{
        skill_id := Id,
        description := Desc,
        context := #{topic_prefix := TopicPrefix, payload_schema := PayloadSchema},
        input_schema := InputSchema
    }
) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"payload_schema">> => PayloadSchema,
        <<"input_schema">> => InputSchema
    };
to_map(
    #{
        skill_id := Id,
        description := Desc,
        context := #{topic_prefix := TopicPrefix},
        input_schema := InputSchema
    }
) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix,
        <<"payload_schema">> => maps:get(
            <<"payload">>, maps:get(<<"properties">>, InputSchema, #{}), ?DEFAULT_PAYLOAD_SCHEMA
        ),
        <<"input_schema">> => InputSchema
    }.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(#{topic_prefix := TopicPrefix}, Request) ->
    do_publish(TopicPrefix, Request).

do_publish(TopicPrefix, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    TopicSuffix = maps:get(<<"topic">>, Args),
    MsgPayload = normalize_payload(maps:get(<<"payload">>, Args)),
    From = maps:get(<<"from">>, Args, <<>>),
    Qos = maps:get(<<"qos">>, Args, 0),

    FullTopic = <<TopicPrefix/binary, TopicSuffix/binary>>,

    try
        Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
        _ = emqx_broker:publish(Msg),
        {ok, #{<<"topic">> => FullTopic}}
    catch
        Class:Reason ->
            ?SLOG(error, #{
                msg => "skill_publish_failed",
                topic => FullTopic,
                error => Class,
                reason => Reason
            }),
            {error, iolist_to_binary(io_lib:format("~p", [Reason]))}
    end.

normalize_payload(Payload) when is_map(Payload) ->
    emqx_utils_json:encode(Payload);
normalize_payload(Payload) ->
    Payload.
