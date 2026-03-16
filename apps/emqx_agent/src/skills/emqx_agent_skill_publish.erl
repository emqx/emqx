%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT message-publish skill.
%%
%% Allows an LLM agent to publish MQTT messages to topics rooted under a
%% configured prefix.  The prefix is fixed at creation time so that the
%% agent cannot publish outside its authorised namespace.
%%
%% Invoke topic:  cap/invoke/message.publish/<skill_id>
%% Reply  topic:  cap/reply/<req_id>
%%
%% Context keys:
%%   skill_id     => binary()  — unique instance identifier
%%   desc         => binary()  — human-readable description
%%   topic_prefix => binary()  — prepended to the agent-supplied topic
%%                               (e.g. <<"devices/room1/">>)
%%
%% Input args (fixed schema):
%%   topic   => binary()  — topic suffix; combined with topic_prefix
%%   payload => binary()  — message payload
%%   from    => binary()  — publisher identity (optional, defaults to skill_id)
%%   qos     => 0 | 1 | 2  — QoS level (optional, default 0)
%%
%% Lifecycle:
%%   init()        — register the message.publish hook (once per node)
%%   create(Ctx)   — register a skill instance
%%   destroy(Id)   — unregister a skill instance
%%   deinit()      — remove the message.publish hook

-module(emqx_agent_skill_publish).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-define(SKILL_TYPE, <<"message.publish">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"topic">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Topic suffix appended to the configured prefix">>
        },
        <<"payload">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Message payload">>
        },
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

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{
            <<"type">> => <<"string">>,
            <<"enum">> => [<<"ok">>, <<"error">>]
        },
        <<"reason">> => #{<<"type">> => <<"string">>},
        <<"topic">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Full topic the message was published to">>
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

%% Context keys:
%%   skill_id     => binary()
%%   desc         => binary()
%%   topic_prefix => binary()
-spec create(Context :: map()) -> ok.
create(#{skill_id := SkillId, desc := Desc, topic_prefix := TopicPrefix}) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<Desc/binary, " — Publish">>,
        description =>
            <<"Publish an MQTT message to a topic under the prefix: ", TopicPrefix/binary>>,
        context => #{skill_id => SkillId, topic_prefix => TopicPrefix},
        input_schema => ?INPUT_SCHEMA,
        output_schema => ?OUTPUT_SCHEMA
    }).

-spec destroy(binary()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, description := Desc, context := #{topic_prefix := TopicPrefix}}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"topic_prefix">> => TopicPrefix
    }.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_message_publish(
    #message{
        topic = <<"cap/invoke/message.publish/", SkillId/binary>>, payload = Payload
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
            do_publish(SkillId, TopicPrefix, Request)
    end.

do_publish(SkillId, TopicPrefix, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    TopicSuffix = maps:get(<<"topic">>, Args),
    MsgPayload = maps:get(<<"payload">>, Args),
    From = maps:get(<<"from">>, Args, SkillId),
    Qos = maps:get(<<"qos">>, Args, 0),

    FullTopic = <<TopicPrefix/binary, TopicSuffix/binary>>,

    Result =
        try
            Msg = emqx_message:make(From, Qos, FullTopic, MsgPayload),
            _ = emqx_broker:publish(Msg),
            #{<<"status">> => <<"ok">>, <<"topic">> => FullTopic}
        catch
            Class:Reason ->
                ?SLOG(error, #{
                    msg => "skill_publish_failed",
                    skill_id => SkillId,
                    topic => FullTopic,
                    error => Class,
                    reason => Reason
                }),
                #{
                    <<"status">> => <<"error">>,
                    <<"reason">> => iolist_to_binary(io_lib:format("~p", [Reason]))
                }
        end,

    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => ?SKILL_TYPE, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Result
    }),
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    ReplyMsg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(ReplyMsg),
    ok.
