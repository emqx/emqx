%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Sample skill: ClickHouse time-series history query.
%%
%% Skill type is fixed per module (?SKILL_TYPE).
%% Multiple instances with different IDs can be created via create/1.
%%
%% Invoke topic:  cap/invoke/<TYPE>/<ID>/<VERSION>
%% Reply  topic:  cap/reply/<req_id>
%%
%% Lifecycle:
%%   init()        — register the message.publish hook (once per node)
%%   create(Ctx)   — register a skill instance; skill_id taken from Ctx
%%   destroy(Id)   — unregister a skill instance from the registry
%%   deinit()      — remove the message.publish hook

-module(emqx_agent_skill_clickhouse).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"clickhouse.history">>).
-define(SKILL_VERSION, <<"1">>).
%% Prefix matched in the hook: cap/invoke/clickhouse.history/<id>/1
-define(INVOKE_PREFIX, <<"cap/invoke/clickhouse.history/">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-export([init/0, deinit/0, create/1, destroy/1]).

%% Hook callback — must be exported
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
%%   skill_id => binary()              — unique instance identifier
%%   desc     => binary()              — human-readable description
%%   input    => #{binary() => map()}  — JSON Schema properties for args (all required)
%%   output   => #{binary() => map()}  — JSON Schema properties for result (all required)
%%   query    => binary()              — SQL template with ${var} placeholders
-spec create(Context :: map()) -> ok | {error, missing_skill_id}.
create(#{skill_id := SkillId, desc := Desc, input := InputProps, output := OutputProps} = Context) ->
    Skill = #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        version => ?SKILL_VERSION,
        display_name => <<"ClickHouse Time-series History">>,
        description => Desc,
        context => Context,
        input_schema => emqx_agent_skill_schema:to_schema(InputProps),
        output_schema => emqx_agent_skill_schema:to_schema(OutputProps)
    },
    emqx_agent_skill_registry:register(Skill).

-spec destroy(emqx_agent_skill_registry:skill_id()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(SkillId).

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

%% Match cap/invoke/clickhouse.history/<id>/<version>
on_message_publish(
    #message{topic = <<"cap/invoke/clickhouse.history/", Rest/binary>>, payload = Payload} = Message
) ->
    case binary:split(Rest, <<"/">>) of
        [Id, ?SKILL_VERSION] -> handle_invoke(Id, Payload);
        _ -> ok
    end,
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    Request = emqx_utils_json:decode(Payload),

    case emqx_agent_skill_registry:lookup(SkillId) of
        {error, not_found} -> ok;
        {ok, _Skill} -> do_reply(SkillId, Request)
    end.

do_reply(SkillId, Request) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{
            <<"type">> => ?SKILL_TYPE,
            <<"id">> => SkillId,
            <<"version">> => ?SKILL_VERSION
        },
        <<"frame">> => <<"unary">>,
        <<"data">> => fake_answer(Request)
    }),

    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    ReplyPayload = emqx_utils_json:encode(Reply),
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, ReplyPayload),
    _ = emqx_broker:publish(Msg),
    ok.

%% Returns a fake response matching output_schema.
%% args in Request are ignored until a real ClickHouse backend is wired.
fake_answer(_Request) ->
    #{
        <<"rows">> => [
            #{
                <<"t">> => <<"2026-03-06T10:00:00Z">>,
                <<"avg">> => 42.1,
                <<"min">> => 40.0,
                <<"max">> => 44.0,
                <<"cnt">> => 12
            },
            #{
                <<"t">> => <<"2026-03-06T10:05:00Z">>,
                <<"avg">> => 43.5,
                <<"min">> => 41.0,
                <<"max">> => 46.0,
                <<"cnt">> => 8
            }
        ]
    }.
