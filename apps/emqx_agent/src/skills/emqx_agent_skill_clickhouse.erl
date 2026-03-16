%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Sample skill: ClickHouse time-series history query.
%%
%% Skill type is fixed per module (?SKILL_TYPE).
%% Multiple instances with different IDs can be created via create/1.
%%
%% Invoke topic:  cap/invoke/<TYPE>/<ID>
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
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1]).

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
%%   skill_id      => binary()  — unique instance identifier
%%   desc          => binary()  — human-readable description
%%   input_schema  => map()     — full JSON Schema for request args
%%   output_schema => map()     — full JSON Schema for response
%%   query         => binary()  — SQL template with ${var} placeholders
-spec create(Context :: map()) -> ok | {error, missing_skill_id}.
create(
    #{skill_id := SkillId, desc := Desc, input_schema := InputSchema, output_schema := OutputSchema} =
        Context
) ->
    Skill = #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"ClickHouse Time-series History">>,
        description => Desc,
        context => Context,
        input_schema => InputSchema,
        output_schema => OutputSchema
    },
    emqx_agent_skill_registry:register(Skill).

-spec destroy(emqx_agent_skill_registry:skill_id()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id,
    description := Desc,
    context := Ctx,
    input_schema := InSchema,
    output_schema := OutSchema
}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"query">> => maps:get(query, Ctx, <<>>),
        <<"input_schema">> => InSchema,
        <<"output_schema">> => OutSchema
    }.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

%% Match cap/invoke/clickhouse.history/<id>
on_message_publish(
    #message{topic = <<"cap/invoke/clickhouse.history/", SkillId/binary>>, payload = Payload} =
        Message
) ->
    handle_invoke(SkillId, Payload),
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    Request = emqx_utils_json:decode(Payload),
    case emqx_agent_skill_registry:lookup(?SKILL_TYPE, SkillId) of
        {error, not_found} -> ok;
        {ok, _Skill} -> do_reply(SkillId, Request)
    end.

do_reply(SkillId, Request) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => ?SKILL_TYPE, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => fake_answer(Request)
    }),
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.

%% Returns a fake response matching output_schema.
%% Timestamps are relative to now so the stub data looks current.
%% Real args (device_id, metric, window_min, etc.) are ignored until a
%% ClickHouse backend is wired up.
fake_answer(_Request) ->
    Now = erlang:system_time(second),
    Row = fun(OffsetSec, Avg, Min, Max, Cnt) ->
        T = calendar:system_time_to_rfc3339(Now - OffsetSec, [{unit, second}, {offset, "Z"}]),
        #{
            <<"t">> => list_to_binary(T),
            <<"avg">> => Avg,
            <<"min">> => Min,
            <<"max">> => Max,
            <<"cnt">> => Cnt
        }
    end,
    #{
        <<"rows">> => [
            Row(300, 42.1, 40.0, 44.0, 12),
            Row(0, 43.5, 41.0, 46.0, 8)
        ]
    }.
