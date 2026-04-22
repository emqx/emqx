%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: delete a registered skill.
%%
%% Refuses deletion if the skill is referenced in any pipeline step
%% (call_skill.skill or llm_loop.tools).
%%
%% Args:
%%   type — skill type, e.g. "message.publish"  (required)
%%   id   — skill instance id                   (required)
%%
%% Invoke topic:  cap/invoke/agent.delete_skill/<skill_id>/request
%% Reply  topic:  cap/invoke/agent.delete_skill/<skill_id>/response/<req_id>

-module(emqx_agent_skill_delete_skill).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.delete_skill">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Skill type, e.g. message.publish">>
        },
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Skill instance id">>}
    },
    <<"required">> => [<<"type">>, <<"id">>]
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"reason">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Present when status=error">>
        },
        <<"used_by">> => #{
            <<"type">> => <<"array">>,
            <<"description">> => <<"Pipeline ids that reference this skill">>
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

-spec create(map()) -> ok.
create(#{skill_id := SkillId}) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"Delete Skill">>,
        description =>
            <<"Delete a registered skill. Refused if the skill is used in any pipeline.">>,
        context => #{skill_id => SkillId},
        input_schema => ?INPUT_SCHEMA,
        output_schema => ?OUTPUT_SCHEMA
    }).

-spec destroy(binary()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, description := Desc, input_schema := In, output_schema := Out}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => In,
        <<"output_schema">> => Out
    }.

%%--------------------------------------------------------------------
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<"cap/invoke/agent.delete_skill/", Rest/binary>>, payload = Payload} = Msg
) ->
    case binary:split(Rest, <<"/">>) of
        [SkillId, <<"request">>] -> handle_invoke(SkillId, Payload);
        _ -> ok
    end,
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    Request = emqx_utils_json:decode(Payload),
    Args = maps:get(<<"args">>, Request, #{}),
    Result = do_delete(Args),
    reply(SkillId, Request, Result).

do_delete(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:skill_delete(Type, Id) of
        ok ->
            #{<<"status">> => <<"ok">>};
        {error, not_found} ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"skill not found">>};
        {error, {in_use, PipelineIds}} ->
            Joined = join_ids(PipelineIds),
            #{
                <<"status">> => <<"error">>,
                <<"reason">> => <<"skill is used in pipeline(s): ", Joined/binary>>,
                <<"used_by">> => PipelineIds
            }
    end;
do_delete(_) ->
    #{<<"status">> => <<"error">>, <<"reason">> => <<"missing required fields: type, id">>}.

join_ids(Ids) ->
    iolist_to_binary(lists:join(<<", ">>, Ids)).

reply(SkillId, Request, Data) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => ?SKILL_TYPE, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Data
    }),
    ReplyTopic = <<"cap/invoke/", ?SKILL_TYPE/binary, "/", SkillId/binary, "/response/", ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.
