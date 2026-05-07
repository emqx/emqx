%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the delete meta-skills:
%%   agent.delete_skill    — emqx_agent_skill_delete_skill
%%   agent.delete_pipeline — emqx_agent_skill_delete_pipeline
%%
%% Guards tested:
%%   delete_skill    — refused when skill is referenced in a pipeline step
%%   delete_pipeline — refused when pipeline is active

-module(emqx_agent_skill_delete_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SK_DEL_SKILL_ID, <<"meta-del-skill">>).
-define(SK_DEL_PIPELINE_ID, <<"meta-del-pipeline">>).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_delete_skill:create(#{skill_id => ?SK_DEL_SKILL_ID}),
    ok = emqx_agent_skill_delete_pipeline:create(#{skill_id => ?SK_DEL_PIPELINE_ID}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:delete_all(),
    ok = emqx_agent_pipeline_registry:delete_all().

%%--------------------------------------------------------------------
%% agent.delete_skill — registration
%%--------------------------------------------------------------------

t_delete_skill_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent.delete_skill">>, ?SK_DEL_SKILL_ID),
    ?assertEqual(<<"agent.delete_skill">>, maps:get(type, Skill)).

t_delete_skill_destroy(_Config) ->
    ok = emqx_agent_skill_delete_skill:destroy(?SK_DEL_SKILL_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent.delete_skill">>, ?SK_DEL_SKILL_ID)
    ).

%%--------------------------------------------------------------------
%% agent.delete_skill — invocation
%%--------------------------------------------------------------------

t_delete_skill_ok(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"to-del-pub">>,
        <<"desc">> => <<"del me">>,
        <<"topic_prefix">> => <<"x/">>
    }),

    ReqId = <<"req-ds-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_skill">>,
        ?SK_DEL_SKILL_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"to-del-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"ok">>}, cap_response(Reply)),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"message.publish">>, <<"to-del-pub">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_skill_not_found(_Config) ->
    ReqId = <<"req-ds-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_skill">>,
        ?SK_DEL_SKILL_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"no-such">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_skill_in_use_by_call_skill(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"used-pub">>,
        <<"desc">> => <<"used">>,
        <<"topic_prefix">> => <<"x/">>
    }),
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-uses-pub">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"evt/x">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"message.publish@used-pub">>,
                <<"args">> => #{}
            }
        ]
    }),

    ReqId = <<"req-ds-inuse-cs">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_skill">>,
        ?SK_DEL_SKILL_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"used-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_skill_in_use_by_llm_tools(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"tool-pub">>,
        <<"desc">> => <<"tool">>,
        <<"topic_prefix">> => <<"t/">>
    }),
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-uses-tool">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"evt/t">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"llm">>,
                <<"type">> => <<"llm_loop">>,
                <<"provider_name">> => <<"some-provider">>,
                <<"model">> => <<"test-model">>,
                <<"instructions">> => <<"test">>,
                <<"tools">> => [<<"message.publish@tool-pub">>],
                <<"set_result_schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}}
                },
                <<"result_path">> => <<"$.result">>
            }
        ]
    }),

    ReqId = <<"req-ds-inuse-llm">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_skill">>,
        ?SK_DEL_SKILL_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"tool-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent.delete_pipeline — registration
%%--------------------------------------------------------------------

t_delete_pipeline_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(
        <<"agent.delete_pipeline">>, ?SK_DEL_PIPELINE_ID
    ),
    ?assertEqual(<<"agent.delete_pipeline">>, maps:get(type, Skill)).

t_delete_pipeline_destroy(_Config) ->
    ok = emqx_agent_skill_delete_pipeline:destroy(?SK_DEL_PIPELINE_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent.delete_pipeline">>, ?SK_DEL_PIPELINE_ID)
    ).

%%--------------------------------------------------------------------
%% agent.delete_pipeline — invocation
%%--------------------------------------------------------------------

t_delete_pipeline_ok(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"to-del-pipe">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"evt/del/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-dp-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_pipeline">>,
        ?SK_DEL_PIPELINE_ID,
        #{<<"id">> => <<"to-del-pipe">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"ok">>}, cap_response(Reply)),
    ?assertEqual({error, not_found}, emqx_agent_service:pipeline_get(<<"to-del-pipe">>)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_pipeline_not_found(_Config) ->
    ReqId = <<"req-dp-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_pipeline">>,
        ?SK_DEL_PIPELINE_ID,
        #{<<"id">> => <<"no-such-pipe">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_pipeline_active(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"active-pipe">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"evt/act/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-dp-active">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_pipeline">>,
        ?SK_DEL_PIPELINE_ID,
        #{<<"id">> => <<"active-pipe">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    %% Pipeline must still exist.
    ?assertMatch({ok, _}, emqx_agent_service:pipeline_get(<<"active-pipe">>)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_pipeline_reply_correlation(_Config) ->
    ReqId = <<"req-dp-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.delete_pipeline">>,
        ?SK_DEL_PIPELINE_ID,
        #{<<"id">> => <<"no-such">>},
        ReqId,
        #{<<"trace_id">> => <<"tr-dp">>, <<"iid">> => <<"iid-dp">>}
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-dp-corr">>,
            <<"trace_id">> := <<"tr-dp">>,
            <<"iid">> := <<"iid-dp">>
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

reply_topic(ReqId) ->
    <<"cap/+/+/response/", ReqId/binary>>.

invoke(Type, SkillId, Args, ReqId) ->
    invoke(Type, SkillId, Args, ReqId, #{}).

invoke(Type, SkillId, Args, ReqId, Extra) ->
    Topic = <<"cap/", Type/binary, "/", SkillId/binary, "/request/", ReqId/binary>>,
    Payload = emqx_utils_json:encode(
        maps:merge(
            #{
                <<"trace_id">> => null,
                <<"iid">> => null,
                <<"sid">> => null,
                <<"args">> => Args
            },
            Extra
        )
    ),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)),
    ok.

recv_reply(ReqId) ->
    receive
        #deliver{message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after 3000 ->
        ct:fail("no reply for req_id=~s within 3 s", [ReqId])
    end.

cap_response(Reply) ->
    emqx_agent_skill_helpers:cap_response(Reply).
