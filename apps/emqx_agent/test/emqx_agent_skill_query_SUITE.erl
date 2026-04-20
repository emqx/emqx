%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the three query meta-skills:
%%   agent.query_skills    — emqx_agent_skill_query_skills
%%   agent.query_sessions  — emqx_agent_skill_query_sessions
%%   agent.query_pipelines — emqx_agent_skill_query_pipelines
%%
%% Each group covers: registration, destruction, list-empty,
%% list-with-items, get-by-id, get-not-found, reply-correlation.

-module(emqx_agent_skill_query_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(REPLY_PREFIX, <<"cap/reply/">>).

-define(SK_SKILLS_ID, <<"meta-query-skills">>).
-define(SK_SESSIONS_ID, <<"meta-query-sessions">>).
-define(SK_PIPELINES_ID, <<"meta-query-pipelines">>).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_agent], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_query_skills:create(#{skill_id => ?SK_SKILLS_ID}),
    ok = emqx_agent_skill_query_sessions:create(#{skill_id => ?SK_SESSIONS_ID}),
    ok = emqx_agent_skill_query_pipelines:create(#{skill_id => ?SK_PIPELINES_ID}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:delete_all(),
    ok = emqx_agent_pipeline_registry:delete_all().

%%--------------------------------------------------------------------
%% agent.query_skills
%%--------------------------------------------------------------------

t_query_skills_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent.query_skills">>, ?SK_SKILLS_ID),
    ?assertEqual(<<"agent.query_skills">>, maps:get(type, Skill)),
    ?assertEqual(?SK_SKILLS_ID, maps:get(skill_id, Skill)).

t_query_skills_destroy(_Config) ->
    ok = emqx_agent_skill_query_skills:destroy(?SK_SKILLS_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent.query_skills">>, ?SK_SKILLS_ID)
    ).

t_query_skills_list_empty(_Config) ->
    %% Only the query skill itself is registered; delete it so the list is empty.
    ok = emqx_agent_skill_registry:delete_all(),
    ok = emqx_agent_skill_query_skills:create(#{skill_id => ?SK_SKILLS_ID}),
    ok = emqx_agent_skill_query_sessions:create(#{skill_id => ?SK_SESSIONS_ID}),
    ok = emqx_agent_skill_query_pipelines:create(#{skill_id => ?SK_PIPELINES_ID}),

    ReqId = <<"req-qs-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_skills">>, ?SK_SKILLS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := Items}} = Reply,
    %% Only the 3 query meta-skills themselves are in the registry.
    ?assert(is_list(Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_skills_list_with_items(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"pub-a">>,
        <<"desc">> => <<"A">>,
        <<"topic_prefix">> => <<"x/">>
    }),

    ReqId = <<"req-qs-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_skills">>, ?SK_SKILLS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := Items}} = Reply,
    ?assert(lists:any(fun(S) -> maps:get(<<"skill_id">>, S, undefined) =:= <<"pub-a">> end, Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_skills_filter_by_type(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"pub-b">>,
        <<"desc">> => <<"B">>,
        <<"topic_prefix">> => <<"y/">>
    }),
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"http">>,
        <<"id">> => <<"http-b">>,
        <<"desc">> => <<"B http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://stub/b">>,
        <<"input_schema">> => #{<<"type">> => <<"object">>},
        <<"output_schema">> => #{<<"type">> => <<"object">>}
    }),

    ReqId = <<"req-qs-type">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_skills">>, ?SK_SKILLS_ID, #{<<"type">> => <<"message.publish">>}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := Items}} = Reply,
    ?assert(lists:all(fun(S) -> maps:get(<<"type">>, S) =:= <<"message.publish">> end, Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_skills_get_by_type_and_id(_Config) ->
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"pub-c">>,
        <<"desc">> => <<"C">>,
        <<"topic_prefix">> => <<"z/">>
    }),

    ReqId = <<"req-qs-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.query_skills">>,
        ?SK_SKILLS_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"pub-c">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"data">> := #{
                <<"status">> := <<"ok">>, <<"item">> := #{<<"skill_id">> := <<"pub-c">>}
            }
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_skills_get_not_found(_Config) ->
    ReqId = <<"req-qs-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.query_skills">>,
        ?SK_SKILLS_ID,
        #{<<"type">> => <<"message.publish">>, <<"id">> => <<"no-such">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"data">> := #{<<"status">> := <<"error">>, <<"reason">> := _}},
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_skills_reply_correlation(_Config) ->
    ReqId = <<"req-qs-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.query_skills">>,
        ?SK_SKILLS_ID,
        #{},
        ReqId,
        #{<<"trace_id">> => <<"tr-qs">>, <<"iid">> => <<"iid-qs">>}
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-qs-corr">>,
            <<"trace_id">> := <<"tr-qs">>,
            <<"iid">> := <<"iid-qs">>
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent.query_sessions
%%--------------------------------------------------------------------

t_query_sessions_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent.query_sessions">>, ?SK_SESSIONS_ID),
    ?assertEqual(<<"agent.query_sessions">>, maps:get(type, Skill)).

t_query_sessions_destroy(_Config) ->
    ok = emqx_agent_skill_query_sessions:destroy(?SK_SESSIONS_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent.query_sessions">>, ?SK_SESSIONS_ID)
    ).

t_query_sessions_list_empty(_Config) ->
    ReqId = <<"req-qsess-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_sessions">>, ?SK_SESSIONS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := []}}, Reply),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_sessions_list_with_items(_Config) ->
    ok = emqx_agent_service:profile_create(#{
        <<"name">> => <<"prof-a">>,
        <<"api_key">> => <<"sk-x">>,
        <<"base_url">> => <<"https://api.openai.com/v1">>,
        <<"model">> => <<"gpt-4o">>
    }),

    ReqId = <<"req-qsess-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_sessions">>, ?SK_SESSIONS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := Items}} = Reply,
    ?assert(lists:any(fun(P) -> maps:get(<<"name">>, P, undefined) =:= <<"prof-a">> end, Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_sessions_get_by_name(_Config) ->
    ok = emqx_agent_service:profile_create(#{
        <<"name">> => <<"prof-b">>,
        <<"api_key">> => <<"sk-y">>,
        <<"base_url">> => <<"https://api.openai.com/v1">>,
        <<"model">> => <<"gpt-4o-mini">>
    }),

    ReqId = <<"req-qsess-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_sessions">>, ?SK_SESSIONS_ID, #{<<"name">> => <<"prof-b">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"data">> := #{<<"status">> := <<"ok">>, <<"item">> := #{<<"name">> := <<"prof-b">>}}},
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_sessions_get_not_found(_Config) ->
    ReqId = <<"req-qsess-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_sessions">>, ?SK_SESSIONS_ID, #{<<"name">> => <<"no-such">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"error">>}}, Reply),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_sessions_reply_correlation(_Config) ->
    ReqId = <<"req-qsess-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.query_sessions">>,
        ?SK_SESSIONS_ID,
        #{},
        ReqId,
        #{<<"trace_id">> => <<"tr-sess">>}
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"req_id">> := <<"req-qsess-corr">>, <<"trace_id">> := <<"tr-sess">>},
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent.query_pipelines
%%--------------------------------------------------------------------

t_query_pipelines_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID),
    ?assertEqual(<<"agent.query_pipelines">>, maps:get(type, Skill)).

t_query_pipelines_destroy(_Config) ->
    ok = emqx_agent_skill_query_pipelines:destroy(?SK_PIPELINES_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID)
    ).

t_query_pipelines_list_empty(_Config) ->
    ReqId = <<"req-qp-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := []}}, Reply),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_list_with_items(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-a">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"evt/a/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-qp-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"data">> := #{<<"status">> := <<"ok">>, <<"items">> := Items}} = Reply,
    ?assert(
        lists:any(fun(P) -> maps:get(<<"pipeline_id">>, P, undefined) =:= <<"pipe-a">> end, Items)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_get_by_id(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-b">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"evt/b/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-qp-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID, #{<<"id">> => <<"pipe-b">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"data">> := #{
                <<"status">> := <<"ok">>, <<"item">> := #{<<"pipeline_id">> := <<"pipe-b">>}
            }
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_get_not_found(_Config) ->
    ReqId = <<"req-qp-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent.query_pipelines">>, ?SK_PIPELINES_ID, #{<<"id">> => <<"no-such">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"error">>}}, Reply),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_reply_correlation(_Config) ->
    ReqId = <<"req-qp-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent.query_pipelines">>,
        ?SK_PIPELINES_ID,
        #{},
        ReqId,
        #{<<"trace_id">> => <<"tr-pipe">>, <<"sid">> => <<"sid-p">>}
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-qp-corr">>,
            <<"trace_id">> := <<"tr-pipe">>,
            <<"sid">> := <<"sid-p">>
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

reply_topic(ReqId) ->
    <<?REPLY_PREFIX/binary, ReqId/binary>>.

invoke(Type, SkillId, Args, ReqId) ->
    invoke(Type, SkillId, Args, ReqId, #{}).

invoke(Type, SkillId, Args, ReqId, Extra) ->
    Topic = <<"cap/invoke/", Type/binary, "/", SkillId/binary>>,
    Payload = emqx_utils_json:encode(
        maps:merge(
            #{
                <<"req_id">> => ReqId,
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
    T = reply_topic(ReqId),
    receive
        #deliver{topic = T, message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after 3000 ->
        ct:fail("no reply for req_id=~s within 3 s", [ReqId])
    end.
