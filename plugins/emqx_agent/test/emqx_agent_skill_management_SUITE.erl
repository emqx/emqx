%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the management meta-skills:
%%   agent__create_skill    — emqx_agent_skill_create_skill
%%   agent__create_pipeline — emqx_agent_skill_create_pipeline
%%
%% Each group covers: registration, destruction, successful invocation,
%% error cases, and reply correlation forwarding.

-module(emqx_agent_skill_management_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SK_SKILL_ID, <<"meta-create-skill">>).
-define(SK_PIPELINE_ID, <<"meta-create-pipeline">>).

-define(VALID_INPUT_SCHEMA,
    <<"{\"type\":\"object\",\"properties\":{},\"required\":[],\"additionalProperties\":false}">>
).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_resource, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"agent__create_skill">>, <<"id">> => ?SK_SKILL_ID
    }),
    ok = emqx_agent_service:skill_create(#{
        <<"type">> => <<"agent__create_pipeline">>, <<"id">> => ?SK_PIPELINE_ID
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% agent__create_skill
%%--------------------------------------------------------------------

t_create_skill_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent__create_skill">>, ?SK_SKILL_ID),
    ?assertMatch(#{type := <<"agent__create_skill">>}, Skill),
    ?assertEqual(?SK_SKILL_ID, maps:get(skill_id, Skill)).

t_create_skill_destroy(_Config) ->
    ok = emqx_agent_service:skill_delete(<<"agent__create_skill">>, ?SK_SKILL_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent__create_skill">>, ?SK_SKILL_ID)
    ).

t_create_skill_invoke_message_publish(_Config) ->
    ReqId = <<"req-cs-pub">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_skill">>,
        ?SK_SKILL_ID,
        #{
            <<"definition">> => #{
                <<"type">> => <<"message__publish">>,
                <<"id">> => <<"dyn-pub">>,
                <<"desc">> => <<"Dynamic publish">>,
                <<"topic_prefix">> => <<"dyn/pub/">>
            }
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{
                <<"skill_id">> := <<"dyn-pub">>,
                <<"type">> := <<"message__publish">>
            }
        },
        cap_response(Reply)
    ),
    ?assertMatch(
        {ok, #{type := <<"message__publish">>}},
        emqx_agent_skill_registry:lookup(<<"message__publish">>, <<"dyn-pub">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_skill_invoke_http(_Config) ->
    ReqId = <<"req-cs-http">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_skill">>,
        ?SK_SKILL_ID,
        #{
            <<"definition">> => #{
                <<"type">> => <<"http">>,
                <<"id">> => <<"dyn-http">>,
                <<"desc">> => <<"Dynamic HTTP">>,
                <<"method">> => <<"post">>,
                <<"url">> => <<"http://stub/api">>,
                <<"input_schema">> => ?VALID_INPUT_SCHEMA
            }
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"type">> := <<"http">>}},
        cap_response(Reply)
    ),
    ?assertMatch(
        {ok, #{type := <<"http">>}},
        emqx_agent_skill_registry:lookup(<<"http">>, <<"dyn-http">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_skill_invoke_unknown_type(_Config) ->
    ReqId = <<"req-cs-unk">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_skill">>,
        ?SK_SKILL_ID,
        #{
            <<"type">> => <<"no_such_type">>,
            <<"id">> => <<"x">>
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := _},
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_skill_invoke_missing_required_field(_Config) ->
    ReqId = <<"req-cs-miss">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    %% message__publish requires topic_prefix
    invoke(
        <<"agent__create_skill">>,
        ?SK_SKILL_ID,
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"x">>,
            <<"desc">> => <<"x">>
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_skill_reply_correlation(_Config) ->
    ReqId = <<"req-cs-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_skill">>,
        ?SK_SKILL_ID,
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"corr-pub">>,
            <<"desc">> => <<"x">>,
            <<"topic_prefix">> => <<"x/">>
        },
        ReqId,
        #{
            <<"trace_id">> => <<"tr-1">>,
            <<"iid">> => <<"iid-1">>,
            <<"sid">> => <<"sid-1">>
        }
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-cs-corr">>,
            <<"trace_id">> := <<"tr-1">>,
            <<"iid">> := <<"iid-1">>,
            <<"sid">> := <<"sid-1">>
        },
        Reply
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent__create_pipeline
%%--------------------------------------------------------------------

t_create_pipeline_registers(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID),
    ?assertMatch(#{type := <<"agent__create_pipeline">>}, Skill),
    ?assertEqual(?SK_PIPELINE_ID, maps:get(skill_id, Skill)).

t_create_pipeline_destroy(_Config) ->
    ok = emqx_agent_service:skill_delete(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID)
    ).

t_create_pipeline_invoke_creates_pipeline(_Config) ->
    ReqId = <<"req-pipe-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"dyn-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"evt/dyn/+">>},
            <<"steps">> => [
                #{
                    <<"id">> => <<"s1">>,
                    <<"type">> => <<"call_skill">>,
                    <<"skill">> => <<"message__publish@some-pub">>,
                    <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"hi">>}
                }
            ]
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{
                <<"pipeline_id">> := <<"dyn-pipeline">>,
                <<"active">> := false
            }
        },
        cap_response(Reply)
    ),
    ?assertMatch(
        {ok, #{<<"pipeline_id">> := <<"dyn-pipeline">>}},
        emqx_agent_config:lookup_pipeline(<<"dyn-pipeline">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%% The LLM cannot activate a pipeline at creation time — active is always false.
t_create_pipeline_enforces_active_false(_Config) ->
    ReqId = <<"req-pipe-active">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"forced-active">>,
            <<"active">> => true,
            <<"trigger">> => #{<<"topic">> => <<"evt/forced/+">>},
            <<"steps">> => []
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"active">> := false}},
        cap_response(Reply)
    ),
    {ok, Def} = emqx_agent_config:lookup_pipeline(<<"forced-active">>),
    ?assertMatch(#{<<"active">> := false}, Def),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_pipeline_invoke_missing_pipeline_id(_Config) ->
    ReqId = <<"req-pipe-miss">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"trigger">> => #{<<"topic">> => <<"evt/x">>},
            <<"steps">> => []
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := _},
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_pipeline_reply_correlation(_Config) ->
    ReqId = <<"req-pipe-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"corr-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"evt/corr/+">>},
            <<"steps">> => []
        },
        ReqId,
        #{
            <<"trace_id">> => <<"tr-p1">>,
            <<"iid">> => <<"iid-p1">>
        }
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-pipe-corr">>,
            <<"trace_id">> := <<"tr-p1">>,
            <<"iid">> := <<"iid-p1">>
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
