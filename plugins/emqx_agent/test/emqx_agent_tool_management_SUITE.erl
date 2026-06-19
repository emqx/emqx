%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the management meta-tools:
%%   agent__create_tool    — emqx_agent_tool_create_tool
%%   agent__create_pipeline — emqx_agent_tool_create_pipeline
%%
%% Each group covers: registration, destruction, successful invocation,
%% error cases, and reply correlation forwarding.

-module(emqx_agent_tool_management_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SK_TOOL_ID, <<"meta-create-tool">>).
-define(SK_PIPELINE_ID, <<"meta-create-pipeline">>).
-define(SK_UPDATE_TOOL_ID, <<"meta-update-tool">>).
-define(SK_UPDATE_PIPELINE_ID, <<"meta-update-pipeline">>).
-define(SK_DELETE_STEP_ID, <<"meta-delete-pipeline-step">>).
-define(SK_INSERT_STEP_ID, <<"meta-insert-pipeline-step">>).
-define(SK_UPDATE_STEP_ID, <<"meta-update-pipeline-step">>).

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
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__create_tool">>, <<"id">> => ?SK_TOOL_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__create_pipeline">>, <<"id">> => ?SK_PIPELINE_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"some-pub">>,
        <<"desc">> => <<"Some publisher">>,
        <<"topic_prefix">> => <<"some/">>
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__update_tool">>, <<"id">> => ?SK_UPDATE_TOOL_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__update_pipeline">>, <<"id">> => ?SK_UPDATE_PIPELINE_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__delete_pipeline_step">>, <<"id">> => ?SK_DELETE_STEP_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__insert_pipeline_step">>, <<"id">> => ?SK_INSERT_STEP_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__update_pipeline_step">>, <<"id">> => ?SK_UPDATE_STEP_ID
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% agent__create_tool
%%--------------------------------------------------------------------

t_create_tool_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__create_tool">>, ?SK_TOOL_ID),
    ?assertMatch(#{type := <<"agent__create_tool">>}, Tool),
    ?assertEqual(?SK_TOOL_ID, maps:get(tool_id, Tool)).

t_create_tool_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__create_tool">>, ?SK_TOOL_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__create_tool">>, ?SK_TOOL_ID)
    ).

t_create_tool_invoke_message_publish(_Config) ->
    ReqId = <<"req-cs-pub">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_tool">>,
        ?SK_TOOL_ID,
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
                <<"tool_id">> := <<"dyn-pub">>,
                <<"type">> := <<"message__publish">>
            }
        },
        cap_response(Reply)
    ),
    ?assertMatch(
        {ok, #{type := <<"message__publish">>}},
        emqx_agent_tool_registry:lookup(<<"message__publish">>, <<"dyn-pub">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_tool_invoke_http(_Config) ->
    ReqId = <<"req-cs-http">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_tool">>,
        ?SK_TOOL_ID,
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
        emqx_agent_tool_registry:lookup(<<"http">>, <<"dyn-http">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_tool_invoke_unknown_type(_Config) ->
    ReqId = <<"req-cs-unk">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_tool">>,
        ?SK_TOOL_ID,
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

t_create_tool_invoke_missing_required_field(_Config) ->
    ReqId = <<"req-cs-miss">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    %% message__publish requires topic_prefix
    invoke(
        <<"agent__create_tool">>,
        ?SK_TOOL_ID,
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

t_create_tool_reply_correlation(_Config) ->
    ReqId = <<"req-cs-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_tool">>,
        ?SK_TOOL_ID,
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

t_concurrent_tool_creates_are_retained_for_pipeline_validation(_Config) ->
    Parent = self(),
    ToolDefs = [
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"dyn-alert">>,
            <<"desc">> => <<"Dynamic alert publisher">>,
            <<"topic_prefix">> => <<"dyn/alert/">>
        },
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"dyn-status">>,
            <<"desc">> => <<"Dynamic status publisher">>,
            <<"topic_prefix">> => <<"dyn/status/">>
        },
        #{
            <<"type">> => <<"message__request">>,
            <<"id">> => <<"dyn-shot">>,
            <<"desc">> => <<"Dynamic shot request">>,
            <<"topic_prefix">> => <<"dyn/shot/">>
        },
        #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => <<"dyn-insert">>,
            <<"desc">> => <<"Dynamic insert">>,
            <<"resource">> => <<"dyn-pg">>,
            <<"query">> => <<"INSERT INTO inspections (box_id) VALUES (${box_id})">>
        }
    ],
    Pids = [
        spawn_link(fun() ->
            Parent !
                {
                    self(),
                    emqx_agent_tool_create_tool:handle_invoke(
                        #{}, #{<<"args">> => #{<<"definition">> => ToolDef}}
                    )
                }
        end)
     || ToolDef <- ToolDefs
    ],
    [?assertMatch({ok, _}, receive_result(Pid)) || Pid <- Pids],

    ?assertMatch({ok, _}, emqx_agent_service:tool_get(<<"message__publish">>, <<"dyn-alert">>)),
    ?assertMatch({ok, _}, emqx_agent_service:tool_get(<<"message__publish">>, <<"dyn-status">>)),
    ?assertMatch({ok, _}, emqx_agent_service:tool_get(<<"message__request">>, <<"dyn-shot">>)),
    ?assertMatch({ok, _}, emqx_agent_service:tool_get(<<"postgresql__query">>, <<"dyn-insert">>)),

    ?assertEqual(
        ok,
        emqx_agent_service:pipeline_create(#{
            <<"pipeline_id">> => <<"dyn-retention-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"$evt/dyn/retention">>},
            <<"steps">> => [
                #{
                    <<"id">> => <<"inspect">>,
                    <<"type">> => <<"llm_loop">>,
                    <<"provider_name">> => <<"openai">>,
                    <<"model">> => <<"gpt-5.4-mini">>,
                    <<"instructions">> => <<"Inspect the dynamic box and call set_result.">>,
                    <<"result_path">> => <<"$.inspection">>,
                    <<"tools">> => [
                        <<"message__request@dyn-shot">>,
                        <<"message__publish@dyn-alert">>
                    ],
                    <<"set_result_schema">> => ?VALID_INPUT_SCHEMA
                },
                #{
                    <<"id">> => <<"store">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => <<"postgresql__query@dyn-insert">>,
                    <<"result_path">> => <<"$.store">>,
                    <<"args">> => #{<<"box_id">> => <<"$.event.box_id">>}
                },
                #{
                    <<"id">> => <<"publish">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => <<"message__publish@dyn-status">>,
                    <<"result_path">> => <<"$.publish">>,
                    <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"ok">>}
                }
            ]
        })
    ).

%%--------------------------------------------------------------------
%% agent__create_pipeline
%%--------------------------------------------------------------------

t_create_pipeline_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID),
    ?assertMatch(#{type := <<"agent__create_pipeline">>}, Tool),
    ?assertEqual(?SK_PIPELINE_ID, maps:get(tool_id, Tool)).

t_create_pipeline_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__create_pipeline">>, ?SK_PIPELINE_ID)
    ).

t_create_pipeline_invoke_creates_pipeline(_Config) ->
    ReqId = <<"req-pipe-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"dyn-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"$evt/dyn/+">>},
            <<"steps">> => [
                #{
                    <<"id">> => <<"s1">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => <<"message__publish@some-pub">>,
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
            <<"trigger">> => #{<<"topic">> => <<"$evt/forced/+">>},
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
            <<"trigger">> => #{<<"topic">> => <<"$evt/x">>},
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

t_create_pipeline_invoke_missing_tool_ref(_Config) ->
    ReqId = <<"req-pipe-missing-tool">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"missing-tool-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"$evt/missing/tool">>},
            <<"steps">> => [
                #{
                    <<"id">> => <<"s1">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => <<"message__publish@missing-pub">>,
                    <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"hi">>}
                }
            ]
        },
        ReqId
    ),

    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    Reason = maps:get(<<"reason">>, cap_response(Reply)),
    ?assertNotEqual(nomatch, binary:match(Reason, <<"message__publish@missing-pub">>)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_create_pipeline_reply_correlation(_Config) ->
    ReqId = <<"req-pipe-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),

    invoke(
        <<"agent__create_pipeline">>,
        ?SK_PIPELINE_ID,
        #{
            <<"pipeline_id">> => <<"corr-pipeline">>,
            <<"trigger">> => #{<<"topic">> => <<"$evt/corr/+">>},
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
%% agent__update_tool
%%--------------------------------------------------------------------

t_update_tool_invoke(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"updateable-pub">>,
        <<"desc">> => <<"Before">>,
        <<"topic_prefix">> => <<"before/">>
    }),

    ReqId = <<"req-ut-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__update_tool">>,
        ?SK_UPDATE_TOOL_ID,
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"updateable-pub">>,
            <<"definition">> => #{<<"desc">> => <<"After">>}
        },
        ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item}} = cap_response(Reply),
    ?assertEqual(<<"After">>, maps:get(<<"desc">>, Item)),
    ?assertEqual(<<"before/">>, maps:get(<<"topic_prefix">>, Item)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_update_tool_not_found(_Config) ->
    ReqId = <<"req-ut-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__update_tool">>,
        ?SK_UPDATE_TOOL_ID,
        #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"no-such">>,
            <<"definition">> => #{<<"desc">> => <<"x">>}
        },
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent__update_pipeline
%%--------------------------------------------------------------------

t_update_pipeline_invoke_retain_steps(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"updateable-pipe">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/up/+">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@some-pub">>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"hi">>}
            }
        ]
    }),

    ReqId = <<"req-up-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__update_pipeline">>,
        ?SK_UPDATE_PIPELINE_ID,
        #{
            <<"id">> => <<"updateable-pipe">>,
            <<"definition">> => #{<<"active">> => false}
        },
        ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item}} = cap_response(Reply),
    ?assertMatch(#{<<"active">> := false}, Item),
    ?assertMatch([#{<<"id">> := <<"s1">>}], maps:get(<<"steps">>, Item)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_update_pipeline_not_found(_Config) ->
    ReqId = <<"req-up-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__update_pipeline">>,
        ?SK_UPDATE_PIPELINE_ID,
        #{<<"id">> => <<"no-such">>, <<"definition">> => #{<<"active">> => false}},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% Pipeline step mutation tools
%%--------------------------------------------------------------------

t_delete_pipeline_step_invoke(_Config) ->
    PipelineId = <<"step-mut-pipe">>,
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/sm/+">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@some-pub">>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"1">>}
            },
            #{<<"id">> => <<"s2">>, <<"type">> => <<"break">>, <<"path">> => <<"$.skip">>}
        ]
    }),

    ReqId = <<"req-ds-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_pipeline_step">>,
        ?SK_DELETE_STEP_ID,
        #{<<"pipeline_id">> => PipelineId, <<"step_id">> => <<"s1">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item}} = cap_response(Reply),
    ?assertEqual(1, length(maps:get(<<"steps">>, Item))),
    ?assertEqual(<<"s2">>, step_id(hd(maps:get(<<"steps">>, Item)))),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_insert_pipeline_step_first_last_after(_Config) ->
    PipelineId = <<"insert-step-pipe">>,
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/ins/+">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@some-pub">>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"1">>}
            }
        ]
    }),

    %% Insert first
    ReqId1 = <<"req-is-first">>,
    ok = emqx:subscribe(reply_topic(ReqId1)),
    invoke(
        <<"agent__insert_pipeline_step">>,
        ?SK_INSERT_STEP_ID,
        #{
            <<"pipeline_id">> => PipelineId,
            <<"step">> => #{
                <<"id">> => <<"first-step">>, <<"type">> => <<"break">>, <<"path">> => <<"$.x">>
            },
            <<"position">> => #{<<"first">> => true}
        },
        ReqId1
    ),
    Reply1 = recv_reply(ReqId1),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item1}} = cap_response(Reply1),
    ?assertEqual(2, length(maps:get(<<"steps">>, Item1))),
    ?assertEqual(<<"first-step">>, step_id(hd(maps:get(<<"steps">>, Item1)))),
    ok = emqx:unsubscribe(reply_topic(ReqId1)),

    %% Insert after
    ReqId2 = <<"req-is-after">>,
    ok = emqx:subscribe(reply_topic(ReqId2)),
    invoke(
        <<"agent__insert_pipeline_step">>,
        ?SK_INSERT_STEP_ID,
        #{
            <<"pipeline_id">> => PipelineId,
            <<"step">> => #{
                <<"id">> => <<"after-step">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@some-pub">>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"2">>}
            },
            <<"position">> => #{<<"after">> => <<"s1">>}
        },
        ReqId2
    ),
    Reply2 = recv_reply(ReqId2),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item2}} = cap_response(Reply2),
    Ids2 = [step_id(S) || S <- maps:get(<<"steps">>, Item2)],
    ?assertEqual([<<"first-step">>, <<"s1">>, <<"after-step">>], Ids2),
    ok = emqx:unsubscribe(reply_topic(ReqId2)),

    %% Insert last
    ReqId3 = <<"req-is-last">>,
    ok = emqx:subscribe(reply_topic(ReqId3)),
    invoke(
        <<"agent__insert_pipeline_step">>,
        ?SK_INSERT_STEP_ID,
        #{
            <<"pipeline_id">> => PipelineId,
            <<"step">> => #{
                <<"id">> => <<"last-step">>, <<"type">> => <<"break">>, <<"path">> => <<"$.y">>
            },
            <<"position">> => #{<<"last">> => true}
        },
        ReqId3
    ),
    Reply3 = recv_reply(ReqId3),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item3}} = cap_response(Reply3),
    ?assertEqual(<<"last-step">>, step_id(lists:last(maps:get(<<"steps">>, Item3)))),
    ok = emqx:unsubscribe(reply_topic(ReqId3)).

t_update_pipeline_step_invoke(_Config) ->
    PipelineId = <<"update-step-pipe">>,
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/us/+">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@some-pub">>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"1">>}
            }
        ]
    }),

    ReqId = <<"req-us-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__update_pipeline_step">>,
        ?SK_UPDATE_STEP_ID,
        #{
            <<"pipeline_id">> => PipelineId,
            <<"step_id">> => <<"s1">>,
            <<"step">> => #{<<"args">> => #{<<"topic">> => <<"changed">>, <<"payload">> => <<"2">>}}
        },
        ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Item}} = cap_response(Reply),
    [UpdatedStep] = maps:get(<<"steps">>, Item),
    ?assertEqual(<<"s1">>, step_id(UpdatedStep)),
    ?assertEqual(
        #{<<"topic">> => <<"changed">>, <<"payload">> => <<"2">>},
        maps:get(<<"args">>, UpdatedStep)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

reply_topic(ReqId) ->
    <<"$cap/+/+/response/", ReqId/binary>>.

invoke(Type, ToolId, Args, ReqId) ->
    invoke(Type, ToolId, Args, ReqId, #{}).

invoke(Type, ToolId, Args, ReqId, Extra) ->
    Topic = <<"$cap/", Type/binary, "/", ToolId/binary, "/request/", ReqId/binary>>,
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
    _ = emqx_broker:publish(emqx_message:make(ToolId, 0, Topic, Payload)),
    ok.

recv_reply(ReqId) ->
    receive
        #deliver{message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after 3000 ->
        ct:fail("no reply for req_id=~s within 3 s", [ReqId])
    end.

receive_result(Pid) ->
    receive
        {Pid, Result} -> Result
    after 3000 ->
        ct:fail("no result from ~p within 3 s", [Pid])
    end.

cap_response(Reply) ->
    emqx_agent_tool_helpers:cap_response(Reply).

step_id(Step) ->
    maps:get(<<"id">>, emqx_agent_tool_helpers:unwrap_union(Step), undefined).
