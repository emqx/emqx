%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the delete meta-tools:
%%   agent__delete_tool    — emqx_agent_tool_delete_tool
%%   agent__delete_pipeline — emqx_agent_tool_delete_pipeline
%%
%% Guards tested:
%%   delete_tool    — refused when tool is referenced in a pipeline step
%%   delete_pipeline — refused when pipeline is active

-module(emqx_agent_tool_delete_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SK_DEL_TOOL_ID, <<"meta-del-tool">>).
-define(SK_DEL_PIPELINE_ID, <<"meta-del-pipeline">>).

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
        <<"type">> => <<"agent__delete_tool">>, <<"id">> => ?SK_DEL_TOOL_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__delete_pipeline">>, <<"id">> => ?SK_DEL_PIPELINE_ID
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% agent__delete_tool — registration
%%--------------------------------------------------------------------

t_delete_tool_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__delete_tool">>, ?SK_DEL_TOOL_ID),
    ?assertEqual(<<"agent__delete_tool">>, maps:get(type, Tool)).

t_delete_tool_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__delete_tool">>, ?SK_DEL_TOOL_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__delete_tool">>, ?SK_DEL_TOOL_ID)
    ).

%%--------------------------------------------------------------------
%% agent__delete_tool — invocation
%%--------------------------------------------------------------------

t_delete_tool_ok(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"to-del-pub">>,
        <<"desc">> => <<"del me">>,
        <<"topic_prefix">> => <<"x/">>
    }),

    ReqId = <<"req-ds-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_tool">>,
        ?SK_DEL_TOOL_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"to-del-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"ok">>}, cap_response(Reply)),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"message__publish">>, <<"to-del-pub">>)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_tool_not_found(_Config) ->
    ReqId = <<"req-ds-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_tool">>,
        ?SK_DEL_TOOL_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"no-such">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_tool_in_use_by_call_tool(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"used-pub">>,
        <<"desc">> => <<"used">>,
        <<"topic_prefix">> => <<"x/">>
    }),
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-uses-pub">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/x">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"s1">>,
                <<"type">> => <<"call_tool">>,
                <<"tool">> => <<"message__publish@used-pub">>,
                <<"args">> => #{}
            }
        ]
    }),

    ReqId = <<"req-ds-inuse-cs">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_tool">>,
        ?SK_DEL_TOOL_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"used-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_delete_tool_in_use_by_llm_tools(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"tool-pub">>,
        <<"desc">> => <<"tool">>,
        <<"topic_prefix">> => <<"t/">>
    }),
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-uses-tool">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/t">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"llm">>,
                <<"type">> => <<"llm_loop">>,
                <<"provider_name">> => <<"some-provider">>,
                <<"model">> => <<"test-model">>,
                <<"instructions">> => <<"test">>,
                <<"tools">> => [<<"message__publish@tool-pub">>],
                <<"set_result_schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}},
                    <<"required">> => [<<"status">>],
                    <<"additionalProperties">> => false
                },
                <<"result_path">> => <<"$.result">>
            }
        ]
    }),

    ReqId = <<"req-ds-inuse-llm">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_tool">>,
        ?SK_DEL_TOOL_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"tool-pub">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"reason">> := _}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

%%--------------------------------------------------------------------
%% agent__delete_pipeline — registration
%%--------------------------------------------------------------------

t_delete_pipeline_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(
        <<"agent__delete_pipeline">>, ?SK_DEL_PIPELINE_ID
    ),
    ?assertEqual(<<"agent__delete_pipeline">>, maps:get(type, Tool)).

t_delete_pipeline_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__delete_pipeline">>, ?SK_DEL_PIPELINE_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__delete_pipeline">>, ?SK_DEL_PIPELINE_ID)
    ).

%%--------------------------------------------------------------------
%% agent__delete_pipeline — invocation
%%--------------------------------------------------------------------

t_delete_pipeline_ok(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"to-del-pipe">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/del/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-dp-ok">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_pipeline">>,
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
        <<"agent__delete_pipeline">>,
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
        <<"trigger">> => #{<<"topic">> => <<"$evt/act/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-dp-active">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__delete_pipeline">>,
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
        <<"agent__delete_pipeline">>,
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

cap_response(Reply) ->
    emqx_agent_tool_helpers:cap_response(Reply).
