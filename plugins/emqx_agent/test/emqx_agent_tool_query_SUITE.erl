%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the three query meta-tools:
%%   agent__query_tools    — emqx_agent_tool_query_tools
%%   agent__query_providers — emqx_agent_tool_query_providers
%%   agent__query_pipelines — emqx_agent_tool_query_pipelines
%%
%% Each group covers: registration, destruction, list-empty,
%% list-with-items, get-by-id, get-not-found, reply-correlation.

-module(emqx_agent_tool_query_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SK_TOOLS_ID, <<"meta-query-tools">>).
-define(SK_PROVIDERS_ID, <<"meta-query-providers">>).
-define(SK_PIPELINES_ID, <<"meta-query-pipelines">>).

-define(VALID_INPUT_SCHEMA,
    <<"{\"type\":\"object\",\"properties\":{},\"required\":[],\"additionalProperties\":false}">>
).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_resource,
            {emqx_ai_completion, #{config => "ai.providers = [], ai.completion_profiles = []"}},
            emqx_agent
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__query_tools">>, <<"id">> => ?SK_TOOLS_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__query_providers">>, <<"id">> => ?SK_PROVIDERS_ID
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__query_pipelines">>, <<"id">> => ?SK_PIPELINES_ID
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = clean_providers(),
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% agent__query_tools
%%--------------------------------------------------------------------

t_query_tools_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__query_tools">>, ?SK_TOOLS_ID),
    ?assertMatch(#{type := <<"agent__query_tools">>}, Tool),
    ?assertEqual(?SK_TOOLS_ID, maps:get(tool_id, Tool)).

t_query_tools_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__query_tools">>, ?SK_TOOLS_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__query_tools">>, ?SK_TOOLS_ID)
    ).

t_query_tools_list_empty(_Config) ->
    ReqId = <<"req-qs-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_tools">>, ?SK_TOOLS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    %% Only the 3 query meta-tools themselves are in the registry.
    ?assert(is_list(Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_list_with_items(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"pub-a">>,
        <<"desc">> => <<"A">>,
        <<"topic_prefix">> => <<"x/">>
    }),

    ReqId = <<"req-qs-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_tools">>, ?SK_TOOLS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    ?assert(lists:any(fun(S) -> maps:get(<<"id">>, S, undefined) =:= <<"pub-a">> end, Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_filter_by_type(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"pub-b">>,
        <<"desc">> => <<"B">>,
        <<"topic_prefix">> => <<"y/">>
    }),
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"http">>,
        <<"id">> => <<"http-b">>,
        <<"desc">> => <<"B http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://stub/b">>,
        <<"input_schema">> => ?VALID_INPUT_SCHEMA
    }),

    ReqId = <<"req-qs-type">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_tools">>, ?SK_TOOLS_ID, #{<<"type">> => <<"message__publish">>}, ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    ?assert(lists:all(fun(S) -> maps:get(<<"type">>, S) =:= <<"message__publish">> end, Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_get_by_type_and_id(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"pub-c">>,
        <<"desc">> => <<"C">>,
        <<"topic_prefix">> => <<"z/">>
    }),

    ReqId = <<"req-qs-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_tools">>,
        ?SK_TOOLS_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"pub-c">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"item">> := #{<<"id">> := <<"pub-c">>}}
        },
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_get_not_found(_Config) ->
    ReqId = <<"req-qs-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_tools">>,
        ?SK_TOOLS_ID,
        #{<<"type">> => <<"message__publish">>, <<"id">> => <<"no-such">>},
        ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := _},
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_reply_correlation(_Config) ->
    ReqId = <<"req-qs-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_tools">>,
        ?SK_TOOLS_ID,
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
%% agent__query_providers
%%--------------------------------------------------------------------

t_query_providers_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__query_providers">>, ?SK_PROVIDERS_ID),
    ?assertEqual(<<"agent__query_providers">>, maps:get(type, Tool)).

t_query_providers_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__query_providers">>, ?SK_PROVIDERS_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__query_providers">>, ?SK_PROVIDERS_ID)
    ).

t_query_providers_list_empty(_Config) ->
    ReqId = <<"req-qsess-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_providers">>, ?SK_PROVIDERS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := []}}, cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_providers_list_with_items(_Config) ->
    ok = add_provider(<<"provider-a">>, <<"sk-x">>),

    ReqId = <<"req-qsess-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_providers">>, ?SK_PROVIDERS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    ?assert(
        lists:any(fun(P) -> maps:get(<<"name">>, P, undefined) =:= <<"provider-a">> end, Items)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_providers_get_by_name(_Config) ->
    ok = add_provider(<<"provider-b">>, <<"sk-y">>),

    ReqId = <<"req-qsess-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_providers">>, ?SK_PROVIDERS_ID, #{<<"name">> => <<"provider-b">>}, ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"item">> := #{<<"name">> := <<"provider-b">>}}
        },
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_providers_get_not_found(_Config) ->
    ReqId = <<"req-qsess-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_providers">>, ?SK_PROVIDERS_ID, #{<<"name">> => <<"no-such">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_providers_reply_correlation(_Config) ->
    ReqId = <<"req-qsess-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_providers">>,
        ?SK_PROVIDERS_ID,
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
%% agent__query_pipelines
%%--------------------------------------------------------------------

t_query_pipelines_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID),
    ?assertEqual(<<"agent__query_pipelines">>, maps:get(type, Tool)).

t_query_pipelines_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID)
    ).

t_query_pipelines_list_empty(_Config) ->
    ReqId = <<"req-qp-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := []}}, cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_list_with_items(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-a">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/a/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-qp-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    ?assert(
        lists:any(fun(P) -> maps:get(<<"pipeline_id">>, P, undefined) =:= <<"pipe-a">> end, Items)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_get_by_id(_Config) ->
    ok = emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => <<"pipe-b">>,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"$evt/b/+">>},
        <<"steps">> => []
    }),

    ReqId = <<"req-qp-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID, #{<<"id">> => <<"pipe-b">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"item">> := #{<<"pipeline_id">> := <<"pipe-b">>}}
        },
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_get_not_found(_Config) ->
    ReqId = <<"req-qp-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_pipelines">>, ?SK_PIPELINES_ID, #{<<"id">> => <<"no-such">>}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_pipelines_reply_correlation(_Config) ->
    ReqId = <<"req-qp-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_pipelines">>,
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

add_provider(Name, ApiKey) ->
    emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"name">> => Name,
            <<"type">> => <<"openai">>,
            <<"api_key">> => ApiKey,
            <<"base_url">> => <<"https://api.openai.com/v1">>
        }}
    ).

clean_providers() ->
    lists:foreach(
        fun(#{<<"name">> := Name}) ->
            ok = emqx_ai_completion_config:update_providers_raw({delete, Name})
        end,
        emqx_ai_completion_config:get_providers_raw()
    ).
