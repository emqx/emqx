%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Smoke tests for the four query meta-tools:
%%   agent__query_tools       — emqx_agent_tool_query_tools
%%   agent__query_providers   — emqx_agent_tool_query_providers
%%   agent__query_pipelines   — emqx_agent_tool_query_pipelines
%%   agent__query_connections — emqx_agent_tool_query_connections
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
-define(SK_CONNECTIONS_ID, <<"meta-query-connections">>).

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
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"agent__query_connections">>, <<"id">> => ?SK_CONNECTIONS_ID
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
    ReqId = unique_req_id(<<"req-qs-empty">>),
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_tools">>, ?SK_TOOLS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    %% Only the 4 query meta-tools themselves are in the registry.
    ?assert(is_list(Items)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_tools_list_with_items(_Config) ->
    ok = emqx_agent_service:tool_create(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => <<"pub-a">>,
        <<"desc">> => <<"A">>,
        <<"topic_prefix">> => <<"x/">>
    }),

    ReqId = unique_req_id(<<"req-qs-list">>),
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

    ReqId = unique_req_id(<<"req-qs-type">>),
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

    ReqId = unique_req_id(<<"req-qs-get">>),
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
    ReqId = unique_req_id(<<"req-qs-nf">>),
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

t_query_providers_do_not_leak_api_key(_Config) ->
    ok = add_provider(<<"provider-secret">>, <<"sk-super-secret">>),
    ReqId = <<"req-qsess-secret">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_providers">>, ?SK_PROVIDERS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    [Provider] = [P || #{<<"name">> := N} = P <- Items, N =:= <<"provider-secret">>],
    ?assertEqual(<<"******">>, maps:get(<<"api_key">>, Provider)),
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
%% agent__query_connections
%%--------------------------------------------------------------------

t_query_connections_registers(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(
        <<"agent__query_connections">>, ?SK_CONNECTIONS_ID
    ),
    ?assertEqual(<<"agent__query_connections">>, maps:get(type, Tool)).

t_query_connections_destroy(_Config) ->
    ok = emqx_agent_service:tool_delete(<<"agent__query_connections">>, ?SK_CONNECTIONS_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_tool_registry:lookup(<<"agent__query_connections">>, ?SK_CONNECTIONS_ID)
    ).

t_query_connections_list_empty(_Config) ->
    ReqId = <<"req-qc-empty">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_connections">>, ?SK_CONNECTIONS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := []}}, cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_connections_list_with_items(_Config) ->
    ok = emqx_agent_service:connection_create(pg_conn_body(<<"conn-a">>)),

    ReqId = <<"req-qc-list">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(<<"agent__query_connections">>, ?SK_CONNECTIONS_ID, #{}, ReqId),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"items">> := Items}} = cap_response(Reply),
    ?assert(
        lists:any(fun(C) -> maps:get(<<"id">>, C, undefined) =:= <<"conn-a">> end, Items)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_connections_get_by_id(_Config) ->
    ok = emqx_agent_service:connection_create(pg_conn_body(<<"conn-b">>)),

    ReqId = <<"req-qc-get">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_connections">>, ?SK_CONNECTIONS_ID, #{<<"id">> => <<"conn-b">>}, ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"item">> := #{<<"id">> := <<"conn-b">>}}
        },
        cap_response(Reply)
    ),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_connections_do_not_leak_password(_Config) ->
    ok = emqx_agent_service:connection_create(pg_conn_body(<<"conn-secret">>)),

    ReqId = <<"req-qc-secret">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_connections">>, ?SK_CONNECTIONS_ID, #{<<"id">> => <<"conn-secret">>}, ReqId
    ),
    Reply = recv_reply(ReqId),
    #{<<"status">> := <<"ok">>, <<"result">> := #{<<"item">> := Conn}} = cap_response(Reply),
    ?assertEqual(<<"******">>, maps:get(<<"password">>, maps:get(<<"config">>, Conn))),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_connections_get_not_found(_Config) ->
    ReqId = <<"req-qc-nf">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_connections">>, ?SK_CONNECTIONS_ID, #{<<"id">> => <<"no-such">>}, ReqId
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(#{<<"status">> := <<"error">>}, cap_response(Reply)),
    ok = emqx:unsubscribe(reply_topic(ReqId)).

t_query_connections_reply_correlation(_Config) ->
    ReqId = <<"req-qc-corr">>,
    ok = emqx:subscribe(reply_topic(ReqId)),
    invoke(
        <<"agent__query_connections">>,
        ?SK_CONNECTIONS_ID,
        #{},
        ReqId,
        #{<<"trace_id">> => <<"tr-conn">>}
    ),
    Reply = recv_reply(ReqId),
    ?assertMatch(
        #{<<"req_id">> := <<"req-qc-corr">>, <<"trace_id">> := <<"tr-conn">>},
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
    erlang:put({reply_tool, ReqId}, {Type, ToolId}),
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
    {Type, ToolId} = erlang:get({reply_tool, ReqId}),
    receive
        #deliver{message = #message{payload = P}} ->
            Reply = emqx_utils_json:decode(P),
            case Reply of
                #{
                    <<"req_id">> := ReqId,
                    <<"tool">> := #{<<"type">> := Type, <<"id">> := ToolId}
                } ->
                    case is_query_tool_reply(Reply) of
                        true -> Reply;
                        false -> recv_reply(ReqId)
                    end;
                _ ->
                    recv_reply(ReqId)
            end
    after 3000 ->
        ct:fail("no reply for req_id=~s within 3 s", [ReqId])
    end.

unique_req_id(Base) ->
    Suffix = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<Base/binary, "-", Suffix/binary>>.

is_query_tool_reply(#{<<"response">> := #{<<"status">> := <<"error">>}}) ->
    true;
is_query_tool_reply(#{<<"response">> := #{<<"result">> := #{<<"items">> := _}}}) ->
    true;
is_query_tool_reply(#{<<"response">> := #{<<"result">> := #{<<"item">> := _}}}) ->
    true;
is_query_tool_reply(_) ->
    false.

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

pg_conn_body(Id) ->
    #{
        <<"id">> => Id,
        <<"type">> => <<"postgresql">>,
        <<"enable">> => false,
        <<"config">> => #{
            <<"server">> => <<"pgsql:5432">>,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => 5000,
            <<"disable_prepared_statements">> => true,
            <<"ssl">> => #{<<"enable">> => false}
        }
    }.
