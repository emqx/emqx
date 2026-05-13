%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Simple integration tests for the agent REST API.
%%
%% Coverage:
%%   Skills           — create (all four types), list, get, delete, validation
%%   Pipelines        — create, list, get, update, delete, validation
%%
%% Each test case starts from a clean registry state (see end_per_testcase).

-module(emqx_agent_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
            emqx_agent,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    Id = atom_to_binary(TestCase, utf8),
    ok = emqx_agent_plugin_config_fixture:setup(),
    [{tc_id, Id} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% UI
%%--------------------------------------------------------------------

t_ui_returns_html(_Config) ->
    {ok, Code, Body} = api_get([agent, ui]),
    ?assertEqual(200, Code),
    ?assert(is_binary(Body)),
    ?assert(binary:match(Body, <<"<!DOCTYPE html">>) =/= nomatch),
    ?assert(binary:match(Body, <<"showTab('connections'">>) =/= nomatch),
    ?assert(binary:match(Body, <<"id=\"tab-connections\"">>) =/= nomatch),
    ?assert(binary:match(Body, <<"assets/app.js">>) =/= nomatch),

    {ok, 200, AppJs} = api_get([agent, assets, <<"app.js">>]),
    ?assert(binary:match(AppJs, <<"./connections.js">>) =/= nomatch),
    ?assert(binary:match(AppJs, <<"window.saveConnection">>) =/= nomatch),

    {ok, 200, ConnectionsJs} = api_get([agent, assets, <<"connections.js">>]),
    ?assert(binary:match(ConnectionsJs, <<"export async function saveConnection">>) =/= nomatch),
    ?assert(binary:match(ConnectionsJs, <<"/connections">>) =/= nomatch),
    ?assert(binary:match(ConnectionsJs, <<"/connections/statuses">>) =/= nomatch),

    {ok, 200, CompatAppJs} = api_get([agent, ui, assets, <<"app.js">>]),
    ?assertEqual(AppJs, CompatAppJs).

%%--------------------------------------------------------------------
%% Skills — individual type tests
%%--------------------------------------------------------------------

t_skill_publish_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => Id,
            <<"desc">> => <<"test publish skill">>,
            <<"topic_prefix">> => <<"test/", Id/binary, "/">>
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"id">> := _, <<"type">> := <<"message__publish">>}},
        api_get([agent, skills, <<"message__publish">>, Id])
    ),

    {ok, 200, List} = api_get([agent, skills]),
    ?assert(lists:any(fun(S) -> maps:get(<<"id">>, S) =:= Id end, List)),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"message__publish">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"message__publish">>, Id])).

t_skill_http_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"http">>,
            <<"id">> => Id,
            <<"desc">> => <<"test http skill">>,
            <<"method">> => <<"post">>,
            <<"url">> => <<"http://stub:8080/api">>,
            <<"input_schema">> => ?VALID_INPUT_SCHEMA
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"id">> := _, <<"type">> := <<"http">>}},
        api_get([agent, skills, <<"http">>, Id])
    ),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"http">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"http">>, Id])).

t_skill_postgresql_crud(Config) ->
    Id = ?config(tc_id, Config),
    ConnId = <<Id/binary, "-conn">>,
    ?assertMatch({ok, 201, _}, api_post([agent, connections], pg_conn_body(ConnId))),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => Id,
            <<"desc">> => <<"test postgresql skill">>,
            <<"resource">> => ConnId,
            <<"query">> => <<"SELECT 1">>
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"id">> := _, <<"type">> := <<"postgresql__query">>}},
        api_get([agent, skills, <<"postgresql__query">>, Id])
    ),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"postgresql__query">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"postgresql__query">>, Id])).

%%--------------------------------------------------------------------
%% Connections
%%--------------------------------------------------------------------

t_connections_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch({ok, 200, []}, api_get([agent, connections])),
    ?assertMatch({ok, 201, _}, api_post([agent, connections], pg_conn_body(Id))),

    {ok, 200, Conn} = api_get([agent, connections, Id]),
    ?assertMatch(#{<<"id">> := Id, <<"type">> := <<"postgresql">>}, Conn),
    ?assertEqual(false, maps:get(<<"enable">>, Conn)),

    Body0 = pg_conn_body(Id),
    Updated = Body0#{
        <<"enable">> => false,
        <<"config">> => (maps:get(<<"config">>, Body0))#{<<"database">> => <<"mqtt2">>}
    },
    ?assertMatch({ok, 200, _}, api_put([agent, connections, Id], Updated)),

    {ok, 200, Conn2} = api_get([agent, connections, Id]),
    ?assertEqual(<<"mqtt2">>, maps:get(<<"database">>, maps:get(<<"config">>, Conn2))),

    ?assertMatch(
        {ok, 200, #{<<"enable">> := true}}, api_post([agent, connections, Id, start], #{})
    ),
    ?assertMatch(
        {ok, 200, #{<<"enable">> := false}}, api_post([agent, connections, Id, stop], #{})
    ),

    ?assertMatch({ok, 204}, api_delete([agent, connections, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, connections, Id])).

t_connection_statuses(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch({ok, 200, #{}}, api_get([agent, connections, statuses])),
    ?assertMatch({ok, 201, _}, api_post([agent, connections], pg_conn_body(Id))),

    {ok, 200, Statuses} = api_get([agent, connections, statuses]),
    ?assertMatch(
        #{
            Id := #{
                <<"status">> := <<"stopped">>,
                <<"error">> := null
            }
        },
        Statuses
    ),

    ?assertMatch({ok, 404, _}, api_get([agent, connections, <<"no_such">>])).

t_connection_delete_in_use(Config) ->
    Id = ?config(tc_id, Config),
    ConnId = <<Id/binary, "-conn">>,
    ?assertMatch({ok, 201, _}, api_post([agent, connections], pg_conn_body(ConnId))),
    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => Id,
            <<"desc">> => <<"test postgresql skill">>,
            <<"resource">> => ConnId,
            <<"query">> => <<"SELECT 1">>
        })
    ),
    ?assertMatch({ok, 409, _}, api_delete([agent, connections, ConnId])).

%%--------------------------------------------------------------------
%% Skills — list and validation
%%--------------------------------------------------------------------

t_skills_list(Config) ->
    Id = ?config(tc_id, Config),

    {ok, 200, []} = api_get([agent, skills]),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => Id,
            <<"desc">> => <<"list test">>,
            <<"topic_prefix">> => <<"x/">>
        })
    ),

    {ok, 200, [Entry]} = api_get([agent, skills]),
    ?assertEqual(Id, maps:get(<<"id">>, Entry)).

t_skill_statuses(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => Id,
            <<"desc">> => <<"bad postgresql skill">>,
            <<"resource">> => <<"missing-connection">>,
            <<"query">> => <<"SELECT 1">>
        })
    ),

    Key = <<"postgresql__query@", Id/binary>>,
    ?assertMatch(
        {ok, 200, #{
            Key := #{
                <<"status">> := <<"failed">>,
                <<"error">> := _
            }
        }},
        api_get([agent, skills, statuses])
    ),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"postgresql__query">>, Id)).

t_skills_validation(_Config) ->
    %% Missing type field
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, skills], #{<<"id">> => <<"x">>, <<"desc">> => <<"x">>})
    ),

    %% Unknown skill type
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, skills], #{
            <<"type">> => <<"no_such_type">>,
            <<"id">> => <<"x">>,
            <<"desc">> => <<"x">>
        })
    ),

    %% message__publish missing topic_prefix
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"x">>,
            <<"desc">> => <<"x">>
        })
    ),

    %% http missing method
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, skills], #{
            <<"type">> => <<"http">>,
            <<"id">> => <<"x">>,
            <<"desc">> => <<"x">>,
            <<"url">> => <<"http://x">>,
            <<"input_schema">> => ?VALID_INPUT_SCHEMA
        })
    ),

    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"message__publish">>, <<"no_such">>])),
    ?assertMatch({ok, 404, _}, api_delete([agent, skills, <<"message__publish">>, <<"no_such">>])).

%%--------------------------------------------------------------------
%% Pipelines
%%--------------------------------------------------------------------

t_pipelines_crud(Config) ->
    Id = ?config(tc_id, Config),

    {ok, 200, []} = api_get([agent, pipelines]),

    Def = #{
        <<"pipeline_id">> => Id,
        <<"active">> => false,
        <<"trigger">> => #{<<"topic">> => <<"evt/test/", Id/binary>>},
        <<"steps">> => [
            #{
                <<"id">> => <<"step1">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"message__publish@", Id/binary>>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"hi">>},
                <<"result_path">> => <<"$.result">>
            }
        ]
    },

    ?assertMatch({ok, 201, _}, api_post([agent, pipelines], Def)),

    {ok, 200, [Entry]} = api_get([agent, pipelines]),
    ?assertMatch(#{<<"pipeline_id">> := Id}, Entry),

    ?assertMatch(
        {ok, 200, #{<<"pipeline_id">> := _, <<"trigger">> := #{<<"topic">> := _}}},
        api_get([agent, pipelines, Id])
    ),

    Def2 = Def#{
        <<"steps">> => maps:get(<<"steps">>, Def) ++
            [
                #{
                    <<"id">> => <<"step2">>,
                    <<"type">> => <<"break">>,
                    <<"path">> => <<"$.event.data.skip">>
                },
                #{
                    <<"id">> => <<"step3">>,
                    <<"type">> => <<"call_skill">>,
                    <<"skill">> => <<"message__publish@", Id/binary>>,
                    <<"args">> => #{<<"topic">> => <<"out2">>, <<"payload">> => <<"bye">>},
                    <<"result_path">> => <<"$.result2">>
                }
            ]
    },
    ?assertMatch({ok, 200, _}, api_put([agent, pipelines, Id], Def2)),

    {ok, 200, Updated} = api_get([agent, pipelines, Id]),
    ?assertEqual(3, length(maps:get(<<"steps">>, Updated))),

    ?assertMatch({ok, 204}, api_delete([agent, pipelines, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, pipelines, Id])).

t_pipelines_validation(_Config) ->
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, pipelines], #{
            <<"trigger">> => #{<<"topic">> => <<"evt/x">>},
            <<"steps">> => []
        })
    ),

    ?assertMatch({ok, 404, _}, api_get([agent, pipelines, <<"no_such">>])),
    ?assertMatch({ok, 404, _}, api_delete([agent, pipelines, <<"no_such">>])).

%%--------------------------------------------------------------------
%% HTTP helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    decode(emqx_agent_app:on_handle_api_call(get, plugin_path(Path), #{}, #{})).

api_post(Path, Body) ->
    decode(emqx_agent_app:on_handle_api_call(post, plugin_path(Path), #{body => Body}, #{})).

api_put(Path, Body) ->
    decode(emqx_agent_app:on_handle_api_call(put, plugin_path(Path), #{body => Body}, #{})).

api_delete(Path) ->
    decode(emqx_agent_app:on_handle_api_call(delete, plugin_path(Path), #{}, #{})).

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

plugin_path([agent | Rest]) ->
    [to_binary(P) || P <- Rest];
plugin_path(Parts) ->
    [to_binary(P) || P <- Parts].

uri(Parts) ->
    emqx_mgmt_api_test_util:uri([to_list(P) || P <- Parts]).

to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.

to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) -> unicode:characters_to_binary(L).

decode({ok, Code, _Headers, <<>>}) ->
    {ok, Code};
decode({ok, Code, _Headers, Body}) ->
    decode({ok, Code, Body});
decode({error, not_found}) ->
    {ok, 404, #{<<"code">> => <<"NOT_FOUND">>}};
decode({ok, Code, <<>>}) ->
    {ok, Code};
decode({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} -> {ok, Code, Decoded};
        {error, _} -> {ok, Code, Body}
    end;
decode(Other) ->
    Other.
