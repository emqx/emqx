%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Simple integration tests for the agent REST API.
%%
%% Coverage:
%%   Skills           — create (all four types), list, get, delete, validation
%%   Session profiles — create, list, get, update, delete, validation
%%   Pipelines        — create, list, get, update, delete, validation
%%
%% Each test case starts from a clean registry state (see end_per_testcase).

-module(emqx_agent_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
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
    [{tc_id, Id} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:delete_all(),
    ok = emqx_agent_pipeline_registry:delete_all().

%%--------------------------------------------------------------------
%% Skills — individual type tests
%%--------------------------------------------------------------------

t_skill_publish_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message.publish">>,
            <<"id">> => Id,
            <<"desc">> => <<"test publish skill">>,
            <<"topic_prefix">> => <<"test/", Id/binary, "/">>
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"skill_id">> := _, <<"type">> := <<"message.publish">>}},
        api_get([agent, skills, <<"message.publish">>, Id])
    ),

    {ok, 200, List} = api_get([agent, skills]),
    ?assert(lists:any(fun(S) -> maps:get(<<"skill_id">>, S) =:= Id end, List)),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"message.publish">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"message.publish">>, Id])).

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
            <<"input_schema">> => #{<<"type">> => <<"object">>},
            <<"output_schema">> => #{<<"type">> => <<"object">>}
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"skill_id">> := _, <<"type">> := <<"http">>}},
        api_get([agent, skills, <<"http">>, Id])
    ),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"http">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"http">>, Id])).

t_skill_kv_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"kv">>,
            <<"id">> => Id,
            <<"desc">> => <<"test kv skill">>,
            <<"data_schema">> => #{<<"type">> => <<"object">>},
            <<"allow_put">> => true
        })
    ),

    %% kv creates two registry entries: kv.lookup and kv.put
    ?assertMatch(
        {ok, 200, #{<<"skill_id">> := _, <<"type">> := <<"kv.lookup">>}},
        api_get([agent, skills, <<"kv.lookup">>, Id])
    ),
    ?assertMatch(
        {ok, 200, #{<<"skill_id">> := _, <<"type">> := <<"kv.put">>}},
        api_get([agent, skills, <<"kv.put">>, Id])
    ),

    %% Deleting via either variant removes both
    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"kv.lookup">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"kv.lookup">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"kv.put">>, Id])).

t_skill_clickhouse_crud(Config) ->
    Id = ?config(tc_id, Config),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"clickhouse.history">>,
            <<"id">> => Id,
            <<"desc">> => <<"test clickhouse skill">>,
            <<"query">> => <<"SELECT 1">>,
            <<"input_schema">> => #{<<"type">> => <<"object">>},
            <<"output_schema">> => #{<<"type">> => <<"object">>}
        })
    ),

    ?assertMatch(
        {ok, 200, #{<<"skill_id">> := _, <<"type">> := <<"clickhouse.history">>}},
        api_get([agent, skills, <<"clickhouse.history">>, Id])
    ),

    ?assertMatch({ok, 204}, api_delete([agent, skills, <<"clickhouse.history">>, Id])),
    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"clickhouse.history">>, Id])).

%%--------------------------------------------------------------------
%% Skills — list and validation
%%--------------------------------------------------------------------

t_skills_list(Config) ->
    Id = ?config(tc_id, Config),

    {ok, 200, []} = api_get([agent, skills]),

    ?assertMatch(
        {ok, 201, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message.publish">>,
            <<"id">> => Id,
            <<"desc">> => <<"list test">>,
            <<"topic_prefix">> => <<"x/">>
        })
    ),

    {ok, 200, [Entry]} = api_get([agent, skills]),
    ?assertEqual(Id, maps:get(<<"skill_id">>, Entry)).

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

    %% message.publish missing topic_prefix
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, skills], #{
            <<"type">> => <<"message.publish">>,
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
            <<"input_schema">> => #{},
            <<"output_schema">> => #{}
        })
    ),

    ?assertMatch({ok, 404, _}, api_get([agent, skills, <<"message.publish">>, <<"no_such">>])),
    ?assertMatch({ok, 404, _}, api_delete([agent, skills, <<"message.publish">>, <<"no_such">>])).

%%--------------------------------------------------------------------
%% Session profiles
%%--------------------------------------------------------------------

t_session_profiles_crud(Config) ->
    Name = ?config(tc_id, Config),

    {ok, 200, []} = api_get([agent, session_profiles]),

    Profile = #{
        <<"name">> => Name,
        <<"api_key">> => <<"sk-test">>,
        <<"base_url">> => <<"https://api.openai.com/v1">>,
        <<"model">> => <<"gpt-4o">>,
        <<"instructions">> => <<"You are a test assistant.">>
    },

    ?assertMatch({ok, 201, _}, api_post([agent, session_profiles], Profile)),

    {ok, 200, [Entry]} = api_get([agent, session_profiles]),
    ?assertEqual(Name, maps:get(<<"name">>, Entry)),

    ?assertMatch(
        {ok, 200, #{<<"name">> := _, <<"model">> := <<"gpt-4o">>}},
        api_get([agent, session_profiles, Name])
    ),

    ?assertMatch(
        {ok, 200, #{<<"model">> := <<"gpt-4o-mini">>}},
        api_put([agent, session_profiles, Name], maps:put(<<"model">>, <<"gpt-4o-mini">>, Profile))
    ),

    ?assertMatch(
        {ok, 200, #{<<"model">> := <<"gpt-4o-mini">>}},
        api_get([agent, session_profiles, Name])
    ),

    ?assertMatch({ok, 204}, api_delete([agent, session_profiles, Name])),
    ?assertMatch({ok, 404, _}, api_get([agent, session_profiles, Name])).

t_session_profiles_validation(_Config) ->
    ?assertMatch(
        {ok, 400, _},
        api_post([agent, session_profiles], #{
            <<"api_key">> => <<"sk">>,
            <<"base_url">> => <<"https://x">>,
            <<"model">> => <<"m">>
        })
    ),

    ?assertMatch({ok, 404, _}, api_get([agent, session_profiles, <<"no_such">>])),
    ?assertMatch({ok, 404, _}, api_delete([agent, session_profiles, <<"no_such">>])).

%%--------------------------------------------------------------------
%% Pipelines
%%--------------------------------------------------------------------

t_pipelines_crud(Config) ->
    Id = ?config(tc_id, Config),

    {ok, 200, []} = api_get([agent, pipelines]),

    Def = #{
        <<"pipeline_id">> => Id,
        <<"trigger">> => #{<<"topic">> => <<"evt/test/", Id/binary>>},
        <<"steps">> => [
            #{
                <<"id">> => <<"step1">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"message.publish@", Id/binary>>,
                <<"args">> => #{<<"topic">> => <<"out">>, <<"payload">> => <<"hi">>},
                <<"result_path">> => <<"$.result">>
            }
        ]
    },

    ?assertMatch({ok, 201, _}, api_post([agent, pipelines], Def)),

    {ok, 200, [Entry]} = api_get([agent, pipelines]),
    ?assertEqual(Id, maps:get(<<"pipeline_id">>, Entry)),

    ?assertMatch(
        {ok, 200, #{<<"pipeline_id">> := _, <<"trigger">> := #{<<"topic">> := _}}},
        api_get([agent, pipelines, Id])
    ),

    Def2 = Def#{
        <<"steps">> => maps:get(<<"steps">>, Def) ++
            [
                #{
                    <<"id">> => <<"step2">>,
                    <<"type">> => <<"wait_for_event">>,
                    <<"topic">> => <<"evt/test/done">>
                }
            ]
    },
    ?assertMatch({ok, 200, _}, api_put([agent, pipelines, Id], Def2)),

    {ok, 200, Updated} = api_get([agent, pipelines, Id]),
    ?assertEqual(2, length(maps:get(<<"steps">>, Updated))),

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
    decode(emqx_mgmt_api_test_util:request(get, uri(Path))).

api_post(Path, Body) ->
    decode(emqx_mgmt_api_test_util:request(post, uri(Path), Body)).

api_put(Path, Body) ->
    decode(emqx_mgmt_api_test_util:request(put, uri(Path), Body)).

api_delete(Path) ->
    decode(emqx_mgmt_api_test_util:request(delete, uri(Path))).

uri(Parts) ->
    emqx_mgmt_api_test_util:uri([to_list(P) || P <- Parts]).

to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.

decode({ok, Code, <<>>}) ->
    {ok, Code};
decode({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} -> {ok, Code, Decoded};
        {error, _} -> {ok, Code, Body}
    end;
decode(Other) ->
    Other.
