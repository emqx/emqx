%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_decision_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_rule_engine,
            {emqx_ai_completion, #{config => "ai.providers = [], ai.completion_profiles = []"}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers(),
    Port = emqx_common_test_helpers:select_free_port(tcp),
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"name">> => <<"decisions-provider">>,
            <<"type">> => <<"openai_decisions">>,
            <<"api_key">> => <<"test-api-key">>,
            <<"base_url">> => <<"http://localhost:", (integer_to_binary(Port))/binary, "/v1">>
        }}
    ),
    [{port, Port} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_bridge_v2_testlib:delete_all_rules(),
    ok = emqx_ai_completion_provider_mock:stop(),
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers().

%% Verify that decision profiles use a Decisions provider and support API CRUD.
t_profile_crud(_Config) ->
    Profile = profile(),
    Path = [ai, completion_profiles, <<"decision-profile">>],
    ?assertMatch({ok, 204}, api(post, [ai, completion_profiles], Profile)),
    {ok, 200, Created} = api(get, Path),
    ?assertEqual(Profile#{<<"model">> => <<"gpt-6-luna">>}, Created),
    ?assertEqual({ok, 200, [Created]}, api(get, [ai, completion_profiles])),

    %% A chat completion profile must not use a Decisions provider.
    ?assertMatch(
        {ok, 400, _},
        api(post, [ai, completion_profiles], #{
            <<"name">> => <<"chat-profile">>,
            <<"type">> => <<"openai">>,
            <<"provider_name">> => <<"decisions-provider">>
        })
    ),
    ?assertMatch({ok, 400, _}, api(delete, [ai, providers, <<"decisions-provider">>])),

    %% Update the question set and preserve the configured model.
    Updated = Profile#{<<"questions">> => [predicate()], <<"model">> => <<"gpt-6-luna">>},
    ?assertMatch({ok, 204}, api(put, Path, maps:remove(<<"name">>, Updated))),
    ?assertEqual({ok, 200, Updated}, api(get, Path)),
    ?assertMatch({ok, 204}, api(delete, Path)),
    ?assertMatch({ok, 404, _}, api(get, Path)).

%% Verify that missing providers and incompatible provider types are rejected.
t_provider_validation(_Config) ->
    Profile = profile(),
    ?assertMatch(
        {error, #{reason := completion_profile_provider_not_found}},
        emqx_ai_completion_config:update_completion_profiles_raw(
            {add, Profile#{<<"provider_name">> => <<"missing">>}}
        )
    ),
    lists:foreach(
        fun(Type) ->
            ok = emqx_ai_completion_config:update_providers_raw(
                {add, #{<<"name">> => Type, <<"type">> => Type, <<"api_key">> => <<"key">>}}
            ),
            ?assertMatch(
                {error, #{reason := completion_profile_provider_type_mismatch}},
                emqx_ai_completion_config:update_completion_profiles_raw(
                    {add, Profile#{<<"provider_name">> => Type}}
                )
            )
        end,
        [<<"openai">>, <<"anthropic">>, <<"openai_response">>]
    ).

%% Verify Decisions provider creation, retrieval, update, and deletion through the API.
t_provider_crud(_Config) ->
    Path = [ai, providers, <<"api-provider">>],
    Provider = #{
        <<"name">> => <<"api-provider">>,
        <<"type">> => <<"openai_decisions">>,
        <<"api_key">> => <<"test-key">>
    },
    ?assertMatch({ok, 204}, api(post, [ai, providers], Provider)),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := <<"openai_decisions">>,
            <<"api_key">> := <<"******">>
        }},
        api(get, Path)
    ),
    ?assertMatch(
        {ok, 204},
        api(put, Path, maps:remove(<<"name">>, Provider#{<<"api_key">> => <<"updated-key">>}))
    ),
    {ok, #{
        type := openai_decisions,
        base_url := <<"https://api.openai.com/v1">>,
        api_key := Key
    }} =
        emqx_ai_completion_config:get_provider(<<"api-provider">>),
    ?assertEqual(<<"updated-key">>, emqx_secret:unwrap(Key)),
    ?assertMatch({ok, 204}, api(delete, Path)),
    ?assertMatch({ok, 404, _}, api(get, Path)).

%% Verify model listing with a saved Decisions provider and an inline provider configuration.
t_provider_models(Config) ->
    ok = emqx_ai_completion_provider_mock:start_link(?config(port, Config), openai_models),
    Expected = {ok, 200, [<<"gpt-4-0613">>, <<"gpt-4">>, <<"gpt-3.5-turbo">>]},
    ?assertEqual(Expected, api(get, [ai, providers, <<"decisions-provider">>, models])),
    Port = integer_to_binary(?config(port, Config)),
    ?assertEqual(
        Expected,
        api(post, [ai, models], #{
            <<"type">> => <<"openai_decisions">>,
            <<"api_key">> => <<"test-key">>,
            <<"base_url">> => <<"http://localhost:", Port/binary, "/v1">>
        })
    ).

%% Verify required question fields, unique names and values, and non-empty lists.
t_question_validation(_Config) ->
    Profile = profile(),
    Predicate = predicate(),
    [_, Choice, Score] = questions(),
    InvalidQuestions = [
        [],
        [Predicate, Predicate],
        [maps:remove(<<"type">>, Predicate)],
        [maps:remove(<<"name">>, Predicate)],
        [maps:remove(<<"instructions">>, Predicate)],
        [Predicate#{<<"name">> => <<>>}],
        [Predicate#{<<"instructions">> => <<>>}],
        [Predicate#{<<"type">> => <<"unknown">>}],
        [maps:remove(<<"choices">>, Choice)],
        [Choice#{<<"choices">> => []}],
        [Choice#{<<"choices">> => [#{<<"value">> => <<"x">>}, #{<<"value">> => <<"x">>}]}],
        [maps:remove(<<"levels">>, Score)],
        [Score#{<<"levels">> => []}],
        [Score#{<<"levels">> => [#{<<"label">> => <<>>}]}]
    ],
    lists:foreach(
        fun(Questions) ->
            ct:pal("Invalid questions: ~p", [Questions]),
            ?assertMatch(
                {ok, 400, _},
                api(post, [ai, completion_profiles], Profile#{<<"questions">> => Questions})
            )
        end,
        InvalidQuestions
    ),
    ?assertMatch(
        {ok, 400, _},
        api(post, [ai, completion_profiles], maps:remove(<<"questions">>, Profile))
    ).

%% Verify that backup import preserves decision questions and provider compatibility.
t_config_import(_Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    Backup = #{<<"ai">> => emqx_config:get_raw([ai])},
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers(),
    ?assertMatch(
        {results, {_, []}},
        emqx_ai_completion_config:import_config(?global_ns, Backup)
    ),
    ?assertMatch(
        {ok, #{type := openai_decisions, provider := #{type := openai_decisions}}},
        emqx_ai_completion_config:get_completion_profile(<<"decision-profile">>)
    ),
    {ok, 200, #{<<"questions">> := Questions}} =
        api(get, [ai, completion_profiles, <<"decision-profile">>]),
    ?assertEqual(questions(), Questions),
    ?assertMatch(
        {error, #{reason := provider_in_use}},
        emqx_ai_completion_config:update_providers_raw(
            {update, #{
                <<"name">> => <<"decisions-provider">>,
                <<"type">> => <<"anthropic">>,
                <<"api_key">> => <<"key">>
            }}
        )
    ).

%% Verify the HTTP request and preserve predicate, choice, and score answers in rule SQL.
t_rule_completion(Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    Answers = answers(),
    Result = #{<<"answers">> => Answers},
    ok = start_server(Config, 200, emqx_utils_json:encode(Result#{<<"id">> => <<"decision-id">>})),
    {ok, _} = emqx_bridge_v2_testlib:create_rule_directly(#{
        <<"id">> => <<"decision-rule">>,
        <<"sql">> => <<
            "SELECT json_decode(ai_completion('decision-profile', payload)) AS decision "
            "FROM 'devices/+/status'"
        >>,
        <<"enable">> => true,
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> => #{
                    <<"topic">> => <<"decisions/result">>,
                    <<"qos">> => 0,
                    <<"retain">> => false,
                    <<"payload">> => <<"${decision}">>,
                    <<"mqtt_properties">> => #{},
                    <<"direct_dispatch">> => false
                }
            }
        ]
    }),
    emqx_ai_completion_SUITE:verify_republish(
        <<"devices/1/status">>,
        <<"motor overheated">>,
        <<"decisions/result">>,
        emqx_utils_json:encode(Result)
    ),
    receive
        {decision_request, Method, Path, Headers, Body} ->
            ?assertEqual(<<"POST">>, Method),
            ?assertEqual(<<"/v1/decisions">>, Path),
            ?assertMatch(#{<<"authorization">> := <<"Bearer test-api-key">>}, Headers),
            ?assertEqual(
                #{
                    <<"model">> => <<"gpt-6-luna">>,
                    <<"input">> => <<"motor overheated">>,
                    <<"questions">> => questions()
                },
                emqx_utils_json:decode(Body)
            )
    after 1000 ->
        ct:fail(no_decision_request)
    end.

%% Verify that a refusal remains an explicit answer alongside successful answers.
t_refusal(Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    [PredicateAnswer | _] = answers(),
    Result = #{
        <<"answers">> => [
            PredicateAnswer,
            #{<<"type">> => <<"refusal">>, <<"name">> => <<"category">>},
            #{<<"type">> => <<"refusal">>, <<"name">> => <<"severity">>}
        ]
    },
    ok = start_server(Config, 200, emqx_utils_json:encode(Result)),
    {ok, Content} = emqx_ai_completion:call_completion(<<"decision-profile">>, <<"input">>, #{}),
    ?assertEqual(Result, emqx_utils_json:decode(Content)).

%% Verify that request failures and malformed responses return empty completions.
t_errors(Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    Cases = [
        {429, <<"rate limited">>},
        {200, <<"{}">>},
        {200, <<"{\"answers\":null}">>},
        {200, <<>>}
    ],
    lists:foreach(
        fun({Status, Body}) ->
            ok = start_server(Config, Status, Body),
            ?assertEqual(
                <<>>,
                emqx_ai_completion_rule_sql:rsf_ai_completion([<<"decision-profile">>, <<"input">>])
            ),
            ok = emqx_ai_completion_provider_mock:stop()
        end,
        Cases
    ),
    %% Transport failures follow the same empty-completion behavior.
    ?assertEqual(
        {ok, <<>>},
        emqx_ai_completion:call_completion(<<"decision-profile">>, <<"input">>, #{})
    ).

%% Verify that decoding an empty completion fails SQL evaluation with a JSON parse error.
t_json_decode_empty_completion(Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    ok = start_server(Config, 429, <<"rate limited">>),
    ?assertException(error, {_, truncated_json}, emqx_rule_funcs:json_decode(<<>>)),
    ?assertMatch(
        {error, {select_and_transform_error, {error, {_, truncated_json}, _}}},
        emqx_rule_sqltester:test(#{
            sql =>
                ~b"""
            SELECT json_decode(ai_completion('decision-profile', payload)) AS decision
            FROM 'devices/+/status'
            """,
            context => #{topic => <<"devices/1/status">>, payload => <<"motor overheated">>}
        })
    ).

%% Verify that prompt overrides fail before sending an HTTP request.
t_prompt_override(_Config) ->
    ok = emqx_ai_completion_config:update_completion_profiles_raw({add, profile()}),
    ?assertError(
        {ai_completion_error, {unsupported_option, prompt}},
        emqx_ai_completion_rule_sql:rsf_ai_completion([
            <<"decision-profile">>, <<"override">>, <<"input">>
        ])
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

profile() ->
    #{
        <<"name">> => <<"decision-profile">>,
        <<"type">> => <<"openai_decisions">>,
        <<"provider_name">> => <<"decisions-provider">>,
        <<"questions">> => questions()
    }.

predicate() ->
    #{
        <<"type">> => <<"predicate">>,
        <<"name">> => <<"fault">>,
        <<"instructions">> => <<"Does this report indicate a hardware fault?">>
    }.

questions() ->
    [
        predicate(),
        #{
            <<"type">> => <<"choice">>,
            <<"name">> => <<"category">>,
            <<"instructions">> => <<"Classify the report, or select false if it is unrelated.">>,
            <<"choices">> => [
                #{<<"value">> => <<"hardware">>, <<"description">> => <<"Hardware faults.">>},
                #{<<"value">> => false},
                #{<<"value">> => <<"false">>}
            ]
        },
        #{
            <<"type">> => <<"score">>,
            <<"name">> => <<"severity">>,
            <<"instructions">> => <<"How severe is the issue?">>,
            <<"levels">> => [
                #{<<"label">> => <<"Normal">>},
                #{<<"label">> => <<"Failure">>, <<"description">> => <<"Device cannot operate.">>}
            ]
        }
    ].

answers() ->
    [
        #{<<"type">> => <<"predicate">>, <<"name">> => <<"fault">>, <<"probability">> => 0.92},
        #{
            <<"type">> => <<"choice">>,
            <<"name">> => <<"category">>,
            <<"choice">> => <<"hardware">>,
            <<"confidence">> => 0.9,
            <<"probabilities">> => [
                #{<<"value">> => <<"hardware">>, <<"probability">> => 0.95},
                #{<<"value">> => false, <<"probability">> => 0.03},
                #{<<"value">> => <<"false">>, <<"probability">> => 0.02}
            ]
        },
        #{
            <<"type">> => <<"score">>,
            <<"name">> => <<"severity">>,
            <<"score">> => 0.9,
            <<"confidence">> => 0.8,
            <<"probabilities">> => [
                #{<<"value">> => 0, <<"label">> => <<"Normal">>, <<"probability">> => 0.1},
                #{<<"value">> => 1, <<"label">> => <<"Failure">>, <<"probability">> => 0.9}
            ]
        }
    ].

start_server(Config, Status, Body) ->
    Parent = self(),
    emqx_ai_completion_provider_mock:start_link(?config(port, Config), fun(Req0, State) ->
        {ok, RequestBody, Req1} = cowboy_req:read_body(Req0),
        #{method := Method, path := Path, headers := Headers} = Req1,
        Parent ! {decision_request, Method, Path, Headers, RequestBody},
        Req = cowboy_req:reply(
            Status, #{<<"content-type">> => <<"application/json">>}, Body, Req1
        ),
        {ok, Req, State}
    end).

api(Method, Path) ->
    decode_response(emqx_mgmt_api_test_util:request(Method, emqx_mgmt_api_test_util:uri(Path))).

api(Method, Path, Body) ->
    decode_response(
        emqx_mgmt_api_test_util:request(Method, emqx_mgmt_api_test_util:uri(Path), Body)
    ).

decode_response({ok, Code, <<>>}) ->
    {ok, Code};
decode_response({ok, Code, Body}) ->
    {ok, Code, emqx_utils_json:decode(Body)};
decode_response(Error) ->
    Error.
