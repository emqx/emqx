%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request/2,
        request/3,
        uri/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            {emqx_ai_completion, #{config => "ai.providers = [], ai.completion_profiles = []"}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers(),
    ok = emqx_ai_completion_provider_mock:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    %% Fail to create invalid providers
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, providers], #{<<"foo">> => <<"bar">>})
    ),
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"ftp://api.openai.com/v1">>
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"http://{}">>
        })
    ),

    %% Create valid providers
    ?assertMatch(
        {ok, 204},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"https://api.openai.com/v1">>
        })
    ),

    %% Fail to create provider with duplicate name
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>
        })
    ),

    %% Succeed to fetch provider
    ?assertMatch(
        {ok, 200, #{<<"name">> := <<"test-provider">>, <<"type">> := <<"openai">>}},
        api_get([ai, providers, <<"test-provider">>])
    ),

    %% Fail to fetch non-existent provider
    ?assertMatch(
        {ok, 404, _},
        api_get([ai, providers, <<"non-existent-provider">>])
    ),

    %% Succeed to fetch providers
    ?assertMatch(
        {ok, 200, [
            #{<<"name">> := <<"test-provider">>, <<"type">> := <<"openai">>}
        ]},
        api_get([ai, providers])
    ),

    %% Fail to create invalid completion profile
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{<<"foo">> => <<"bar">>})
    ),

    %% Fail to create with invalid provider
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            provider_name => <<"non-existent-provider">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Fail to create with mismatching provider type
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"anthropic">>,
            provider_name => <<"test-provider">>
        })
    ),

    %% Create valid completion profile
    ?assertMatch(
        {ok, 204},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            provider_name => <<"test-provider">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Fail to create completion profile with duplicate name
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            provider_name => <<"test-provider">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Succeed to fetch completion profile by name
    ?assertMatch(
        {ok, 200, #{<<"name">> := <<"test-completion-profile">>, <<"type">> := <<"openai">>}},
        api_get([ai, completion_profiles, <<"test-completion-profile">>])
    ),

    %% Fail to fetch non-existent completion profile
    ?assertMatch(
        {ok, 404, _},
        api_get([ai, completion_profiles, <<"non-existent-completion-profile">>])
    ),

    %% Succeed to fetch completion profiles
    ?assertMatch(
        {ok, 200, [
            #{<<"name">> := <<"test-completion-profile">>, <<"type">> := <<"openai">>}
        ]},
        api_get([ai, completion_profiles])
    ),

    %% Fail to update provider type of the used provider
    ?assertMatch(
        {ok, 400, _},
        api_put([ai, providers, <<"test-provider">>], #{
            type => <<"anthropic">>,
            api_key => <<"test-api-key">>
        })
    ),

    %% Fail to delete the used provider
    ?assertMatch(
        {ok, 400, _},
        api_delete([ai, providers, <<"test-provider">>])
    ),

    %% Succeed to update the used provider
    ?assertMatch(
        {ok, 204},
        api_put([ai, providers, <<"test-provider">>], #{
            type => <<"openai">>,
            api_key => <<"new-test-api-key">>
        })
    ),

    %% Fail to change completion profile type
    ?assertMatch(
        {ok, 400, _},
        api_put([ai, completion_profiles, <<"test-completion-profile">>], #{
            type => <<"anthropic">>,
            provider_name => <<"test-provider">>
        })
    ),

    %% Succeed to delete unknown completion profile
    ?assertMatch(
        {ok, 204},
        api_delete([ai, completion_profiles, <<"unknown-completion-profile">>])
    ),

    %% Succeed to update completion profile
    ?assertMatch(
        {ok, 204},
        api_put([ai, completion_profiles, <<"test-completion-profile">>], #{
            type => <<"openai">>,
            provider_name => <<"test-provider">>,
            model => <<"gpt-4o-mini">>
        })
    ),

    %% Succeed to delete the completion profile
    ?assertMatch(
        {ok, 204},
        api_delete([ai, completion_profiles, <<"test-completion-profile">>])
    ),

    %% Succeed to delete unknown provider
    ?assertMatch(
        {ok, 204},
        api_delete([ai, providers, <<"unknown-provider">>])
    ),

    %% Succeed to delete the provider
    ?assertMatch(
        {ok, 204},
        api_delete([ai, providers, <<"test-provider">>])
    ).

t_api_key_redact(_Config) ->
    ?assertMatch(
        {ok, 204},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"https://api.openai.com/v1">>
        })
    ),

    %% Check that the API key is redacted
    ?assertMatch(
        {ok, 200, #{<<"name">> := <<"test-provider">>, <<"api_key">> := <<"******">>}},
        api_get([ai, providers, <<"test-provider">>])
    ),
    ?assertMatch(
        {ok, 200, [#{<<"name">> := <<"test-provider">>, <<"api_key">> := <<"******">>}]},
        api_get([ai, providers])
    ),

    %% Update the provider with the redacted API key
    ?assertMatch(
        {ok, 204},
        api_put([ai, providers, <<"test-provider">>], #{
            type => <<"openai">>,
            base_url => <<"https://api.openai.com/v2">>,
            api_key => <<"******">>
        })
    ),

    %% Check that the API key remained the same
    {ok, #{
        name := <<"test-provider">>,
        api_key := ApiKey0,
        base_url := <<"https://api.openai.com/v2">>
    }} =
        emqx_ai_completion_config:get_provider(<<"test-provider">>),
    ?assertEqual(<<"test-api-key">>, emqx_secret:unwrap(ApiKey0)),
    ?assertMatch(
        [
            #{
                <<"name">> := <<"test-provider">>,
                <<"api_key">> := <<"test-api-key">>,
                <<"base_url">> := <<"https://api.openai.com/v2">>
            }
        ],
        emqx_ai_completion_config:get_providers_raw()
    ),

    %% Now update the provider with a new API key
    ?assertMatch(
        {ok, 204},
        api_put([ai, providers, <<"test-provider">>], #{
            type => <<"openai">>,
            base_url => <<"https://api.openai.com/v3">>,
            api_key => <<"new-test-api-key">>
        })
    ),

    %% Check that the API key is actually updated
    {ok, #{
        name := <<"test-provider">>,
        api_key := ApiKey1,
        base_url := <<"https://api.openai.com/v3">>
    }} =
        emqx_ai_completion_config:get_provider(<<"test-provider">>),
    ?assertEqual(<<"new-test-api-key">>, emqx_secret:unwrap(ApiKey1)),
    ?assertMatch(
        [
            #{
                <<"name">> := <<"test-provider">>,
                <<"api_key">> := <<"new-test-api-key">>,
                <<"base_url">> := <<"https://api.openai.com/v3">>
            }
        ],
        emqx_ai_completion_config:get_providers_raw()
    ).

t_models(_Config) ->
    %% Create provider
    ?assertMatch(
        {ok, 204},
        api_post([ai, providers], #{
            name => <<"test-provider">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"http://localhost:33330/v1">>
        })
    ),

    %% Setup mock
    ok = emqx_ai_completion_provider_mock:start_link(33330, openai_models),

    %% Succeed to fetch models of the provider
    ?assertMatch(
        {ok, 200, [<<"gpt-4-0613">>, <<"gpt-4">>, <<"gpt-3.5-turbo">>]},
        api_get([ai, providers, <<"test-provider">>, models])
    ),

    %% Fail to fetch models of non-existent provider
    ?assertMatch(
        {ok, 404, _},
        api_get([ai, providers, <<"non-existent-provider">>, models])
    ).

t_models_no_provider(_Config) ->
    %% Setup mock
    ok = emqx_ai_completion_provider_mock:start_link(33330, openai_models),

    %% Succeed to fetch models of the test provider
    ?assertMatch(
        {ok, 200, [<<"gpt-4-0613">>, <<"gpt-4">>, <<"gpt-3.5-turbo">>]},
        api_post([ai, models], #{
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            base_url => <<"http://localhost:33330/v1">>
        })
    ),

    %% Fail to fetch models of invalid provider
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, models], #{
            xxx => <<"test-provider">>
        })
    ),
    ?assertMatch(
        {ok, 503, _},
        api_post([ai, models], #{
            type => <<"openai">>,
            api_key => <<"test-api-key">>,
            %% invalid port
            base_url => <<"http://localhost:33333/v1">>
        })
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    decode_body(request(get, uri(Path))).

api_post(Path, Data) ->
    decode_body(request(post, uri(Path), Data)).

api_put(Path, Data) ->
    decode_body(request(put, uri(Path), Data)).

api_delete(Path) ->
    decode_body(request(delete, uri(Path))).

decode_body(Response) ->
    ct:pal("Response: ~p", [Response]),
    do_decode_body(Response).

do_decode_body({ok, Code, <<>>}) ->
    {ok, Code};
do_decode_body({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} ->
            {ok, Code, Decoded};
        {error, _} = Error ->
            ct:pal("Invalid body: ~p", [Body]),
            Error
    end;
do_decode_body(Error) ->
    Error.
