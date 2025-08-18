%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(RULE_ID, <<"emqx_ai_completion_rule">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_rule_engine,
            {emqx_ai_completion, #{config => "ai.providers = [], ai.completion_profiles = []"}}
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
    ok = emqx_bridge_v2_testlib:delete_all_rules(),
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers(),
    ok = emqx_ai_completion_provider_mock:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_openai_chat_completions_completion(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"openai">>,
            <<"name">> => <<"openai-chat-completions-provider">>,
            <<"api_key">> => <<"sk-proj-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_config:update_completion_profiles_raw(
        {add, #{
            <<"name">> => <<"openai-chat-completions-profile">>,
            <<"type">> => <<"openai">>,
            <<"model">> => <<"gpt-4o">>,
            <<"provider_name">> => <<"openai-chat-completions-provider">>,
            <<"system_prompt">> => <<"pls do something">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, openai_chat_completions),
    %% Setup republish rule
    RepublishTopic = <<"republish/ai_completion">>,
    Params = #{
        <<"id">> => ?RULE_ID,
        <<"sql">> =>
            <<
                "SELECT\n"
                "  ai_completion('openai-chat-completions-profile', 'some prompt', payload) as result_openai_1,\n"
                "  ai_completion('openai-chat-completions-profile', payload) as result_openai_2\n"
                "FROM\n"
                "  't/#'"
            >>,
        <<"enable">> => true,
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> =>
                    #{
                        <<"topic">> => RepublishTopic,
                        <<"qos">> => 0,
                        <<"retain">> => false,
                        <<"payload">> => <<"${result_openai_1}-${result_openai_2}">>,
                        <<"mqtt_properties">> => #{},
                        <<"direct_dispatch">> => false
                    }
            }
        ]
    },
    {ok, _} = emqx_bridge_v2_testlib:create_rule_directly(Params),

    %% Verify rule is triggered correctly
    verify_republish(
        <<"t/1">>,
        <<"hello">>,
        RepublishTopic,
        <<"some completion-some completion">>
    ).

t_openai_response_completion(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"openai_response">>,
            <<"name">> => <<"openai-provider">>,
            <<"api_key">> => <<"sk-proj-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_config:update_completion_profiles_raw(
        {add, #{
            <<"name">> => <<"openai-profile">>,
            <<"type">> => <<"openai_response">>,
            <<"model">> => <<"gpt-4o">>,
            <<"provider_name">> => <<"openai-provider">>,
            <<"system_prompt">> => <<"pls do something">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, openai_responses),
    %% Setup republish rule
    RepublishTopic = <<"republish/ai_completion">>,
    Params = #{
        <<"id">> => ?RULE_ID,
        <<"sql">> =>
            <<
                "SELECT\n"
                "  ai_completion('openai-profile', 'some prompt', payload) as result_openai_1,\n"
                "  ai_completion('openai-profile', payload) as result_openai_2\n"
                "FROM\n"
                "  't/#'"
            >>,
        <<"enable">> => true,
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> =>
                    #{
                        <<"topic">> => RepublishTopic,
                        <<"qos">> => 0,
                        <<"retain">> => false,
                        <<"payload">> => <<"${result_openai_1}-${result_openai_2}">>,
                        <<"mqtt_properties">> => #{},
                        <<"direct_dispatch">> => false
                    }
            }
        ]
    },
    {ok, _} = emqx_bridge_v2_testlib:create_rule_directly(Params),

    %% Verify rule is triggered correctly
    verify_republish(
        <<"t/1">>,
        <<"hello">>,
        RepublishTopic,
        <<"some completion-some completion">>
    ).

t_anthropic_completion(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"anthropic">>,
            <<"name">> => <<"anthropic-provider">>,
            <<"api_key">> => <<"sk-ant-api03-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_config:update_completion_profiles_raw(
        {add, #{
            <<"name">> => <<"anthropic-profile">>,
            <<"type">> => <<"anthropic">>,
            <<"model">> => <<"claude-3-5-sonnet-20240620">>,
            <<"provider_name">> => <<"anthropic-provider">>,
            <<"system_prompt">> => <<"pls do something else">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, anthropic_messages),

    %% Setup republish rule
    RepublishTopic = <<"republish/ai_completion">>,
    Params = #{
        <<"id">> => ?RULE_ID,
        <<"sql">> =>
            <<
                "SELECT\n"
                "  ai_completion('anthropic-profile', 'some prompt', payload) as result_anthropic_1,\n"
                "  ai_completion('anthropic-profile', payload) as result_anthropic_2\n"
                "FROM\n"
                "  't/#'"
            >>,
        <<"enable">> => true,
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> =>
                    #{
                        <<"topic">> => RepublishTopic,
                        <<"qos">> => 0,
                        <<"retain">> => false,
                        <<"payload">> => <<"${result_anthropic_1}-${result_anthropic_2}">>,
                        <<"mqtt_properties">> => #{},
                        <<"direct_dispatch">> => false
                    }
            }
        ]
    },
    {ok, _} = emqx_bridge_v2_testlib:create_rule_directly(Params),

    %% Verify rule is triggered correctly
    verify_republish(
        <<"t/1">>,
        <<"hello">>,
        RepublishTopic,
        <<"some completion-some completion">>
    ).

t_hackney_pool_config(_Config) ->
    ProviderRaw0 = #{
        <<"type">> => <<"openai">>,
        <<"name">> => <<"openai-provider">>,
        <<"api_key">> => <<"sk-proj-1234567890">>,
        <<"base_url">> => <<"http://localhost:33330/v1">>,
        <<"transport_options">> => #{
            <<"max_connections">> => 10
        }
    },
    Pool = emqx_ai_completion_provider:hackney_pool(#{name => <<"openai-provider">>}),
    ok = emqx_ai_completion_config:update_providers_raw({add, ProviderRaw0}),
    ?assertEqual(10, hackney_pool:max_connections(Pool)),
    ProviderRaw1 = emqx_utils_maps:deep_put(
        [<<"transport_options">>, <<"max_connections">>], ProviderRaw0, 20
    ),
    ok = emqx_ai_completion_config:update_providers_raw({update, ProviderRaw1}),
    ?assertEqual(20, hackney_pool:max_connections(Pool)),
    ok = emqx_ai_completion_config:update_providers_raw({delete, <<"openai-provider">>}),
    ?assertEqual([], ets:tab2list(hackney_pool)),
    ?assertException(
        exit,
        {noproc, _},
        hackney_pool:max_connections(Pool)
    ).

t_openai_models(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"openai">>,
            <<"name">> => <<"openai-provider">>,
            <<"api_key">> => <<"sk-proj-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, openai_models),
    ?assertEqual(
        {ok, [<<"gpt-4-0613">>, <<"gpt-4">>, <<"gpt-3.5-turbo">>]},
        emqx_ai_completion:list_models(<<"openai-provider">>)
    ).

t_anthropic_models(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"anthropic">>,
            <<"name">> => <<"anthropic-provider">>,
            <<"api_key">> => <<"sk-ant-api03-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, anthropic_models),
    ?assertEqual(
        {ok, [<<"claude-opus-4-20250514">>, <<"claude-3-opus-20240229">>]},
        emqx_ai_completion:list_models(<<"anthropic-provider">>)
    ).

t_anthropic_models_paginated(_Config) ->
    %% Setup completion profiles
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"anthropic">>,
            <<"name">> => <<"anthropic-provider">>,
            <<"api_key">> => <<"sk-ant-api03-1234567890">>,
            <<"base_url">> => <<"http://localhost:33330/v1">>
        }}
    ),
    ok = emqx_ai_completion_provider_mock:start_link(33330, anthropic_models_paginated),
    ?assertEqual(
        {ok, [<<"claude-opus-4-20250514">>, <<"claude-3-opus-20240229">>]},
        emqx_ai_completion:list_models(<<"anthropic-provider">>)
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

verify_republish(Topic, Payload, ExpectedRepublishTopic, ExpectedRepublishPayload) ->
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, ExpectedRepublishTopic, 0),
    emqtt:publish(C, Topic, Payload),

    %% Verify rule is triggered
    receive
        {publish, #{payload := ReceivedPayload}} ->
            ?assertEqual(
                ExpectedRepublishPayload,
                ReceivedPayload
            )
    after 1000 ->
        ct:fail("message not republished")
    end,

    %% Cleanup
    emqtt:disconnect(C),
    ok.
