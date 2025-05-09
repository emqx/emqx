%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_config_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_config_backup_and_restore(_Config) ->
    %% Setup some data
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"openai">>,
            <<"name">> => <<"openai-provider">>,
            <<"api_key">> => <<"sk-proj-1234567890">>
        }}
    ),
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"type">> => <<"anthropic">>,
            <<"name">> => <<"anthropic-provider">>,
            <<"api_key">> => <<"sk-ant-api03-1234567890">>
        }}
    ),
    ok = emqx_ai_completion_config:update_completion_profiles_raw(
        {add, #{
            <<"name">> => <<"openai-profile">>,
            <<"type">> => <<"openai">>,
            <<"model">> => <<"gpt-4o">>,
            <<"provider_name">> => <<"openai-provider">>,
            <<"system_prompt">> => <<"pls do something">>
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

    %% Ok to import the empty configs
    ?assertEqual(
        {ok, #{root_key => ai, changed => []}},
        emqx_ai_completion_config:import_config(#{})
    ),
    ?assertMatch(
        {results, {_, []}},
        emqx_ai_completion_config:import_config(#{<<"ai">> => #{}})
    ),

    %% Ok to import the same config
    BackupConfig0 = #{
        <<"ai">> => emqx_config:get_raw([ai])
    },
    ?assertEqual(
        {results,
            {
                [
                    #{root_key => ai, changed => [[ai, providers]]},
                    #{root_key => ai, changed => [[ai, completion_profiles]]}
                ],
                []
            }},
        emqx_ai_completion_config:import_config(BackupConfig0)
    ),

    %% Fail to import Completion Profile without a matching provider
    BackupConfig1 = #{
        <<"ai">> => #{
            <<"completion_profiles">> => [
                #{
                    <<"name">> => <<"anthropic-profile">>,
                    %% Wrong type, the existing provider is anthropic
                    <<"type">> => <<"openai">>,
                    <<"model">> => <<"claude-3-5-sonnet-20240620">>,
                    <<"provider_name">> => <<"anthropic-provider">>,
                    <<"system_prompt">> => <<"pls do something else">>
                }
            ]
        }
    },
    ?assertMatch(
        {results,
            {
                [
                    #{root_key := ai, changed := [[ai, providers]]}
                ],
                [
                    #{root_key := ai, reason := _}
                ]
            }},
        emqx_ai_completion_config:import_config(BackupConfig1)
    ),

    %% Fail to change type of a referenced provider
    BackupConfig2 = #{
        <<"ai">> => #{
            <<"providers">> => [
                #{
                    %% This provider is referenced by an anthropic-type completion profile
                    <<"type">> => <<"openai">>,
                    <<"name">> => <<"anthropic-provider">>,
                    <<"api_key">> => <<"sk-ant-api03-1234567890">>
                }
            ]
        }
    },
    ?assertMatch(
        {results,
            {
                [
                    #{root_key := ai, changed := [[ai, completion_profiles]]}
                ],
                [
                    #{root_key := ai, reason := _}
                ]
            }},
        emqx_ai_completion_config:import_config(BackupConfig2)
    ).
