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
    ok = emqx_ai_completion_test_helpers:unmock_ai_client(),
    ok = emqx_rule_engine:delete_rule(?RULE_ID),
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_completion(_Config) ->
    %% Setup completion profiles
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
    %% Mock actual API client
    %% TODO:
    %% Use container with OpenAI/Anthropic compatible API
    ok = emqx_ai_completion_test_helpers:mock_ai_client(<<"some completion">>),

    %% Setup republish rule
    RepublishTopic = <<"republish/ai_completion">>,
    Params = #{
        id => ?RULE_ID,
        sql =>
            <<
                "SELECT\n"
                "  ai_completion('openai-profile', 'some prompt', payload) as result_openai_1,\n"
                "  ai_completion('openai-profile', payload) as result_openai_2,\n"
                "  ai_completion('anthropic-profile', 'some prompt', payload) as result_anthropic_1,\n"
                "  ai_completion('anthropic-profile', payload) as result_anthropic_2\n"
                "FROM\n"
                "  't/#'"
            >>,
        enable => true,
        actions => [
            #{
                function => <<"republish">>,
                args =>
                    #{
                        topic => RepublishTopic,
                        qos => 0,
                        retain => false,
                        payload => <<
                            "${result_openai_1}-${result_openai_2}-"
                            "${result_anthropic_1}-${result_anthropic_2}"
                        >>,
                        mqtt_properties => #{},
                        user_properties => #{},
                        direct_dispatch => false
                    }
            }
        ]
    },
    {ok, _} = emqx_rule_engine:create_rule(Params),

    %% Publish message to trigger rule
    {ok, C} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, 0),
    emqtt:publish(C, <<"t/1">>, <<"hello">>),

    %% Verify rule is triggered
    receive
        {publish, #{payload := Payload}} ->
            ?assertEqual(
                <<"some completion-some completion-some completion-some completion">>,
                Payload
            )
    after 1000 ->
        ct:fail("message not republished")
    end,

    %% Cleanup
    emqtt:disconnect(C),
    ok.
