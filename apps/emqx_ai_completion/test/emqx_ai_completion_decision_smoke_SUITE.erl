%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_decision_smoke_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case os:getenv("OPENAI_API_KEY") of
        Key when Key =:= false; Key =:= "" ->
            {skip, "OPENAI_API_KEY is not set"};
        _ ->
            start_suite(Config)
    end.

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_ai_completion_test_helpers:clean_completion_profiles(),
    ok = emqx_ai_completion_test_helpers:clean_providers().

%% Verify that the live Decisions API returns a named predicate with a valid probability.
t_decisions(_Config) ->
    ct:timetrap({seconds, 60}),
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"name">> => <<"smoke-provider">>,
            <<"type">> => <<"openai_decisions">>,
            <<"api_key">> => list_to_binary(os:getenv("OPENAI_API_KEY")),
            <<"transport_options">> => #{
                <<"connect_timeout">> => <<"10s">>,
                <<"recv_timeout">> => <<"30s">>
            }
        }}
    ),
    ok = emqx_ai_completion_config:update_completion_profiles_raw(
        {add, #{
            <<"name">> => <<"smoke-profile">>,
            <<"type">> => <<"openai_decisions">>,
            <<"provider_name">> => <<"smoke-provider">>,
            <<"questions">> => [
                #{
                    <<"type">> => <<"predicate">>,
                    <<"name">> => <<"fault">>,
                    <<"instructions">> => <<"Does this device report describe a hardware fault?">>
                }
            ]
        }}
    ),
    Result = emqx_ai_completion_rule_sql:rsf_ai_completion([
        <<"smoke-profile">>, <<"The motor has overheated and the device cannot operate.">>
    ]),
    #{
        <<"answers">> := [
            #{
                <<"type">> := <<"predicate">>,
                <<"name">> := <<"fault">>,
                <<"probability">> := Probability
            }
        ]
    } = emqx_utils_json:decode(Result),
    ?assert(is_number(Probability)),
    ?assert(Probability >= 0 andalso Probability =< 1).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

start_suite(Config) ->
    Conf = """
    listeners.tcp.default.enable = false
    listeners.ssl.default.enable = false
    listeners.ws.default.enable = false
    listeners.wss.default.enable = false
    ai.providers = []
    ai.completion_profiles = []
    """,
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{config => Conf}},
            {emqx, #{config => false}},
            emqx_rule_engine,
            emqx_ai_completion
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].
