%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_resource_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%===========================================================================
%% Test cases
%%===========================================================================

health_check_interval_validator_test_() ->
    [
        ?_assertMatch(
            #{<<"resource_opts">> := #{<<"health_check_interval">> := 150_000}},
            parse_and_check_webhook_bridge(webhook_bridge_health_check_hocon(<<"150s">>))
        ),
        ?_assertMatch(
            #{<<"resource_opts">> := #{<<"health_check_interval">> := 3_600_000}},
            parse_and_check_webhook_bridge(webhook_bridge_health_check_hocon(<<"3600000ms">>))
        ),
        ?_assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := <<"Health Check Interval (3600001ms) is out of range", _/binary>>,
                    value := 3600001
                }
            ]},
            parse_and_check_webhook_bridge(webhook_bridge_health_check_hocon(<<"3600001ms">>))
        ),
        {"bad parse: negative number",
            ?_assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := "Not a valid duration",
                        value := <<"-10ms">>
                    }
                ]},
                parse_and_check_webhook_bridge(webhook_bridge_health_check_hocon(<<"-10ms">>))
            )},
        {"bad parse: underscores",
            ?_assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := "Not a valid duration",
                        value := <<"3_600_000ms">>
                    }
                ]},
                parse_and_check_webhook_bridge(webhook_bridge_health_check_hocon(<<"3_600_000ms">>))
            )},
        ?_assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := "timeout value too large" ++ _
                }
            ]},
            parse_and_check_webhook_bridge(
                webhook_bridge_health_check_hocon(<<"150000000000000s">>)
            )
        )
    ].

worker_pool_size_test_() ->
    BaseConf = parse(webhook_bridge_health_check_hocon(<<"15s">>)),
    Check = fun(WorkerPoolSize) ->
        Conf = emqx_utils_maps:deep_put(
            [
                <<"bridges">>,
                <<"webhook">>,
                <<"simple">>,
                <<"resource_opts">>,
                <<"worker_pool_size">>
            ],
            BaseConf,
            WorkerPoolSize
        ),
        #{<<"bridges">> := #{<<"webhook">> := #{<<"simple">> := CheckedConf}}} = check(Conf),
        #{<<"resource_opts">> := #{<<"worker_pool_size">> := WPS}} = CheckedConf,
        WPS
    end,
    AssertThrow = fun(WorkerPoolSize) ->
        ?assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := #{expected := _},
                    value := WorkerPoolSize
                }
            ]},
            Check(WorkerPoolSize)
        )
    end,
    [
        ?_assertEqual(1, Check(1)),
        ?_assertEqual(100, Check(100)),
        ?_assertEqual(1024, Check(1024)),
        ?_test(AssertThrow(0)),
        ?_test(AssertThrow(1025))
    ].

%%===========================================================================
%% Helper functions
%%===========================================================================

parse_and_check_webhook_bridge(Hocon) ->
    #{<<"bridges">> := #{<<"webhook">> := #{<<"simple">> := Conf}}} = check(parse(Hocon)),
    Conf.

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

%%===========================================================================
%% Data section
%%===========================================================================

%% erlfmt-ignore
webhook_bridge_health_check_hocon(HealthCheckInterval) ->
io_lib:format(
"
bridges.webhook.simple {
  url = \"http://localhost:4000\"
  body = \"body\"
  resource_opts {
    health_check_interval = \"~s\"
  }
}
",
[HealthCheckInterval]).
