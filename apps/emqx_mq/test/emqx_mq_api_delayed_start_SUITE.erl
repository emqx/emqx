%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api_delayed_start_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_mq_api_helpers,
    [
        api_get/1,
        api_put/2
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, emqx_mq_test_utils:cth_config(emqx)},
            {emqx_mq, #{config => #{<<"mq">> => #{<<"enable">> => false}}}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that MQ subsystem may be started in runtime.
t_config(_Config) ->
    %% We started with disabled MQ subsystem, so queue API should be unavailable.
    ?assertMatch(
        {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>, <<"message">> := <<"Not enabled">>}},
        api_get([message_queues, queues])
    ),

    %% Start MQ subsystem via API.
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{<<"enable">> => true})
    ),
    ok = emqx_mq_app:wait_readiness(5000),

    %% Verify that queue API is now available.
    ?assertMatch(
        {ok, 200, _},
        api_get([message_queues, queues])
    ),

    %% Verify that we cannot disable MQ subsystem via API.
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := <<"Cannot disable MQ subsystem via API">>
        }},
        api_put([message_queues, config], #{<<"enable">> => false})
    ).
