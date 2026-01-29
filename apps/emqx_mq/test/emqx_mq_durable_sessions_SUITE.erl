%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_durable_sessions_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_mq_internal.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx,
                    emqx_mq_test_utils:cth_config(emqx, #{
                        <<"durable_sessions">> => #{
                            <<"enable">> => true
                        }
                    })},
                {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that a client cannot subscribe to an MQ if the session is durable.
t_forbid_subscribe_to_mq_for_durable_sessions(_Config) ->
    %% Create an MQ
    _ = emqx_mq_test_utils:ensure_mq_created(#{topic_filter => <<"t/#">>, is_lastvalue => false}),

    %% When having a durable session, try to subscribe to an MQ and expect an error.
    CSub0 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),
    {ok, _, [?RC_NOT_AUTHORIZED]} = emqtt:subscribe(CSub0, {<<"$q/t/#">>, 1}),
    ok = emqtt:disconnect(CSub0),

    %% Reconnect and still expect an error.
    CSub1 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),
    {ok, _, [?RC_NOT_AUTHORIZED]} = emqtt:subscribe(CSub1, {<<"$q/t/#">>, 1}),
    ok = emqtt:disconnect(CSub1).
