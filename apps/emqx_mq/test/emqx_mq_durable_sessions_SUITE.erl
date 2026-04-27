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

%% Verify that a client with a durable session CAN subscribe to an MQ and
%% that messages are delivered both on first connect and after session resume.
t_subscribe_to_mq_for_durable_sessions(_Config) ->
    %% Create an MQ and publish some messages before subscribing
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"q1">>, topic_filter => <<"t/#">>, is_lastvalue => false
    }),
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Connect with a durable session and subscribe to the MQ
    CSub0 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),
    {ok, _, [1]} = emqtt:subscribe(CSub0, {<<"$queue/q1/t/#">>, 1}),

    %% All pre-published messages should be delivered
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 5000),
    ?assertEqual(10, length(Msgs0)),

    %% Disconnect and publish more messages while the session is offline
    ok = emqtt:disconnect(CSub0),
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Reconnect — the session is resumed and the subscription is restored
    CSub1 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),

    %% Messages published while offline should be delivered after resume
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 10, _Timeout1 = 5000),
    ?assertEqual(10, length(Msgs1)),

    ok = emqtt:disconnect(CSub1).
