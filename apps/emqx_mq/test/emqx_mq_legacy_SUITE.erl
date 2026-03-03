%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_legacy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_mq_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx, emqx_mq_test_utils:cth_config(emqx)},
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
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = emqx_mq_test_utils:reset_config().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that we can consume messages from a legacy queue via $q prefix without a name
t_smoke(_Config) ->
    %% Create a non-lastvalue Queue
    MQ0 = emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"t/#">>, is_lastvalue => false}),
    ok = emqx_mq_registry:create_pre_611_queue(MQ0),
    {ok, MQ} = emqx_mq_registry:find(<<"/t/#">>),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    AllMessages = emqx_mq_message_db:dirty_read_all(MQ),
    ?assertEqual(10, length(AllMessages)),

    % Consume the messages from the queue via $q prefix without a name
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    {ok, _, _} = emqtt:subscribe(CSub, {<<"$q/t/#">>, 1}),

    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),

    %% Verify the messages
    ?assertEqual(10, length(Msgs)),
    ok.
