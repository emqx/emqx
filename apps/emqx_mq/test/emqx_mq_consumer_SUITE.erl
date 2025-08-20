%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_mq_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_durable_storage, #{override_env => [{poll_batch_size, 1}]}},
            emqx,
            emqx_mq
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that the consumer stops itself after there are no active subscribers for a while
t_auto_shutdown(_Config) ->
    %% Create a non-compacted Queue
    _ = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"t1/#">>, is_compacted => false, consumer_max_inactive => 50
    }),

    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t1/#">>),

    ?assertWaitEvent(
        emqtt:disconnect(CSub),
        #{?snk_kind := mq_consumer_shutdown, mq_topic_filter := <<"t1/#">>},
        1000
    ).
