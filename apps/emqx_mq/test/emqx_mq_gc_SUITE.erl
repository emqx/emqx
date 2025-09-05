%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_gc_SUITE).

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

init_per_testcase(TestCase, Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                emqx,
                {emqx_mq,
                    emqx_mq_test_utils:cth_config(#{
                        <<"mq">> =>
                            #{
                                <<"regular_queue_retention_period">> =>
                                    <<"1s">>
                            }
                    })}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    ok = snabbkaffe:start_trace(),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that the GC works as expected:
%% *drops expired generations for regular queues
%% *drops expired messages for lastvalue queues
t_gc(_Config) ->
    ct:sleep(500),
    % %% Create a lastvalue Queue
    MQC = emqx_mq_test_utils:create_mq(#{topic_filter => <<"tc/#">>, is_lastvalue => true}),
    %% Create a non-lastvalue Queue
    MQR = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"tr/#">>, is_lastvalue => false, data_retention_period => 1000
    }),

    % Publish 10 messages to the queues
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"tc/">>,
        payload_prefix => <<"payload-old-">>
    }),
    emqx_mq_test_utils:populate(10, #{
        topic_prefix => <<"tr/">>,
        payload_prefix => <<"payload-old-">>
    }),

    %% Wait for data retention period
    ct:sleep(1000),
    %% This gc should create a new generation
    ?assertWaitEvent(emqx_mq_gc:gc(), #{?snk_kind := mq_gc_regular_done}, 1000),
    RegularDBGens0 = maps:values(emqx_mq_message_db:initial_generations(MQR)),
    ?assertEqual([1], lists:usort(RegularDBGens0)),

    % Publish 10 messages to the queue
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"tc/">>,
        payload_prefix => <<"payload-new-">>
    }),
    emqx_mq_test_utils:populate(10, #{
        topic_prefix => <<"tr/">>,
        payload_prefix => <<"payload-new-">>
    }),

    %% Wait for the data retention period
    ct:sleep(1000),
    %% This gc should also create a new generation and drop the first one
    ?assertWaitEvent(emqx_mq_gc:gc(), #{?snk_kind := mq_gc_done}, 1000),
    RegularDBGens1 = maps:values(emqx_mq_message_db:initial_generations(MQR)),
    ?assertEqual([2], lists:usort(RegularDBGens1)),

    %% Check that only last messages are available
    Records = emqx_mq_message_db:dirty_read_all(MQC),
    ?assertEqual(10, length(Records)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

now_ms() ->
    erlang:system_time(millisecond).

wait_for_consumer_stop(#{id := Id} = _MQ, Ms) when Ms > 5 ->
    ?retry(
        5,
        1 + Ms div 5,
        ?assert(emqx_mq_consumer:find(Id) == not_found)
    ).
