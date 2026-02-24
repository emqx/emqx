%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_mq_api_helpers,
    [
        api_get/1,
        api_post/2,
        api_put/2,
        api_delete/1,
        urlencode/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, emqx_mq_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
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

%% Verify basic CRUD operations on message queues.
t_crud(_Config) ->
    ?assertMatch(
        {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
        api_get([message_queues, queues])
    ),
    %% We may work with legacy queues, which had not passed name validation,
    %% so we allow any name in get requests.
    ?assertMatch(
        {ok, 404, _},
        api_get([message_queues, queues, urlencode(<<"invalid/queue/name">>)])
    ),
    ?assertMatch(
        {ok, 404, _},
        api_get([message_queues, queues, <<"unknown_queue">>])
    ),
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"crud_11">>,
            <<"topic_filter">> => <<"t/1/1">>,
            <<"ping_interval">> => 9999
        })
    ),
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"crud_12">>,
            <<"topic_filter">> => <<"t/1/2">>,
            <<"ping_interval">> => 9999,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),
    ?retry(
        5,
        20,
        begin
            {ok, 200, #{
                <<"data">> := Data,
                <<"meta">> := #{<<"hasnext">> := false}
            }} =
                api_get([message_queues, queues]),
            ?assertMatch(
                [
                    #{
                        <<"name">> := <<"crud_11">>,
                        <<"topic_filter">> := <<"t/1/1">>,
                        <<"ping_interval">> := 9999,
                        %% Lastvalue flag is true by default
                        <<"is_lastvalue">> := true,
                        <<"limits">> := #{
                            <<"max_shard_message_count">> := <<"infinity">>,
                            <<"max_shard_message_bytes">> := <<"infinity">>
                        }
                    },
                    #{
                        <<"name">> := <<"crud_12">>,
                        <<"topic_filter">> := <<"t/1/2">>,
                        <<"ping_interval">> := 9999,
                        <<"limits">> := #{
                            <<"max_shard_message_count">> := 1000,
                            <<"max_shard_message_bytes">> := 104857600
                        }
                    }
                ],
                sort_by(fun(#{<<"name">> := Name}) -> Name end, Data)
            )
        end
    ),
    ?assertMatch(
        {ok, 404, _},
        api_put([message_queues, queues, <<"crud_2">>], #{<<"ping_interval">> => 10000})
    ),
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{
                <<"name">> := <<"crud_11">>,
                <<"topic_filter">> := <<"t/1/1">>,
                <<"ping_interval">> := 10000
            }},
            api_put([message_queues, queues, <<"crud_11">>], #{
                <<"ping_interval">> => 10000
            })
        )
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"name">> := <<"crud_11">>,
            <<"topic_filter">> := <<"t/1/1">>,
            <<"ping_interval">> := 10000
        }},
        api_get([message_queues, queues, <<"crud_11">>])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, <<"crud_11">>])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, <<"crud_12">>])
    ),
    ?assertMatch(
        {ok, 404, _},
        api_delete([message_queues, queues, <<"crud_11">>])
    ),
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
            api_get([message_queues, queues])
        )
    ).

%% Verify basic CRUD operations on legacy message queues.
t_legacy_queues_crud(_Config) ->
    %% Cannot create a legacy queue with API
    ?assertMatch(
        {ok, 400, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"/t/1">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => false
        })
    ),

    %% Create a legacy queue directly in the database
    MQ0 = emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"t/#">>, is_lastvalue => false}),
    ok = emqx_mq_registry:create_pre_611_queue(MQ0),
    ?assertMatch(
        {ok, _},
        emqx_mq_registry:find(<<"/t/#">>)
    ),

    %% Find queue via API
    ?assertMatch(
        {ok, 200, _},
        api_get([message_queues, queues, urlencode(<<"/t/#">>)])
    ),

    %% Update queue via API
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, urlencode(<<"/t/#">>)], #{
            <<"is_lastvalue">> => false,
            <<"ping_interval">> => 10000
        })
    ),

    %% Delete queue via API
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, urlencode(<<"/t/#">>)])
    ),

    ?assertEqual(not_found, emqx_mq_registry:find(<<"/t/#">>)).

%% Verify pagination logic of message queue listing.
t_pagination(_Config) ->
    %% Create 10 MQs and fetch them in batches of 6.
    lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            api_post([message_queues, queues], #{
                <<"name">> => <<"pagination_", IBin/binary>>,
                <<"topic_filter">> => <<"t/", IBin/binary>>
            })
        end,
        lists:seq(1, 10)
    ),
    {ok, 200, #{
        <<"data">> := Data0, <<"meta">> := #{<<"hasnext">> := true, <<"cursor">> := Cursor}
    }} =
        api_get([message_queues, queues, "?limit=6"]),
    ?assertEqual(6, length(Data0)),
    {ok, 200, #{<<"data">> := Data1, <<"meta">> := #{<<"hasnext">> := false}}} =
        api_get([message_queues, queues, "?limit=6&cursor=" ++ urlencode(Cursor)]),
    ?assertEqual(4, length(Data1)),

    %% Check that the last page does not have `hasnext
    {ok, 200, #{<<"data">> := Data2, <<"meta">> := #{<<"hasnext">> := false}}} =
        api_get([message_queues, queues, "?limit=4&cursor=" ++ urlencode(Cursor)]),
    ?assertEqual(4, length(Data2)),

    %% Check that we do not crash on invalid cursor
    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        api_get([message_queues, queues, "?limit=6&cursor=%10%13"])
    ),
    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        api_get([message_queues, queues, "?limit=6&cursor=" ++ urlencode(base64:encode(<<"{{{">>))])
    ).

%% Verify MQ subsystem (re)configuration via API.
t_config(_Config) ->
    ?assertMatch(
        {ok, 200, _},
        api_get([message_queues, config])
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, config], #{<<"gc_interval">> => <<"-10h">>})
    ),
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{
            <<"gc_interval">> => <<"2h">>,
            <<"regular_queue_retention_period">> => <<"14d">>,
            <<"find_queue_retry_interval">> => <<"20s">>
        })
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"gc_interval">> := <<"2h">>,
            <<"regular_queue_retention_period">> := <<"14d">>,
            <<"find_queue_retry_interval">> := <<"20s">>
        }},
        api_get([message_queues, config])
    ),
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{
            <<"auto_create">> => #{
                <<"regular">> => false,
                <<"lastvalue">> => #{}
            }
        })
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"auto_create">> := #{
                <<"regular">> := false,
                <<"lastvalue">> := #{
                    <<"key_expression">> := <<"message.from">>
                }
            }
        }},
        api_get([message_queues, config])
    ),
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Queues should be configured to be automatically created either as regular or lastvalue">>
        }},
        api_put([message_queues, config], #{
            <<"auto_create">> => #{
                <<"regular">> => #{}, <<"lastvalue">> => #{}
            }
        })
    ).

%% Verify queue state creation failure is handled gracefully.
t_queue_state_creation_failure(_Config) ->
    ok = meck:new(emqx_ds, [passthrough, no_history]),
    ok = meck:expect(emqx_ds, trans, fun(_, _) -> {error, recoverable, leader_unavailable} end),
    ?assertMatch(
        {ok, 503, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"queue_state_creation_failure">>, <<"topic_filter">> => <<"t/1">>
        })
    ),
    ok = meck:unload(emqx_ds).

%% Verify that regular queue cannot be created with key expression.
t_lastvalue_vs_regular(_Config) ->
    %% Cannot create a regular queue with key expression
    ?assertMatch(
        {ok, 400, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"regular1">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"key_expression">> => <<"message.from">>,
            <<"is_lastvalue">> => false
        })
    ),

    %% Cannot update a regular queue to lastvalue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"regular2">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => false
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, <<"regular2">>], #{<<"is_lastvalue">> => true})
    ),

    %% Key expression is not allowed to be updated for regular queues
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, <<"regular2">>], #{
            <<"key_expression">> => <<"message.from">>
        })
    ),

    %% Cannot update a lastvalue queue to regular
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"lastvalue1">>,
            <<"topic_filter">> => <<"t/2">>,
            <<"is_lastvalue">> => true
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, <<"lastvalue1">>], #{<<"is_lastvalue">> => false})
    ).

%% Verify that regular queue cannot be converted from limited to unlimited and vice versa.
t_limited_vs_unlimited(_Config) ->
    %% Cannot create a regular queue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"regular">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => false
        })
    ),

    %% Cannot update an unlimited regular queue to limited
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Regular queues cannot be updated from limited to unlimited and vice versa">>
        }},
        api_put([message_queues, queues, <<"regular">>], #{
            <<"is_lastvalue">> => false,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Create a limited regular queue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"regular_limited">>,
            <<"topic_filter">> => <<"t/2">>,
            <<"is_lastvalue">> => false,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Cannot update a limited regular queue to unlimited
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Regular queues cannot be updated from limited to unlimited and vice versa">>
        }},
        api_put([message_queues, queues, <<"regular_limited">>], #{<<"is_lastvalue">> => false})
    ),

    %% Create an unlimited lastvalue queue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"lastvalue">>,
            <<"topic_filter">> => <<"t/3">>,
            <<"is_lastvalue">> => true
        })
    ),

    %% Successfully update an unlimited lastvalue queue to limited
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, <<"lastvalue">>], #{
            <<"is_lastvalue">> => true,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Successfully remove limits back from a limited lastvalue queue
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, <<"lastvalue">>], #{
            <<"is_lastvalue">> => true,
            <<"limits">> => #{
                <<"max_shard_message_count">> => <<"infinity">>,
                <<"max_shard_message_bytes">> => <<"infinity">>
            }
        })
    ).

%% Verify that default values are good enough for lastvalue queues
t_defaults(_Config) ->
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => <<"defaults">>, <<"topic_filter">> => <<"t/#">>
        })
    ),
    %% Publish 10 messages to the queue
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-">>,
        n_keys => 10
    }),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"defaults">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages. Default key expression is clientid, so we should receive only one message.
    ?assertEqual(1, length(Msgs)).

%% Verify that the max queue count is respected
t_max_queue_count(_Config) ->
    emqx_config:put([mq, max_queue_count], 5),
    %% Create 5 MQs to fill the limit
    lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            ?assertMatch(
                {ok, 200, _},
                api_post([message_queues, queues], #{
                    <<"name">> => <<"max_queue_count_", IBin/binary>>,
                    <<"topic_filter">> => <<"t/", IBin/binary>>
                })
            )
        end,
        lists:seq(1, 5)
    ),
    %% Try to create a 6th MQ, expect an error
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"MAX_QUEUE_COUNT_REACHED">>,
            <<"message">> := <<"Max queue count reached">>
        }},
        api_post([message_queues, queues], #{
            <<"name">> => <<"max_queue_count_6">>,
            <<"topic_filter">> => <<"t/6">>
        })
    ).

%% Verify that a legacy queue can be updated and subsequently deleted via API.
t_update_legacy_queue(_Config) ->
    %% Create a legacy lastvalue queue directly in the database
    MQ0 = emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"t/#">>, is_lastvalue => true}),
    ok = emqx_mq_registry:create_pre_611_queue(MQ0),

    %% Update the legacy queue via API
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, urlencode(<<"/t/#">>)], #{
            <<"is_lastvalue">> => true,
            <<"key_expression">> => <<"message.from">>
        })
    ),

    %% Delete the legacy queue via API
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, urlencode(<<"/t/#">>)])
    ),
    ?assertEqual(not_found, emqx_mq_registry:find(<<"/t/#">>)),

    %% Check list is empty
    ?assertMatch(
        {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
        api_get([message_queues, queues])
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

sort_by(Fun, List) ->
    lists:sort(fun(A, B) -> Fun(A) < Fun(B) end, List).
