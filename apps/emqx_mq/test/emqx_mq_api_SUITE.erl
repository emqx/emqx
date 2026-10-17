%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx.hrl").

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
    Standalone = emqx_common_test_helpers:all(?MODULE) -- purge_cases(),
    Standalone ++ [{group, Group} || {Group, _, _} <- groups()].

groups() ->
    [
        {purge_regular, [], purge_cases()},
        {purge_limited_regular, [], purge_cases()},
        {purge_lastvalue, [], purge_cases()},
        {purge_limited_lastvalue, [], purge_cases()}
    ].

purge_cases() ->
    [t_purge_all, t_purge_before].

init_per_group(Group, Config) ->
    {IsLastvalue, IsLimited} =
        case Group of
            purge_regular -> {false, false};
            purge_limited_regular -> {false, true};
            purge_lastvalue -> {true, false};
            purge_limited_lastvalue -> {true, true}
        end,
    [{is_lastvalue, IsLastvalue}, {is_limited, IsLimited} | Config].

end_per_group(_Group, _Config) ->
    ok.

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
    ok = emqx_common_test_helpers:call_janitor(),
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

%% Verify that an update of a queue applies to its running consumer.
t_update_running_queue(_Config) ->
    Name = <<"update_running_queue">>,
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"name">> => Name,
            <<"topic_filter">> => <<"t/#">>,
            <<"is_lastvalue">> => false,
            <<"ping_interval">> => 100,
            <<"data_retention_period">> => 86_400_000
        })
    ),
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(CSub, Name),
    ok = emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),
    %% The consumer is running.
    {ok, [_]} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 1, _Timeout0 = 1000),

    %% An unchanged update keeps the consumer running.
    {ok, #{id := Id}} = emqx_mq_registry:find(Name),
    {ok, ConsumerRef} = emqx_mq_consumer:find(Id),
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, Name], #{
            <<"is_lastvalue">> => false,
            <<"ping_interval">> => 100,
            <<"data_retention_period">> => 86_400_000
        })
    ),
    ?assertEqual({ok, ConsumerRef}, emqx_mq_consumer:find(Id)),

    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, Name], #{
            <<"is_lastvalue">> => false,
            <<"ping_interval">> => 100,
            <<"data_retention_period">> => 600_000
        })
    ),
    %% One hour old, so within the old retention period but not the new one.
    %% Both messages are from one client, so they are read in order.
    Stale = emqx_message:make(<<"pub">>, 1, <<"t/1">>, <<"stale">>),
    Fresh = emqx_message:make(<<"pub">>, 1, <<"t/1">>, <<"fresh">>),
    _ = emqx:publish(Stale#message{timestamp = erlang:system_time(millisecond) - 3_600_000}),
    _ = emqx:publish(Fresh),

    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 1, _Timeout1 = 1000),
    ?assertEqual([<<"fresh">>], [Payload || #{payload := Payload} <- Msgs]),
    ok = emqtt:disconnect(CSub).

%% Verify an unset cutoff stays absent and configured timestamps persist for both queue types.
t_deliver_cutoff_date(_Config) ->
    lists:foreach(
        fun(IsLastvalue) ->
            Name = <<"deliver_cutoff_date">>,
            {ok, 200, Created} = api_post([message_queues, queues], #{
                <<"name">> => Name,
                <<"topic_filter">> => <<"t/#">>,
                <<"is_lastvalue">> => IsLastvalue
            }),
            ?assertNot(maps:is_key(<<"deliver_cutoff_date">>, Created)),
            {ok, 200, Stored} = api_get([message_queues, queues, Name]),
            ?assertNot(maps:is_key(<<"deliver_cutoff_date">>, Stored)),
            {ok, MQ} = emqx_mq_registry:find(Name),
            ?assertNot(maps:is_key(deliver_cutoff_date, MQ)),
            lists:foreach(
                fun({Raw, Expected}) ->
                    ?assertMatch(
                        {ok, 200, _},
                        api_put([message_queues, queues, Name], #{
                            <<"is_lastvalue">> => IsLastvalue,
                            <<"deliver_cutoff_date">> => Raw
                        })
                    ),
                    ?assertMatch(
                        {ok, 200, #{<<"deliver_cutoff_date">> := Expected}},
                        api_get([message_queues, queues, Name])
                    )
                end,
                [
                    {<<"1970-01-01T00:00:01.234Z">>, 1234},
                    {1_790_000_000_123, 1_790_000_000_123},
                    {0, 0}
                ]
            ),
            lists:foreach(
                fun(Invalid) ->
                    ?assertMatch(
                        {ok, 400, _},
                        api_put([message_queues, queues, Name], #{
                            <<"is_lastvalue">> => IsLastvalue,
                            <<"deliver_cutoff_date">> => Invalid
                        })
                    )
                end,
                [-1, <<"none">>, <<"not-a-date">>, <<"1969-12-31T23:59:59Z">>]
            ),
            ?assertMatch({ok, 204}, api_delete([message_queues, queues, Name]))
        end,
        [false, true]
    ).

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

%% Verify retention accepts finite durations beyond the timer limit and preserves milliseconds.
t_retention_period(_Config) ->
    Field = <<"regular_queue_retention_period">>,
    lists:foreach(
        fun({Raw, Milliseconds}) ->
            ?assertMatch({ok, 204}, api_put([message_queues, config], #{Field => Raw})),
            ?assertMatch({ok, 200, #{Field := Raw}}, api_get([message_queues, config])),
            ?assertEqual(Milliseconds, emqx_config:get([mq, regular_queue_retention_period]))
        end,
        [
            {<<"0ms">>, 0},
            {<<"7d">>, 604800000},
            {<<"90d">>, 7776000000},
            {<<"365d">>, 31536000000},
            {7776000000, 7776000000}
        ]
    ),
    lists:foreach(
        fun(Raw) ->
            ?assertMatch({ok, 400, _}, api_put([message_queues, config], #{Field => Raw}))
        end,
        [<<"-1ms">>, -1, <<"infinity">>]
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, config], #{<<"gc_interval">> => <<"90d">>})
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

%% Verify that /queues/* alias paths work identically to /message_queues/* paths.
t_queues_alias_crud(_Config) ->
    %% List via /queues
    ?assertMatch(
        {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
        api_get([queues])
    ),
    %% Create via /queues
    ?assertMatch(
        {ok, 200, _},
        api_post([queues], #{
            <<"name">> => <<"alias_q1">>,
            <<"topic_filter">> => <<"t/alias/1">>
        })
    ),
    %% Get via /queue/:name
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"name">> := <<"alias_q1">>, <<"topic_filter">> := <<"t/alias/1">>}},
            api_get([queue, <<"alias_q1">>])
        )
    ),
    %% Update via /queue/:name
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"name">> := <<"alias_q1">>, <<"ping_interval">> := 8888}},
            api_put([queue, <<"alias_q1">>], #{<<"ping_interval">> => 8888})
        )
    ),
    %% Verify queue is visible via old /message_queues path too
    ?assertMatch(
        {ok, 200, #{<<"name">> := <<"alias_q1">>}},
        api_get([message_queues, queues, <<"alias_q1">>])
    ),
    %% Delete via /queue/:name
    ?assertMatch(
        {ok, 204},
        api_delete([queue, <<"alias_q1">>])
    ),
    %% Verify deleted
    ?assertMatch(
        {ok, 404, _},
        api_get([queue, <<"alias_q1">>])
    ).

%% Verify that /queues/config alias path works identically to /message_queues/config.
t_queues_alias_config(_Config) ->
    ?assertMatch(
        {ok, 200, _},
        api_get([queues, config])
    ),
    ?assertMatch(
        {ok, 204},
        api_put([queues, config], #{
            <<"gc_interval">> => <<"3h">>
        })
    ),
    ?assertMatch(
        {ok, 200, #{<<"gc_interval">> := <<"3h">>}},
        api_get([queues, config])
    ).

%% Check that full purge keeps the queue and MQTT connection and delivers messages published afterward.
t_purge_all(Config) ->
    %% Create the queue through HTTP and establish delivery before purging it.
    #{name := Name} = MQ0 = create_purge_queue(Config),
    Client = emqx_mq_test_utils:emqtt_connect([{reconnect, 0}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Client, Name),
    Old = emqx_message:make(<<"old">>, 1, <<"purge/1">>, <<"before">>),
    ok = emqx_mq_message_db:insert(MQ0, Old),
    {ok, [#{payload := <<"before">>}]} = emqx_mq_test_utils:emqtt_drain(1, 1000),

    %% Purge synchronously and verify that the queue keeps its identity and settings.
    StartMs = erlang:system_time(millisecond),
    ?assertEqual({ok, 204}, api_delete([queue, Name, messages])),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ0)),
    {ok, #{deliver_cutoff_date := CutoffMs} = MQ1} = emqx_mq_registry:find(Name),
    ?assert(CutoffMs >= StartMs),
    ?assert(CutoffMs =< erlang:system_time(millisecond)),
    ?assertEqual(MQ0#{deliver_cutoff_date => CutoffMs}, MQ1),
    ?assertMatch({ok, 200, #{<<"name">> := Name}}, api_get([queue, Name])),

    %% Keep the existing MQTT connection and subscription when publishing after purge.
    {ok, _} = emqx_mq_test_utils:emqtt_pub_mq(Client, <<"purge/1">>, <<"after">>),
    {ok, [#{payload := <<"after">>}]} = emqx_mq_test_utils:emqtt_drain(1, 2000),
    ?assertEqual(connected, emqtt:status(Client)),
    ?assertEqual(pong, emqtt:ping(Client)),
    ok = emqtt:disconnect(Client),
    ?assertEqual({ok, 204}, api_delete([queue, Name, messages])),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ0)).

%% Check that purge accepts epoch and RFC 3339 cutoffs and delivers only messages at or after the cutoff.
t_purge_before(Config) ->
    %% Store messages with timestamps on both sides of the delivery boundary.
    #{name := Name} = MQ = create_purge_queue(Config),
    BeforeMs = erlang:system_time(millisecond) - 60000,
    Old = emqx_message:make(<<"old">>, 1, <<"purge/1">>, <<"old">>),
    Boundary = emqx_message:make(<<"boundary">>, 1, <<"purge/1">>, <<"boundary">>),
    ok = emqx_mq_message_db:insert(MQ, Old#message{timestamp = BeforeMs - 1}),
    ok = emqx_mq_message_db:insert(MQ, Boundary#message{timestamp = BeforeMs}),
    Records = lists:sort(emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual(2, length(Records)),

    %% Validate query conversion, including zero and an encoded timezone offset.
    lists:foreach(
        fun(#{query := Query, expected := Expected}) ->
            ?assertEqual(
                {ok, 204}, api_delete([queue, Name, messages, "?before=" ++ urlencode(Query)])
            ),
            ?assertEqual({ok, MQ#{deliver_cutoff_date => Expected}}, emqx_mq_registry:find(Name))
        end,
        [
            #{query => <<"0">>, expected => 0},
            #{query => integer_to_binary(BeforeMs), expected => BeforeMs},
            #{
                query => calendar:system_time_to_rfc3339(BeforeMs, [
                    {unit, millisecond}, {offset, "Z"}
                ]),
                expected => BeforeMs
            },
            #{
                query => calendar:system_time_to_rfc3339(BeforeMs, [
                    {unit, millisecond}, {offset, "+01:00"}
                ]),
                expected => BeforeMs
            }
        ]
    ),
    %% Both DS timestamps follow the cutoff, so physical records remain.
    ?assertEqual(Records, lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    Client = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Client, Name),
    {ok, [#{payload := <<"boundary">>}]} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqtt:disconnect(Client).

%% Check that malformed and negative purge cutoffs return 400 without changing the queue.
t_purge_invalid_before(_Config) ->
    Name = <<"invalid_purge">>,
    ?assertMatch(
        {ok, 200, _}, api_post([queues], #{<<"name">> => Name, <<"topic_filter">> => <<"purge/#">>})
    ),
    {ok, MQ} = emqx_mq_registry:find(Name),
    lists:foreach(
        fun(Before) ->
            ?assertMatch(
                {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
                api_delete([queue, Name, messages, "?before=" ++ urlencode(Before)])
            )
        end,
        [<<>>, <<"-1">>, <<"none">>, <<"not-a-date">>, <<"1.5">>, <<"1969-12-31T23:59:59Z">>]
    ),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

%% Check that both purge modes return 404 for an unknown queue.
t_purge_not_found(_Config) ->
    ?assertMatch(
        {ok, 404, #{<<"code">> := <<"NOT_FOUND">>}},
        api_delete([queue, <<"missing">>, messages])
    ),
    ?assertMatch(
        {ok, 404, #{<<"code">> := <<"NOT_FOUND">>}},
        api_delete([queue, <<"missing">>, messages, "?before=0"])
    ).

%% Check that both purge modes return 503 on storage failure without changing the queue settings.
t_purge_storage_failure(_Config) ->
    Name = <<"failed_purge">>,
    ?assertMatch(
        {ok, 200, _}, api_post([queues], #{<<"name">> => Name, <<"topic_filter">> => <<"purge/#">>})
    ),
    {ok, MQ} = emqx_mq_registry:find(Name),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_mq_message_db) end),
    ok = meck:expect(emqx_mq_message_db, purge, fun(_) ->
        {error, {recoverable, leader_unavailable}}
    end),
    ok = meck:expect(emqx_mq_message_db, purge, fun(_, _) ->
        {error, {recoverable, leader_unavailable}}
    end),
    ?assertMatch(
        {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>}}, api_delete([queue, Name, messages])
    ),
    ?assertMatch(
        {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>}},
        api_delete([queue, Name, messages, "?before=0"])
    ),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

%% Check that purge returns 503 while the queue subsystem is stopped or starting.
t_purge_unavailable(_Config) ->
    ok = meck:new(emqx_mq_controller, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_mq_controller) end),
    lists:foreach(
        fun(Status) ->
            ok = meck:expect(emqx_mq_controller, status, fun() -> Status end),
            ?assertMatch(
                {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>}},
                api_delete([queue, <<"unavailable">>, messages])
            )
        end,
        [stopped, stopping, starting]
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

create_purge_queue(Config) ->
    IsLastvalue = ?config(is_lastvalue, Config),
    Limits = #{<<"max_shard_message_count">> => 100, <<"max_shard_message_bytes">> => 100000},
    Queue0 = #{
        <<"name">> => <<"purge">>,
        <<"topic_filter">> => <<"purge/#">>,
        <<"is_lastvalue">> => IsLastvalue,
        <<"ping_interval">> => 200
    },
    Queue1 =
        case IsLastvalue of
            true -> Queue0#{<<"key_expression">> => <<"message.from">>};
            false -> Queue0
        end,
    Queue2 =
        case ?config(is_limited, Config) of
            true -> Queue1#{<<"limits">> => Limits};
            false -> Queue1
        end,
    ?assertMatch({ok, 200, _}, api_post([queues], Queue2)),
    {ok, MQ} = emqx_mq_registry:find(<<"purge">>),
    MQ.

sort_by(Fun, List) ->
    lists:sort(fun(A, B) -> Fun(A) < Fun(B) end, List).
