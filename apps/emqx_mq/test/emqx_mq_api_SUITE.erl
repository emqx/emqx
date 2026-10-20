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
    ?assertMatch(
        {ok, 404, _},
        api_get([message_queues, queues, urlencode(<<"t/1">>)])
    ),
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/1/1">>, <<"ping_interval">> => 9999
        })
    ),
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
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
                        <<"topic_filter">> := <<"t/1/2">>,
                        <<"ping_interval">> := 9999,
                        <<"limits">> := #{
                            <<"max_shard_message_count">> := 1000,
                            <<"max_shard_message_bytes">> := 104857600
                        }
                    }
                ],
                sort_by(fun(#{<<"topic_filter">> := TopicFilter}) -> TopicFilter end, Data)
            )
        end
    ),
    ?assertMatch(
        {ok, 404, _},
        api_put([message_queues, queues, urlencode(<<"t/2">>)], #{<<"ping_interval">> => 10000})
    ),
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"topic_filter">> := <<"t/1/1">>, <<"ping_interval">> := 10000}},
            api_put([message_queues, queues, urlencode(<<"t/1/1">>)], #{
                <<"ping_interval">> => 10000
            })
        )
    ),
    ?assertMatch(
        {ok, 200, #{<<"topic_filter">> := <<"t/1/1">>, <<"ping_interval">> := 10000}},
        api_get([message_queues, queues, urlencode(<<"t/1/1">>)])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, urlencode(<<"t/1/1">>)])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, queues, urlencode(<<"t/1/2">>)])
    ),
    ?assertMatch(
        {ok, 404, _},
        api_delete([message_queues, queues, urlencode(<<"t/1/1">>)])
    ),
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
            api_get([message_queues, queues])
        )
    ).

%% Verify pagination logic of message queue listing.
t_pagination(_Config) ->
    %% Create 10 MQs and fetch them in batches of 6.
    lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            api_post([message_queues, queues], #{<<"topic_filter">> => <<"t/", IBin/binary>>})
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
        api_post([message_queues, queues], #{<<"topic_filter">> => <<"t/1">>})
    ),
    ok = meck:unload(emqx_ds).

%% Verify that regular queue cannot be created with key expression.
t_lastvalue_vs_regular(_Config) ->
    %% Cannot create a regular queue with key expression
    ?assertMatch(
        {ok, 400, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/1">>,
            <<"key_expression">> => <<"message.from">>,
            <<"is_lastvalue">> => false
        })
    ),

    %% Cannot update a regular queue to lastvalue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/1">>, <<"is_lastvalue">> => false
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, urlencode(<<"t/1">>)], #{<<"is_lastvalue">> => true})
    ),

    %% Key expression is not allowed to be updated for regular queues
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, urlencode(<<"t/1">>)], #{
            <<"key_expression">> => <<"message.from">>
        })
    ),

    %% Cannot update a lastvalue queue to regular
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/2">>, <<"is_lastvalue">> => true
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_queues, queues, urlencode(<<"t/2">>)], #{<<"is_lastvalue">> => false})
    ).

%% Verify that regular queue cannot be converted from limited to unlimited and vice versa.
t_limited_vs_unlimited(_Config) ->
    %% Cannot create a regular queue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/1">>, <<"is_lastvalue">> => false
        })
    ),

    %% Cannot update an unlimited regular queue to limited
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Regular queues cannot be updated from limited to unlimited and vice versa">>
        }},
        api_put([message_queues, queues, urlencode(<<"t/1">>)], #{
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
        api_put([message_queues, queues, urlencode(<<"t/2">>)], #{<<"is_lastvalue">> => false})
    ),

    %% Create an unlimited lastvalue queue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues, queues], #{
            <<"topic_filter">> => <<"t/3">>, <<"is_lastvalue">> => true
        })
    ),

    %% Successfully update an unlimited lastvalue queue to limited
    ?assertMatch(
        {ok, 200, _},
        api_put([message_queues, queues, urlencode(<<"t/3">>)], #{
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
        api_put([message_queues, queues, urlencode(<<"t/3">>)], #{
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
        api_post([message_queues, queues], #{<<"topic_filter">> => <<"t/#">>})
    ),
    %% Publish 10 messages to the queue
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-">>,
        n_keys => 10
    }),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
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
                api_post([message_queues, queues], #{<<"topic_filter">> => <<"t/", IBin/binary>>})
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
        api_post([message_queues, queues], #{<<"topic_filter">> => <<"t/6">>})
    ).

-doc """
A partial `PUT /configs?mode=merge` that omits `mq.auto_create` succeeds
and keeps the stored `auto_create` value.
""".
t_configs_merge_without_auto_create(_Config) ->
    ?assertMatch(
        {ok, _},
        put_configs_merge(<<"mq { regular_queue_retention_period = \"30d\" }">>)
    ),
    ?assertMatch(
        #{regular_queue_retention_period := 2592000000},
        emqx_config:get([mq])
    ),
    ?assertMatch(
        #{<<"regular">> := false, <<"lastvalue">> := false},
        emqx:get_raw_config([mq, auto_create])
    ),
    ?assertEqual(
        #{regular => false, lastvalue => false},
        emqx_config:get([mq, auto_create])
    ).

-doc """
A merge that submits `mq.auto_create` still runs the converter,
which fills the defaults of the submitted struct.
""".
t_configs_merge_with_auto_create(_Config) ->
    ?assertMatch(
        {ok, _},
        put_configs_merge(<<"mq { auto_create { regular { ping_interval = 9999 } } }">>)
    ),
    #{<<"regular">> := RawRegular, <<"lastvalue">> := false} =
        emqx:get_raw_config([mq, auto_create]),
    ?assertMatch(
        #{<<"ping_interval">> := 9999, <<"dispatch_strategy">> := _},
        RawRegular
    ),
    ?assertMatch(
        #{regular := #{ping_interval := 9999}, lastvalue := false},
        emqx_config:get([mq, auto_create])
    ).

-doc """
A merge of the complete `mq` root, as exported by `GET /configs`, still succeeds.
""".
t_configs_merge_complete_root(_Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs?key=mq"]),
    Headers = [{"accept", "text/plain"}, emqx_mgmt_api_test_util:auth_header_()],
    {ok, Hocon} = emqx_mgmt_api_test_util:request_api(get, Path, [], Headers),
    ?assertMatch({ok, #{<<"mq">> := #{<<"auto_create">> := _}}}, hocon:binary(Hocon)),
    ?assertMatch({ok, _}, put_configs_merge(Hocon)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

sort_by(Fun, List) ->
    lists:sort(fun(A, B) -> Fun(A) < Fun(B) end, List).

put_configs_merge(Hocon) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs?mode=merge"]),
    Headers = [emqx_mgmt_api_test_util:auth_header_()],
    case httpc:request(put, {Path, Headers, "text/plain", Hocon}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} -> {ok, Body};
        {ok, {{_, Code, _}, _, Body}} -> {error, Code, Body}
    end.
