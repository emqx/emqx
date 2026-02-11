%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_api_SUITE).

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
            {emqx,
                emqx_streams_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{
                        <<"streams_messages">> => #{
                            <<"n_shards">> => 1
                        }
                    }
                })},
            {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
            {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_streams_test_utils:reset_config().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify basic CRUD operations on message queues.
t_crud(_Config) ->
    %% Fetch all streams when there are no streams, expect an empty list and hasnext is false
    ?assertMatch(
        {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
        api_get([message_streams, streams])
    ),
    %% We may work with legacy streams, which had not passed name validation,
    %% so we allow any name in get requests.
    ?assertMatch(
        {ok, 404, _},
        api_get([message_streams, streams, urlencode(<<"invalid/stream/name">>)])
    ),
    %% Fetch a non-existent stream, expect 404
    ?assertMatch(
        {ok, 404, _},
        api_get([message_streams, streams, <<"unknown_stream">>])
    ),
    %% Create a stream
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"crud_11">>,
            <<"topic_filter">> => <<"t/1/1">>,
            <<"read_max_unacked">> => 1000
        })
    ),
    %% Create a capped stream
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"crud_12">>,
            <<"topic_filter">> => <<"t/1/2">>,
            <<"read_max_unacked">> => 9999,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),
    %% Verify that the streams are created
    ?retry(
        5,
        20,
        begin
            {ok, 200, #{
                <<"data">> := Data,
                <<"meta">> := #{<<"hasnext">> := false}
            }} =
                api_get([message_streams, streams]),
            ?assertMatch(
                [
                    #{
                        <<"name">> := <<"crud_11">>,
                        <<"topic_filter">> := <<"t/1/1">>,
                        <<"read_max_unacked">> := 1000,
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
                        <<"read_max_unacked">> := 9999,
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
    %% Try to update a non-existent stream, expect 404
    ?assertMatch(
        {ok, 404, _},
        api_put([message_streams, streams, <<"crud_2">>], #{<<"read_max_unacked">> => 10000})
    ),
    %% Update an existing stream
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{
                <<"name">> := <<"crud_11">>,
                <<"topic_filter">> := <<"t/1/1">>,
                <<"read_max_unacked">> := 10000
            }},
            api_put([message_streams, streams, <<"crud_11">>], #{
                <<"read_max_unacked">> => 10000
            })
        )
    ),
    %% Verify that the stream is updated
    ?assertMatch(
        {ok, 200, #{
            <<"name">> := <<"crud_11">>,
            <<"topic_filter">> := <<"t/1/1">>,
            <<"read_max_unacked">> := 10000
        }},
        api_get([message_streams, streams, <<"crud_11">>])
    ),
    %% Delete both streams
    ?assertMatch(
        {ok, 204},
        api_delete([message_streams, streams, <<"crud_11">>])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_streams, streams, <<"crud_12">>])
    ),
    %% Verify that the streams are deleted
    ?assertMatch(
        {ok, 404, _},
        api_delete([message_streams, streams, <<"crud_11">>])
    ),
    %% Verify that the streams are not listed
    ?retry(
        5,
        20,
        ?assertMatch(
            {ok, 200, #{<<"data">> := [], <<"meta">> := #{<<"hasnext">> := false}}},
            api_get([message_streams, streams])
        )
    ).

%% Verify basic CRUD operations on legacy message streams.
t_legacy_streams_crud(_Config) ->
    %% Cannot create a legacy stream with API
    ?assertMatch(
        {ok, 400, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"/t/1">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => true
        })
    ),

    %% Create a legacy stream directly in the database
    Stream0 = emqx_streams_test_utils:fill_stream_defaults(#{topic_filter => <<"t/#">>}),
    {ok, _} = emqx_streams_registry:create_pre_611_stream(Stream0),
    ?assertMatch(
        {ok, _},
        emqx_streams_registry:find(<<"/t/#">>)
    ),

    %% Find stream via API
    ?assertMatch(
        {ok, 200, _},
        api_get([message_streams, streams, urlencode(<<"/t/#">>)])
    ),

    %% Update stream via API
    ?assertMatch(
        {ok, 200, _},
        api_put([message_streams, streams, urlencode(<<"/t/#">>)], #{
            <<"is_lastvalue">> => false,
            <<"read_max_unacked">> => 10000
        })
    ),

    %% Delete stream via API
    ?assertMatch(
        {ok, 204},
        api_delete([message_streams, streams, urlencode(<<"/t/#">>)])
    ),

    ?assertEqual(not_found, emqx_streams_registry:find(<<"/t/#">>)).

%% Verify pagination logic of message stream listing.
t_pagination(_Config) ->
    %% Create 10 streams and fetch them in batches of 6.
    lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            api_post([message_streams, streams], #{
                <<"name">> => <<"pagination_", IBin/binary>>,
                <<"topic_filter">> => <<"t/", IBin/binary>>
            })
        end,
        lists:seq(1, 10)
    ),
    ct:sleep(100),
    {ok, 200, #{
        <<"data">> := Data0, <<"meta">> := #{<<"hasnext">> := true, <<"cursor">> := Cursor}
    }} =
        api_get([message_streams, streams, "?limit=6"]),
    ?assertEqual(6, length(Data0)),
    {ok, 200, #{<<"data">> := Data1, <<"meta">> := #{<<"hasnext">> := false}}} =
        api_get([message_streams, streams, "?limit=6&cursor=" ++ urlencode(Cursor)]),
    ?assertEqual(4, length(Data1)),

    %% Check that the last page does not have `hasnext
    {ok, 200, #{<<"data">> := Data2, <<"meta">> := #{<<"hasnext">> := false}}} =
        api_get([message_streams, streams, "?limit=4&cursor=" ++ urlencode(Cursor)]),
    ?assertEqual(4, length(Data2)),

    %% Check that we do not crash on invalid cursor
    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        api_get([message_streams, streams, "?limit=6&cursor=%10%13"])
    ),
    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        api_get([
            message_streams, streams, "?limit=6&cursor=" ++ urlencode(base64:encode(<<"{{{">>))
        ])
    ).

%% Verify streams subsystem (re)configuration via API.
t_config(_Config) ->
    %% Get the current config
    ?assertMatch(
        {ok, 200, _},
        api_get([message_streams, config])
    ),
    %% Fail to update the config with an invalid GC interval
    ?assertMatch(
        {ok, 400, _},
        api_put([message_streams, config], #{<<"gc_interval">> => <<"-10h">>})
    ),
    %% Update the config successfully
    ?assertMatch(
        {ok, 204},
        api_put([message_streams, config], #{
            <<"gc_interval">> => <<"2h">>,
            <<"regular_stream_retention_period">> => <<"14d">>
        })
    ),
    %% Verify that the config is updated
    ?assertMatch(
        {ok, 200, #{
            <<"gc_interval">> := <<"2h">>,
            <<"regular_stream_retention_period">> := <<"14d">>
        }},
        api_get([message_streams, config])
    ),
    %% Successfully enable auto-creation of lastvalue streams
    ?assertMatch(
        {ok, 204},
        api_put([message_streams, config], #{
            <<"auto_create">> => #{
                <<"regular">> => false,
                <<"lastvalue">> => #{}
            }
        })
    ),
    %% Verify that the auto-creation of lastvalue streams is enabled
    ?assertMatch(
        {ok, 200, #{
            <<"auto_create">> := #{
                <<"regular">> := false,
                <<"lastvalue">> := #{
                    <<"key_expression">> := <<"message.from">>
                }
            }
        }},
        api_get([message_streams, config])
    ),
    %% Fail to enable auto-creation of both regular and lastvalue streams
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Streams should be configured to be automatically created either as regular or lastvalue">>
        }},
        api_put([message_streams, config], #{
            <<"auto_create">> => #{
                <<"regular">> => #{}, <<"lastvalue">> => #{}
            }
        })
    ).

%% Verify is_lastvalue change limitations.
t_lastvalue_vs_regular(_Config) ->
    %% Cannot update a regular stream to lastvalue
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"regular1">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => false
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_streams, streams, <<"regular1">>], #{<<"is_lastvalue">> => true})
    ),

    %% Cannot update a lastvalue stream to regular
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"lastvalue1">>,
            <<"topic_filter">> => <<"t/2">>,
            <<"is_lastvalue">> => true
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        api_put([message_streams, streams, <<"lastvalue1">>], #{<<"is_lastvalue">> => false})
    ).

%% Verify that regular stream cannot be converted from limited to unlimited and vice versa.
t_limited_vs_unlimited(_Config) ->
    %% Cannot create a regular stream
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"regular">>,
            <<"topic_filter">> => <<"t/1">>,
            <<"is_lastvalue">> => false
        })
    ),

    %% Cannot update an unlimited regular stream to limited
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Regular streams cannot be updated from limited to unlimited and vice versa">>
        }},
        api_put([message_streams, streams, <<"regular">>], #{
            <<"is_lastvalue">> => false,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Create a limited regular stream
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"regular_limited">>,
            <<"topic_filter">> => <<"t/2">>,
            <<"is_lastvalue">> => false,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Cannot update a limited regular stream to unlimited
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Regular streams cannot be updated from limited to unlimited and vice versa">>
        }},
        api_put([message_streams, streams, <<"regular_limited">>], #{<<"is_lastvalue">> => false})
    ),

    %% Create an unlimited lastvalue stream
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
            <<"name">> => <<"lastvalue">>,
            <<"topic_filter">> => <<"t/3">>,
            <<"is_lastvalue">> => true
        })
    ),

    %% Successfully update an unlimited lastvalue stream to limited
    ?assertMatch(
        {ok, 200, _},
        api_put([message_streams, streams, <<"lastvalue">>], #{
            <<"is_lastvalue">> => true,
            <<"limits">> => #{
                <<"max_shard_message_count">> => 1000,
                <<"max_shard_message_bytes">> => <<"100MB">>
            }
        })
    ),

    %% Successfully remove limits back from a limited lastvalue stream
    ?assertMatch(
        {ok, 200, _},
        api_put([message_streams, streams, <<"lastvalue">>], #{
            <<"is_lastvalue">> => true,
            <<"limits">> => #{
                <<"max_shard_message_count">> => <<"infinity">>,
                <<"max_shard_message_bytes">> => <<"infinity">>
            }
        })
    ).

%% Verify that default values are good enough for lastvalue streams
t_defaults(_Config) ->
    ?assertMatch(
        {ok, 200, _},
        api_post([message_streams, streams], #{
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
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/defaults">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 5000),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages. Default key expression is clientid, so we should receive only one message.
    ?assertEqual(1, length(Msgs)).

%% Verify that the max stream count is respected
t_max_stream_count(_Config) ->
    emqx_config:put([streams, max_stream_count], 5),
    %% Create 5 streams to fill the limit
    lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            ?assertMatch(
                {ok, 200, _},
                api_post([message_streams, streams], #{
                    <<"name">> => <<"max_", IBin/binary>>,
                    <<"topic_filter">> => <<"t/", IBin/binary>>
                })
            )
        end,
        lists:seq(1, 5)
    ),
    %% Try to create a 6th stream, expect an error
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"MAX_STREAM_COUNT_REACHED">>,
            <<"message">> := <<"Max stream count reached">>
        }},
        api_post([message_streams, streams], #{
            <<"name">> => <<"max_6">>, <<"topic_filter">> => <<"t/6">>
        })
    ).

% %%--------------------------------------------------------------------
% %% Internal functions
% %%--------------------------------------------------------------------

sort_by(Fun, List) ->
    lists:sort(fun(A, B) -> Fun(A) < Fun(B) end, List).
