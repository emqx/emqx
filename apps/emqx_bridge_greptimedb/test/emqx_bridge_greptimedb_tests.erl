%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_tests).

-include_lib("eunit/include/eunit.hrl").

check_action(InnerConfigMap) ->
    check_action(<<"greptimedb">>, InnerConfigMap).

check_action(ActionType, InnerConfigMap) ->
    Conf = #{
        <<"actions">> => #{
            ActionType => #{
                <<"test_action">> => InnerConfigMap
            }
        }
    },
    hocon_tconf:check_plain(emqx_bridge_v2_schema, Conf, #{
        required => false,
        atom_key => false,
        make_serializable => false
    }).

structured_action_config() ->
    #{
        <<"connector">> => <<"greptimedb_connector">>,
        <<"parameters">> => #{
            <<"write">> => [structured_point()]
        }
    }.

structured_point() ->
    #{
        <<"measurement">> => <<"${topic}">>,
        <<"fields">> => [
            #{
                <<"key">> => <<"temperature">>,
                <<"type">> => <<"float">>,
                <<"value">> => <<"${payload.temperature}">>
            }
        ],
        <<"timestamp">> => #{
            <<"value">> => <<"${payload.ts}">>,
            <<"source_precision">> => <<"ms">>
        }
    }.

structured_action_schema_test() ->
    ?assertMatch(
        #{
            <<"actions">> := #{
                <<"greptimedb">> := #{
                    <<"test_action">> := #{
                        <<"parameters">> := #{
                            <<"write">> := [
                                #{
                                    <<"timestamp">> := #{
                                        <<"source_precision">> := ms
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        },
        check_action(structured_action_config())
    ).

emqx_tables_structured_action_schema_test() ->
    ?assertMatch(
        #{
            <<"actions">> := #{
                <<"emqx_tables">> := #{
                    <<"test_action">> := #{
                        <<"parameters">> := #{
                            <<"write">> := [_]
                        }
                    }
                }
            }
        },
        check_action(<<"emqx_tables">>, structured_action_config())
    ).

mixed_structured_template_is_rejected_test_() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    Mixed = Conf#{
        <<"parameters">> => #{
            <<"write">> => [Point0#{<<"measurement">> => <<"mqtt-${topic}">>}]
        }
    },
    [
        ?_assertThrow({emqx_bridge_v2_schema, _}, check_action(Mixed)),
        ?_assertThrow({emqx_bridge_v2_schema, _}, check_action(<<"emqx_tables">>, Mixed))
    ].

action_requires_write_config_test() ->
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(#{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{}
        })
    ).

raw_line_write_syntax_is_not_supported_test() ->
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(#{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{
                <<"write_syntax">> => <<"${payload.line}">>
            }
        })
    ).

emqx_tables_requires_write_config_test() ->
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(<<"emqx_tables">>, #{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{}
        })
    ).

legacy_write_syntax_api_compatibility_test() ->
    ?assertMatch(
        #{
            <<"actions">> := #{
                <<"greptimedb">> := #{
                    <<"test_action">> := #{
                        <<"parameters">> := #{
                            <<"write_syntax">> := [_]
                        }
                    }
                }
            }
        },
        check_action(#{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{
                <<"write_syntax">> => <<"m field=1">>
            }
        })
    ).

emqx_tables_legacy_write_syntax_api_compatibility_test() ->
    ?assertMatch(
        #{
            <<"actions">> := #{
                <<"emqx_tables">> := #{
                    <<"test_action">> := #{
                        <<"parameters">> := #{
                            <<"write_syntax">> := [_]
                        }
                    }
                }
            }
        },
        check_action(<<"emqx_tables">>, #{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{
                <<"write_syntax">> => <<"m field=1">>
            }
        })
    ).

connectors_parse_structured_action_parameters_test() ->
    Write = [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    ChannelConfig = #{
        parameters => #{
            precision => ms,
            write => Write
        }
    },
    {ok, #{channels := #{channel := GreptimeChannel}}} =
        emqx_bridge_greptimedb_connector:on_add_channel(
            connector, #{channels => #{}}, channel, ChannelConfig
        ),
    {ok, #{installed_channels := #{channel := NativeChannel}}} =
        emqx_bridge_greptimedb_rs_impl:on_add_channel(
            connector, #{installed_channels => #{}}, channel, ChannelConfig
        ),
    ?assertMatch(
        #{write_syntax := [#{measurement := {structured, _}}]},
        GreptimeChannel
    ),
    ?assertMatch(
        #{write_syntax := [#{measurement := {structured, _}}]},
        NativeChannel
    ).

write_config_forms_are_mutually_exclusive_test() ->
    Conf = structured_action_config(),
    Parameters0 = maps:get(<<"parameters">>, Conf),
    Parameters = Parameters0#{<<"write_syntax">> => <<"m field=1">>},
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(Conf#{<<"parameters">> => Parameters})
    ).

uint_field_type_is_not_supported_test() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    [Field0] = maps:get(<<"fields">>, Point0),
    Field = Field0#{<<"type">> => <<"uint">>},
    Point = Point0#{<<"fields">> => [Field]},
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(Conf#{
            <<"parameters">> => #{
                <<"write">> => [Point]
            }
        })
    ).

source_precision_enum_test() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    Timestamp0 = maps:get(<<"timestamp">>, Point0),
    Timestamp = Timestamp0#{<<"source_precision">> => <<"minutes">>},
    Point = Point0#{<<"timestamp">> => Timestamp},
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(Conf#{
            <<"parameters">> => #{
                <<"write">> => [Point]
            }
        })
    ).

source_precision_is_required_test() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    Timestamp0 = maps:get(<<"timestamp">>, Point0),
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(Conf#{
            <<"parameters">> => #{
                <<"write">> => [
                    Point0#{<<"timestamp">> => maps:remove(<<"source_precision">>, Timestamp0)}
                ]
            }
        })
    ).

timestamp_false_schema_test() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    ?assertMatch(
        #{
            <<"actions">> := #{
                <<"greptimedb">> := #{
                    <<"test_action">> := #{
                        <<"parameters">> := #{
                            <<"write">> := [#{<<"timestamp">> := false}]
                        }
                    }
                }
            }
        },
        check_action(Conf#{
            <<"parameters">> => #{
                <<"write">> => [Point0#{<<"timestamp">> => false}]
            }
        })
    ).

write_points_must_not_be_empty_test() ->
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(#{
            <<"connector">> => <<"greptimedb_connector">>,
            <<"parameters">> => #{<<"write">> => []}
        })
    ).

write_fields_must_not_be_empty_test() ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    ?assertThrow(
        {emqx_bridge_v2_schema, _},
        check_action(Conf#{
            <<"parameters">> => #{
                <<"write">> => [Point0#{<<"fields">> => []}]
            }
        })
    ).

duplicate_static_keys_are_rejected_test_() ->
    [
        ?_assertThrow(
            {emqx_bridge_v2_schema, _},
            check_duplicate_static_keys(tags)
        ),
        ?_assertThrow(
            {emqx_bridge_v2_schema, _},
            check_duplicate_static_keys(fields)
        )
    ].

check_duplicate_static_keys(Key) ->
    Conf = structured_action_config(),
    [Point0] = maps:get(<<"write">>, maps:get(<<"parameters">>, Conf)),
    Entry = duplicate_entry(Key),
    Point = Point0#{Key => [Entry, Entry]},
    check_action(Conf#{
        <<"parameters">> => #{
            <<"write">> => [Point]
        }
    }).

duplicate_entry(tags) ->
    #{<<"key">> => <<"clientid">>, <<"value">> => <<"${clientid}">>};
duplicate_entry(fields) ->
    #{
        <<"key">> => <<"value">>,
        <<"type">> => <<"integer">>,
        <<"value">> => <<"1">>
    }.

structured_write_rendering_test_() ->
    [
        {"Erlang driver receives typed GreptimeDB values",
            ?_assertEqual(
                expected_erlang_points(),
                render_structured_write(erlang)
            )},
        {"Native driver receives primitive values",
            ?_assertEqual(
                expected_native_points(),
                render_structured_write(native)
            )}
    ].

native_single_query_aggregates_points_test() ->
    Write = native_test_write(<<"mqtt">>, 2),
    State = native_rs_state(Write),
    Insert = fun(_Client, <<"mqtt">>, Points) ->
        ?assertEqual(2, length(Points)),
        {ok, 2}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {ok, 2},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{}}, State
            )
        )
    end).

native_single_query_keeps_cross_table_error_test() ->
    Write = [
        native_test_point(<<"ok_table">>, 1),
        native_test_point(<<"bad_table">>, 2)
    ],
    State = native_rs_state(Write),
    Insert = fun
        (_Client, <<"ok_table">>, _Points) -> {ok, 1};
        (_Client, <<"bad_table">>, _Points) -> {error, ecpool_empty}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertMatch(
            {error,
                {unrecoverable_error, #{
                    reason := multiline_partial_write,
                    successful_rows := 1
                }}},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{}}, State
            )
        )
    end).

native_single_query_sums_cross_table_point_counts_test() ->
    Write = [
        native_test_point(<<"first">>, 1),
        native_test_point(<<"second">>, 2)
    ],
    State = native_rs_state(Write),
    Insert = fun(_Client, _Table, _Points) -> {ok, 1} end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {ok, 2},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{}}, State
            )
        )
    end).

native_single_query_uses_millisecond_timestamp_test() ->
    Write = [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ],
    State = native_rs_state(Write, ns),
    Insert = fun(_Client, <<"mqtt">>, [Point]) ->
        ?assertEqual(1700000000123, maps:get(timestamp, Point)),
        {ok, 1}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {ok, 1},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{payload => #{ts => 1700000000123}}}, State
            )
        )
    end).

native_batch_query_returns_one_result_per_message_test() ->
    Write = native_test_write(<<"${payload.table}">>, 2),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}}
    ],
    Insert = fun(_Client, Table, Points) ->
        ?assertEqual(2, length(Points)),
        {ok, Table}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            [{ok, 2}, {ok, 2}],
            emqx_bridge_greptimedb_rs_impl:on_batch_query(connector, Batch, State)
        )
    end).

native_batch_query_counts_points_per_message_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}}
    ],
    Insert = fun(_Client, <<"mqtt">>, Points) ->
        ?assertEqual(2, length(Points)),
        {ok, 2}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            [{ok, 1}, {ok, 1}],
            emqx_bridge_greptimedb_rs_impl:on_batch_query(connector, Batch, State)
        )
    end).

native_batch_with_partial_success_is_unrecoverable_test() ->
    Write = native_test_write(<<"${payload.table}">>, 1),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}},
        {channel, #{payload => #{table => <<"third">>}}}
    ],
    Insert = fun
        (_Client, <<"first">>, _Points) -> {ok, 1};
        (_Client, _Table, _Points) -> {error, ecpool_empty}
    end,
    with_rs_insert_mock(Insert, fun() ->
        Results = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector, Batch, State
        ),
        [First, Second, Third] = Results,
        ?assertEqual({ok, 1}, First),
        assert_unrecoverable(Second, partial_batch_write),
        assert_unrecoverable(Third, partial_batch_write)
    end).

native_batch_with_multiline_partial_success_is_unrecoverable_test() ->
    Write = [
        native_test_point(<<"${payload.first_table}">>, 1),
        native_test_point(<<"${payload.second_table}">>, 2)
    ],
    State = native_rs_state(Write),
    Batch = [
        {channel, #{
            payload => #{
                first_table => <<"ok">>,
                second_table => <<"partial">>
            }
        }},
        {channel, #{
            payload => #{
                first_table => <<"retry_a">>,
                second_table => <<"retry_b">>
            }
        }}
    ],
    Insert = fun
        (_Client, <<"ok">>, _Points) -> {ok, 1};
        (_Client, _Table, _Points) -> {error, ecpool_empty}
    end,
    with_rs_insert_mock(Insert, fun() ->
        [First, Second] = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector, Batch, State
        ),
        assert_unrecoverable(First, multiline_partial_write),
        assert_unrecoverable(Second, partial_batch_write)
    end).

native_batch_without_success_and_permanent_error_is_unrecoverable_test() ->
    Write = [
        #{
            measurement => <<"${payload.table}">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ],
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"valid">>, ts => 1}}},
        {channel, #{payload => #{table => <<"invalid">>, ts => <<"bad">>}}}
    ],
    Insert = fun(_Client, <<"valid">>, _Points) -> {error, ecpool_empty} end,
    with_rs_insert_mock(Insert, fun() ->
        Results = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector, Batch, State
        ),
        [First, Second] = Results,
        assert_unrecoverable(First, batch_contains_unrecoverable),
        ?assertMatch({error, {unrecoverable_error, _}}, Second)
    end).

native_batch_with_success_and_permanent_error_keeps_success_test() ->
    Write = [
        #{
            measurement => <<"${payload.table}">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ],
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"valid">>, ts => 1}}},
        {channel, #{payload => #{table => <<"invalid">>, ts => <<"bad">>}}}
    ],
    Insert = fun(_Client, <<"valid">>, _Points) -> {ok, 1} end,
    with_rs_insert_mock(Insert, fun() ->
        Results = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector, Batch, State
        ),
        [First, Second] = Results,
        ?assertEqual({ok, 1}, First),
        ?assertMatch({error, {unrecoverable_error, _}}, Second)
    end).

native_batch_with_only_transient_errors_is_recoverable_test() ->
    Write = native_test_write(<<"${payload.table}">>, 1),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}}
    ],
    Insert = fun(_Client, _Table, _Points) -> {error, ecpool_empty} end,
    with_rs_insert_mock(Insert, fun() ->
        Results = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector, Batch, State
        ),
        assert_all_recoverable(Results, mixed_batch_failure)
    end).

native_nif_error_is_unrecoverable_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Insert = fun(_Client, <<"mqtt">>, _Points) ->
        {error, {nif_error, bad_schema}}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {error, {unrecoverable_error, bad_schema}},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{}}, State
            )
        )
    end).

native_timeout_is_recoverable_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Insert = fun(_Client, <<"mqtt">>, _Points) ->
        {error, timeout}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {error, {recoverable_error, timeout}},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector, {channel, #{}}, State
            )
        )
    end).

native_async_single_query_aggregates_points_test() ->
    Write = native_test_write(<<"mqtt">>, 2),
    State = native_rs_state(Write),
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, Points, Callback) ->
        ?assertEqual(2, length(Points)),
        ok = invoke_rs_callback(Callback, {ok, 2}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector, {channel, #{}}, FinalReply, State
            )
        ),
        receive
            {native_async_result, Result} ->
                ?assertEqual({ok, 2}, Result)
        after 1_000 ->
            error(native_async_result_timeout)
        end
    end).

native_async_single_query_sums_cross_table_points_test() ->
    Write = [
        native_test_point(<<"first">>, 1),
        native_test_point(<<"second">>, 2)
    ],
    State = native_rs_state(Write),
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_cross_table_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, _Table, Points, Callback) ->
        ?assertEqual(1, length(Points)),
        _ = invoke_rs_callback(Callback, {ok, 1}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector, {channel, #{}}, FinalReply, State
            )
        ),
        receive
            {native_async_cross_table_result, Result} ->
                ?assertEqual({ok, 2}, Result)
        after 1_000 ->
            error(native_async_cross_table_result_timeout)
        end
    end).

native_async_submission_error_replies_and_returns_ok_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_submission_error, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, Points, _Callback) ->
        ?assertEqual(1, length(Points)),
        {error, ecpool_empty}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertEqual(
            ok,
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector, {channel, #{}}, FinalReply, State
            )
        ),
        receive
            {native_async_submission_error, Result} ->
                ?assertEqual({error, {recoverable_error, ecpool_empty}}, Result)
        after 1_000 ->
            error(native_async_submission_error_timeout)
        end
    end).

native_async_batch_query_returns_one_result_per_message_test() ->
    Write = native_test_write(<<"${payload.table}">>, 2),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}}
    ],
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_batch_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, Table, Points, Callback) ->
        ?assertEqual(2, length(Points)),
        _ = invoke_rs_callback(Callback, {ok, Table}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_batch_query_async(
                connector, Batch, FinalReply, State
            )
        ),
        receive
            {native_async_batch_result, Result} ->
                ?assertEqual(
                    [{ok, 2}, {ok, 2}],
                    Result
                )
        after 1_000 ->
            error(native_async_batch_result_timeout)
        end
    end).

native_async_batch_query_counts_points_per_message_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}}
    ],
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_shared_table_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, Points, Callback) ->
        ?assertEqual(2, length(Points)),
        ok = invoke_rs_callback(Callback, {ok, 2}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_batch_query_async(
                connector, Batch, FinalReply, State
            )
        ),
        receive
            {native_async_shared_table_result, Result} ->
                ?assertEqual([{ok, 1}, {ok, 1}], Result)
        after 1_000 ->
            error(native_async_shared_table_result_timeout)
        end
    end).

native_async_later_table_submission_error_completes_batch_test() ->
    Write = native_test_write(<<"${payload.table}">>, 1),
    State = native_rs_state(Write),
    Batch = [
        {channel, #{payload => #{table => <<"first">>}}},
        {channel, #{payload => #{table => <<"second">>}}},
        {channel, #{payload => #{table => <<"third">>}}}
    ],
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_submit_error, Result} end,
        []
    },
    InsertAsync = fun
        (_Client, <<"first">>, _Points, Callback) ->
            _ = invoke_rs_callback(Callback, {ok, 1}),
            {ok, self()};
        (_Client, <<"second">>, _Points, _Callback) ->
            {error, ecpool_empty};
        (_Client, <<"third">>, _Points, _Callback) ->
            error(unexpected_third_submission)
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_batch_query_async(
                connector, Batch, FinalReply, State
            )
        ),
        receive
            {native_async_submit_error, Result} ->
                [First, Second, Third] = Result,
                ?assertEqual({ok, 1}, First),
                assert_unrecoverable(Second, partial_batch_write),
                assert_unrecoverable(Third, partial_batch_write)
        after 1_000 ->
            error(native_async_submit_error_timeout)
        end
    end).

native_async_transient_error_is_recoverable_test() ->
    Write = native_test_write(<<"mqtt">>, 1),
    State = native_rs_state(Write),
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {native_async_recoverable_error, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, _Points, Callback) ->
        ok = invoke_rs_callback(Callback, {error, disconnected}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector, {channel, #{}}, FinalReply, State
            )
        ),
        receive
            {native_async_recoverable_error, Result} ->
                ?assertEqual({error, {recoverable_error, disconnected}}, Result)
        after 1_000 ->
            error(native_async_recoverable_error_timeout)
        end
    end).

legacy_async_reply_context_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_async_context_result, Result} end,
        []
    },
    Context = #{
        iter => maps:iterator(#{}),
        last_indices => [1],
        final_cont => FinalReply,
        final_res => [],
        client => test_client,
        is_batch => false
    },
    emqx_bridge_greptimedb_rs_impl:reply_callback(Context, {ok, 1}),
    receive
        {legacy_async_context_result, Result} ->
            ?assertEqual({ok, 1}, Result)
    after 1_000 ->
        error(legacy_async_context_timeout)
    end.

legacy_batch_async_reply_context_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_batch_async_context_result, Result} end,
        []
    },
    Context = #{
        iter => maps:iterator(#{}),
        last_indices => [2],
        final_cont => FinalReply,
        final_res => [{1, {ok, 1}}],
        client => test_client,
        is_batch => true
    },
    emqx_bridge_greptimedb_rs_impl:reply_callback(Context, {ok, 1}),
    receive
        {legacy_batch_async_context_result, Result} ->
            ?assertEqual([{ok, 1}, {ok, 1}], Result)
    after 1_000 ->
        error(legacy_batch_async_context_timeout)
    end.

legacy_async_reply_context_with_remaining_table_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_remaining_table_result, Result} end,
        []
    },
    Iter = maps:iterator(#{
        <<"next">> => [{2, #{value_data => {i64_value, 1}}}]
    }),
    Context = #{
        iter => Iter,
        last_indices => [1],
        final_cont => FinalReply,
        final_res => [],
        client => test_client,
        is_batch => true
    },
    InsertAsync = fun(_Client, <<"next">>, _Points, Callback) ->
        ok = invoke_rs_callback(Callback, {ok, 1}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:reply_callback(Context, {ok, 1})
        ),
        receive
            {legacy_remaining_table_result, Result} ->
                ?assertEqual([{ok, 1}, {ok, 1}], Result)
        after 1_000 ->
            error(legacy_remaining_table_timeout)
        end
    end).

intermediate_async_reply_context_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {intermediate_async_context_result, Result} end,
        []
    },
    Context = #{
        iter => maps:iterator(#{}),
        last_counts => #{1 => 2},
        final_cont => FinalReply,
        final_res => [{1, {error, {recoverable_error, ecpool_empty}}}],
        client => test_client
    },
    emqx_bridge_greptimedb_rs_impl:reply_callback(Context, {ok, 1}),
    receive
        {intermediate_async_context_result, Result} ->
            ?assertMatch(
                {error,
                    {unrecoverable_error, #{
                        reason := multiline_partial_write,
                        successful_rows := 2
                    }}},
                Result
            )
    after 1_000 ->
        error(intermediate_async_context_timeout)
    end.

legacy_async_pair_results_count_each_old_table_point_once_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_pair_counts_result, Result} end,
        []
    },
    Context = #{
        iter => maps:iterator(#{}),
        last_counts => #{1 => 2, 2 => 2},
        final_cont => FinalReply,
        final_res => [{1, {ok, 1}}, {2, {ok, 1}}],
        client => test_client,
        batch_size => 2
    },
    emqx_bridge_greptimedb_rs_impl:reply_callback(Context, {ok, 1}),
    receive
        {legacy_pair_counts_result, Result} ->
            ?assertEqual([{ok, 3}, {ok, 3}], Result)
    after 1_000 ->
        error(legacy_pair_counts_timeout)
    end.

assert_unrecoverable(Result, Reason) ->
    ?assertMatch(
        {error, {unrecoverable_error, #{reason := Reason}}},
        Result
    ).

assert_all_recoverable(Results, _Reason) ->
    ?assert(is_list(Results)),
    ?assert(Results =/= []),
    lists:foreach(
        fun
            ({error, {recoverable_error, _}}) ->
                ok;
            (Other) ->
                error({unexpected_recoverable_batch_result, Other})
        end,
        Results
    ).

native_test_write(Measurement, PointCount) ->
    [native_test_point(Measurement, I) || I <- lists:seq(1, PointCount)].

native_test_point(Measurement, Value) ->
    #{
        measurement => Measurement,
        fields => [
            #{key => <<"value">>, type => integer, value => integer_to_binary(Value)}
        ],
        timestamp => false
    }.

native_rs_state(Write) ->
    native_rs_state(Write, ms).

native_rs_state(Write, Precision) ->
    WriteSyntax = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write => Write}, Precision
    ),
    #{
        database => <<"mydb">>,
        client => test_client,
        installed_channels => #{
            channel => #{
                write_syntax => WriteSyntax,
                precision => ms
            }
        }
    }.

with_rs_insert_mock(Insert, TestFun) ->
    ok = meck:new(greptimedb_rs, [passthrough, no_link]),
    try
        ok = meck:expect(greptimedb_rs, insert, Insert),
        TestFun()
    after
        ok = meck:unload(greptimedb_rs)
    end.

with_rs_insert_async_mock(InsertAsync, TestFun) ->
    ok = meck:new(greptimedb_rs, [passthrough, no_link]),
    try
        ok = meck:expect(greptimedb_rs, insert_async, InsertAsync),
        TestFun()
    after
        ok = meck:unload(greptimedb_rs)
    end.

invoke_rs_callback({ReplyFun, Args}, Result) ->
    apply(ReplyFun, Args ++ [Result]).

render_structured_write(DriverType) ->
    Write = [
        #{
            measurement => <<"${payload.table}">>,
            tags => [
                #{key => <<"clientid">>, value => <<"${clientid}">>}
            ],
            fields => [
                #{key => <<"temperature">>, type => float, value => <<"${payload.temperature}">>},
                #{key => <<"count">>, type => integer, value => <<"${payload.count}">>},
                #{key => <<"active">>, type => boolean, value => <<"${payload.active}">>},
                #{key => <<"label">>, type => string, value => <<"${payload.label}">>},
                #{key => <<"automatic">>, type => auto, value => <<"${payload.count}">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ],
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write => Write}, ns
    ),
    Data = #{
        clientid => <<"client-1">>,
        payload => #{
            table => <<"sensors">>,
            temperature => 21.5,
            count => 7,
            active => true,
            label => <<"ok">>,
            ts => 1700000000123
        }
    },
    {ok, Points} = emqx_bridge_greptimedb_utils:data_to_points(
        Data, <<"mydb">>, SyntaxLines, DriverType
    ),
    Points.

expected_erlang_points() ->
    [
        {
            #{dbname => <<"mydb">>, table => <<"sensors">>, timeunit => ns},
            [
                #{
                    tags => #{
                        <<"clientid">> => #{value_data => {string_value, <<"client-1">>}}
                    },
                    fields => #{
                        <<"temperature">> => #{value_data => {f64_value, 21.5}},
                        <<"count">> => #{value_data => {i64_value, 7}},
                        <<"active">> => #{value_data => {bool_value, true}},
                        <<"label">> => #{value_data => {string_value, <<"ok">>}},
                        <<"automatic">> => #{value_data => {f64_value, 7.0}}
                    },
                    timestamp => 1700000000123000000
                }
            ]
        }
    ].

expected_native_points() ->
    [
        {
            #{dbname => <<"mydb">>, table => <<"sensors">>, timeunit => ms},
            [
                #{
                    tags => #{<<"clientid">> => <<"client-1">>},
                    fields => #{
                        <<"temperature">> => 21.5,
                        <<"count">> => 7,
                        <<"active">> => true,
                        <<"label">> => <<"ok">>,
                        <<"automatic">> => 7.0
                    },
                    timestamp => 1700000000123
                }
            ]
        }
    ].

multiple_structured_points_preserve_order_test_() ->
    Write = [
        simple_point(<<"first">>, <<"${payload.first}">>),
        simple_point(<<"second">>, <<"${payload.second}">>)
    ],
    Data = #{payload => #{first => 1, second => 2}},
    [
        ?_assertMatch(
            [
                {#{table := <<"first">>}, [#{fields := #{}}]},
                {#{table := <<"second">>}, [#{fields := #{}}]}
            ],
            render_points(erlang, Write, Data)
        ),
        ?_assertMatch(
            [
                {#{table := <<"first">>}, [#{fields := #{}}]},
                {#{table := <<"second">>}, [#{fields := #{}}]}
            ],
            render_points(native, Write, Data)
        )
    ].

simple_point(Measurement, Value) ->
    #{
        measurement => Measurement,
        fields => [
            #{key => <<"value">>, type => integer, value => Value}
        ],
        timestamp => false
    }.

render_points(DriverType, Write, Data) ->
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write => Write}, ns
    ),
    {ok, Points} = emqx_bridge_greptimedb_utils:data_to_points(
        Data, <<"mydb">>, SyntaxLines, DriverType
    ),
    Points.

timestamp_false_uses_current_time_test_() ->
    [
        ?_test(assert_timestamp_false_uses_current_time(erlang)),
        ?_test(assert_timestamp_false_uses_current_time(native))
    ].

assert_timestamp_false_uses_current_time(DriverType) ->
    {ExpectedUnit, MinimumTimestamp} =
        case DriverType of
            erlang -> {ns, 1_000_000_000_000_000_000};
            native -> {ms, 1_000_000_000_000}
        end,
    [{#{timeunit := ExpectedUnit}, [Point]}] =
        render_structured_write_without_timestamp(DriverType),
    Timestamp = maps:get(timestamp, Point),
    ?assert(is_integer(Timestamp)),
    ?assert(Timestamp > MinimumTimestamp),
    ?assertEqual(
        expected_fields(DriverType),
        maps:get(fields, Point)
    ).

expected_fields(erlang) ->
    #{<<"value">> => #{value_data => {i64_value, 1}}};
expected_fields(native) ->
    #{<<"value">> => 1}.

render_structured_write_without_timestamp(DriverType) ->
    Write = [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => false
        }
    ],
    render_points(DriverType, Write, #{timestamp => 1}).

source_precision_conversion_test_() ->
    [
        ?_assertMatch(
            [{#{timeunit := ns}, [#{timestamp := 1700000000123000000}]}],
            render_timestamp(ms, ns)
        ),
        ?_assertMatch(
            [{#{timeunit := ms}, [#{timestamp := 1700000000123}]}],
            render_timestamp(ms, ms)
        ),
        ?_assertMatch(
            [{#{timeunit := us}, [#{timestamp := 1700000000123000}]}],
            render_timestamp(ms, us)
        )
    ].

omitted_timestamp_uses_message_timestamp_test_() ->
    Write = [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ]
        }
    ],
    Data = #{timestamp => 1700000000123},
    [
        ?_assertMatch(
            [{#{timeunit := ns}, [#{timestamp := 1700000000123000000}]}],
            render_points(erlang, Write, Data)
        ),
        ?_assertMatch(
            [{#{timeunit := ms}, [#{timestamp := 1700000000123}]}],
            render_points(native, Write, Data)
        )
    ].

native_timestamps_are_always_milliseconds_test_() ->
    [
        ?_assertMatch(
            [{#{timeunit := ms}, [#{timestamp := ExpectedTimestamp}]}],
            render_timestamp(native, ms, Precision)
        )
     || {Precision, ExpectedTimestamp} <- [
            {s, 1700000000000},
            {ms, 1700000000123},
            {us, 1700000000123},
            {ns, 1700000000123}
        ]
    ].

legacy_missing_custom_timestamp_fallback_unit_test_() ->
    [
        ?_test(assert_legacy_missing_custom_timestamp_fallback(s)),
        ?_test(assert_legacy_missing_custom_timestamp_fallback(us)),
        ?_test(assert_legacy_missing_custom_timestamp_fallback(ns))
    ].

assert_legacy_missing_custom_timestamp_fallback(TargetPrecision) ->
    Lines = emqx_bridge_influxdb:to_influx_lines(
        <<"mqtt,clientid=${clientid} value=1 ${payload.ts}">>
    ),
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write_syntax => Lines}, TargetPrecision
    ),
    Before = timestamp_in_precision(TargetPrecision),
    {ok, [{#{timeunit := TargetPrecision}, [#{timestamp := Timestamp}]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(
            #{clientid => <<"client-1">>, payload => #{}},
            <<"mydb">>,
            SyntaxLines,
            erlang
        ),
    After = timestamp_in_precision(TargetPrecision),
    ?assert(Timestamp >= Before - 1),
    ?assert(Timestamp =< After + 1).

timestamp_in_precision(Precision) ->
    erlang:convert_time_unit(
        erlang:system_time(millisecond),
        millisecond,
        timestamp_unit(Precision)
    ).

timestamp_unit(s) -> second;
timestamp_unit(us) -> microsecond;
timestamp_unit(ns) -> nanosecond.

render_timestamp(SourcePrecision, TargetPrecision) ->
    render_timestamp(erlang, SourcePrecision, TargetPrecision).

render_timestamp(DriverType, SourcePrecision, TargetPrecision) ->
    Write = [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => SourcePrecision
            }
        }
    ],
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write => Write}, TargetPrecision
    ),
    {ok, Points} = emqx_bridge_greptimedb_utils:data_to_points(
        #{payload => #{ts => 1700000000123}}, <<"mydb">>, SyntaxLines, DriverType
    ),
    Points.

all_missing_fields_error_test_() ->
    [
        ?_assertMatch(
            {error, [{error, no_fields}]},
            render_write_result(erlang, all_missing_fields_write(), #{payload => #{}})
        ),
        ?_assertMatch(
            {error, [{error, no_fields}]},
            render_write_result(native, all_missing_fields_write(), #{payload => #{}})
        )
    ].

all_missing_fields_write() ->
    [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"${payload.missing}">>}
            ],
            timestamp => false
        }
    ].

render_write_result(DriverType, Write, Data) ->
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write => Write}, ms
    ),
    emqx_bridge_greptimedb_utils:data_to_points(Data, <<"mydb">>, SyntaxLines, DriverType).

explicit_missing_timestamp_error_test_() ->
    [
        ?_assertMatch(
            {error, [{error, {bad_timestamp, missing_timestamp}}]},
            render_write_result(erlang, explicit_timestamp_write(), #{payload => #{}})
        ),
        ?_assertMatch(
            {error, [{error, {bad_timestamp, missing_timestamp}}]},
            render_write_result(native, explicit_timestamp_write(), #{payload => #{}})
        )
    ].

explicit_timestamp_write() ->
    [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ].

invalid_typed_values_error_test_() ->
    [
        ?_assertMatch(
            {error, [{error, {invalid_integer_value, <<"1.5">>}}]},
            render_write_result(
                erlang,
                typed_value_write(integer, <<"${payload.value}">>),
                #{payload => #{value => <<"1.5">>}}
            )
        ),
        ?_assertMatch(
            {error, [{error, {invalid_float_value, <<"not-a-float">>}}]},
            render_write_result(
                erlang,
                typed_value_write(float, <<"${payload.value}">>),
                #{payload => #{value => <<"not-a-float">>}}
            )
        ),
        ?_assertMatch(
            {error, [{error, {invalid_boolean_value, <<"maybe">>}}]},
            render_write_result(
                erlang,
                typed_value_write(boolean, <<"${payload.value}">>),
                #{payload => #{value => <<"maybe">>}}
            )
        ),
        ?_assertMatch(
            {error, [{error, {invalid_float_value, <<"1e999">>}}]},
            render_write_result(
                erlang,
                typed_value_write(float, <<"${payload.value}">>),
                #{payload => #{value => <<"1e999">>}}
            )
        ),
        ?_assertMatch(
            {error, [{error, {invalid_integer_value, 9223372036854775808}}]},
            render_write_result(
                erlang,
                typed_value_write(integer, <<"${payload.value}">>),
                #{payload => #{value => 9223372036854775808}}
            )
        )
    ].

large_integer_float_conversion_error_is_contained_test() ->
    TooLarge = 1 bsl 1024,
    ?assertMatch(
        {error, [{error, {invalid_float_value, TooLarge}}]},
        render_write_result(
            erlang,
            typed_value_write(float, <<"${payload.value}">>),
            #{payload => #{value => TooLarge}}
        )
    ).

invalid_utf8_boolean_conversion_error_is_contained_test() ->
    InvalidUtf8 = <<16#ff>>,
    ?assertMatch(
        {error, [{error, {invalid_boolean_value, InvalidUtf8}}]},
        render_write_result(
            erlang,
            typed_value_write(boolean, <<"${payload.value}">>),
            #{payload => #{value => InvalidUtf8}}
        )
    ).

float_string_forms_are_accepted_test_() ->
    [
        ?_assertEqual(
            #{<<"value">> => #{value_data => {f64_value, 1000.0}}},
            rendered_typed_field(erlang, <<"1e3">>)
        ),
        ?_assertEqual(
            #{<<"value">> => #{value_data => {f64_value, 0.5}}},
            rendered_typed_field(erlang, <<".5">>)
        ),
        ?_assertEqual(
            #{<<"value">> => #{value_data => {f64_value, 1.0}}},
            rendered_typed_field(erlang, <<"1.">>)
        ),
        ?_assertEqual(
            #{<<"value">> => 1.0e-3},
            rendered_typed_field(native, <<"1E-3">>)
        )
    ].

rendered_typed_field(DriverType, Value) ->
    [{_, [#{fields := Fields}]}] = render_points(
        DriverType,
        [
            #{
                measurement => <<"mqtt">>,
                fields => [
                    #{key => <<"value">>, type => float, value => Value}
                ],
                timestamp => false
            }
        ],
        #{}
    ),
    Fields.

typed_value_write(Type, Value) ->
    [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"value">>, type => Type, value => Value}
            ],
            timestamp => false
        }
    ].

rendered_key_errors_test_() ->
    [
        ?_assertMatch(
            {error, [{error, {empty_rendered_key, field_key}}]},
            render_write_result(
                erlang,
                rendered_key_write(<<"${payload.key}">>, <<"${payload.value}">>),
                #{payload => #{key => <<>>, value => 1}}
            )
        ),
        ?_assertMatch(
            {error, [
                {error,
                    {duplicate_rendered_key, #{
                        type := field_key,
                        key := <<"same">>
                    }}}
            ]},
            render_write_result(
                erlang,
                duplicate_rendered_key_write(),
                #{payload => #{key1 => <<"same">>, key2 => <<"same">>}}
            )
        )
    ].

rendered_key_write(Key, Value) ->
    [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => Key, type => integer, value => Value}
            ],
            timestamp => false
        }
    ].

duplicate_rendered_key_write() ->
    [
        #{
            measurement => <<"mqtt">>,
            fields => [
                #{key => <<"${payload.key1}">>, type => integer, value => <<"1">>},
                #{key => <<"${payload.key2}">>, type => integer, value => <<"2">>}
            ],
            timestamp => false
        }
    ].

measurement_errors_test_() ->
    [
        ?_assertMatch(
            {error, [{error, missing_measurement}]},
            render_write_result(
                erlang,
                measurement_write(<<"${payload.measurement}">>),
                #{payload => #{}}
            )
        ),
        ?_assertMatch(
            {error, [{error, empty_measurement}]},
            render_write_result(
                erlang,
                measurement_write(<<"${payload.measurement}">>),
                #{payload => #{measurement => <<>>}}
            )
        )
    ].

measurement_write(Measurement) ->
    [
        #{
            measurement => Measurement,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => false
        }
    ].

legacy_write_syntax_rendering_test() ->
    Lines = emqx_bridge_influxdb:to_influx_lines(
        <<"mqtt,clientid=${clientid} value=${payload.value}i ${payload.ts}">>
    ),
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write_syntax => Lines}, ns
    ),
    {ok, [{#{table := <<"mqtt">>}, [#{fields := Fields}]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(
            #{
                clientid => <<"client-1">>,
                payload => #{value => 7, ts => 1700000000123}
            },
            <<"mydb">>,
            SyntaxLines,
            erlang
        ),
    ?assertEqual(
        #{value_data => {i64_value, 7}},
        maps:get(<<"value">>, Fields)
    ).

point_contains_only_driver_fields_test() ->
    Lines = emqx_bridge_influxdb:to_influx_lines(<<"mqtt value=1 ${timestamp}">>),
    [Item] = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write_syntax => Lines}, ms
    ),
    {ok, [{_, [Point]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(
            #{timestamp => 1_700_000_000_123}, <<"mydb">>, [Item#{internal => secret}], erlang
        ),
    ?assertEqual([fields, tags, timestamp], lists:sort(maps:keys(Point))).

legacy_tag_literal_suffix_keeps_type_test_() ->
    [
        ?_assertEqual(
            #{<<"device">> => #{value_data => {i64_value, 65}}},
            render_legacy_tag(erlang, <<"${payload.tag}i">>, #{tag => 65})
        ),
        ?_assertEqual(
            #{<<"device">> => 65},
            render_legacy_tag(native, <<"${payload.tag}i">>, #{tag => 65})
        )
    ].

legacy_tag_placeholder_suffix_is_text_test_() ->
    Data = #{tag => 65, suffix => <<"i">>},
    [
        ?_assertEqual(
            #{<<"device">> => #{value_data => {string_value, <<"65i">>}}},
            render_legacy_tag(erlang, <<"${payload.tag}${payload.suffix}">>, Data)
        ),
        ?_assertEqual(
            #{<<"device">> => <<"65i">>},
            render_legacy_tag(native, <<"${payload.tag}${payload.suffix}">>, Data)
        )
    ].

render_legacy_tag(DriverType, TagTemplate, Payload) ->
    Lines = emqx_bridge_influxdb:to_influx_lines(
        <<"mqtt,device=", TagTemplate/binary, " value=1 ${timestamp}">>
    ),
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_action_parameters(
        #{write_syntax => Lines}, ms
    ),
    {ok, [{_, [#{tags := Tags}]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(
            #{
                payload => Payload,
                timestamp => 1700000000123
            },
            <<"mydb">>,
            SyntaxLines,
            DriverType
        ),
    Tags.

legacy_tag_from_old_channel_state_keeps_literal_type_test_() ->
    [
        ?_assertEqual(
            #{<<"device">> => #{value_data => {i64_value, 65}}},
            render_old_legacy_tag_state(erlang)
        ),
        ?_assertEqual(
            #{<<"device">> => 65},
            render_old_legacy_tag_state(native)
        )
    ].

render_old_legacy_tag_state(DriverType) ->
    OldSyntaxLines = [
        #{
            measurement => emqx_placeholder:preproc_tmpl(<<"mqtt">>),
            tags => #{
                emqx_placeholder:preproc_tmpl(<<"device">>) =>
                    emqx_placeholder:preproc_tmpl(<<"${payload.tag}i">>)
            },
            fields => #{
                emqx_placeholder:preproc_tmpl(<<"value">>) =>
                    emqx_placeholder:preproc_tmpl(<<"1i">>)
            },
            timestamp => emqx_placeholder:preproc_tmpl(<<"${timestamp}">>),
            precision => {ms, ms}
        }
    ],
    {ok, [{_, [#{tags := Tags}]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(
            #{
                payload => #{tag => 65},
                timestamp => 1700000000123
            },
            <<"mydb">>,
            OldSyntaxLines,
            DriverType
        ),
    Tags.
