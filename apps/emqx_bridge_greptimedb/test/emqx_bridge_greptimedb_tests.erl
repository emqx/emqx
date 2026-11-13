%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_tests).

-include_lib("eunit/include/eunit.hrl").

native_transport_error_is_recoverable_test() ->
    Details = native_transport_error(),
    ?assertEqual(
        {error, {recoverable_error, {greptimedb_error, Details}}},
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(
            {error, {greptimedb_error, Details}}
        )
    ).

native_transport_error_with_unknown_outcome_is_recoverable_test() ->
    Details = (native_transport_error())#{
        stage => insert,
        write_outcome => unknown
    },
    ?assertEqual(
        {error, {recoverable_error, {greptimedb_error, Details}}},
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(
            {error, {greptimedb_error, Details}}
        )
    ).

cancelled_not_sent_error_is_recoverable_test() ->
    Details = (native_transport_error())#{
        kind => cancelled,
        code => cancelled,
        stage => metadata,
        write_outcome => not_sent
    },
    ?assertEqual(
        {error, {recoverable_error, {greptimedb_error, Details}}},
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(
            {error, {greptimedb_error, Details}}
        )
    ).

cancelled_unknown_outcome_is_unrecoverable_test() ->
    Details = (native_transport_error())#{
        kind => cancelled,
        code => cancelled,
        stage => stream_write,
        write_outcome => unknown
    },
    ?assertEqual(
        {error, {unrecoverable_error, {greptimedb_error, Details}}},
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(
            {error, {greptimedb_error, Details}}
        )
    ).

native_permanent_error_is_unrecoverable_test() ->
    Details = #{
        domain => greptimedb,
        kind => invalid_argument,
        source => schema_mismatch,
        code => none,
        stage => insert,
        write_outcome => not_sent,
        retry_hint => true,
        message => <<"schema mismatch">>
    },
    ?assertEqual(
        {error, {unrecoverable_error, {greptimedb_error, Details}}},
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(
            {error, {greptimedb_error, Details}}
        )
    ).

pool_and_timeout_errors_are_recoverable_test() ->
    lists:foreach(
        fun(Reason) ->
            ?assertEqual(
                {error, {recoverable_error, Reason}},
                emqx_bridge_greptimedb_rs_impl:normalize_insert_result({error, Reason})
            )
        end,
        [ecpool_empty, not_connected, disconnected, timeout]
    ).

already_classified_error_is_not_rewrapped_test() ->
    Recoverable = {error, {recoverable_error, disconnected}},
    Unrecoverable = {error, {unrecoverable_error, bad_schema}},
    ?assertEqual(
        Recoverable,
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(Recoverable)
    ),
    ?assertEqual(
        Unrecoverable,
        emqx_bridge_greptimedb_rs_impl:normalize_insert_result(Unrecoverable)
    ).

partial_batch_success_prevents_retrying_the_whole_batch_test() ->
    Transient = {error, {recoverable_error, transient}},
    Results = emqx_bridge_greptimedb_rs_impl:coalesce_mixed_batch_results([
        {ok, 1},
        Transient
    ]),
    [First, Second] = Results,
    ?assertEqual({ok, 1}, First),
    assert_unrecoverable(Second, partial_batch_write).

mixed_batch_without_success_promotes_only_recoverable_results_test() ->
    Transient = {error, {recoverable_error, transient}},
    Permanent = {error, {unrecoverable_error, permanent}},
    [First, Second] = emqx_bridge_greptimedb_rs_impl:coalesce_mixed_batch_results([
        Transient,
        Permanent
    ]),
    assert_unrecoverable(First, batch_contains_unrecoverable),
    ?assertEqual(Permanent, Second).

success_and_permanent_error_keep_per_message_results_test() ->
    Permanent = {error, {unrecoverable_error, permanent}},
    ?assertEqual(
        [{ok, 1}, Permanent],
        emqx_bridge_greptimedb_rs_impl:coalesce_mixed_batch_results([
            {ok, 1},
            Permanent
        ])
    ).

multiline_results_are_aggregated_per_original_message_test() ->
    ?assertEqual(
        [{ok, 2}],
        emqx_bridge_greptimedb_rs_impl:aggregate_message_results(
            1,
            [{1, 1, {ok, 1}}, {1, 1, {ok, 1}}],
            []
        )
    ).

native_insert_transport_error_is_recoverable_test() ->
    Details = native_transport_error(),
    Insert = fun(_Client, <<"mqtt">>, _Points) ->
        {error, {greptimedb_error, Details}}
    end,
    with_rs_insert_mock(Insert, fun() ->
        ?assertEqual(
            {error, {recoverable_error, {greptimedb_error, Details}}},
            emqx_bridge_greptimedb_rs_impl:on_query(
                connector,
                {channel, native_data(1)},
                native_rs_state(native_write_syntax())
            )
        )
    end).

sync_batch_keeps_success_and_marks_recoverable_result_unrecoverable_test() ->
    WriteSyntax = native_table_write_syntax(),
    State = native_rs_state(WriteSyntax),
    Batch = [
        {channel, native_table_data(<<"first">>)},
        {channel, native_table_data(<<"second">>)}
    ],
    Insert = fun
        (_Client, <<"first">>, _Points) -> {ok, 1};
        (_Client, <<"second">>, _Points) -> {error, ecpool_empty}
    end,
    with_rs_insert_mock(Insert, fun() ->
        [First, Second] = emqx_bridge_greptimedb_rs_impl:on_batch_query(
            connector,
            Batch,
            State
        ),
        ?assertEqual({ok, 1}, First),
        assert_unrecoverable(Second, partial_batch_write)
    end).

async_submission_error_replies_and_returns_ok_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {async_submission_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, _Points, _Callback) ->
        {error, ecpool_empty}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertEqual(
            ok,
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector,
                {channel, native_data(1)},
                FinalReply,
                native_rs_state(native_write_syntax())
            )
        ),
        receive
            {async_submission_result, Result} ->
                ?assertEqual({error, {recoverable_error, ecpool_empty}}, Result)
        after 1_000 ->
            error(async_submission_result_timeout)
        end
    end).

async_batch_submission_error_replies_for_every_message_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {async_batch_result, Result} end,
        []
    },
    Batch = [
        {channel, native_table_data(<<"first">>)},
        {channel, native_table_data(<<"second">>)},
        {channel, native_table_data(<<"third">>)}
    ],
    InsertAsync = fun
        (_Client, <<"first">>, _Points, Callback) ->
            ok = invoke_rs_callback(Callback, {ok, 1}),
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
                connector,
                Batch,
                FinalReply,
                native_rs_state(native_table_write_syntax())
            )
        ),
        receive
            {async_batch_result, [First, Second, Third]} ->
                ?assertEqual({ok, 1}, First),
                assert_unrecoverable(Second, partial_batch_write),
                assert_unrecoverable(Third, partial_batch_write)
        after 1_000 ->
            error(async_batch_result_timeout)
        end
    end).

async_transport_callback_result_is_recoverable_test() ->
    Self = self(),
    Details = native_transport_error(),
    FinalReply = {
        fun(Result) -> Self ! {async_callback_result, Result} end,
        []
    },
    InsertAsync = fun(_Client, <<"mqtt">>, _Points, Callback) ->
        ok = invoke_rs_callback(Callback, {error, {greptimedb_error, Details}}),
        {ok, self()}
    end,
    with_rs_insert_async_mock(InsertAsync, fun() ->
        ?assertMatch(
            {ok, _},
            emqx_bridge_greptimedb_rs_impl:on_query_async(
                connector,
                {channel, native_data(1)},
                FinalReply,
                native_rs_state(native_write_syntax())
            )
        ),
        receive
            {async_callback_result, Result} ->
                ?assertEqual({error, {recoverable_error, {greptimedb_error, Details}}}, Result)
        after 1_000 ->
            error(async_callback_result_timeout)
        end
    end).

legacy_async_reply_context_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_async_result, Result} end,
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
        {legacy_async_result, Result} ->
            ?assertEqual({ok, 1}, Result)
    after 1_000 ->
        error(legacy_async_result_timeout)
    end.

legacy_batch_async_reply_context_is_normalized_test() ->
    Self = self(),
    FinalReply = {
        fun(Result) -> Self ! {legacy_batch_async_result, Result} end,
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
        {legacy_batch_async_result, Result} ->
            ?assertEqual([{ok, 1}, {ok, 1}], Result)
    after 1_000 ->
        error(legacy_batch_async_result_timeout)
    end.

native_transport_error() ->
    #{
        domain => greptimedb,
        kind => transport,
        source => server,
        code => unknown,
        stage => metadata,
        write_outcome => not_sent,
        retry_hint => true,
        message => <<"transport error">>
    }.

native_write_syntax() ->
    [
        #{
            measurement => <<"mqtt">>,
            tags => [],
            fields => [
                {<<"value">>, <<"${payload.value}">>}
            ],
            timestamp => undefined
        }
    ].

native_table_write_syntax() ->
    [
        #{
            measurement => <<"${payload.table}">>,
            tags => [],
            fields => [
                {<<"value">>, <<"${payload.value}">>}
            ],
            timestamp => undefined
        }
    ].

native_data(Value) ->
    #{
        clientid => <<"client">>,
        timestamp => 1700000000000,
        payload => #{value => Value}
    }.

native_table_data(Table) ->
    #{
        clientid => <<"client">>,
        timestamp => 1700000000000,
        payload => #{table => Table, value => 1}
    }.

native_rs_state(WriteSyntax) ->
    #{
        database => <<"public">>,
        client => test_client,
        installed_channels => #{
            channel => #{
                write_syntax => emqx_bridge_greptimedb_utils:parse_write_syntax(WriteSyntax, ms),
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

assert_unrecoverable(Result, Reason) ->
    ?assertMatch(
        {error, {unrecoverable_error, #{reason := Reason}}},
        Result
    ).

%% https://github.com/emqx/emqx/issues/19814
missing_write_syntax_test() ->
    ?assertThrow(
        {_, [#{reason := required_field, path := "parameters.write_syntax"}]},
        check_action_parameters(#{<<"precision">> => <<"ms">>})
    ).

check_action_parameters(Conf) ->
    Schema = #{
        roots => [{parameters, hoconsc:mk(hoconsc:ref(emqx_bridge_greptimedb, action_parameters))}]
    },
    hocon_tconf:check_plain(Schema, #{<<"parameters">> => Conf}, #{required => false}).
