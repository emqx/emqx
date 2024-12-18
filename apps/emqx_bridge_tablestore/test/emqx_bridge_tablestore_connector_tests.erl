-module(emqx_bridge_tablestore_connector_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CONF, #{
    instance_name => <<"instance">>,
    endpoint => <<"endpoint">>,
    access_key_id => <<"access_key_id">>,
    access_key_secret => <<"access_key_secret">>,
    pool_size => 8
}).

-define(ACT_CONF, #{
    parameters => #{
        storage_model_type => timeseries,
        timestamp => <<"NOW">>,
        table_name => <<"${table}">>,
        measurement => <<"${measurement}">>,
        meta_update_model => 'MUM_NORMAL',
        data_source => <<"data_source">>,
        tags => #{
            '${tag1}' => <<"${tag1_value}">>,
            '${tag2}' => <<"${tag2_value}">>
        },
        fields => [
            #{column => <<"str_field0">>, value => <<"str_val0">>},
            #{column => <<"${str_field}">>, value => <<"${str_val}">>},
            #{column => <<"${int_field}">>, value => <<"${int_val}">>, isint => true},
            #{column => <<"${float_field}">>, value => <<"${float_val}">>, isint => false},
            #{column => <<"${bool_field}">>, value => <<"${bool_val}">>},
            #{column => <<"${binary_field}">>, value => <<"${binary_val}">>, isbinary => true}
        ]
    }
}).

-define(MSG, #{
    table => <<"table">>,
    measurement => <<"measurement">>,
    tag1 => <<"tag1">>,
    tag2 => <<"tag2">>,
    tag1_value => <<"tag1_value">>,
    tag2_value => <<"tag2_value">>,
    str_field => <<"str_field">>,
    str_val => <<"str_val">>,
    int_field => <<"int_field">>,
    int_val => 123,
    float_field => <<"float_field">>,
    float_val => 123.456,
    bool_field => <<"bool_field">>,
    bool_val => true,
    binary_field => <<"binary_field">>,
    binary_val => <<"binary_val">>
}).

start_connector_test_() ->
    {timeout, 30,
        {setup,
            fun() ->
                meck:new(ots_ts_client, [no_history]),
                ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                    {ok, dummy_client_ref}
                end),
                ok = meck:expect(ots_ts_client, list_tables, fun(_CRef) ->
                    {ok, []}
                end),
                ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                    ok
                end),
                emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF)
            end,
            fun(_) ->
                meck:unload(ots_ts_client)
            end,
            fun({ok, #{client_ref := ClientRef, ots_opts := OtsOpts}}) ->
                [
                    ?_assertEqual(dummy_client_ref, ClientRef),
                    ?_assertEqual(<<"endpoint">>, proplists:get_value(endpoint, OtsOpts)),
                    ?_assertEqual(8, proplists:get_value(pool_size, OtsOpts))
                ]
            end}}.

start_connector_failure_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, list_tables, fun(_CRef) ->
                {error, not_found}
            end),
            ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                ok
            end)
        end,
        fun(_) ->
            meck:unload(ots_ts_client)
        end,
        fun(_) ->
            [
                ?_assertMatch(
                    {error, not_found}, emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF)
                )
            ]
        end}.

on_query_test_() ->
    {setup,
        fun() ->
            ets:new(on_query_test, [named_table, public]),
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, put, fun(_CRef, Query) ->
                ets:insert(on_query_test, {query, Query}),
                {ok, []}
            end),
            emqx_bridge_tablestore_connector:on_add_channel(
                test_inst,
                #{channels => #{}},
                channelid1,
                ?ACT_CONF
            )
        end,
        fun(_) ->
            meck:unload(ots_ts_client),
            ets:delete(on_query_test)
        end,
        fun({ok, State}) ->
            ok = emqx_bridge_tablestore_connector:on_query(
                test_inst,
                {channelid1, ?MSG},
                State#{client_ref => dummy_client_ref}
            ),
            [{query, Query}] = ets:lookup(on_query_test, query),
            #{
                table_name := TableName,
                rows_data := [Row],
                meta_update_mode := MetaUpdateMode
            } = Query,
            [
                ?_assertMatch('MUM_NORMAL', MetaUpdateMode),
                ?_assertMatch(<<"table">>, TableName),
                ?_assertMatch(Ts when is_integer(Ts), maps:get(time, Row)),
                ?_assertMatch(<<"measurement">>, maps:get(measurement, Row)),
                ?_assertMatch(<<"data_source">>, maps:get(data_source, Row)),
                ?_assertMatch(
                    #{<<"tag1">> := <<"tag1_value">>, <<"tag2">> := <<"tag2_value">>},
                    maps:get(tags, Row)
                ),
                ?_assertMatch(
                    [
                        {<<"str_field0">>, <<"str_val0">>, #{}},
                        {<<"str_field">>, <<"str_val">>, #{}},
                        {<<"int_field">>, 123, #{isint := true}},
                        {<<"float_field">>, 123.456, #{isint := false}},
                        {<<"bool_field">>, true, #{}},
                        {<<"binary_field">>, <<"binary_val">>, #{isbinary := true}}
                    ],
                    maps:get(fields, Row)
                )
            ]
        end}.

on_batch_query_test_() ->
    {setup,
        fun() ->
            ets:new(on_query_test, [named_table, public]),
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, put, fun(_CRef, Query) ->
                ets:insert(on_query_test, {query, Query}),
                {ok, []}
            end),
            emqx_bridge_tablestore_connector:on_add_channel(
                test_inst,
                #{channels => #{}},
                channelid1,
                ?ACT_CONF
            )
        end,
        fun(_) ->
            meck:unload(ots_ts_client),
            ets:delete(on_query_test)
        end,
        fun({ok, State}) ->
            BatchMsgs = [{channelid1, ?MSG} || _ <- lists:seq(1, 3)],
            ok = emqx_bridge_tablestore_connector:on_batch_query(
                test_inst,
                BatchMsgs,
                State#{client_ref => dummy_client_ref}
            ),
            [{query, Query}] = ets:lookup(on_query_test, query),
            #{
                table_name := TableName,
                rows_data := Rows,
                meta_update_mode := MetaUpdateMode
            } = Query,
            Row = hd(Rows),
            [
                ?_assert(length(BatchMsgs) =:= length(Rows)),
                ?_assertMatch('MUM_NORMAL', MetaUpdateMode),
                ?_assertMatch(<<"table">>, TableName),
                ?_assertMatch(Ts when is_integer(Ts), maps:get(time, Row)),
                ?_assertMatch(<<"measurement">>, maps:get(measurement, Row)),
                ?_assertMatch(<<"data_source">>, maps:get(data_source, Row)),
                ?_assertMatch(
                    #{<<"tag1">> := <<"tag1_value">>, <<"tag2">> := <<"tag2_value">>},
                    maps:get(tags, Row)
                ),
                ?_assertMatch(
                    [
                        {<<"str_field0">>, <<"str_val0">>, #{}},
                        {<<"str_field">>, <<"str_val">>, #{}},
                        {<<"int_field">>, 123, #{isint := true}},
                        {<<"float_field">>, 123.456, #{isint := false}},
                        {<<"bool_field">>, true, #{}},
                        {<<"binary_field">>, <<"binary_val">>, #{isbinary := true}}
                    ],
                    maps:get(fields, Row)
                )
            ]
        end}.
