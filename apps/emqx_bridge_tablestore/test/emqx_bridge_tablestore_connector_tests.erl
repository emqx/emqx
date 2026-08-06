%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tablestore_connector_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(CONF, #{
    instance_name => <<"instance">>,
    endpoint => <<"https://test.cn-hangzhou.ots.aliyuncs.com">>,
    access_key_id => <<"access_key_id">>,
    access_key_secret => <<"access_key_secret">>,
    pool_size => 8,
    probe_table_name => <<"probe_table">>
}).

-define(CONF_NO_PROBE, #{
    instance_name => <<"instance">>,
    endpoint => <<"https://test.cn-hangzhou.ots.aliyuncs.com">>,
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
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, describe_table, fun(
                _CRef, #{table_name := <<"probe_table">>}
            ) ->
                {ok, #{table_name => "probe_table", status => "ACTIVE", time_to_live => 3}}
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
                ?_assertEqual(
                    <<"https://test.cn-hangzhou.ots.aliyuncs.com">>,
                    proplists:get_value(endpoint, OtsOpts)
                ),
                ?_assertEqual(8, proplists:get_value(pool_size, OtsOpts))
            ]
        end}.

start_connector_failure_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, describe_table, fun(_CRef, _SQL) ->
                {error, #{code => <<"OTSObjectNotExist">>, message => <<"table not found">>}}
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
                    {error, #{code := <<"OTSObjectNotExist">>}},
                    emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF)
                )
            ]
        end}.

start_connector_tcp_probe_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                ok
            end),
            meck:new(gen_tcp, [no_history, unstick]),
            ok = meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
                {ok, dummy_sock}
            end),
            ok = meck:expect(gen_tcp, close, fun(_Sock) ->
                ok
            end),
            emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF_NO_PROBE)
        end,
        fun(_) ->
            meck:unload(ots_ts_client),
            meck:unload(gen_tcp)
        end,
        fun({ok, #{client_ref := ClientRef}}) ->
            [
                ?_assertEqual(dummy_client_ref, ClientRef)
            ]
        end}.

start_connector_tcp_probe_failure_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                ok
            end),
            meck:new(gen_tcp, [no_history, unstick]),
            ok = meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
                {error, econnrefused}
            end),
            emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF_NO_PROBE)
        end,
        fun(_) ->
            meck:unload(ots_ts_client),
            meck:unload(gen_tcp)
        end,
        fun(_) ->
            [
                ?_assertMatch(
                    {error, #{error := tcp_probe_failed, reason := econnrefused}},
                    emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF_NO_PROBE)
                )
            ]
        end}.

on_get_status_describe_probe_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, describe_table, fun(_CRef, _SQL) ->
                {ok, #{}}
            end),
            ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                ok
            end),
            {ok, State} = emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF),
            State
        end,
        fun(_) ->
            meck:unload(ots_ts_client)
        end,
        fun(State) ->
            [
                ?_test(begin
                    ok = meck:expect(ots_ts_client, describe_table, fun(_CRef, _SQL) ->
                        {ok, #{table_name => "probe_table", status => "ACTIVE"}}
                    end),
                    ?assertEqual(
                        connected,
                        emqx_bridge_tablestore_connector:on_get_status(test_inst, State)
                    )
                end),
                ?_test(begin
                    ok = meck:expect(ots_ts_client, describe_table, fun(_CRef, _SQL) ->
                        {error, #{code => <<"OTSAuthFailed">>}}
                    end),
                    ?assertEqual(
                        connecting,
                        emqx_bridge_tablestore_connector:on_get_status(test_inst, State)
                    )
                end)
            ]
        end}.

on_get_status_tcp_probe_test_() ->
    {setup,
        fun() ->
            meck:new(ots_ts_client, [no_history]),
            ok = meck:expect(ots_ts_client, start, fun(_OtsOpts) ->
                {ok, dummy_client_ref}
            end),
            ok = meck:expect(ots_ts_client, stop, fun(_CRef) ->
                ok
            end),
            meck:new(gen_tcp, [no_history, unstick]),
            ok = meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
                {ok, dummy_sock}
            end),
            ok = meck:expect(gen_tcp, close, fun(_Sock) ->
                ok
            end),
            {ok, State} = emqx_bridge_tablestore_connector:on_start(test_inst, ?CONF_NO_PROBE),
            State
        end,
        fun(_) ->
            meck:unload(ots_ts_client),
            meck:unload(gen_tcp)
        end,
        fun(State) ->
            [
                ?_test(begin
                    ok = meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
                        {ok, dummy_sock}
                    end),
                    ?assertEqual(
                        connected,
                        emqx_bridge_tablestore_connector:on_get_status(test_inst, State)
                    )
                end),
                ?_test(begin
                    ok = meck:expect(gen_tcp, connect, fun(_Host, _Port, _Opts, _Timeout) ->
                        {error, timeout}
                    end),
                    ?assertEqual(
                        connecting,
                        emqx_bridge_tablestore_connector:on_get_status(test_inst, State)
                    )
                end)
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
