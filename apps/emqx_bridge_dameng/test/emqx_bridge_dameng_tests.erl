%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dameng_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("kernel/include/file.hrl").

-include_lib("emqx_bridge_dameng/include/emqx_bridge_dameng.hrl").

-define(DEFAULT_INSERT, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload) "
    "values ( ${id}, ${topic}, ${qos}, ${payload} )"
>>).

%%------------------------------------------------------------------------------
%% Schema validation
%%------------------------------------------------------------------------------

connector_schema_test_() ->
    [
        {"defaults are filled in",
            ?_assertMatch(
                #{
                    <<"server">> := <<"127.0.0.1">>,
                    <<"port">> := ?DAMENG_DEFAULT_PORT,
                    <<"driver">> := <<"DM8 ODBC DRIVER">>,
                    <<"charset">> := <<"utf8">>,
                    <<"ssl_path">> := <<>>
                },
                parse_and_check_connector(connector_config(#{}))
            )},
        {"server may carry an explicit port",
            ?_assertMatch(
                #{<<"server">> := <<"127.0.0.1:5237">>},
                parse_and_check_connector(
                    connector_config(#{
                        <<"server">> => <<"127.0.0.1:5237">>
                    })
                )
            )},
        {"dsn mode is accepted",
            ?_assertMatch(
                #{<<"dsn">> := <<"dm8">>},
                parse_and_check_connector(connector_config(#{<<"dsn">> => <<"dm8">>}))
            )},
        {"dsn alone is enough: server is optional",
            ?_assertMatch(
                #{<<"dsn">> := <<"dm8">>, <<"username">> := <<"SYSDBA">>},
                parse_and_check_connector(#{
                    <<"dsn">> => <<"dm8">>,
                    <<"username">> => <<"SYSDBA">>,
                    <<"password">> => <<"secretpass">>,
                    <<"pool_size">> => 1,
                    <<"resource_opts">> => #{}
                })
            )},
        {"neither dsn nor server is rejected",
            ?_assertException(
                throw,
                {_, [_ | _]},
                parse_and_check_connector(maps:remove(<<"server">>, connector_config(#{})))
            )},
        {"a malformed server is rejected",
            ?_assertException(
                throw,
                {_, [_ | _]},
                parse_and_check_connector(connector_config(#{<<"server">> => <<"127.0.0.1:33x">>}))
            )}
    ].

action_schema_test_() ->
    [
        {"insert sql template is accepted",
            ?_assertMatch(
                #{<<"parameters">> := #{<<"sql">> := _}},
                emqx_bridge_v2_testlib:parse_and_check(action, <<"dameng">>, <<"act">>, #{
                    <<"enable">> => true,
                    <<"connector">> => <<"conn">>,
                    <<"parameters">> => #{<<"sql">> => ?DEFAULT_INSERT},
                    <<"resource_opts">> => #{}
                })
            )}
    ].

parse_and_check_connector(InnerConfig) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"dameng">>, <<"conn">>, InnerConfig).

connector_config(Overrides) ->
    Base = #{
        <<"server">> => <<"127.0.0.1">>,
        <<"username">> => <<"SYSDBA">>,
        <<"password">> => <<"secretpass">>,
        <<"driver">> => <<"DM8 ODBC DRIVER">>,
        <<"charset">> => <<"utf8">>,
        <<"pool_size">> => 8,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
    },
    emqx_utils_maps:deep_merge(Base, Overrides).

%%------------------------------------------------------------------------------
%% parse_server/2
%%------------------------------------------------------------------------------

parse_server_test_() ->
    [
        {"only host, no port -> default port",
            ?_assertEqual(
                #{hostname => <<"127.0.0.1">>, port => ?DAMENG_DEFAULT_PORT},
                emqx_bridge_dameng_connector:parse_server(<<"127.0.0.1">>, ?DAMENG_DEFAULT_PORT)
            )},
        {"host:port -> explicit port",
            ?_assertEqual(
                #{hostname => <<"127.0.0.1">>, port => 5237},
                emqx_bridge_dameng_connector:parse_server(
                    <<"127.0.0.1:5237">>, ?DAMENG_DEFAULT_PORT
                )
            )},
        {"host with default port override",
            ?_assertEqual(
                #{hostname => <<"localhost">>, port => 5236},
                emqx_bridge_dameng_connector:parse_server(<<"localhost">>, 5236)
            )}
    ].

%%------------------------------------------------------------------------------
%% build_conn_map/1
%%------------------------------------------------------------------------------

build_conn_map_test_() ->
    [
        {"a server without port uses the configured port",
            ?_assertMatch(
                {ok, #{server := <<"127.0.0.1">>, port := 5236, dsn := undefined}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    server => <<"127.0.0.1">>,
                    port => 5236
                })
            )},
        {"a server with an embedded port wins",
            ?_assertMatch(
                {ok, #{server := <<"127.0.0.1">>, port := 5237}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    server => <<"127.0.0.1:5237">>,
                    port => 5236
                })
            )},
        {"a dsn needs no server",
            ?_assertMatch(
                {ok, #{dsn := <<"dm8">>, server := undefined}},
                emqx_bridge_dameng_connector:build_conn_map(#{dsn => <<"dm8">>})
            )},
        {"an empty dsn is ignored",
            ?_assertMatch(
                {ok, #{dsn := undefined, server := <<"localhost">>}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    dsn => <<>>,
                    server => <<"localhost">>
                })
            )},
        {"neither dsn nor server is a config error",
            ?_assertMatch(
                {error, {invalid_config, _}},
                emqx_bridge_dameng_connector:build_conn_map(#{})
            )}
    ].

%% `build_conn_map/1' is called with atom keys from the checked config and with
%% binary keys from a persisted/namespaced config; both must behave the same.
build_conn_map_key_shape_test_() ->
    [
        {"atom keys",
            ?_assertMatch(
                {ok, #{dsn := <<"dm8">>, username := <<"U1">>, password := _}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    dsn => <<"dm8">>, username => <<"U1">>, password => <<"p1">>
                })
            )},
        {"binary keys",
            ?_assertMatch(
                {ok, #{dsn := <<"dm8">>, username := <<"U1">>, password := _}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    <<"dsn">> => <<"dm8">>,
                    <<"username">> => <<"U1">>,
                    <<"password">> => <<"p1">>
                })
            )},
        {"binary server keys",
            ?_assertMatch(
                {ok, #{server := <<"127.0.0.1">>, port := 5237}},
                emqx_bridge_dameng_connector:build_conn_map(#{
                    <<"server">> => <<"127.0.0.1">>, <<"port">> => 5237
                })
            )}
    ].

username_has_no_schema_default_test() ->
    Parsed = parse_and_check_connector(maps:remove(<<"username">>, connector_config(#{}))),
    ?assertNot(maps:is_key(<<"username">>, Parsed)).

%% A DSN entry carries the credentials, so only explicitly configured ones may
%% override them.
build_conn_map_dsn_credentials_test() ->
    {ok, ConnMap} = emqx_bridge_dameng_connector:build_conn_map(#{<<"dsn">> => <<"dm8">>}),
    ?assertNot(maps:is_key(username, ConnMap)),
    ?assertNot(maps:is_key(password, ConnMap)).

build_conn_map_dsn_credentials_override_test() ->
    {ok, ConnMap} = emqx_bridge_dameng_connector:build_conn_map(#{
        <<"dsn">> => <<"dm8">>,
        <<"username">> => <<"U1">>,
        <<"password">> => <<"p1">>
    }),
    ?assertEqual(<<"U1">>, maps:get(username, ConnMap)),
    ?assertEqual(<<"p1">>, emqx_secret:unwrap(maps:get(password, ConnMap))).

%% Without a DSN the connector must supply the credentials itself.
build_conn_map_server_credentials_test() ->
    {ok, ConnMap} = emqx_bridge_dameng_connector:build_conn_map(#{
        <<"server">> => <<"127.0.0.1">>
    }),
    ?assertEqual(<<"SYSDBA">>, maps:get(username, ConnMap)),
    ?assertNot(maps:is_key(password, ConnMap)).

%%------------------------------------------------------------------------------
%% optional DM SSL connection attributes
%%------------------------------------------------------------------------------

%% The DM8 ODBC driver performs the TLS handshake itself, so the connector only
%% passes `SSL_PATH'/`SSL_PWD' through. They must be dropped when unset so that
%% a connector without them builds exactly the connection string it used before.
build_conn_map_extra_conn_attrs_test_() ->
    [
        {"no SSL attributes are passed through by default",
            ?_assertEqual(
                [],
                maps:get(extra_conn_attrs, conn_map(#{<<"server">> => <<"127.0.0.1">>}))
            )},
        {"blank SSL attributes are dropped",
            ?_assertEqual(
                [],
                maps:get(
                    extra_conn_attrs,
                    conn_map(#{
                        <<"server">> => <<"127.0.0.1">>,
                        <<"ssl_path">> => <<>>,
                        <<"ssl_pwd">> => <<>>
                    })
                )
            )},
        {"SSL attributes are appended to the DSN-less connection string",
            ?_assertEqual(
                "Driver={DM8 ODBC DRIVER};Server=127.0.0.1:5236;UID=SYSDBA;"
                "Charset=utf8;SSL_PATH=/opt/dmdbms/bin/client_ssl/SYSDBA;SSL_PWD=Abcd1234",
                emqx_odbc:build_conn_string(
                    conn_map(#{
                        <<"server">> => <<"127.0.0.1">>,
                        <<"ssl_path">> => <<"/opt/dmdbms/bin/client_ssl/SYSDBA">>,
                        <<"ssl_pwd">> => <<"Abcd1234">>
                    })
                )
            )},
        {"SSL attributes may accompany a DSN",
            ?_assertEqual(
                "DSN=dm8;SSL_PWD=Abcd1234",
                emqx_odbc:build_conn_string(
                    conn_map(#{<<"dsn">> => <<"dm8">>, <<"ssl_pwd">> => <<"Abcd1234">>})
                )
            )}
    ].

%% The schema wraps `ssl_pwd' in a secret; it must be unwrapped only when the
%% connection string is built.
build_conn_map_ssl_pwd_secret_test() ->
    Parsed = parse_and_check_connector(
        connector_config(#{
            <<"ssl_path">> => <<"/opt/dmdbms/bin/client_ssl/SYSDBA">>,
            <<"ssl_pwd">> => <<"Abcd1234">>
        })
    ),
    {ok, ConnMap} = emqx_bridge_dameng_connector:build_conn_map(Parsed),
    Attrs = maps:get(extra_conn_attrs, ConnMap),
    ?assertEqual(
        [{"SSL_PATH", <<"/opt/dmdbms/bin/client_ssl/SYSDBA">>}, {"SSL_PWD", <<"Abcd1234">>}],
        [{Key, emqx_secret:unwrap(Value)} || {Key, Value} <- Attrs]
    ),
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=127.0.0.1:5236;UID=SYSDBA;PWD=secretpass;"
        "Charset=utf8;SSL_PATH=/opt/dmdbms/bin/client_ssl/SYSDBA;SSL_PWD=Abcd1234",
        emqx_odbc:build_conn_string(ConnMap)
    ).

conn_map(Config) ->
    {ok, ConnMap} = emqx_bridge_dameng_connector:build_conn_map(Config),
    ConnMap.

%%------------------------------------------------------------------------------
%% validate_dsn_or_server/1
%%------------------------------------------------------------------------------

%% hocon calls the validator both with the `#{Name => Config}' map of the
%% `connectors.dameng' field and with each single connector config.  An empty
%% map means "no connectors" and must stay valid, otherwise removing the last
%% connector fails and leaves the previous one behind.
validate_dsn_or_server_maps_test() ->
    ?assertEqual(ok, emqx_bridge_dameng_connector_info:validate_dsn_or_server(#{})),
    ?assertEqual(
        ok,
        emqx_bridge_dameng_connector_info:validate_dsn_or_server(#{
            <<"c">> => #{<<"server">> => <<"127.0.0.1">>}
        })
    ),
    ?assertEqual(
        {error, <<"either 'dsn' or 'server' must be configured">>},
        emqx_bridge_dameng_connector_info:validate_dsn_or_server(#{
            <<"c">> => #{<<"dsn">> => <<>>}
        })
    ).

%% A single connector config without a DSN or a server is invalid.
validate_dsn_or_server_config_test() ->
    ?assertEqual(
        ok,
        emqx_bridge_dameng_connector_info:validate_dsn_or_server(#{<<"server">> => <<"h">>})
    ),
    ?assertEqual(
        {error, <<"either 'dsn' or 'server' must be configured">>},
        emqx_bridge_dameng_connector_info:validate_dsn_or_server(#{<<"enable">> => true})
    ).

%%------------------------------------------------------------------------------
%% parse_sql_template/1
%%------------------------------------------------------------------------------

parse_sql_template_ok_test() ->
    Channel = emqx_bridge_dameng_connector:parse_sql_template(#{sql => ?DEFAULT_INSERT}),
    ?assertMatch({ok, #{statement_type := insert}}, Channel),
    {ok, Ch} = Channel,
    ?assertEqual(
        [<<"msgid">>, <<"topic">>, <<"qos">>, <<"payload">>],
        maps:get(insert_columns, Ch)
    ),
    %% param_sql must be the full INSERT statement with `?' placeholders in
    %% the VALUES part (regression: it used to be only the `(?, ?, ?, ?)' part).
    ParamSQL = maps:get(param_sql, Ch),
    ?assertMatch(
        <<"insert into t_mqtt_msg(msgid, topic, qos, payload) values ", _/binary>>,
        ParamSQL
    ),
    ?assertEqual(4, length(binary:matches(ParamSQL, <<"?">>))),
    %% no remaining `${...}` template markers
    ?assertMatch(nomatch, re:run(ParamSQL, <<"\\$\\{">>)),
    %% the number of vars must equal the number of columns
    ?assertEqual(4, length(maps:get(values_tokens, Ch))).

parse_sql_template_no_columns_test() ->
    %% INSERT without explicit column list must fail (strict DM8 requirement).
    ?assertMatch(
        {error, {unrecoverable_error, {invalid_request, insert_must_specify_columns}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<"insert into t_mqtt_msg values ( ${id} )">>
        })
    ).

parse_sql_template_on_clause_test() ->
    ?assertMatch(
        {error,
            {unrecoverable_error, {invalid_request, <<"ON clause is not supported: ", _/binary>>}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<
                "insert into t_mqtt_msg(id) values ( ${id} )"
                " ON DUPLICATE KEY UPDATE id = 0"
            >>
        })
    ).

parse_sql_template_select_test() ->
    %% Non-insert statements follow the literal path.
    ?assertMatch(
        {ok, #{statement_type := select}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<"select id, topic from t_mqtt_msg where id = ${id}">>
        })
    ).

parse_sql_template_vars_mismatch_test() ->
    ?assertMatch(
        {error, {unrecoverable_error, {invalid_request, columns_vars_mismatch}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<"insert into t_mqtt_msg(a, b) values ( ${id} )">>
        })
    ).

parse_sql_template_quoted_columns_test() ->
    %% Quoted identifiers are accepted and matched against the unquoted names
    %% reported by `describe_table'.
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t_mqtt_msg(\"msgid\", \"topic\") values ( ${id}, ${topic} )">>
    }),
    ?assertEqual([<<"msgid">>, <<"topic">>], maps:get(insert_columns, Ch)).

%%------------------------------------------------------------------------------
%% build_param_params/4 (batch Params construction)
%%------------------------------------------------------------------------------

build_param_params_test() ->
    Params = #{sql => ?DEFAULT_INSERT},
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(Params),
    %% Simulate the types returned by `describe_table' for the 4 columns.
    ColTypes = [sql_integer, {sql_varchar, 100}, sql_tinyint, {sql_varchar, 200}],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [
        #{id => 1, topic => <<"t/1">>, qos => 1, payload => <<"p1">>},
        #{id => 2, topic => <<"t/2">>, qos => 0, payload => <<"p2">>}
    ],
    {ok, ParamsList} = emqx_bridge_dameng_connector:build_param_params(
        Tokens, Msgs, ColTypes, false
    ),
    %% Character parameters keep the declared column size when it already fits
    %% the values: see emqx_odbc:fit_param_type/2.
    ?assertEqual(
        [
            {sql_integer, [1, 2]},
            {{sql_varchar, 100}, [<<"t/1">>, <<"t/2">>]},
            {sql_tinyint, [1, 0]},
            {{sql_varchar, 200}, [<<"p1">>, <<"p2">>]}
        ],
        ParamsList
    ).

build_param_params_undefined_as_null_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{sql => ?DEFAULT_INSERT}),
    ColTypes = [sql_integer, {sql_varchar, 100}, sql_tinyint, {sql_varchar, 200}],
    Tokens = maps:get(values_tokens, Ch),
    %% `id' is missing, so with UndefinedAsNull=true it should become `null'.
    Msgs = [#{topic => <<"t/1">>, qos => 1, payload => <<"p1">>}],
    {ok, ParamsList} = emqx_bridge_dameng_connector:build_param_params(
        Tokens, Msgs, ColTypes, true
    ),
    ?assertEqual(
        {sql_integer, [null]},
        hd(ParamsList)
    ).

build_param_params_coercible_types_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t_big(id, v_bigint, v_date) values ( ${id}, ${big}, ${date} )">>
    }),
    ColTypes = [sql_integer, 'SQL_BIGINT', 'SQL_TYPE_DATE'],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [#{id => 1, big => <<"9999999999">>, date => <<"2026-01-02">>}],
    {ok, ParamsList} = emqx_bridge_dameng_connector:build_param_params(
        Tokens, Msgs, ColTypes, false
    ),
    %% BIGINT/DATE columns are bound as varchar string params.
    ?assertEqual(
        [
            {sql_integer, [1]},
            {{sql_varchar, 24}, [<<"9999999999">>]},
            {{sql_varchar, 16}, [<<"2026-01-02">>]}
        ],
        ParamsList
    ).

%% `odbc' binds a DECIMAL/NUMERIC parameter with a precision above 15 as a
%% string; the value must be a string as well, otherwise the driver writes
%% uninitialized memory into the column.
build_param_params_decimal_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t_dec(id, d38, n10) values ( ${id}, ${d38}, ${n10} )">>
    }),
    ColTypes = [sql_integer, {sql_decimal, 38, 2}, {sql_numeric, 10, 2}],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [#{id => 1, d38 => <<"12345678901234567890.12">>, n10 => <<"1.5">>}],
    {ok, ParamsList} = emqx_bridge_dameng_connector:build_param_params(
        Tokens, Msgs, ColTypes, false
    ),
    ?assertEqual(
        [
            {sql_integer, [1]},
            {{sql_decimal, 38, 2}, [<<"12345678901234567890.12", 0>>]},
            {{sql_numeric, 10, 2}, [1.5]}
        ],
        ParamsList
    ).

build_param_params_value_too_long_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t(id, topic) values ( ${id}, ${topic} )">>
    }),
    ColTypes = [sql_integer, {sql_varchar, 3}],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [#{id => 1, topic => <<"toolong">>}],
    %% A value that does not fit the column is rejected instead of being bound
    %% into a buffer that cannot hold it.
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_value, {value_too_long, {sql_varchar, 3}, 3, 7}}}},
        emqx_bridge_dameng_connector:build_param_params(Tokens, Msgs, ColTypes, false)
    ).

build_param_params_undefined_var_error_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{sql => ?DEFAULT_INSERT}),
    ColTypes = [sql_integer, {sql_varchar, 100}, sql_tinyint, {sql_varchar, 200}],
    Tokens = maps:get(values_tokens, Ch),
    %% `id' is missing and undefined_vars_as_null is `false' (the default), so
    %% the message must be rejected instead of silently writing something.
    Msgs = [#{topic => <<"t/1">>, qos => 1, payload => <<"p1">>}],
    ?assertEqual(
        {error, {unrecoverable_error, undefined_var}},
        emqx_bridge_dameng_connector:build_param_params(Tokens, Msgs, ColTypes, false)
    ).

build_param_params_unsupported_type_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t_blob(id, blob_col) values ( ${id}, ${blob} )">>
    }),
    ColTypes = [sql_integer, 'SQL_BINARY'],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [#{id => 1, blob => <<1, 2, 3>>}],
    ?assertMatch(
        {error, {unrecoverable_error, {unsupported_odbc_type, 'SQL_BINARY'}}},
        emqx_bridge_dameng_connector:build_param_params(Tokens, Msgs, ColTypes, false)
    ).

%%------------------------------------------------------------------------------
%% on_add_channel/4: describe probe and column type validation
%%------------------------------------------------------------------------------

on_add_channel_rejects_unsupported_column_type_test() ->
    meck:new(ecpool, [passthrough]),
    meck:expect(ecpool, pick_and_do, fun(_Pool, {_Mod, worker_describe, [_Table, _Timeout]}, _Mode) ->
        {ok, [{<<"ID">>, sql_integer}, {<<"BLOB_COL">>, 'SQL_BINARY'}]}
    end),
    try
        State = #{pool_name => <<"p">>, installed_channels => #{}, resource_opts => #{}},
        ChannelConfig = #{
            parameters => #{
                sql => <<"insert into t(id, blob_col) values ( ${id}, ${blob} )">>
            }
        },
        ?assertMatch(
            {error,
                {unrecoverable_error,
                    {invalid_request, {unsupported_odbc_types, [{<<"blob_col">>, 'SQL_BINARY'}]}}}},
            emqx_bridge_dameng_connector:on_add_channel(<<"i">>, State, <<"c">>, ChannelConfig)
        )
    after
        meck:unload(ecpool)
    end.

on_add_channel_installs_supported_columns_test() ->
    meck:new(ecpool, [passthrough]),
    meck:expect(ecpool, pick_and_do, fun(_Pool, {_Mod, worker_describe, [_Table, _Timeout]}, _Mode) ->
        {ok, [{<<"ID">>, sql_integer}, {<<"TOPIC">>, {sql_varchar, 100}}]}
    end),
    try
        State = #{pool_name => <<"p">>, installed_channels => #{}, resource_opts => #{}},
        ChannelConfig = #{
            parameters => #{sql => <<"insert into t(id, topic) values ( ${id}, ${topic} )">>}
        },
        {ok, NewState} = emqx_bridge_dameng_connector:on_add_channel(
            <<"i">>, State, <<"c">>, ChannelConfig
        ),
        Channels = maps:get(installed_channels, NewState),
        ?assertEqual(
            [sql_integer, {sql_varchar, 100}],
            maps:get(column_types, maps:get(<<"c">>, Channels))
        )
    after
        meck:unload(ecpool)
    end.

%%------------------------------------------------------------------------------
%% worker_do_literal/4 (non-insert statements)
%%------------------------------------------------------------------------------

worker_do_literal_selected_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {selected, [<<"ID">>], [{1}, {2}]}
    end),
    try
        ChannelState = literal_channel_state(),
        ?assertEqual(
            {ok, [{1}, {2}]},
            emqx_bridge_dameng_connector:worker_do_literal(
                self(), ChannelState, #{id => 1}, #{resource_opts => #{}}
            )
        )
    after
        meck:unload(emqx_odbc)
    end.

worker_do_literal_updated_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {updated, 3}
    end),
    try
        %% A write that affected rows is a plain success, like the insert path.
        ?assertEqual(
            ok,
            emqx_bridge_dameng_connector:worker_do_literal(
                self(), literal_channel_state(), #{id => 1}, #{resource_opts => #{}}
            )
        )
    after
        meck:unload(emqx_odbc)
    end.

%% DM8 is Oracle compatible: a single quote is escaped by doubling it and the
%% backslash has no special meaning.
worker_do_literal_quoting_test() ->
    meck:new(emqx_odbc, [passthrough]),
    Self = self(),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, SQL, _Timeout) ->
        Self ! {sql, SQL},
        {updated, 1}
    end),
    try
        ChannelState = #{
            values_tokens =>
                emqx_placeholder:preproc_tmpl(<<"update t set s = ${s}, n = ${n}">>),
            channel_conf => #{}
        },
        ?assertEqual(
            ok,
            emqx_bridge_dameng_connector:worker_do_literal(
                self(), ChannelState, #{s => <<"a'b\\c">>, n => 5}, #{resource_opts => #{}}
            )
        ),
        ?assertEqual(
            {sql, <<"update t set s = 'a''b\\c', n = 5">>},
            receive
                {sql, _} = SQL -> SQL
            after 1000 -> timeout
            end
        )
    after
        meck:unload(emqx_odbc)
    end.

worker_do_literal_undefined_test() ->
    ChannelState = #{
        values_tokens => emqx_placeholder:preproc_tmpl(<<"update t set s = ${s}">>),
        channel_conf => #{}
    },
    ?assertEqual(
        {error, {unrecoverable_error, undefined_var}},
        emqx_bridge_dameng_connector:worker_do_literal(
            self(), ChannelState, #{}, #{resource_opts => #{}}
        )
    ).

worker_do_literal_error_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {error, <<"SQLSTATE IS: 42S02 table not found">>}
    end),
    try
        ?assertEqual(
            {error, {unrecoverable_error, {invalid_request, <<"table_not_found">>}}},
            emqx_bridge_dameng_connector:worker_do_literal(
                self(), literal_channel_state(), #{id => 1}, #{resource_opts => #{}}
            )
        )
    after
        meck:unload(emqx_odbc)
    end.

literal_channel_state() ->
    #{
        values_tokens => emqx_placeholder:preproc_tmpl(<<"select id from t where id = ${id}">>),
        channel_conf => #{}
    }.

%%------------------------------------------------------------------------------
%% do_get_status/1 (health check)
%%------------------------------------------------------------------------------

do_get_status_accepts_dameng_select_1_test() ->
    %% The DM8 driver returns `{selected, ["1"], [{1}]}' for `SELECT 1'
    %% (column name is `"1"', not `[]' like SQL Server).  The health check must
    %% accept that shape and report the connection as healthy.
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {selected, ["1"], [{1}]}
    end),
    try
        ?assertEqual(ok, emqx_bridge_dameng_connector:do_get_status(self()))
    after
        meck:unload(emqx_odbc)
    end.

do_get_status_rejects_bad_shape_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {selected, [], []}
    end),
    meck:expect(emqx_odbc, disconnect, fun(_Conn) -> ok end),
    try
        ?assertMatch(
            {error, #{cause := "unexpected_SELECT_1_result"}},
            emqx_bridge_dameng_connector:do_get_status(self())
        )
    after
        meck:unload(emqx_odbc)
    end.

%%------------------------------------------------------------------------------
%% ensure_odbcserver_executable/1
%%------------------------------------------------------------------------------

%% Making the `odbcserver' port program runnable is a best effort repair, but it
%% must actually happen when it can and must not crash when it cannot: either
%% failure mode is logged so that a later `odbc:connect/2' error stays
%% diagnosable.
ensure_odbcserver_executable_test() ->
    Dir = mktemp_dir(),
    try
        Bin = filename:join(Dir, "odbcserver"),
        ok = file:write_file(Bin, <<"#!/bin/sh\n">>),
        ok = file:change_mode(Bin, 8#644),
        ?assertEqual(ok, ensure_odbcserver_executable(Bin)),
        ?assertEqual({ok, 8#755}, file_mode(Bin)),
        %% An unreadable path is reported by `odbc:connect/2' itself, so the
        %% helper only logs and returns `ok' instead of failing the start.
        ?assertEqual(ok, ensure_odbcserver_executable(filename:join(Dir, "missing"))),
        ?assertEqual(ok, ensure_odbcserver_executable(filename:join(Dir, "missing/odbcserver")))
    after
        ok = file:del_dir_r(Dir)
    end.

ensure_odbcserver_executable(Path) ->
    emqx_bridge_dameng_connector:ensure_odbcserver_executable(Path).

file_mode(Path) ->
    {ok, #file_info{mode = Mode}} = file:read_file_info(Path),
    {ok, Mode band 8#777}.

mktemp_dir() ->
    Dir = filename:join(
        os:getenv("TMPDIR", "/tmp"),
        "emqx_bridge_dameng_tests_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    ok = file:make_dir(Dir),
    Dir.
