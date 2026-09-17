%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dameng_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("kernel/include/file.hrl").

-include_lib("emqx_bridge_dameng/include/emqx_bridge_dameng.hrl").

-define(INSERT_ONLY, <<"Only INSERT statements are supported">>).

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
            )},
        {"the port must be within 1..65535",
            ?_assertException(
                throw,
                {_, [_ | _]},
                parse_and_check_connector(connector_config(#{<<"port">> => 0}))
            )},
        {"an out of range port is rejected",
            ?_assertException(
                throw,
                {_, [_ | _]},
                parse_and_check_connector(connector_config(#{<<"port">> => 70000}))
            )},
        {"the largest valid port is accepted",
            ?_assertMatch(
                #{<<"port">> := 65535},
                parse_and_check_connector(connector_config(#{<<"port">> => 65535}))
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

action_schema_rejects_non_insert_test_() ->
    [
        ?_assertException(
            throw,
            {emqx_bridge_v2_schema, [#{kind := validation_error, reason := ?INSERT_ONLY}]},
            emqx_bridge_v2_testlib:parse_and_check(action, <<"dameng">>, <<"act">>, #{
                <<"connector">> => <<"conn">>,
                <<"parameters">> => #{<<"sql">> => SQL}
            })
        )
     || SQL <- non_insert_templates()
    ].

sql_schema_validation_test_() ->
    [
        {Name, fun() ->
            Schema = proplists:get_value(sql, emqx_bridge_dameng:fields(Field)),
            Validate = maps:get(validator, Schema),
            ?assertEqual(ok, Validate(?DEFAULT_INSERT)),
            ?assertEqual({error, ?INSERT_ONLY}, Validate(<<"SELECT 1">>)),
            ?assertEqual(
                {error, insert_must_specify_columns},
                Validate(<<"INSERT INTO t VALUES (${id})">>)
            )
        end}
     || {Name, Field} <- [{"action SQL", action_parameters}, {"legacy SQL", "config"}]
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

%% again, it's better to avoid tests such as these that manually construct configs and
%% connector states........
action_resource_opts_atom_keys() ->
    RawOpts = emqx_bridge_v2_testlib:common_action_resource_opts(),
    Schema = #{roots => [{x, hoconsc:mk(hoconsc:ref(emqx_resource_schema, "creation_opts"))}]},
    {ok, #{x := #{} = ResourceOpts}} = emqx_hocon:check(Schema, #{~"x" => RawOpts}),
    ResourceOpts.

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

parse_sql_template_rejects_non_insert_test_() ->
    [
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_request, ?INSERT_ONLY}}},
            emqx_bridge_dameng_connector:parse_sql_template(#{sql => SQL})
        )
     || SQL <- non_insert_templates()
    ].

non_insert_templates() ->
    [
        <<"select id from t where id = ${id}">>,
        <<" UPDATE t SET id = ${id}">>,
        <<"\nDeLeTe FROM t WHERE id = ${id}">>,
        <<"MERGE INTO t USING s ON t.id = s.id">>,
        <<"DROP TABLE t">>,
        <<"CREATE TABLE t(id INT)">>,
        <<"INSERTED INTO t(id) VALUES (${id})">>,
        <<"WITH t AS (SELECT 1) SELECT * FROM t">>,
        <<"unknown">>,
        <<>>,
        <<" \t\n">>
    ].

parse_sql_template_insert_case_test_() ->
    [
        ?_assertMatch(
            {ok, #{statement_type := insert, insert_columns := [<<"id">>]}},
            emqx_bridge_dameng_connector:parse_sql_template(#{sql => SQL})
        )
     || SQL <- [
            <<"insert into t(id) values (${id})">>,
            <<"INSERT INTO t(id) VALUES (${id})">>,
            <<" \t\nInSeRt InTo t(id) VaLuEs (${id})">>
        ]
    ].

parse_sql_template_missing_test() ->
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_request, missing_sql}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{})
    ).

parse_sql_template_multiple_statements_test() ->
    ?assertEqual(
        {error,
            {unrecoverable_error,
                {invalid_request, <<"Not an INSERT statement or incorrect SQL syntax">>}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<"INSERT INTO t(id) VALUES (${id}); DELETE FROM t">>
        })
    ).

parse_sql_template_insert_select_test() ->
    %% `INSERT ... SELECT' has no VALUES part, so it is not a supported template.
    ?assertEqual(
        {error,
            {unrecoverable_error,
                {invalid_request, <<"Not an INSERT statement or incorrect SQL syntax">>}}},
        emqx_bridge_dameng_connector:parse_sql_template(#{
            sql => <<"INSERT INTO t(id) SELECT id FROM other_table">>
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

%% A JSON object or array in the message must be stored as JSON text: maps and
%% non-string lists must not reach the driver as raw Erlang terms.
build_param_params_structured_values_test() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t(id, j) values ( ${id}, ${j} )">>
    }),
    ColTypes = [sql_integer, {sql_varchar, 100}],
    Tokens = maps:get(values_tokens, Ch),
    Msgs = [
        #{id => 1, j => #{<<"a">> => 1}},
        #{id => 2, j => [<<"x">>, <<"y">>]},
        #{id => 3, j => <<"plain">>}
    ],
    ?assertEqual(
        {ok, [
            {sql_integer, [1, 2, 3]},
            {{sql_varchar, 100}, [<<"{\"a\":1}">>, <<"[\"x\",\"y\"]">>, <<"plain">>]}
        ]},
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
            },
            resource_opts => action_resource_opts_atom_keys()
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
            parameters => #{sql => <<"insert into t(id, topic) values ( ${id}, ${topic} )">>},
            resource_opts => action_resource_opts_atom_keys()
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

on_add_channel_uppercase_insert_test() ->
    %% Regression: `extract_table/1' matched `insert into' case sensitively, so
    %% an upper case template was rejected as `table_not_found' even though
    %% `get_statement_type/1' and `split_insert/1' accept any case.
    Self = self(),
    meck:new(ecpool, [passthrough]),
    meck:expect(ecpool, pick_and_do, fun(_Pool, {_Mod, worker_describe, [Table, _T]}, _Mode) ->
        Self ! {described, Table},
        {ok, [{<<"ID">>, sql_integer}, {<<"TOPIC">>, {sql_varchar, 100}}]}
    end),
    try
        State = #{pool_name => <<"p">>, installed_channels => #{}, resource_opts => #{}},
        ChannelConfig = #{
            parameters => #{
                sql => <<"INSERT INTO T_MQTT_MSG(id, topic) VALUES ( ${id}, ${topic} )">>
            },
            resource_opts => action_resource_opts_atom_keys()
        },
        ?assertMatch(
            {ok, _},
            emqx_bridge_dameng_connector:on_add_channel(<<"i">>, State, <<"c">>, ChannelConfig)
        ),
        receive
            {described, Table} -> ?assertEqual(<<"T_MQTT_MSG">>, Table)
        after 1000 -> error(describe_not_called)
        end
    after
        meck:unload(ecpool)
    end.

%%------------------------------------------------------------------------------
%% INSERT-only execution and parameter binding
%%------------------------------------------------------------------------------

on_add_channel_rejects_non_insert_test() ->
    meck:new(ecpool, [passthrough]),
    try
        State = #{pool_name => <<"p">>, installed_channels => #{}, resource_opts => #{}},
        lists:foreach(
            fun(SQL) ->
                ?assertEqual(
                    {error, {unrecoverable_error, {invalid_request, ?INSERT_ONLY}}},
                    emqx_bridge_dameng_connector:on_add_channel(
                        <<"i">>, State, <<"c">>, #{
                            parameters => #{sql => SQL},
                            resource_opts => action_resource_opts_atom_keys()
                        }
                    )
                )
            end,
            non_insert_templates()
        ),
        ?assertNot(meck:called(ecpool, pick_and_do, '_'))
    after
        meck:unload(ecpool)
    end.

legacy_non_insert_execution_is_rejected_test() ->
    meck:new(ecpool, [passthrough]),
    meck:new(emqx_odbc, [passthrough]),
    try
        lists:foreach(
            fun(Type) ->
                Ch = #{statement_type => Type, values_tokens => [], channel_conf => #{}},
                State = #{pool_name => <<"p">>, installed_channels => #{<<"c">> => Ch}},
                Error = {error, {unrecoverable_error, {invalid_request, ?INSERT_ONLY}}},
                ?assertEqual(
                    Error,
                    emqx_bridge_dameng_connector:on_query(
                        <<"r">>, {<<"c">>, #{}}, State
                    )
                ),
                ?assertEqual(
                    Error,
                    emqx_bridge_dameng_connector:on_batch_query(
                        <<"r">>, [{<<"c">>, #{}}, {<<"c">>, #{}}], State
                    )
                ),
                ?assertEqual(
                    Error,
                    emqx_bridge_dameng_connector:worker_do_literal(
                        self(), Ch, #{}, State
                    )
                ),
                ?assertEqual(
                    Error,
                    emqx_bridge_dameng_connector:worker_do_literal_batch(
                        self(), Ch, [#{}, #{}], State
                    )
                )
            end,
            [select, update, delete]
        ),
        ?assertNot(meck:called(ecpool, pick_and_do, '_')),
        ?assertNot(meck:called(emqx_odbc, sql_query, '_')),
        ?assertNot(meck:called(emqx_odbc, param_query, '_'))
    after
        meck:unload([ecpool, emqx_odbc])
    end.

insert_execution_binds_values_test() ->
    Ch = insert_channel_state(),
    Payloads = [<<"a'b\\c">>, <<"'); DELETE FROM t; --">>, <<"${id} /* comment */">>],
    Msgs = [#{id => N, payload => P} || {N, P} <- lists:zip([1, 2, 3], Payloads)],
    State = #{
        pool_name => <<"p">>,
        installed_channels => #{<<"c">> => Ch},
        resource_opts => #{request_ttl => 4321}
    },
    meck:new(ecpool, [passthrough]),
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(ecpool, pick_and_do, fun(<<"p">>, {Mod, Fun, Args}, handover) ->
        ?assertEqual(emqx_bridge_dameng_connector, Mod),
        ?assertEqual(worker_do_insert, Fun),
        apply(Mod, Fun, [self() | Args])
    end),
    meck:expect(emqx_odbc, param_query, fun(_Conn, SQL, Params, Timeout) ->
        ?assertEqual(<<"insert into t(id, payload) values (?, ?)">>, SQL),
        ?assertEqual(4321, Timeout),
        ?assertMatch([{sql_integer, _}, {{sql_varchar, 200}, _}], Params),
        {updated, length(element(2, hd(Params)))}
    end),
    try
        lists:foreach(
            fun(Msg) ->
                ?assertEqual(
                    ok,
                    emqx_bridge_dameng_connector:on_query(
                        <<"r">>, {<<"c">>, Msg}, State
                    )
                )
            end,
            Msgs
        ),
        ?assertEqual(
            ok,
            emqx_bridge_dameng_connector:on_batch_query(
                <<"r">>, [{<<"c">>, Msg} || Msg <- Msgs], State
            )
        ),
        Bound = [
            Params
         || {_, {emqx_odbc, param_query, [_, _, Params, _]}, _} <-
                meck:history(emqx_odbc)
        ],
        ?assertEqual(
            [
                [{sql_integer, [N]}, {{sql_varchar, 200}, [P]}]
             || {N, P} <- lists:zip([1, 2, 3], Payloads)
            ] ++
                [[{sql_integer, [1, 2, 3]}, {{sql_varchar, 200}, Payloads}]],
            Bound
        ),
        ?assertNot(meck:called(emqx_odbc, sql_query, '_')),
        ?assert(meck:validate([ecpool, emqx_odbc]))
    after
        meck:unload([ecpool, emqx_odbc])
    end.

insert_channel_state() ->
    {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{
        sql => <<"insert into t(id, payload) values (${id}, ${payload})">>
    }),
    Ch#{column_types => [sql_integer, {sql_varchar, 200}]}.

%% `odbc' exits with `timeout' when the request TTL elapses (`odbc:call/3');
%% that is a transient failure and must stay recoverable so the buffered
%% messages are retried instead of dropped.
worker_do_insert_timeout_is_recoverable_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, param_query, fun(_Conn, _SQL, _Params, _Timeout) ->
        exit(timeout)
    end),
    try
        {ok, Ch} = emqx_bridge_dameng_connector:parse_sql_template(#{sql => ?DEFAULT_INSERT}),
        ChannelState = Ch#{
            column_types => [sql_integer, {sql_varchar, 100}, sql_tinyint, {sql_varchar, 200}]
        },
        Msgs = [#{id => 1, topic => <<"t/1">>, qos => 1, payload => <<"p1">>}],
        ?assertEqual(
            {error, {recoverable_error, <<"timeout">>}},
            emqx_bridge_dameng_connector:worker_do_insert(
                self(), ChannelState, Msgs, #{resource_opts => #{}}
            )
        )
    after
        meck:unload(emqx_odbc)
    end.

worker_do_insert_driver_error_test_() ->
    [
        {binary_to_list(DriverError), fun() ->
            meck:new(emqx_odbc, [passthrough]),
            meck:expect(emqx_odbc, param_query, fun(_, _, _, _) -> {error, DriverError} end),
            try
                ?assertEqual(
                    {error, Expected},
                    emqx_bridge_dameng_connector:worker_do_insert(
                        self(),
                        insert_channel_state(),
                        [#{id => 1, payload => <<"p">>}],
                        #{resource_opts => #{}}
                    )
                )
            after
                meck:unload(emqx_odbc)
            end
        end}
     || {DriverError, Expected} <- [
            {<<"SQLSTATE IS: 42S02 table not found">>,
                {unrecoverable_error, {invalid_request, <<"table_not_found">>}}},
            {<<"SQLSTATE IS: 08S01 connection broken">>,
                {recoverable_error, <<"connection_closed">>}}
        ]
    ].

worker_do_insert_null_policy_test() ->
    Ch = insert_channel_state(),
    State = #{resource_opts => #{}},
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, param_query, fun(_, _, Params, _) ->
        ?assertEqual([{sql_integer, [1]}, {{sql_varchar, 200}, [null]}], Params),
        {updated, 1}
    end),
    try
        %% Explicit JSON null is a value, even when missing variables are errors.
        ?assertEqual(
            ok,
            emqx_bridge_dameng_connector:worker_do_insert(
                self(), Ch, [#{id => 1, payload => null}], State
            )
        ),
        ?assertEqual(
            {error, {unrecoverable_error, undefined_var}},
            emqx_bridge_dameng_connector:worker_do_insert(self(), Ch, [#{id => 1}], State)
        ),
        ?assertEqual(1, meck:num_calls(emqx_odbc, param_query, '_')),
        NullableCh = Ch#{channel_conf => #{undefined_vars_as_null => true}},
        ?assertEqual(
            ok,
            emqx_bridge_dameng_connector:worker_do_insert(
                self(), NullableCh, [#{id => 1}], State
            )
        ),
        ?assertEqual(2, meck:num_calls(emqx_odbc, param_query, '_')),
        ?assert(meck:validate(emqx_odbc))
    after
        meck:unload(emqx_odbc)
    end.

worker_do_insert_crash_is_unrecoverable_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, param_query, fun(_, _, _, _) -> error(boom) end),
    try
        ?assertEqual(
            {error, {unrecoverable_error, {invalid_request, boom}}},
            emqx_bridge_dameng_connector:worker_do_insert(
                self(),
                insert_channel_state(),
                [#{id => 1, payload => <<"p">>}],
                #{resource_opts => #{}}
            )
        )
    after
        meck:unload(emqx_odbc)
    end.

%%------------------------------------------------------------------------------
%% do_get_status/2 (health check)
%%------------------------------------------------------------------------------

do_get_status_accepts_dameng_select_1_test() ->
    %% The DM8 driver returns `{selected, ["1"], [{1}]}' for `SELECT 1'
    %% (column name is `"1"', not `[]' like SQL Server).  The health check must
    %% accept that shape and report the connection as healthy.
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {selected, ["1"], [{1}]}
    end),
    Timeout = 15_000,
    try
        ?assertEqual(ok, emqx_bridge_dameng_connector:do_get_status(self(), Timeout))
    after
        meck:unload(emqx_odbc)
    end.

do_get_status_rejects_bad_shape_test() ->
    meck:new(emqx_odbc, [passthrough]),
    meck:expect(emqx_odbc, sql_query, fun(_Conn, _SQL, _Timeout) ->
        {selected, [], []}
    end),
    meck:expect(emqx_odbc, disconnect, fun(_Conn) -> ok end),
    Timeout = 15_000,
    try
        ?assertMatch(
            {error, #{cause := "unexpected_SELECT_1_result"}},
            emqx_bridge_dameng_connector:do_get_status(self(), Timeout)
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
