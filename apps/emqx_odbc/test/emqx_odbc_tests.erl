%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_odbc_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% build_conn_string/1
%%------------------------------------------------------------------------------

build_conn_string_dsn_test() ->
    ?assertEqual(
        "DSN=dm8;UID=SYSDBA;PWD=Abcd1234",
        emqx_odbc:build_conn_string(#{
            dsn => <<"dm8">>,
            username => <<"SYSDBA">>,
            password => emqx_secret:wrap(<<"Abcd1234">>)
        })
    ).

build_conn_string_dsn_no_secret_test() ->
    ?assertEqual(
        "DSN=dm8",
        emqx_odbc:build_conn_string(#{dsn => <<"dm8">>})
    ).

build_conn_string_dsn_blank_credentials_test() ->
    %% Blank attributes must be dropped instead of overriding the credentials
    %% carried by the DSN entry with empty values.
    ?assertEqual(
        "DSN=dm8",
        emqx_odbc:build_conn_string(#{
            dsn => <<"dm8">>,
            username => <<>>,
            password => emqx_secret:wrap(<<>>)
        })
    ).

build_conn_string_field_driver_name_test() ->
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=127.0.0.1:5236;UID=SYSDBA;PWD=Abcd1234;Charset=utf8",
        emqx_odbc:build_conn_string(#{
            server => <<"127.0.0.1">>,
            port => 5236,
            username => <<"SYSDBA">>,
            password => emqx_secret:wrap(<<"Abcd1234">>),
            driver => <<"DM8 ODBC DRIVER">>,
            charset => <<"utf8">>
        })
    ).

build_conn_string_field_driver_path_test() ->
    ?assertEqual(
        "Driver=/opt/dmdbms/bin/libdodbc.so;Server=localhost:5237;Charset=utf8",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            port => 5237,
            driver => <<"/opt/dmdbms/bin/libdodbc.so">>,
            charset => <<"utf8">>
        })
    ).

build_conn_string_driver_name_with_dot_test() ->
    %% A registered driver name may contain dots; only paths are used verbatim.
    ?assertEqual(
        "Driver={MySQL ODBC 8.0 Driver};Server=localhost:3306",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            port => 3306,
            driver => <<"MySQL ODBC 8.0 Driver">>
        })
    ).

build_conn_string_server_embedded_port_test() ->
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=127.0.0.1:5237",
        emqx_odbc:build_conn_string(#{
            server => <<"127.0.0.1:5237">>,
            port => 5236,
            driver => <<"DM8 ODBC DRIVER">>
        })
    ).

build_conn_string_no_port_test() ->
    %% Without a `port', the server is used as given: this module has no
    %% database specific default.
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=localhost",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            driver => <<"DM8 ODBC DRIVER">>
        })
    ).

build_conn_string_ipv6_test_() ->
    [
        {"bare ipv6 gets bracketed and the port appended",
            ?_assertEqual(
                "Driver={DM8 ODBC DRIVER};Server=[::1]:5236",
                emqx_odbc:build_conn_string(#{
                    server => <<"::1">>,
                    port => 5236,
                    driver => <<"DM8 ODBC DRIVER">>
                })
            )},
        {"bracketed ipv6 keeps an explicit port",
            ?_assertEqual(
                "Driver={DM8 ODBC DRIVER};Server=[::1]:5237",
                emqx_odbc:build_conn_string(#{
                    server => <<"[::1]:5237">>,
                    port => 5236,
                    driver => <<"DM8 ODBC DRIVER">>
                })
            )},
        {"bracketed ipv6 without port gets the configured port",
            ?_assertEqual(
                "Driver={DM8 ODBC DRIVER};Server=[::1]:5237",
                emqx_odbc:build_conn_string(#{
                    server => <<"[::1]">>,
                    port => 5237,
                    driver => <<"DM8 ODBC DRIVER">>
                })
            )}
    ].

build_conn_string_undefined_driver_test() ->
    %% An explicitly `undefined' driver is omitted instead of being rendered.
    ?assertEqual(
        "Server=127.0.0.1:5236",
        emqx_odbc:build_conn_string(#{
            server => <<"127.0.0.1">>,
            port => 5236,
            driver => undefined
        })
    ).

build_conn_string_null_password_test() ->
    ?assertEqual(
        "DSN=dm8",
        emqx_odbc:build_conn_string(#{dsn => <<"dm8">>, password => null})
    ).

%% Driver specific attributes (used by the DM8 driver for TLS) are appended
%% after the built-in ones; a secret value is only revealed here.
build_conn_string_extra_attrs_test() ->
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=localhost:5237;UID=SYSDBA;"
        "Charset=utf8;SSL_PATH=/opt/dmdbms/bin/client_ssl/SYSDBA;SSL_PWD=Abcd1234",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            port => 5237,
            driver => <<"DM8 ODBC DRIVER">>,
            username => <<"SYSDBA">>,
            charset => <<"utf8">>,
            extra_conn_attrs => [
                {"SSL_PATH", <<"/opt/dmdbms/bin/client_ssl/SYSDBA">>},
                {<<"SSL_PWD">>, emqx_secret:wrap(<<"Abcd1234">>)}
            ]
        })
    ).

build_conn_string_dsn_extra_attrs_test() ->
    %% Attributes may accompany a DSN: ODBC lets the connection string override
    %% the values carried by the DSN entry.
    ?assertEqual(
        "DSN=dm8;UID=SYSDBA;SSL_PATH=/opt/dmdbms/bin/client_ssl/SYSDBA",
        emqx_odbc:build_conn_string(#{
            dsn => <<"dm8">>,
            username => <<"SYSDBA">>,
            extra_conn_attrs => [{"SSL_PATH", <<"/opt/dmdbms/bin/client_ssl/SYSDBA">>}]
        })
    ).

build_conn_string_extra_attrs_blank_test() ->
    %% Blank values (including blank secrets) must be dropped, otherwise they
    %% would override a non-empty value carried by a DSN entry.
    ?assertEqual(
        "DSN=dm8",
        emqx_odbc:build_conn_string(#{
            dsn => <<"dm8">>,
            extra_conn_attrs => [
                {"SSL_PATH", <<>>},
                {"SSL_PWD", emqx_secret:wrap(<<>>)},
                {"EMPTY", undefined},
                {"NULL", null}
            ]
        })
    ).

build_conn_string_extra_attrs_none_test() ->
    ?assertEqual(
        "DSN=dm8",
        emqx_odbc:build_conn_string(#{dsn => <<"dm8">>, extra_conn_attrs => []})
    ).

%% A credential or a driver specific value may contain the `;' delimiter or a
%% brace. Such a value must be braced (and its closing braces doubled),
%% otherwise it would change the attribute boundaries of the connection string.
build_conn_string_escapes_delimiters_test() ->
    ?assertEqual(
        "Driver={DM8 ODBC DRIVER};Server=localhost:5236;UID={sy;dba};"
        "PWD={p}};w};SSL_PWD={ x }",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            port => 5236,
            driver => <<"DM8 ODBC DRIVER">>,
            username => <<"sy;dba">>,
            password => emqx_secret:wrap(<<"p};w">>),
            extra_conn_attrs => [{"SSL_PWD", <<" x ">>}]
        })
    ).

build_conn_string_escapes_registered_driver_brace_test() ->
    ?assertEqual(
        "Driver={My}}Driver};Server=localhost:5236",
        emqx_odbc:build_conn_string(#{
            server => <<"localhost">>,
            port => 5236,
            driver => <<"My}Driver">>
        })
    ).

%%------------------------------------------------------------------------------
%% connect/1 validation
%%------------------------------------------------------------------------------

connect_requires_dsn_or_driver_and_server_test() ->
    %% No connection is attempted: the config is rejected before `odbc:connect'.
    ?assertEqual(
        {error, {missing_conn_attribute, [driver, server]}},
        emqx_odbc:connect(#{})
    ),
    ?assertEqual(
        {error, {missing_conn_attribute, [driver]}},
        emqx_odbc:connect(#{server => <<"127.0.0.1">>})
    ),
    ?assertEqual(
        {error, {missing_conn_attribute, [server]}},
        emqx_odbc:connect(#{driver => <<"DM8 ODBC DRIVER">>})
    ).

%%------------------------------------------------------------------------------
%% to_param_type/1
%%------------------------------------------------------------------------------

to_param_type_test_() ->
    [
        ?_assertEqual(sql_integer, emqx_odbc:to_param_type(sql_integer)),
        ?_assertEqual(sql_smallint, emqx_odbc:to_param_type(sql_smallint)),
        ?_assertEqual({sql_varchar, 100}, emqx_odbc:to_param_type({sql_varchar, 100})),
        ?_assertEqual({sql_decimal, 10, 2}, emqx_odbc:to_param_type({sql_decimal, 10, 2})),
        ?_assertEqual({sql_float, 53}, emqx_odbc:to_param_type({sql_float, 53})),
        ?_assertEqual(sql_bit, emqx_odbc:to_param_type(sql_bit)),
        ?_assertEqual(sql_timestamp, emqx_odbc:to_param_type(sql_timestamp)),
        %% Coercible types map to a varchar binding
        ?_assertEqual({sql_varchar, 24}, emqx_odbc:to_param_type('SQL_BIGINT')),
        ?_assertEqual({sql_varchar, 16}, emqx_odbc:to_param_type('SQL_TYPE_DATE')),
        ?_assertEqual({sql_varchar, 16}, emqx_odbc:to_param_type('SQL_TYPE_TIME')),
        %% `param_query' has no binary binding; such columns must be rejected
        %% (a string binding would truncate the value at the first NUL byte).
        ?_assertEqual(
            {error, {unrecoverable_error, {unsupported_odbc_type, 'SQL_VARBINARY'}}},
            emqx_odbc:to_param_type('SQL_VARBINARY')
        ),
        ?_assertEqual(
            {error, {unrecoverable_error, {unsupported_odbc_type, 'SQL_LONGVARCHAR'}}},
            emqx_odbc:to_param_type('SQL_LONGVARCHAR')
        ),
        %% Large object columns report an unbounded size and cannot be bound
        %% safely, so they are rejected as well.
        ?_assertEqual(
            {error, {unrecoverable_error, {unsupported_odbc_type, sql_wlongvarchar}}},
            emqx_odbc:to_param_type({sql_wlongvarchar, 2147483647})
        ),
        %% Unsupported type -> error
        ?_assertEqual(
            {error, {unrecoverable_error, {unsupported_odbc_type, sql_unsupported_atom}}},
            emqx_odbc:to_param_type(sql_unsupported_atom)
        )
    ].

%%------------------------------------------------------------------------------
%% fit_param_type/2
%%------------------------------------------------------------------------------

fit_param_type_test_() ->
    Utf16 = fun(S) -> unicode:characters_to_binary(S, utf8, {utf16, little}) end,
    [
        {"the declared size is kept when it already fits the values",
            ?_assertEqual(
                {sql_varchar, 200},
                emqx_odbc:fit_param_type({sql_varchar, 200}, [<<"abc">>, <<"defg">>])
            )},
        {"null values do not affect the size",
            ?_assertEqual(
                {sql_varchar, 200},
                emqx_odbc:fit_param_type({sql_varchar, 200}, [null, <<"abc">>])
            )},
        {"a value that fills the column gets room for the NUL `odbc' appends",
            ?_assertEqual(
                {sql_varchar, 4},
                emqx_odbc:fit_param_type({sql_varchar, 3}, [<<"abc">>])
            )},
        {"a value longer than the declared size is fitted",
            ?_assertEqual(
                {sql_varchar, 5},
                emqx_odbc:fit_param_type({sql_varchar, 2}, [<<"abcd">>])
            )},
        {"char columns are sized in bytes",
            ?_assertEqual({sql_char, 8}, emqx_odbc:fit_param_type({sql_char, 8}, [<<"abc">>]))},
        {"wide columns are sized in UTF-16 code units",
            ?_assertEqual(
                {sql_wvarchar, 50},
                emqx_odbc:fit_param_type({sql_wvarchar, 50}, [Utf16(<<"abc">>)])
            )},
        {"wide values that fill the column get room for the NUL `odbc' appends",
            ?_assertEqual(
                {sql_wvarchar, 3},
                emqx_odbc:fit_param_type({sql_wvarchar, 2}, [Utf16(<<"abc">>)])
            )},
        {"non character types are kept as they are",
            ?_assertEqual(
                sql_integer,
                emqx_odbc:fit_param_type(sql_integer, [1, 2, 3])
            )}
    ].

%%------------------------------------------------------------------------------
%% to_odbc_value/2 & /3
%%------------------------------------------------------------------------------

to_odbc_value_test_() ->
    Utf16 = fun(S) -> unicode:characters_to_binary(S, utf8, {utf16, little}) end,
    [
        ?_assertEqual({ok, 123}, emqx_odbc:to_odbc_value(<<"123">>, sql_integer)),
        ?_assertEqual({ok, 112}, emqx_odbc:to_odbc_value(112, sql_smallint)),
        %% A float is accepted for an integer column only when it has no
        %% fractional part; otherwise it would be silently truncated (1.9 -> 1).
        ?_assertEqual({ok, 1}, emqx_odbc:to_odbc_value(1.0, sql_integer)),
        ?_assertEqual({ok, -2}, emqx_odbc:to_odbc_value(-2.0, sql_tinyint)),
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_value, 1.9}}},
            emqx_odbc:to_odbc_value(1.9, sql_integer)
        ),
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_value, 1.9}}},
            emqx_odbc:to_odbc_value(1.9, {sql_decimal, 5, 0})
        ),
        ?_assertEqual({ok, 1.5}, emqx_odbc:to_odbc_value(<<"1.5">>, {sql_float, 53})),
        ?_assertEqual({ok, true}, emqx_odbc:to_odbc_value(<<"true">>, sql_bit)),
        ?_assertEqual(
            {ok, {{2026, 1, 2}, {12, 34, 56}}},
            emqx_odbc:to_odbc_value(<<"2026-01-02 12:34:56">>, sql_timestamp)
        ),
        ?_assertEqual(
            {ok, {{2026, 1, 2}, {0, 0, 0}}},
            emqx_odbc:to_odbc_value(<<"2026-01-02">>, sql_timestamp)
        ),
        %% `sql_timestamp' has no sub-second field, so fractional seconds are
        %% rejected instead of being silently truncated.
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_timestamp, <<"2026-01-02 12:34:56.123">>}}},
            emqx_odbc:to_odbc_value(<<"2026-01-02 12:34:56.123">>, sql_timestamp)
        ),
        ?_assertEqual({ok, <<"hello">>}, emqx_odbc:to_odbc_value(<<"hello">>, {sql_varchar, 20})),
        %% Wide char columns are sent as UTF-16LE binaries; `odbc' appends the
        %% terminating NUL itself (`odbc:string_terminate_value/1').
        ?_assertEqual(
            {ok, Utf16(<<"abc">>)},
            emqx_odbc:to_odbc_value(<<"abc">>, {sql_wvarchar, 20})
        ),
        %% Non-character data cannot be encoded as a wide string: report an
        %% error instead of crashing.
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_value, {not_a_string, {sql_wvarchar, 20}}}}},
            emqx_odbc:to_odbc_value(123, {sql_wvarchar, 20})
        ),
        %% Binary columns are not bindable by `param_query'.
        ?_assertMatch(
            {error, {unrecoverable_error, {unsupported_odbc_type, 'SQL_BINARY', _}}},
            emqx_odbc:to_odbc_value(<<1, 0, 2>>, 'SQL_BINARY')
        ),
        %% Coercible types pass the string representation
        ?_assertEqual(
            {ok, <<"9999999999">>}, emqx_odbc:to_odbc_value(<<"9999999999">>, 'SQL_BIGINT')
        ),
        ?_assertEqual(
            {ok, <<"2026-01-02">>}, emqx_odbc:to_odbc_value(<<"2026-01-02">>, 'SQL_TYPE_DATE')
        ),
        %% Defensive: a driver may report TIMESTAMP as 'SQL_TYPE_TIMESTAMP'
        ?_assertEqual(
            {ok, {{2026, 1, 2}, {12, 34, 56}}},
            emqx_odbc:to_odbc_value(<<"2026-01-02 12:34:56">>, 'SQL_TYPE_TIMESTAMP')
        ),
        %% Unsupported type -> error
        ?_assertMatch(
            {error, {unrecoverable_error, {unsupported_odbc_type, _, _}}},
            emqx_odbc:to_odbc_value(<<"x">>, sql_unsupported_atom)
        ),
        %% undefined handling
        ?_assertEqual({ok, null}, emqx_odbc:to_odbc_value(undefined, sql_integer, true)),
        ?_assertEqual(
            {error, {unrecoverable_error, undefined_var}},
            emqx_odbc:to_odbc_value(undefined, sql_integer, false)
        ),
        ?_assertEqual({ok, null}, emqx_odbc:to_odbc_value(null, sql_integer))
    ].

%% Values which `odbc' cannot bind faithfully are rejected instead of being
%% silently truncated: character parameters are bound as NUL terminated strings
%% and the parameter buffer is sized from the column definition.
to_odbc_value_bounds_test_() ->
    [
        ?_assertEqual(
            {ok, <<"abc">>},
            emqx_odbc:to_odbc_value(<<"abc">>, {sql_varchar, 3})
        ),
        ?_assertEqual(
            {error,
                {unrecoverable_error, {invalid_value, {value_too_long, {sql_varchar, 3}, 3, 4}}}},
            emqx_odbc:to_odbc_value(<<"abcd">>, {sql_varchar, 3})
        ),
        ?_assertEqual(
            {error, {unrecoverable_error, {invalid_value, {value_too_long, 'SQL_BIGINT', 24, 30}}}},
            emqx_odbc:to_odbc_value(binary:copy(<<"1">>, 30), 'SQL_BIGINT')
        ),
        ?_assertEqual(
            {error,
                {unrecoverable_error, {invalid_value, {nul_byte_in_string, {sql_varchar, 10}}}}},
            emqx_odbc:to_odbc_value(<<"a", 0, "b">>, {sql_varchar, 10})
        ),
        ?_assertEqual(
            {error,
                {unrecoverable_error, {invalid_value, {nul_byte_in_string, {sql_wvarchar, 10}}}}},
            emqx_odbc:to_odbc_value(<<"a", 0, "b">>, {sql_wvarchar, 10})
        ),
        %% A wide value is measured in characters, not in UTF-16 bytes.
        ?_assertMatch(
            {ok, _},
            emqx_odbc:to_odbc_value(<<"abc">>, {sql_wvarchar, 3})
        ),
        ?_assertMatch(
            {error, {unrecoverable_error, {invalid_value, {value_too_long, _, 3, 4}}}},
            emqx_odbc:to_odbc_value(<<"abcd">>, {sql_wvarchar, 3})
        )
    ].

%% `odbc' binds DECIMAL/NUMERIC parameters according to the precision and scale
%% (see `map_dec_num_2_c_column/3' in `odbcserver.c'); the Erlang value must
%% match the binding, otherwise the driver writes uninitialized memory into the
%% column.
to_odbc_value_decimal_test_() ->
    [
        ?_assertEqual({ok, 42}, emqx_odbc:to_odbc_value(<<"42">>, {sql_decimal, 5, 0})),
        ?_assertEqual({ok, 42}, emqx_odbc:to_odbc_value(<<"42">>, {sql_numeric, 9, 0})),
        ?_assertEqual({ok, 1.5}, emqx_odbc:to_odbc_value(<<"1.5">>, {sql_decimal, 10, 2})),
        ?_assertEqual({ok, 1.5}, emqx_odbc:to_odbc_value(<<"1.5">>, {sql_decimal, 15, 0})),
        ?_assertEqual({ok, 1.5}, emqx_odbc:to_odbc_value(<<"1.5">>, {sql_numeric, 15, 4})),
        %% precision >= 16 is bound as a NUL terminated string (`odbc' does not
        %% terminate decimal parameters itself, but passes them as `SQL_NTS')
        ?_assertEqual(
            {ok, <<"12345678901234567890.12", 0>>},
            emqx_odbc:to_odbc_value(<<"12345678901234567890.12">>, {sql_decimal, 38, 2})
        ),
        ?_assertEqual(
            {ok, <<"12345678901234567890", 0>>},
            emqx_odbc:to_odbc_value(12345678901234567890, {sql_numeric, 20, 0})
        )
    ].

%%------------------------------------------------------------------------------
%% classify_error/1
%%------------------------------------------------------------------------------

%% With `extended_errors' enabled (which emqx_odbc always turns on) `odbc'
%% reports the reason as `{SqlState, NativeError, Message}'.
classify_error_test_() ->
    [
        ?_assertEqual(
            {recoverable_error, <<"connection_closed">>},
            emqx_odbc:classify_error(<<"connection_closed">>)
        ),
        ?_assertEqual(
            {recoverable_error, <<"connection_closed">>},
            emqx_odbc:classify_error(connection_closed)
        ),
        ?_assertEqual(
            {recoverable_error, <<"connection_closed">>},
            emqx_odbc:classify_error(
                {"08S01", -1, "Communication link failure"}
            )
        ),
        ?_assertEqual(
            {recoverable_error, <<"connection_closed">>},
            emqx_odbc:classify_error(
                {<<"08006">>, -1, <<"connection failure">>}
            )
        ),
        ?_assertEqual(
            {recoverable_error, <<"connection_closed">>},
            emqx_odbc:classify_error(<<"[ODBC][SQL Server] broken connection SQLSTATE IS: 08S01">>)
        ),
        ?_assertEqual(
            {recoverable_error, <<"timeout">>},
            emqx_odbc:classify_error(timeout)
        ),
        ?_assertEqual(
            {recoverable_error, <<"timeout">>},
            emqx_odbc:classify_error({"HYT00", 0, "Timeout expired"})
        ),
        ?_assertEqual(
            {unrecoverable_error, {invalid_request, <<"table_not_found">>}},
            emqx_odbc:classify_error({"42S02", -1, "table or view not found"})
        ),
        ?_assertEqual(
            {unrecoverable_error, {invalid_request, <<"table_not_found">>}},
            emqx_odbc:classify_error(<<"SQLSTATE IS: 42S02 table not found">>)
        ),
        ?_assertMatch(
            {unrecoverable_error, {invalid_request, _}},
            emqx_odbc:classify_error({"42000", -1, "syntax error near X"})
        ),
        %% Regression: `HY000' is the generic "general error" SQLSTATE. A data
        %% error carrying it must stay unrecoverable, otherwise the resource
        %% would retry a permanently failing request forever.
        ?_assertMatch(
            {unrecoverable_error, {invalid_request, _}},
            emqx_odbc:classify_error({"HY000", -1, "invalid data type conversion"})
        ),
        %% Regression: a SQLSTATE-like sequence inside the driver message must
        %% not be mistaken for the statement's SQLSTATE.
        ?_assertMatch(
            {unrecoverable_error, {invalid_request, _}},
            emqx_odbc:classify_error({"22003", -1, "numeric value 108006 out of range"})
        )
    ].

%%------------------------------------------------------------------------------
%% Error classification predicates
%%------------------------------------------------------------------------------

is_connection_closed_test() ->
    ?assert(emqx_odbc:is_connection_closed_error(<<"connection_closed">>)),
    ?assertNot(emqx_odbc:is_connection_closed_error(<<"SELECT 1">>)),
    %% Regression: must NOT match a bare `closed' or the config error `IM002'.
    ?assertNot(emqx_odbc:is_connection_closed_error(<<"Connection is closed">>)),
    ?assertNot(
        emqx_odbc:is_connection_closed_error(
            <<"IM002 data source name not found">>
        )
    ).

is_connection_broken_test() ->
    ?assert(emqx_odbc:is_connection_broken_error(<<"SQLSTATE IS: 08S01">>)),
    ?assert(emqx_odbc:is_connection_broken_error(<<"SQLSTATE IS: 08006">>)),
    ?assert(emqx_odbc:is_connection_broken_error(<<"SQLSTATE IS: IMC01">>)),
    ?assert(emqx_odbc:is_connection_broken_error({"08S01", -1, "broken"})),
    ?assertNot(emqx_odbc:is_connection_broken_error(<<"SQLSTATE IS: HY000">>)),
    ?assertNot(emqx_odbc:is_connection_broken_error(<<"SQLSTATE IS: 42S02">>)),
    %% The SQLSTATE is read from the state, not from the message text.
    ?assertNot(emqx_odbc:is_connection_broken_error({"42000", -1, "value 08006 rejected"})).

is_timeout_test() ->
    ?assert(emqx_odbc:is_timeout_error(timeout)),
    ?assert(emqx_odbc:is_timeout_error(<<"timeout">>)),
    ?assert(emqx_odbc:is_timeout_error({"HYT00", 0, "Timeout expired"})),
    ?assertNot(emqx_odbc:is_timeout_error({"42000", 0, "timeout"})),
    ?assertNot(emqx_odbc:is_timeout_error(<<"table T_ABC not exist">>)).

is_table_not_found_test() ->
    ?assert(emqx_odbc:is_table_not_found_error(<<"SQLSTATE IS: 42S02">>)),
    ?assert(emqx_odbc:is_table_not_found_error(<<"table T_ABC not exist">>)),
    ?assert(emqx_odbc:is_table_not_found_error({"42S02", -1, "table or view not found"})),
    ?assertNot(emqx_odbc:is_table_not_found_error(<<"invalid column">>)).
