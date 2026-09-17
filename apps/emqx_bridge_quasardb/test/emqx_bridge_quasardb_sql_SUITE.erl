%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_quasardb_sql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_compile_and_render(_Config) ->
    SQL =
        ~b"""
    insert into "mqtt table"("$timestamp", qos, payload, created)
    values (now(), ${qos}, 'pre:${payload}:post', ${created});
    """,
    {ok, Plan} = emqx_sql_plan:compile(emqx_bridge_quasardb_sql, SQL),
    Data = #{
        qos => 1,
        payload => <<"a'b\\c">>,
        created => <<"2026-03-09T11:38:55.123456789Z">>
    },
    ?assertEqual(
        <<
            "INSERT INTO \"mqtt table\" (\"$timestamp\", qos, payload, created) VALUES "
            "(now(), 1, 'pre:a\\'b\\\\c:post', 2026-03-09T11:38:55.123456789Z)"
        >>,
        rendered(emqx_sql_plan:render(Plan, Data, render_opts()))
    ).

t_batch_and_errors(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, v, text) VALUES (now, ${v}, '${v}')">>
    ),
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, v, text) VALUES "
            "(now, 1, '1'), (now, 'x\\'y\\\\', 'x\\'y\\\\')"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render_batch(
                Plan,
                [#{v => 1}, #{v => <<"x'y\\">>}],
                render_opts()
            )
        )
    ),
    ExpectedReason =
        {invalid_sql_template_value, #{
            placeholder => "v",
            reason => {error, nul_character_not_allowed}
        }},
    ?assertEqual(
        {error,
            {quasardb_template_render_failed, #{
                batch_index => 2,
                reason => ExpectedReason
            }}},
        emqx_bridge_quasardb_sql:render_batch(
            Plan,
            [#{v => <<"ok">>}, #{v => <<"bad", 0, "value">>}],
            render_opts()
        )
    ).

t_string_context_and_escaping(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<
            "INSERT INTO t ($timestamp, a, b, c) VALUES "
            "(now(), '${$}{amount}', '\\${ignored}', 'x${v}y')"
        >>
    ),
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, a, b, c) VALUES "
            "(now(), '${amount}', '${ignored}', 'xa\\'b\\\\cy')"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render(
                Plan,
                #{amount => {must_not_resolve}, ignored => {must_not_resolve}, v => <<"a'b\\c">>},
                render_opts()
            )
        )
    ).

t_value_conversions(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<
            "INSERT INTO t ($timestamp, missing, nullv, boolv, mapv, listv, floatv) "
            "VALUES (now(), ${missing}, ${nullv}, ${boolv}, ${mapv}, ${listv}, ${floatv})"
        >>
    ),
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, missing, nullv, boolv, mapv, listv, floatv) VALUES "
            "(now(), NULL, NULL, 'true', '{\"n\":2}', '[1,2]', 1.25)"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render(
                Plan,
                #{nullv => null, boolv => true, mapv => #{n => 2}, listv => [1, 2], floatv => 1.25},
                render_opts()
            )
        )
    ),
    {ok, MissingPlan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), ${missing})">>
    ),
    ?assertEqual(
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 'undefined')">>,
        rendered(
            emqx_bridge_quasardb_sql:render(
                MissingPlan,
                #{},
                #{undefined_vars_as_null => false}
            )
        )
    ),
    {ok, QuotedPlan} = emqx_bridge_quasardb_sql:compile(
        <<
            "INSERT INTO t ($timestamp, missing, nullv, mixed) "
            "VALUES (now(), '${missing}', '${nullv}', 'pre:${missing}')"
        >>
    ),
    ?assertEqual(
        <<"INSERT INTO t ($timestamp, missing, nullv, mixed) VALUES (now(), NULL, NULL, 'pre:null')">>,
        rendered(emqx_bridge_quasardb_sql:render(QuotedPlan, #{nullv => null}, render_opts()))
    ),
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, missing, nullv, mixed) VALUES "
            "(now(), 'undefined', NULL, 'pre:undefined')"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render(
                QuotedPlan,
                #{nullv => null},
                #{undefined_vars_as_null => false}
            )
        )
    ).

t_timestamp_forms(_Config) ->
    Accepted = [
        <<"2018">>,
        <<"2018-01-01">>,
        <<"2018-01-01T03:00">>,
        <<"2018-01-01T03:00Z">>,
        <<"2018-01-01T03:00:00">>,
        <<"2018-01-01t03:00:00z">>,
        <<"2018-01-01T03:00:00.1Z">>,
        <<"2018-01-01T03:00:00.123456789Z">>,
        <<"now">>,
        <<"now()">>,
        <<"today">>,
        <<"today()">>,
        <<"yesterday">>,
        <<"yesterday()">>,
        <<"tomorrow">>,
        <<"tomorrow()">>,
        <<"epoch">>,
        <<"epoch()">>,
        <<"end_of_time">>,
        <<"end_of_time()">>
    ],
    lists:foreach(
        fun(Value) ->
            ?assertMatch({ok, _}, emqx_bridge_quasardb_sql:compile(value_sql(Value)), Value)
        end,
        Accepted
    ),
    Rejected = [
        <<"2018-01">>,
        <<"2018-01-01T03">>,
        <<"2018-01-01T03:00:00.">>,
        <<"2018-01-01T03:00:00.1234567890Z">>,
        <<"2018-01-01T03:00:00+01:00">>
    ],
    lists:foreach(
        fun(Value) ->
            ?assertMatch({error, _}, emqx_bridge_quasardb_sql:compile(value_sql(Value)), Value)
        end,
        Rejected
    ),
    {ok, DynamicPlan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, v) VALUES (${v}, '${v}')">>
    ),
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, v) VALUES "
            "(2026-03-09T11:38:55Z, '2026-03-09T11:38:55Z')"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render(
                DynamicPlan,
                #{v => <<"2026-03-09T11:38:55Z">>},
                render_opts()
            )
        )
    ),
    ?assertEqual(
        <<"INSERT INTO t ($timestamp, v) VALUES (EpOcH(), 'EpOcH()')">>,
        rendered(
            emqx_bridge_quasardb_sql:render(DynamicPlan, #{v => <<"EpOcH()">>}, render_opts())
        )
    ),
    Attack = <<"2026-01-01),('owned'); DROP TABLE t">>,
    ?assertEqual(
        <<
            "INSERT INTO t ($timestamp, v) VALUES "
            "('2026-01-01),(\\'owned\\'); DROP TABLE t', "
            "'2026-01-01),(\\'owned\\'); DROP TABLE t')"
        >>,
        rendered(
            emqx_bridge_quasardb_sql:render(DynamicPlan, #{v => Attack}, render_opts())
        )
    ),
    ?assertEqual(
        <<"INSERT INTO t ($timestamp, v) VALUES ('now\n', 'now\n')">>,
        rendered(
            emqx_bridge_quasardb_sql:render(DynamicPlan, #{v => <<"now\n">>}, render_opts())
        )
    ).

t_float_precision(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), ${v})">>
    ),
    lists:foreach(
        fun(Value) ->
            Encoded = float_to_binary(Value, [short]),
            ?assertEqual(
                <<"INSERT INTO t ($timestamp, v) VALUES (now(), ", Encoded/binary, ")">>,
                rendered(emqx_bridge_quasardb_sql:render(Plan, #{v => Value}, render_opts()))
            )
        end,
        [1.0e-12, 1.23456789012345, 1.7976931348623157e308, 5.0e-324]
    ).

t_identifier_and_whitespace(_Config) ->
    SQL = <<
        "InSeRt",
        11,
        "InTo \"table name\"(\"$timestamp\", \"value-name\")",
        12,
        "VaLuEs(now, 1);"
    >>,
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(SQL),
    ?assertEqual(
        <<"INSERT INTO \"table name\" (\"$timestamp\", \"value-name\") VALUES (now, 1)">>,
        rendered(emqx_bridge_quasardb_sql:render(Plan, #{}, render_opts()))
    ),
    {ok, KeywordCasePlan} = emqx_bridge_quasardb_sql:compile(
        <<"InSeRt InTo VaLuEs ($timestamp, NuLl) VaLuEs (NoW(), 1)">>
    ),
    ?assertEqual(
        <<"INSERT INTO VaLuEs ($timestamp, NuLl) VALUES (NoW(), 1)">>,
        rendered(emqx_bridge_quasardb_sql:render(KeywordCasePlan, #{}, render_opts()))
    ),
    lists:foreach(
        fun(Identifier) ->
            KeywordSQL = <<
                "INSERT INTO ",
                Identifier/binary,
                " ($timestamp, ",
                Identifier/binary,
                ") VALUES (now(), 1)"
            >>,
            ?assertMatch({ok, _}, emqx_bridge_quasardb_sql:compile(KeywordSQL), Identifier)
        end,
        [
            <<"insert">>,
            <<"into">>,
            <<"values">>,
            <<"null">>,
            <<"now">>,
            <<"today">>,
            <<"yesterday">>,
            <<"tomorrow">>,
            <<"epoch">>,
            <<"end_of_time">>
        ]
    ),
    lists:foreach(
        fun(Byte) ->
            Spaced = <<"INSERT", Byte, "INTO t ($timestamp, v) VALUES (now, 1)">>,
            ?assertMatch({ok, _}, emqx_bridge_quasardb_sql:compile(Spaced), Byte)
        end,
        [9, 10, 11, 12, 13, 32]
    ),
    lists:foreach(
        fun(Byte) ->
            Spaced = <<"INSERT", Byte, "INTO t ($timestamp, v) VALUES (now, 1)">>,
            ?assertMatch({error, _}, emqx_bridge_quasardb_sql:compile(Spaced), Byte)
        end,
        lists:seq(1, 8) ++ lists:seq(14, 31)
    ).

t_number_forms(_Config) ->
    Accepted = [
        <<"0">>,
        <<"007">>,
        <<"0.">>,
        <<".0">>,
        <<"12.34">>,
        <<"-0.0">>,
        <<"+7">>,
        <<"-.5">>,
        <<"1e0">>,
        <<"1E+3">>,
        <<"1e-3">>,
        <<"1.25e2">>,
        <<".5e2">>,
        <<"1.e2">>
    ],
    lists:foreach(
        fun(Value) ->
            ?assertMatch({ok, _}, emqx_bridge_quasardb_sql:compile(value_sql(Value)), Value)
        end,
        Accepted
    ),
    lists:foreach(
        fun(Value) ->
            ?assertMatch({error, _}, emqx_bridge_quasardb_sql:compile(value_sql(Value)), Value)
        end,
        [<<"1e">>, <<"1e+">>, <<"- 7">>, <<"--1">>, <<"+-1">>]
    ).

t_placeholder_paths(_Config) ->
    lists:foreach(
        fun(Source) -> ?assertMatch({ok, _}, emqx_sql_plan:parse_placeholder(Source)) end,
        [<<"${}">>, <<"${.}">>, <<"${payload}">>, <<"${.payload.n}">>]
    ),
    lists:foreach(
        fun(Source) ->
            ?assertEqual({error, invalid_placeholder}, emqx_sql_plan:parse_placeholder(Source))
        end,
        [<<"${bad-name}">>, <<"${v..n}">>, <<"${v.}">>, <<"${ v}">>, <<"${v">>]
    ),
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, a, b) VALUES (now(), ${}, '${.}')">>
    ),
    ?assertEqual(
        <<"INSERT INTO t ($timestamp, a, b) VALUES (now(), '{\"n\":2}', '{\"n\":2}')">>,
        rendered(emqx_bridge_quasardb_sql:render(Plan, #{n => 2}, render_opts()))
    ).

t_rejected_statement_forms(_Config) ->
    Rejected = [
        <<>>,
        <<"SELECT 1">>,
        <<"INSERT INTO t VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp) VALUES (now())">>,
        <<"INSERT INTO t (v, $timestamp) VALUES (1, now())">>,
        <<"INSERT INTO t ($timestamp, v, v) VALUES (now(), 1, 2)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now())">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1, 2)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1), (now(), 2)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1); SELECT 1">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1) -- comment">>,
        <<"INSERT /* comment */ INTO t ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1) # comment">>,
        <<"INSERT INTO ${table} ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT INTO \"${table}\" ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp, ${column}) VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp, \"${column}\") VALUES (now(), 1)">>,
        <<"INSERT INTO db.t ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), round(1.5))">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 1 + 2)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), (1))">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), true)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), \"text\")">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), ${value}0)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), ${bad-name})">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 'unterminated)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 'a''b')">>,
        <<"INSERTINTO t ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT", 16#C2, 16#A0, "INTO t ($timestamp, v) VALUES (now(), 1)">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), 'a", 0, "b')">>,
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), '", 16#FF, "')">>
    ],
    lists:foreach(
        fun(SQL) ->
            ?assertMatch({error, _}, emqx_bridge_quasardb_sql:compile(SQL), SQL)
        end,
        Rejected
    ).

t_all_non_nul_value_bytes(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, v) VALUES (now(), ${v})">>
    ),
    Prefix = <<"INSERT INTO t ($timestamp, v) VALUES (now(), '">>,
    lists:foreach(
        fun(Byte) ->
            Escaped =
                case Byte of
                    $' -> <<"\\'">>;
                    $\\ -> <<"\\\\">>;
                    _ -> <<Byte>>
                end,
            Expected = <<Prefix/binary, Escaped/binary, "')">>,
            ?assertEqual(
                Expected,
                rendered(
                    emqx_bridge_quasardb_sql:render(Plan, #{v => <<Byte>>}, render_opts())
                ),
                Byte
            )
        end,
        lists:seq(1, 255)
    ),
    ?assertMatch(
        {error, {invalid_sql_template_value, #{reason := {error, nul_character_not_allowed}}}},
        emqx_bridge_quasardb_sql:render(Plan, #{v => <<0>>}, render_opts())
    ).

t_row_lookup_cache(_Config) ->
    {ok, Plan} = emqx_bridge_quasardb_sql:compile(
        <<"INSERT INTO t ($timestamp, a, b, c) VALUES (now(), ${payload.v}, '${payload.v}', ${payload.v})">>
    ),
    ok = meck:new(emqx_jsonish, [passthrough]),
    try
        ?assertEqual(
            <<"INSERT INTO t ($timestamp, a, b, c) VALUES (now(), 2, '2', 2)">>,
            rendered(
                emqx_bridge_quasardb_sql:render(
                    Plan,
                    #{payload => #{v => 2}},
                    render_opts()
                )
            )
        ),
        ?assertEqual(1, meck:num_calls(emqx_jsonish, lookup, 2))
    after
        meck:unload(emqx_jsonish)
    end.

value_sql(Value) ->
    <<"INSERT INTO t ($timestamp, v) VALUES (now(), ", Value/binary, ")">>.

rendered({ok, SQL}) ->
    iolist_to_binary(SQL).

render_opts() ->
    #{undefined_vars_as_null => true}.
