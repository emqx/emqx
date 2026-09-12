%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_doris_sql_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_compile_and_render(_Config) ->
    SQL =
        ~b"""
    INSERT INTO mqtt(payload, arrived)
    VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))
    """,
    {ok, Plan} = emqx_doris_sql:compile(SQL),
    ?assertEqual(
        <<"INSERT INTO `mqtt` (`payload`, `arrived`) VALUES ('hello', `FROM_UNIXTIME`(2000 / 1000))">>,
        rendered(emqx_doris_sql:render(Plan, #{payload => <<"hello">>, timestamp => 2000}, #{}))
    ),
    ?assertEqual(
        <<
            "INSERT INTO `mqtt` (`payload`, `arrived`) VALUES "
            "(1, `FROM_UNIXTIME`(2 / 1000)), (3, `FROM_UNIXTIME`(4 / 1000))"
        >>,
        rendered(
            emqx_doris_sql:render_batch(
                Plan, [#{payload => 1, timestamp => 2}, #{payload => 3, timestamp => 4}], #{}
            )
        )
    ),
    ?assertMatch(
        {ok, _}, emqx_sql_plan:compile(emqx_doris_sql, "INSERT INTO mqtt VALUES (1)")
    ).

t_static_plan_compaction(_Config) ->
    Prefix = <<"INSERT INTO `db`.`t` (`c`) VALUES ">>,
    {ok, Static} = emqx_doris_sql:compile(
        ~b"""
        INSERT INTO db.t(c) VALUES (
            f(1 + 2, 'a${$}b'),
            CASE WHEN TRUE THEN -(3) ELSE 0 END,
            R'raw'
        )
        """
    ),
    ?assertEqual(
        {ok, [Prefix, [<<"(`f`(1 + 2, 'a$b'), CASE WHEN TRUE THEN -((3)) ELSE 0 END, 'raw')">>]]},
        emqx_doris_sql:render(Static, #{}, #{})
    ),
    {ok, Mixed} = emqx_doris_sql:compile(
        <<"INSERT INTO db.t(c) VALUES (f(1, ${v} + 2), 'a${v}b${v}c', '${v}${v}', '', t.c)">>
    ),
    Text = [<<"'">>, "7", <<"'">>],
    ?assertEqual(
        {ok, [
            Prefix,
            [
                <<"(`f`(1, ">>,
                <<"7">>,
                <<" + 2), ">>,
                [
                    <<"CONCAT(">>,
                    [
                        <<"'a'">>,
                        <<", ">>,
                        Text,
                        <<", ">>,
                        <<"'b'">>,
                        <<", ">>,
                        Text,
                        <<", ">>,
                        <<"'c'">>
                    ],
                    <<")">>
                ],
                <<", ">>,
                [<<"CONCAT(">>, [Text, <<", ">>, Text], <<")">>],
                <<", '', `t`.`c`)">>
            ]
        ]},
        emqx_doris_sql:render(Mixed, #{v => 7}, #{})
    ).

t_raw_static_compaction(_Config) ->
    Prefix = <<"INSERT INTO `t` VALUES ">>,
    Cases = [
        {<<>>, <<"''">>},
        {<<"plain\\n\\q\\">>, <<"'plain\\\\n\\\\q\\\\'">>},
        {<<"${$}{amount}">>, <<"'${amount}'">>},
        {<<0, 255>>, <<"UNHEX('00FF')">>}
    ],
    lists:foreach(
        fun({R, Quote, Body, Literal}) ->
            {ok, Plan} = emqx_doris_sql:compile(
                <<Prefix/binary, "(", R, Quote, Body/binary, Quote, ")">>
            ),
            Row = <<"(", Literal/binary, ")">>,
            Data = #{amount => {must_not_resolve}},
            ?assertEqual({ok, [Prefix, [Row]]}, emqx_doris_sql:render(Plan, Data, #{})),
            ?assertEqual(
                <<Prefix/binary, Row/binary, ", ", Row/binary>>,
                rendered(emqx_doris_sql:render_batch(Plan, [#{}, Data], #{}))
            )
        end,
        [{R, Q, B, L} || R <- "Rr", Q <- "'\"", {B, L} <- Cases]
    ).

t_row_lookup_cache(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES (${payload.v}, '${payload.v}', ${payload.v}, R'${payload.v}')">>
    ),
    ok = meck:new(emqx_jsonish, [passthrough]),
    try
        Rows = [#{payload => <<"{\"v\":1}">>}, #{}, #{payload => #{v => 2}}],
        ?assertEqual(
            <<"INSERT INTO `t` VALUES (1, '1', 1, '1'), (NULL, 'null', NULL, 'null'), (2, '2', 2, '2')">>,
            rendered(emqx_doris_sql:render_batch(Plan, Rows, #{}))
        ),
        ?assertEqual(3, meck:num_calls(emqx_jsonish, lookup, 2)),
        ?assertEqual(
            <<"INSERT INTO `t` VALUES (3, '3', 3, '3')">>,
            rendered(emqx_doris_sql:render(Plan, #{payload => #{v => 3}}, #{}))
        ),
        ?assertEqual(4, meck:num_calls(emqx_jsonish, lookup, 2)),
        ok = meck:expect(emqx_jsonish, lookup, fun(_, _) -> error(lookup_failed) end),
        Error =
            {invalid_sql_template_value, #{
                placeholder => "payload.v",
                reason => {placeholder_lookup_failed, {error, lookup_failed}}
            }},
        ?assertEqual({error, Error}, emqx_doris_sql:render(Plan, #{}, #{})),
        ?assertEqual(
            {error, {doris_template_render_failed, #{batch_index => 1, reason => Error}}},
            emqx_doris_sql:render_batch(Plan, [#{}], #{})
        )
    after
        meck:unload(emqx_jsonish)
    end.

t_lexical_rules(_Config) ->
    Accepted = [
        <<"INSERT\r\nINTO\tt VALUES (1)">>,
        <<"INSERT INTO $table VALUES (1)">>,
        unicode:characters_to_binary(["INSERT INTO ", 16#1F642, " VALUES (1)"]),
        <<"INSERT INTO `a``b` VALUES ('a''b', \"a\"\"b\", 'a\\q', 'a\\\nb')">>,
        <<"INSERT INTO t VALUES (R'a\\', r\"b\\\")">>,
        <<"INSERT INTO t VALUES (R'${v}')">>,
        <<"INSERT INTO t VALUES (1, 1., .2, 1e2, 1.2E-3, DEFAULT)">>,
        <<"INSERT INTO t VALUES (1 == 1, 1 !> 2, 1 !< 0)">>
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({ok, _}, emqx_doris_sql:compile(SQL)) end, Accepted),
    Rejected = [
        <<"INSERT", 11, "INTO t VALUES (1)">>,
        <<"INSERT", 12, "INTO t VALUES (1)">>,
        <<"INSERT INTO t VALUES (X'61')">>,
        <<"INSERT INTO t VALUES (_utf8mb4 'a')">>,
        <<"INSERT INTO t VALUES (2.3W)">>,
        <<"INSERT INTO t VALUES (2.3_)">>,
        <<"INSERT INTO t VALUES ('unclosed)">>,
        <<"INSERT INTO t VALUES (R'a''b')">>,
        <<"INSERT INTO t VALUES ('prefix\\${v}')">>
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({error, _}, emqx_doris_sql:compile(SQL)) end, Rejected).

t_identifier_starts(_Config) ->
    Accepted = [<<"a123">>, <<"Z123">>, <<"_123">>, <<"$123">>, <<128, "123">>, <<255, "123">>],
    Rejected = [
        <<"123abc">>,
        <<"123d">>,
        <<"123bd">>,
        <<"0x12">>,
        <<"1L">>,
        <<"123_">>,
        <<"123$">>,
        <<"123", 255>>
    ],
    lists:foreach(
        fun({Name, Expected}) ->
            Templates = [
                <<"INSERT INTO ", Name/binary, " VALUES (1)">>,
                <<"INSERT INTO db.", Name/binary, " VALUES (1)">>,
                <<"INSERT INTO ", Name/binary, ".t VALUES (1)">>,
                <<"INSERT INTO t (", Name/binary, ") VALUES (1)">>,
                <<"INSERT INTO t VALUES (", Name/binary, ")">>,
                <<"INSERT INTO t VALUES (t.", Name/binary, ")">>,
                <<"INSERT INTO t VALUES (", Name/binary, ".c)">>,
                <<"INSERT INTO t VALUES (", Name/binary, "(1))">>,
                <<"INSERT INTO t VALUES (db.", Name/binary, "(1))">>,
                <<"INSERT INTO t VALUES (", Name/binary, ".f(1))">>,
                <<"INSERT INTO t VALUES (f(", Name/binary, "))">>
            ],
            lists:foreach(
                fun(SQL) ->
                    ?assertEqual(Expected, element(1, emqx_doris_sql:compile(SQL)), SQL)
                end,
                Templates
            )
        end,
        [{Name, ok} || Name <- Accepted] ++
            [{Name, error} || Name <- Rejected] ++
            [{<<"`", Name/binary, "`">>, ok} || Name <- Rejected]
    ),
    lists:foreach(
        fun(Source) ->
            ?assertEqual(
                {error, {1, emqx_doris_sql_lexer, {user, unsupported_number}}, 1},
                emqx_doris_sql_lexer:string(binary_to_list(Source))
            )
        end,
        Rejected
    ).

t_generated_byte_rules(_Config) ->
    lists:foreach(
        fun(Byte) ->
            Text = <<Byte>>,
            ?assertEqual(
                {ok, [{identifier, 1, Text}], 1},
                emqx_doris_sql_lexer:string([Byte])
            ),
            lists:foreach(
                fun(Chars) ->
                    Source = list_to_binary(Chars),
                    ?assertEqual(
                        {ok, [{string, 1, Source}], 1},
                        emqx_doris_sql_lexer:string(Chars)
                    ),
                    SQL = <<"INSERT INTO t VALUES (", Source/binary, ")">>,
                    {ok, Plan} = emqx_doris_sql:compile(SQL),
                    Expected =
                        case Chars of
                            [$r | _] -> <<"UNHEX('", (binary:encode_hex(Text))/binary, "')">>;
                            _ -> Source
                        end,
                    ?assertEqual(
                        <<"INSERT INTO `t` VALUES (", Expected/binary, ")">>,
                        rendered(emqx_doris_sql:render(Plan, #{}, #{}))
                    )
                end,
                [[$', Byte, $'], [$', $\\, Byte, $'], [$r, $', Byte, $']]
            )
        end,
        lists:seq(16#80, 16#FF)
    ),
    lists:foreach(
        fun(Bytes) ->
            SQL = <<"INSERT INTO ", Bytes/binary, " VALUES ('", Bytes/binary, "')">>,
            {ok, Plan} = emqx_doris_sql:compile(SQL),
            ?assertEqual(
                <<"INSERT INTO `", Bytes/binary, "` VALUES ('", Bytes/binary, "')">>,
                rendered(emqx_doris_sql:render(Plan, #{}, #{}))
            )
        end,
        [<<255>>, <<16#ED, 16#A0, 16#80>>, <<16#F4, 16#90, 16#80, 16#80>>]
    ),
    ?assertMatch({ok, _}, emqx_doris_sql:compile(<<"INSERT INTO t VALUES ('hello')">>)),
    lists:foreach(
        fun(Byte) ->
            ?assertEqual(
                {ok, [{string, 1, <<$', $\\, Byte, $'>>}], 1},
                emqx_doris_sql_lexer:string([$', $\\, Byte, $'])
            )
        end,
        [0, $[, $], $^, $-, ${, $}, $%, $\\, $', $"]
    ).

t_generated_all_bytes(_Config) ->
    lists:foreach(
        fun({Text, Token}) ->
            ?assertEqual({ok, [{Token, 1}], 1}, emqx_doris_sql_lexer:string(Text))
        end,
        [
            {";", ';'},
            {"(", '('},
            {")", ')'},
            {",", ','},
            {".", '.'},
            {"=", '='},
            {"==", '='},
            {"<=>", '<=>'},
            {"<>", '<>'},
            {"!=", '<>'},
            {"<", '<'},
            {"<=", '<='},
            {"!>", '<='},
            {">", '>'},
            {">=", '>='},
            {"!<", '>='},
            {"+", '+'},
            {"-", '-'},
            {"*", '*'},
            {"/", '/'},
            {"%", '%'}
        ]
    ),
    lists:foreach(
        fun(Byte) ->
            lists:foreach(
                fun(Quote) ->
                    lists:foreach(
                        fun({Prefix, Body, Expected}) ->
                            Chars = Prefix ++ [Quote] ++ Body ++ [Quote],
                            Source = list_to_binary(Chars),
                            Matches =
                                case emqx_doris_sql_lexer:string(Chars) of
                                    {ok, [{string, _, Source}], _} -> true;
                                    _ -> false
                                end,
                            ?assertEqual(Expected, Matches, {Byte, Quote, Prefix, Body})
                        end,
                        [
                            {[], [Byte], Byte =/= Quote andalso Byte =/= $\\},
                            {[], [$\\, Byte], true},
                            {[], [Quote, Quote, $\\, Byte], true},
                            {"R", [Byte], Byte =/= Quote},
                            {"r", [Byte], Byte =/= Quote}
                        ]
                    )
                end,
                [$', $"]
            ),
            Backticks = [$`, Byte, $`],
            BacktickSource = list_to_binary(Backticks),
            BacktickMatches =
                case emqx_doris_sql_lexer:string(Backticks) of
                    {ok, [{bt_identifier, _, BacktickSource}], _} -> true;
                    _ -> false
                end,
            ?assertEqual(Byte =/= $`, BacktickMatches),
            Identifier = [$a, Byte, $z],
            IdentifierSource = list_to_binary(Identifier),
            IdentifierMatches =
                case emqx_doris_sql_lexer:string(Identifier) of
                    {ok, [{identifier, _, IdentifierSource}], _} -> true;
                    _ -> false
                end,
            ?assertEqual(
                (Byte >= $A andalso Byte =< $Z) orelse
                    (Byte >= $a andalso Byte =< $z) orelse
                    (Byte >= $0 andalso Byte =< $9) orelse
                    Byte =:= $$ orelse Byte =:= $_ orelse Byte >= 16#80,
                IdentifierMatches
            )
        end,
        lists:seq(0, 255)
    ).

t_grammar_subset(_Config) ->
    ?assertMatch(
        {ok, _},
        emqx_doris_sql:compile(
            ~b"""
            INSERT INTO t VALUES (
                DEFAULT,
                -2 * 3,
                CASE
                    WHEN ${v} IS NOT NULL AND NOT FALSE THEN IF(TRUE, 1, 2)
                    ELSE 0
                END
            );
            """
        )
    ),
    Rejected = [
        <<"INSERT INTO t VALUES ((DEFAULT))">>,
        <<"INSERT INTO t VALUES (f(DEFAULT))">>,
        <<"INSERT INTO t VALUES (DEFAULT + 1)">>,
        <<"INSERT INTO t VALUES (1) AS new">>,
        <<"INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE c = 1">>,
        <<"INSERT INTO t VALUES (1), (2)">>,
        <<"INSERT INTO t SELECT 1">>,
        <<"INSERT INTO ${table} VALUES (1)">>,
        <<"INSERT INTO `t${suffix}` VALUES (1)">>,
        <<"INSERT INTO t VALUES (1) -- note">>,
        <<"INSERT INTO t VALUES (1) /* note */">>
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({error, _}, emqx_doris_sql:compile(SQL)) end, Rejected).

t_strings_and_binary(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES (${v}, 'prefix ${v} suffix', \"${v}\")">>
    ),
    Value = <<"a'b\\c", 0, 255>>,
    Result = rendered(emqx_doris_sql:render(Plan, #{v => Value}, #{})),
    ?assertEqual(
        <<
            "INSERT INTO `t` VALUES (UNHEX('6127625C6300FF'), "
            "CONCAT('prefix ', UNHEX('6127625C6300FF'), ' suffix'), UNHEX('6127625C6300FF'))"
        >>,
        Result
    ),
    ?assertMatch({ok, _}, emqx_doris_sql:compile(Result)),
    {ok, Escaped} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES ('a''${v}', \"b\\\"${v}\", '${$}{v}')">>
    ),
    ?assertEqual(
        <<"INSERT INTO `t` VALUES (CONCAT('a''', 'x'), CONCAT(\"b\\\"\", 'x'), '${v}')">>,
        rendered(emqx_doris_sql:render(Escaped, #{v => <<"x">>}, #{}))
    ).

t_text_escaping(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES (${v}, 'prefix ${v} suffix', \"${v}\")">>
    ),
    Unicode = unicode:characters_to_binary([16#E9, 16#4E2D, 16#1F642]),
    Cases = [
        {<<>>, <<"''">>},
        {<<"hello">>, <<"'hello'">>},
        {<<"a''b\"c\\">>, <<"'a\\'\\'b\\\"c\\\\'">>},
        {<<0, 8, 9, 10, 13, 26>>, <<"'\\0\\b\\t\\n\\r\\Z'">>},
        {<<1, 11, 12, 31, 127>>, <<"'", 1, 11, 12, 31, 127, "'">>},
        {<<"%_\\%\\_\\n\\0\\q">>, <<"'%_\\\\%\\\\_\\\\n\\\\0\\\\q'">>},
        {Unicode, <<"'", Unicode/binary, "'">>},
        {<<255>>, <<"UNHEX('FF')">>},
        {<<16#C3>>, <<"UNHEX('C3')">>},
        {<<16#C0, 16#80>>, <<"UNHEX('C080')">>},
        {<<16#ED, 16#A0, 16#80>>, <<"UNHEX('EDA080')">>},
        {<<16#F4, 16#90, 16#80, 16#80>>, <<"UNHEX('F4908080')">>}
    ],
    lists:foreach(
        fun({Value, Literal}) ->
            Expected = <<
                "INSERT INTO `t` VALUES (",
                Literal/binary,
                ", CONCAT('prefix ', ",
                Literal/binary,
                ", ' suffix'), ",
                Literal/binary,
                ")"
            >>,
            ?assertEqual(Expected, rendered(emqx_doris_sql:render(Plan, #{v => Value}, #{})))
        end,
        Cases
    ).

t_missing_and_errors(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(<<"INSERT INTO t VALUES (${v}, '${v}')">>),
    ?assertEqual(
        <<"INSERT INTO `t` VALUES (NULL, 'null')">>,
        rendered(emqx_doris_sql:render(Plan, #{}, #{undefined_vars_as_null => true}))
    ),
    ?assertEqual(
        <<"INSERT INTO `t` VALUES ('undefined', 'undefined')">>,
        rendered(emqx_doris_sql:render(Plan, #{}, #{undefined_vars_as_null => false}))
    ),
    ?assertMatch(
        {error, {doris_template_render_failed, #{batch_index := 2}}},
        emqx_doris_sql:render_batch(Plan, [#{v => 1}, #{v => {unsupported}}], #{})
    ).

t_compiler_dispatch(_Config) ->
    DorisOpts = #{sql_compiler => emqx_doris_sql},
    {ok, #{query_templates := #{{test, batch} := Plan}}} =
        emqx_mysql:parse_prepare_sql(test, <<"INSERT INTO t VALUES (${v})">>, true, DorisOpts),
    ?assertEqual(
        <<"INSERT INTO `t` VALUES (UNHEX('FF'))">>,
        rendered(emqx_sql_plan:render(Plan, #{v => <<255>>}, #{}))
    ),
    %% The Doris compiler rejects this statement, so batch parsing fails.
    %% The MySQL compiler accepts it.
    Rejected = <<"INSERT INTO t VALUES (f(DEFAULT))">>,
    ?assertMatch({error, _}, emqx_mysql:parse_prepare_sql(test, Rejected, true, DorisOpts)),
    {ok, #{query_templates := MySQLTemplates}} =
        emqx_mysql:parse_prepare_sql(test, Rejected, true),
    ?assert(maps:is_key({test, batch}, MySQLTemplates)).

t_escaped_dollar(_Config) ->
    Cases = [
        {<<>>, <<>>},
        {<<"plain\\n\\q">>, <<"plain\\n\\q">>},
        {<<"${$}">>, <<"$">>},
        {<<"cost: ${$}{amount}">>, <<"cost: ${amount}">>},
        {<<"${$}${$}{amount}${$}">>, <<"$${amount}$">>},
        {<<"${$}{$}">>, <<"${$}">>},
        {<<"\\${$}{amount}">>, <<"\\${amount}">>},
        {<<"\\\\${$}{amount}">>, <<"\\\\${amount}">>},
        {<<"${$}\\\\">>, <<"$\\\\">>}
    ],
    lists:foreach(
        fun({Quote, Body, Expected}) ->
            Prefix = <<"INSERT INTO `t` VALUES ">>,
            {ok, Plan} = emqx_doris_sql:compile(
                <<Prefix/binary, "(", Quote, Body/binary, Quote, ")">>
            ),
            Row = <<"(", Quote, Expected/binary, Quote, ")">>,
            lists:foreach(
                fun(Data) ->
                    ?assertEqual({ok, [Prefix, [Row]]}, emqx_doris_sql:render(Plan, Data, #{}))
                end,
                [#{}, #{amount => {must_not_resolve}}]
            ),
            ?assertEqual(
                <<Prefix/binary, Row/binary, ", ", Row/binary>>,
                rendered(emqx_doris_sql:render_batch(Plan, [#{}, #{amount => 99}], #{}))
            )
        end,
        [{Q, B, E} || Q <- "'\"", {B, E} <- Cases ++ [{<<Q, Q>>, <<Q, Q>>}]]
    ),
    lists:foreach(
        fun(Quote) ->
            {ok, Plan} = emqx_doris_sql:compile(
                <<"INSERT INTO t VALUES (", Quote, "${$}{amount}${v}${$}{$}", Quote, ")">>
            ),
            ?assertEqual(
                <<"INSERT INTO `t` VALUES (CONCAT(", Quote, "${amount}", Quote, ", 'x', ", Quote,
                    "${$}", Quote, "))">>,
                rendered(
                    emqx_doris_sql:render(Plan, #{v => <<"x">>, amount => {must_not_resolve}}, #{})
                )
            )
        end,
        "'\""
    ).

t_raw_strings(_Config) ->
    Cases = [
        {<<>>, <<"''">>},
        {<<"a${$}b\\n">>, <<"'a$b\\\\n'">>},
        {<<"${$}">>, <<"'$'">>},
        {<<"cost: ${$}{amount}">>, <<"'cost: ${amount}'">>},
        {<<"${$}${$}{amount}${$}">>, <<"'$${amount}$'">>},
        {<<"${$}{$}">>, <<"'${$}'">>},
        {<<"\\${$}{amount}\\">>, <<"'\\\\${amount}\\\\'">>},
        {<<0, 255, "${$}">>, <<"UNHEX('00FF24')">>},
        {<<"${$}{amount}${v}${$}{$}">>, <<"CONCAT('${amount}', 'x', '${$}')">>},
        {<<"${v}">>, <<"'x'">>},
        {<<"${v}${v}">>, <<"CONCAT('x', 'x')">>},
        {<<"${$}${v}${$}">>, <<"CONCAT('$', 'x', '$')">>},
        {<<"\\${v}\\">>, <<"CONCAT('\\\\', 'x', '\\\\')">>},
        {<<"\\\\${v}">>, <<"CONCAT('\\\\\\\\', 'x')">>},
        {<<"\\n${v}\\q">>, <<"CONCAT('\\\\n', 'x', '\\\\q')">>},
        {<<0, 255, "${v}">>, <<"CONCAT(UNHEX('00FF'), 'x')">>}
    ],
    lists:foreach(
        fun({R, Quote}) ->
            OtherQuote =
                case Quote of
                    $' -> $";
                    $" -> $'
                end,
            lists:foreach(
                fun({Body, Expected}) ->
                    SQL = <<"INSERT INTO t VALUES (", R, Quote, Body/binary, Quote, ")">>,
                    {ok, Plan} = emqx_doris_sql:compile(SQL),
                    ?assertEqual(
                        <<"INSERT INTO `t` VALUES (", Expected/binary, ")">>,
                        rendered(
                            emqx_doris_sql:render(
                                Plan, #{v => <<"x">>, amount => {must_not_resolve}}, #{}
                            )
                        ),
                        SQL
                    )
                end,
                Cases ++
                    [
                        {
                            <<OtherQuote, "${v}", OtherQuote>>,
                            <<"CONCAT('\\", OtherQuote, "', 'x', '\\", OtherQuote, "')">>
                        }
                    ]
            ),
            {ok, Plan} = emqx_doris_sql:compile(
                <<"INSERT INTO t VALUES (${v}, ", R, Quote, "${v}", Quote, ")">>
            ),
            ?assertEqual(
                <<"INSERT INTO `t` VALUES (NULL, 'null')">>,
                rendered(emqx_doris_sql:render(Plan, #{}, #{undefined_vars_as_null => true}))
            ),
            ?assertEqual(
                <<"INSERT INTO `t` VALUES ('undefined', 'undefined')">>,
                rendered(emqx_doris_sql:render(Plan, #{}, #{undefined_vars_as_null => false}))
            ),
            ?assertEqual(
                <<"INSERT INTO `t` VALUES (UNHEX('FF'), UNHEX('FF'))">>,
                rendered(emqx_doris_sql:render(Plan, #{v => <<255>>}, #{}))
            ),
            ?assertMatch(
                {error, {invalid_sql_template_value, #{placeholder := "v"}}},
                emqx_doris_sql:render(Plan, #{v => {unsupported}}, #{})
            ),
            {ok, TextPlan} = emqx_doris_sql:compile(
                <<"INSERT INTO t VALUES (", R, Quote, "${v}", Quote, ")">>
            ),
            ?assertMatch(
                {error, {invalid_sql_template_value, #{placeholder := "v"}}},
                emqx_doris_sql:render(TextPlan, #{v => {unsupported}}, #{})
            ),
            ?assertMatch(
                {error, {doris_template_render_failed, #{batch_index := 2}}},
                emqx_doris_sql:render_batch(TextPlan, [#{v => 1}, #{v => {unsupported}}], #{})
            )
        end,
        [{R, Q} || R <- "Rr", Q <- "'\""]
    ).

t_expression_rendering(_Config) ->
    Cases = [
        {<<"TRUE, FALSE, NULL, DEFAULT, 1., .2, 1.2E-3">>,
            <<"TRUE, FALSE, NULL, DEFAULT, 1., .2, 1.2E-3">>},
        {
            <<
                "current_date, Current_Time, CURRENT_TIMESTAMP, localtime, "
                "LocalTimestamp, CURRENT_USER, session_user"
            >>,
            <<
                "CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, "
                "LOCALTIMESTAMP, CURRENT_USER, SESSION_USER"
            >>
        },
        {<<"${v} + 2 * 3, (${v} + 2) * 3, 8 - 3 - 1, 8 / (4 / 2), -${v} % +2">>,
            <<"4 + 2 * 3, (4 + 2) * 3, 8 - 3 - 1, 8 / (4 / 2), -(4) % +(2)">>},
        {<<"CASE ${v} WHEN 1 THEN 'one' WHEN 4 THEN 'four' END">>,
            <<"CASE 4 WHEN 1 THEN 'one' WHEN 4 THEN 'four' END">>},
        {
            ~b"""
            CASE
                WHEN ${v} IS NOT NULL AND NOT ${v} <=> 0 OR FALSE THEN IF(TRUE, ${v}, 0)
                ELSE NULL
            END
            """,
            <<"CASE WHEN 4 IS NOT NULL AND NOT(4 <=> 0) OR FALSE THEN `IF`(TRUE, 4, 0) ELSE NULL END">>
        },
        {<<"${v} == 4, ${v} != 3, ${v} !> 5, ${v} !< 3, ${v} < 5, ${v} > 3">>,
            <<"4 = 4, 4 <> 3, 4 <= 5, 4 >= 3, 4 < 5, 4 > 3">>},
        {<<"Db.Fn(), `a``b`.`c``d`, KeY, UpDaTe">>,
            <<"`Db`.`Fn`(), `a``b`.`c``d`, `KeY`, `UpDaTe`">>}
    ],
    lists:foreach(
        fun({Source, Expected}) ->
            {ok, Plan} = emqx_doris_sql:compile(
                <<"insert into `a``b`(KeY) values (", Source/binary, ");">>
            ),
            ?assertEqual(
                <<"INSERT INTO `a``b` (`KeY`) VALUES (", Expected/binary, ")">>,
                rendered(emqx_doris_sql:render(Plan, #{v => 4}, #{})),
                Source
            )
        end,
        Cases
    ).

t_value_conversions(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES (${v}, '${v}', R'${v}')">>
    ),
    Unicode = unicode:characters_to_binary([16#E9, 16#1F642]),
    Cases = [
        {42, <<"42">>, <<"'42'">>},
        {-12345678901234567890, <<"-12345678901234567890">>, <<"'-12345678901234567890'">>},
        {1.25, <<"1.25">>, <<"'1.25'">>},
        {true, <<"'true'">>, <<"'true'">>},
        {false, <<"'false'">>, <<"'false'">>},
        {null, <<"'null'">>, <<"'null'">>},
        {ready, <<"'ready'">>, <<"'ready'">>},
        {[], <<"''">>, <<"''">>},
        {[16#E9, 16#1F642], <<"'", Unicode/binary, "'">>, <<"'", Unicode/binary, "'">>},
        {#{}, <<"'{}'">>, <<"'{}'">>},
        {#{n => 2}, <<"'{\\\"n\\\":2}'">>, <<"'{\\\"n\\\":2}'">>},
        {[true, 2, null], <<"'[true,2,null]'">>, <<"'[true,2,null]'">>}
    ],
    lists:foreach(
        fun({Value, Scalar, Text}) ->
            ?assertEqual(
                <<"INSERT INTO `t` VALUES (", Scalar/binary, ", ", Text/binary, ", ", Text/binary,
                    ")">>,
                rendered(emqx_doris_sql:render(Plan, #{v => Value}, #{})),
                Value
            )
        end,
        Cases
    ).

t_placeholder_paths(_Config) ->
    {ok, Plan} = emqx_doris_sql:compile(
        <<"INSERT INTO t VALUES (${payload.n}, '${.payload.n}', R'${payload.n}', ${missing.n})">>
    ),
    lists:foreach(
        fun(Data) ->
            ?assertEqual(
                <<"INSERT INTO `t` VALUES (2, '2', '2', NULL)">>,
                rendered(emqx_doris_sql:render(Plan, Data, #{}))
            )
        end,
        [#{payload => #{n => 2}}, #{<<"payload">> => <<"{\"n\":2}">>}]
    ),
    {ok, Root} = emqx_doris_sql:compile(<<"INSERT INTO t VALUES (${}, '${.}', R'${}')">>),
    ?assertEqual(
        <<"INSERT INTO `t` VALUES ('{}', '{}', '{}')">>,
        rendered(emqx_doris_sql:render(Root, #{}, #{}))
    ),
    lists:foreach(
        fun(Source) ->
            ?assertEqual(
                {error, invalid_placeholder}, emqx_sql_plan:parse_placeholder(Source)
            ),
            lists:foreach(
                fun({Open, Close}) ->
                    ?assertMatch(
                        {error, {invalid_doris_insert_template, _}},
                        emqx_doris_sql:compile(
                            <<"INSERT INTO t VALUES (", Open/binary, Source/binary, Close/binary,
                                ")">>
                        )
                    )
                end,
                [{<<>>, <<>>}, {<<"'">>, <<"'">>}, {<<"\"">>, <<"\"">>}, {<<"R'">>, <<"'">>}]
            )
        end,
        [<<"${bad-name}">>, <<"${v..n}">>, <<"${v.}">>, <<"${ v}">>, <<"${v">>]
    ).

t_rejected_statement_forms(_Config) ->
    Rejected = [
        <<>>,
        <<"SELECT 1">>,
        <<"UPDATE t SET c = 1">>,
        <<"INSERT INTO t VALUES ()">>,
        <<"INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)">>,
        <<"INSERT INTO t VALUES (1) # note">>,
        <<"INSERT /* note */ INTO t VALUES (1)">>,
        <<"INSERT INTO \"t\" VALUES (1)">>,
        <<"INSERT INTO t (${column}) VALUES (1)">>,
        <<"INSERT INTO t (`${column}`) VALUES (1)">>,
        <<"INSERT INTO t VALUES (`${function}`(1))">>,
        <<"INSERT INTO t VALUES (t.`${column}`)">>,
        <<"INSERT INTO t VALUES (CASE WHEN TRUE THEN DEFAULT END)">>,
        <<"INSERT INTO t VALUES (1 IN (1, 2))">>,
        <<"INSERT INTO t VALUES ((SELECT 1))">>,
        <<"INSERT INTO t VALUES (1) alias">>
    ],
    lists:foreach(
        fun(SQL) ->
            ?assertMatch(
                {error, {invalid_doris_insert_template, _}}, emqx_doris_sql:compile(SQL), SQL
            )
        end,
        Rejected
    ).

t_render_error_details(_Config) ->
    lists:foreach(
        fun(Expression) ->
            {ok, Plan} = emqx_doris_sql:compile(
                <<"INSERT INTO t VALUES (", Expression/binary, ")">>
            ),
            Bad = #{payload => #{v => {unsupported}}},
            {error, Reason} = emqx_doris_sql:render(Plan, Bad, #{}),
            ?assertMatch(
                {invalid_sql_template_value, #{
                    placeholder := "payload.v", reason := {error, function_clause}
                }},
                Reason
            ),
            lists:foreach(
                fun(Index) ->
                    Rows = lists:duplicate(Index - 1, #{payload => #{v => 1}}) ++ [Bad, #{}],
                    ?assertEqual(
                        {error,
                            {doris_template_render_failed, #{
                                batch_index => Index, reason => Reason
                            }}},
                        emqx_doris_sql:render_batch(Plan, Rows, #{})
                    )
                end,
                [1, 2, 3]
            ),
            ?assertMatch({ok, _}, emqx_doris_sql:render(Plan, #{payload => #{v => 2}}, #{}))
        end,
        [<<"${payload.v}">>, <<"'prefix ${payload.v}'">>, <<"R'prefix ${payload.v}'">>]
    ).

rendered({ok, SQL}) -> iolist_to_binary(SQL).
