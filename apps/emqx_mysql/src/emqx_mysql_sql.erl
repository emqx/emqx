%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted MySQL INSERT INTO VALUES templates.
-module(emqx_mysql_sql).

-export([compile/1, render/3, render_batch/3, parse_placeholder/1]).
-export_type([plan/0]).

-type placeholder() :: emqx_template:placeholder().

-record(raw, {sql :: binary()}).
-record(value, {placeholder :: placeholder()}).
-record(string_raw, {sql :: binary()}).
-record(string_placeholder, {placeholder :: placeholder()}).
-type string_part() :: #string_raw{} | #string_placeholder{}.
-record(string_template, {parts :: [string_part()]}).

-type render_op() :: #raw{} | #value{} | #string_template{}.
-type render_plan() :: [render_op()].

-record(mysql_plan, {
    insert_prefix :: binary(),
    row_plan :: render_plan(),
    suffix :: binary()
}).

-opaque plan() :: #mysql_plan{}.

-define(BATCH_SEPARATOR, <<", ">>).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL0) ->
    SQL = unicode:characters_to_binary(SQL0),
    try
        case emqx_mysql_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_mysql_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_mysql_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_mysql_insert_template, Reason}}
        end
    catch
        Class:CatchReason -> {error, {invalid_mysql_insert_template, {Class, CatchReason}}}
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#mysql_plan{insert_prefix = Prefix, row_plan = Plan, suffix = Suffix}, Data, Opts) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} -> {ok, [Prefix, Rendered, Suffix]};
        {error, _} = Error -> Error
    end.

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(
    #mysql_plan{insert_prefix = Prefix, row_plan = Plan, suffix = Suffix}, DataList, Opts
) ->
    case render_batch_units(DataList, Plan, Opts, _Index = 1, _Acc = []) of
        {ok, Rendered} -> {ok, [Prefix, Rendered, Suffix]};
        {error, _} = Error -> Error
    end.

-spec parse_placeholder(binary()) -> {ok, placeholder()} | {error, invalid_placeholder}.
parse_placeholder(Source) ->
    case valid_placeholder_source(Source) of
        true ->
            case emqx_template:parse(Source) of
                [{var, _, _} = Placeholder] -> {ok, Placeholder};
                _ -> {error, invalid_placeholder}
            end;
        false ->
            {error, invalid_placeholder}
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

render_batch_units([Data | Rest], Plan, Opts, Index, Acc) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Plan, Opts, Index + 1, [Rendered | Acc]);
        {error, Reason} ->
            {error, {mysql_template_render_failed, #{batch_index => Index, reason => Reason}}}
    end;
render_batch_units([], _Plan, _Opts, _Index, Acc) ->
    {ok, lists:join(?BATCH_SEPARATOR, lists:reverse(Acc))}.

render_unit(Plan, Data, Opts) ->
    render_plan(Plan, Data, Opts, #{}, []).

render_plan([#raw{sql = SQL} | Rest], Data, Opts, Cache, Acc) ->
    render_plan(Rest, Data, Opts, Cache, [SQL | Acc]);
render_plan([#value{placeholder = Placeholder} | Rest], Data, Opts, Cache0, Acc) ->
    case resolve_placeholder(Placeholder, Data, Cache0) of
        {ok, Value, Cache} ->
            case encode_result(fun() -> encode_value(Value, Opts) end) of
                {ok, Encoded} -> render_plan(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} -> {error, render_error(Placeholder, Reason)}
            end;
        {error, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end;
render_plan([#string_template{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    case render_string_parts(Parts, Data, Opts, Cache0, []) of
        {ok, RenderedParts, Cache} ->
            Rendered = render_concat(RenderedParts),
            render_plan(Rest, Data, Opts, Cache, [Rendered | Acc]);
        {error, Placeholder, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end;
render_plan([], _Data, _Opts, _Cache, Acc) ->
    {ok, lists:reverse(Acc)}.

render_string_parts([#string_raw{sql = SQL} | Rest], Data, Opts, Cache, Acc) ->
    render_string_parts(Rest, Data, Opts, Cache, [SQL | Acc]);
render_string_parts(
    [#string_placeholder{placeholder = Placeholder} | Rest], Data, Opts, Cache0, Acc
) ->
    case resolve_placeholder(Placeholder, Data, Cache0) of
        {ok, Value, Cache} ->
            case encode_result(fun() -> encode_string(string_value(Value, Opts)) end) of
                {ok, Encoded} ->
                    render_string_parts(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} ->
                    {error, Placeholder, Reason}
            end;
        {error, Reason} ->
            {error, Placeholder, Reason}
    end;
render_string_parts([], _Data, _Opts, Cache, Acc) ->
    {ok, lists:reverse(Acc), Cache}.

render_concat([Only]) ->
    Only;
render_concat(Parts) ->
    [<<"CONCAT(">>, lists:join(<<", ">>, Parts), <<")">>].

resolve_placeholder({var, _Name, Accessor} = Placeholder, Data, Cache) ->
    case Cache of
        #{Placeholder := Value} ->
            {ok, Value, Cache};
        #{} ->
            try emqx_jsonish:lookup(Accessor, Data) of
                {ok, Value} -> {ok, Value, Cache#{Placeholder => Value}};
                {error, _Reason} -> {ok, undefined, Cache#{Placeholder => undefined}}
            catch
                Class:Reason -> {error, {placeholder_lookup_failed, {Class, Reason}}}
            end
    end.

render_error({var, Name, _Accessor}, Reason) ->
    {invalid_sql_template_value, #{placeholder => Name, reason => Reason}}.

encode_result(Encoder) ->
    try Encoder() of
        {error, _} = Error -> Error;
        Encoded -> {ok, Encoded}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

compile_ast(
    {insert, #{
        target := Target,
        columns := Columns,
        row := Row,
        alias := Alias,
        on_duplicate := OnDuplicate
    }}
) ->
    TargetSQL = serialize_target(Target),
    ColumnsSQL = serialize_columns(Columns),
    Prefix = <<"INSERT INTO ", TargetSQL/binary, ColumnsSQL/binary, " VALUES ">>,
    RowPlan = merge_render_ops(compile_row(Row)),
    Suffix = iolist_to_binary([serialize_alias(Alias), serialize_on_duplicate(OnDuplicate)]),
    {ok, #mysql_plan{insert_prefix = Prefix, row_plan = RowPlan, suffix = Suffix}}.

serialize_target(Identifiers) ->
    iolist_to_binary(lists:join($., [serialize_identifier(I) || I <- Identifiers])).

serialize_columns(undefined) ->
    <<>>;
serialize_columns(Identifiers) ->
    Content = lists:join(<<", ">>, [serialize_identifier(I) || I <- Identifiers]),
    iolist_to_binary([" (", Content, ")"]).

serialize_identifier({identifier, bare, Name}) ->
    quote_identifier(Name);
serialize_identifier({identifier, backtick, Source}) ->
    ok = assert_no_placeholder(Source),
    Source;
serialize_identifier({identifier, double_opaque, Source}) ->
    Source.

serialize_reference_path(Identifiers) ->
    iolist_to_binary(lists:join($., [serialize_reference_part(I) || I <- Identifiers])).

serialize_reference_part({identifier, bare, Name}) ->
    Name;
serialize_reference_part({identifier, backtick, Source}) ->
    ok = assert_no_placeholder(Source),
    Source;
serialize_reference_part({identifier, double_opaque, Source}) ->
    Source.

quote_identifier(Name) ->
    Escaped = binary:replace(Name, <<"`">>, <<"``">>, [global]),
    <<"`", Escaped/binary, "`">>.

assert_no_placeholder(Source) ->
    case binary:match(Source, <<"${">>) of
        nomatch -> ok;
        _ -> error(dynamic_identifier_not_allowed)
    end.

serialize_alias(undefined) ->
    <<>>;
serialize_alias({alias, Identifier, Columns}) ->
    [<<" AS ">>, serialize_identifier(Identifier), serialize_alias_columns(Columns)].

serialize_alias_columns(undefined) ->
    <<>>;
serialize_alias_columns(Columns) ->
    [<<"(">>, lists:join(<<", ">>, [serialize_identifier(I) || I <- Columns]), <<")">>].

serialize_on_duplicate(undefined) ->
    <<>>;
serialize_on_duplicate({on_duplicate, Assignments}) ->
    [
        <<" ON DUPLICATE KEY UPDATE ">>,
        lists:join(<<", ">>, [serialize_assignment(A) || A <- Assignments])
    ].

serialize_assignment({assignment, Target, Expression}) ->
    ExpressionSQL = serialize_static_expression(Expression),
    [serialize_reference_path(Target), <<" = ">>, ExpressionSQL].

serialize_static_expression(Expression) ->
    case merge_render_ops(compile_expression(Expression)) of
        [#raw{sql = SQL}] -> SQL;
        _ -> error(dynamic_on_duplicate_key_update_not_allowed)
    end.

compile_row({row, Expressions}) ->
    [#raw{sql = <<"(">>} | join_ops(<<", ">>, [compile_expression(E) || E <- Expressions])] ++
        [#raw{sql = <<")">>}].

compile_expression({var, Placeholder}) ->
    [#value{placeholder = Placeholder}];
compile_expression({string, Source}) ->
    case parse_sql_string(Source) of
        {static, SQL} -> [#raw{sql = SQL}];
        {dynamic, Parts} -> [#string_template{parts = Parts}]
    end;
compile_expression({number, Number}) ->
    [#raw{sql = Number}];
compile_expression({hex, Hex}) ->
    [#raw{sql = Hex}];
compile_expression(null) ->
    [#raw{sql = <<"NULL">>}];
compile_expression(true) ->
    [#raw{sql = <<"TRUE">>}];
compile_expression(false) ->
    [#raw{sql = <<"FALSE">>}];
compile_expression(default) ->
    [#raw{sql = <<"DEFAULT">>}];
compile_expression({identifier_ref, Name}) ->
    [#raw{sql = serialize_reference_path(Name)}];
compile_expression({call, Name, Args}) ->
    FunctionName = serialize_reference_path(Name),
    [
        #raw{sql = <<FunctionName/binary, "(">>}
        | join_ops(<<", ">>, [
            compile_expression(E)
         || E <- Args
        ])
    ] ++ [#raw{sql = <<")">>}];
compile_expression({group, Expression}) ->
    [#raw{sql = <<"(">>} | compile_expression(Expression)] ++ [#raw{sql = <<")">>}];
compile_expression({unary, Operator, Expression}) ->
    [#raw{sql = <<(atom_to_binary(Operator))/binary, "(">>} | compile_expression(Expression)] ++
        [#raw{sql = <<")">>}];
compile_expression({binary, Operator, Left, Right}) ->
    compile_expression(Left) ++
        [#raw{sql = <<" ", (atom_to_binary(Operator))/binary, " ">>}] ++
        compile_expression(Right).

parse_sql_string(Source) ->
    Body = binary:part(Source, 1, byte_size(Source) - 2),
    parse_sql_string_body(Body, [], [], false).

parse_sql_string_body(<<>>, Text, _Parts, false) ->
    {static, quote_static_string(iolist_to_binary(lists:reverse(Text)))};
parse_sql_string_body(<<>>, Text, Parts, true) ->
    ok = assert_safe_string_split(Text),
    {dynamic, lists:reverse(flush_string_text(Text, Parts))};
parse_sql_string_body(<<"${$}", Rest/binary>>, Text, Parts, Dynamic) ->
    parse_sql_string_body(Rest, [$$ | Text], Parts, Dynamic);
parse_sql_string_body(<<"${", _/binary>> = Bin, Text, Parts, _Dynamic) ->
    ok = assert_safe_string_split(Text),
    {Placeholder, Rest} = take_placeholder(Bin),
    PartsNext = [
        #string_placeholder{placeholder = Placeholder}
        | flush_string_text(Text, Parts)
    ],
    parse_sql_string_body(Rest, [], PartsNext, true);
parse_sql_string_body(<<Char, Rest/binary>>, Text, Parts, Dynamic) ->
    parse_sql_string_body(Rest, [Char | Text], Parts, Dynamic).

flush_string_text([], Parts) ->
    Parts;
flush_string_text(Text, Parts) ->
    Body = iolist_to_binary(lists:reverse(Text)),
    [#string_raw{sql = quote_static_string(Body)} | Parts].

quote_static_string(Body) ->
    <<"'", Body/binary, "'">>.

assert_safe_string_split(ReversedText) ->
    case count_leading_backslashes(ReversedText, 0) rem 2 of
        0 -> ok;
        1 -> error(ambiguous_string_placeholder_boundary)
    end.

count_leading_backslashes([$\\ | Rest], Count) ->
    count_leading_backslashes(Rest, Count + 1);
count_leading_backslashes(_, Count) ->
    Count.

join_ops(_Separator, []) ->
    [];
join_ops(Separator, [Ops | Rest]) ->
    lists:foldl(fun(Next, Acc) -> Acc ++ [#raw{sql = Separator} | Next] end, Ops, Rest).

-spec merge_render_ops(render_plan()) -> render_plan().
merge_render_ops(Ops) ->
    lists:reverse(
        lists:foldl(
            fun
                (#raw{sql = <<>>}, Acc) ->
                    Acc;
                (#raw{sql = SQL}, [#raw{sql = Previous} | Acc]) ->
                    [#raw{sql = <<Previous/binary, SQL/binary>>} | Acc];
                (Op, Acc) ->
                    [Op | Acc]
            end,
            [],
            Ops
        )
    ).

valid_placeholder_source(<<"${}">>) ->
    true;
valid_placeholder_source(<<"${.}">>) ->
    true;
valid_placeholder_source(Source) ->
    re:run(
        Source,
        <<"^\\$\\{\\.?[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+)*\\}$">>,
        [{capture, none}]
    ) =:= match.

take_placeholder(Bin) ->
    case binary:match(Bin, <<"}">>) of
        {End, 1} ->
            Size = End + 1,
            Source = binary:part(Bin, 0, Size),
            Rest = binary:part(Bin, Size, byte_size(Bin) - Size),
            case parse_placeholder(Source) of
                {ok, Placeholder} -> {Placeholder, Rest};
                {error, _} -> error({invalid_placeholder, Source})
            end;
        nomatch ->
            error(unterminated_placeholder)
    end.

encode_value(undefined, #{undefined_vars_as_null := false}) ->
    encode_string(<<"undefined">>);
encode_value(undefined, _Opts) ->
    <<"NULL">>;
encode_value(Value, _Opts) when is_integer(Value) ->
    integer_to_binary(Value);
encode_value(Value, _Opts) when is_float(Value) ->
    case Value =:= Value of
        true -> emqx_template:to_string(Value);
        false -> {error, non_finite_number}
    end;
encode_value(Value, _Opts) ->
    encode_string(to_text(Value)).

string_value(undefined, Opts) ->
    case maps:get(undefined_vars_as_null, Opts, true) of
        true -> <<"null">>;
        false -> <<"undefined">>
    end;
string_value(Value, _Opts) ->
    to_text(Value).

to_text(Value) when is_binary(Value) ->
    Value;
to_text(Value) ->
    case unicode:characters_to_binary(emqx_template:to_string(Value)) of
        Text when is_binary(Text) -> Text;
        Error -> error({invalid_unicode, Error})
    end.

encode_string(Text) ->
    case unicode:characters_to_list(Text) of
        Chars when is_list(Chars) ->
            case lists:all(fun safe_single_quoted_character/1, Chars) of
                true ->
                    Escaped = binary:replace(Text, <<"'">>, <<"''">>, [global]),
                    <<"'", Escaped/binary, "'">>;
                false ->
                    encode_hex_string(<<"_utf8mb4 ">>, Text)
            end;
        _ ->
            encode_hex_string(<<>>, Text)
    end.

encode_hex_string(Prefix, Text) ->
    Hex = binary:encode_hex(Text),
    <<Prefix/binary, "X'", Hex/binary, "'">>.

safe_single_quoted_character($\\) -> false;
safe_single_quoted_character(Char) when Char < 16#20 -> false;
safe_single_quoted_character(Char) when Char >= 16#7F, Char =< 16#9F -> false;
safe_single_quoted_character(_Char) -> true.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_compile_and_render_test() ->
    {ok, Plan} = compile(
        <<"INSERT INTO mqtt_test(payload, arrived) ",
            "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))">>
    ),
    ?assertEqual(
        {ok,
            <<"INSERT INTO `mqtt_test` (`payload`, `arrived`) ",
                "VALUES ('hello', FROM_UNIXTIME(2 / 1000))">>},
        rendered_binary(render(Plan, #{payload => <<"hello">>, timestamp => 2}, null_opts()))
    ).

single_quote_doubling_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${v})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES ('a''b')">>},
        rendered_binary(render(Plan, #{v => <<"a'b">>}, null_opts()))
    ).

mode_sensitive_value_uses_hex_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${v})">>),
    Attack = <<"\\'); DROP TABLE t; --">>,
    Hex = binary:encode_hex(Attack),
    Expected = <<"INSERT INTO `t` (`v`) VALUES (_utf8mb4 X'", Hex/binary, "')">>,
    ?assertEqual(
        {ok, Expected},
        rendered_binary(render(Plan, #{v => Attack}, null_opts()))
    ).

batch_shape_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${v})">>),
    Unsafe = <<"a\\b">>,
    Hex = binary:encode_hex(Unsafe),
    Expected = <<"INSERT INTO `t` (`v`) VALUES (_utf8mb4 X'", Hex/binary, "'), ('ok')">>,
    ?assertEqual(
        {ok, Expected},
        rendered_binary(render_batch(Plan, [#{v => Unsafe}, #{v => <<"ok">>}], null_opts()))
    ).

single_quoted_template_segments_test() ->
    {ok, Plan} = compile(
        <<"INSERT INTO t(v) VALUES ('prefix\\n ${v} suffix')">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES ", "(CONCAT('prefix\\n ', 'a''b', ' suffix'))">>},
        rendered_binary(render(Plan, #{v => <<"a'b">>}, null_opts()))
    ).

opaque_double_quoted_token_test() ->
    SQL = <<"INSERT INTO \"t${ignored}\"(\"v\") VALUES (\"${ignored}\")">>,
    {ok, Plan} = compile(SQL),
    ?assertEqual(
        {ok, <<"INSERT INTO \"t${ignored}\" (\"v\") VALUES (\"${ignored}\")">>},
        rendered_binary(render(Plan, #{ignored => <<"changed">>}, null_opts()))
    ),
    {ok, BacktickPlan} = compile(<<"INSERT INTO `a``b` (`c``d`) VALUES (${value})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `a``b` (`c``d`) VALUES ('changed')">>},
        rendered_binary(render(BacktickPlan, #{value => <<"changed">>}, null_opts()))
    ).

reject_ambiguous_quoted_tokens_test() ->
    ?assertMatch(
        {error, _},
        compile(<<"INSERT INTO t(v) VALUES ('prefix\\'${v} suffix')">>)
    ),
    ?assertMatch(
        {error, _},
        compile(<<"INSERT INTO t(v) VALUES (\"prefix\\\"${v} suffix\")">>)
    ),
    ?assertMatch(
        {error, _},
        compile(<<"INSERT INTO t(v) VALUES ('prefix\\${v} suffix')">>)
    ).

keyword_identifier_case_test() ->
    {ok, Plan} = compile(
        <<"INSERT INTO VaLuEs(KeY, UpDaTe) VALUES (${key}, ${update})">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `VaLuEs` (`KeY`, `UpDaTe`) VALUES (1, 2)">>},
        rendered_binary(render(Plan, #{key => 1, update => 2}, null_opts()))
    ).

charset_and_unicode_value_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${v})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES ('🙂')"/utf8>>},
        rendered_binary(render(Plan, #{v => [16#1F642]}, null_opts()))
    ),
    InvalidUTF8 = <<16#FF>>,
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES (X'FF')">>},
        rendered_binary(render(Plan, #{v => InvalidUTF8}, null_opts()))
    ).

on_duplicate_key_update_test() ->
    SQL = <<
        "INSERT INTO t(c1, c2) VALUES (${a}, ${b}) AS new ",
        "ON DUPLICATE KEY UPDATE c1 = new.c1, c2 = VALUES(c2)"
    >>,
    {ok, Plan} = compile(SQL),
    ?assertEqual(
        {ok,
            <<"INSERT INTO `t` (`c1`, `c2`) VALUES (1, 2), (3, 4) AS `new` ",
                "ON DUPLICATE KEY UPDATE c1 = new.c1, c2 = VALUES(c2)">>},
        rendered_binary(
            render_batch(Plan, [#{a => 1, b => 2}, #{a => 3, b => 4}], null_opts())
        )
    ),
    ?assertMatch(
        {error, _},
        compile(
            <<"INSERT INTO t(c1) VALUES (${a}) ON DUPLICATE KEY UPDATE c1 = ${a}">>
        )
    ).

undefined_value_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${missing})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES (NULL)">>},
        rendered_binary(render(Plan, #{}, #{undefined_vars_as_null => true}))
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `t` (`v`) VALUES ('undefined')">>},
        rendered_binary(render(Plan, #{}, #{undefined_vars_as_null => false}))
    ).

reject_unsupported_syntax_test() ->
    Rejected = [
        <<"INSERT INTO t VALUES (${a}), (${b})">>,
        <<"INSERT INTO t VALUES (${a}) -- comment">>,
        <<"INSERT INTO ${table} VALUES (${a})">>,
        <<"INSERT INTO `t${suffix}` VALUES (${a})">>,
        <<"INSERT INTO t VALUES (${a}) alias">>,
        <<"INSERT INTO t SELECT ${a}">>
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({error, _}, compile(SQL)) end, Rejected).

rendered_binary({ok, SQL}) ->
    ?assert(is_list(SQL)),
    {ok, iolist_to_binary(SQL)}.

null_opts() ->
    #{undefined_vars_as_null => true}.

-endif.
