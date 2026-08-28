%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted SQL Server INSERT INTO VALUES templates.
-module(emqx_bridge_sqlserver_sql).

-export([compile/1, render/3, render_batch/3, parse_placeholder/1]).
-export_type([plan/0]).

-type placeholder() :: emqx_template:placeholder().

%% Represent string templates, e.g. 'aaa ${bbb} ccc' as
%% [
%%   #tpl_text{text = <<"aaa ">>},
%%   #tpl_placeholder{placeholder = {var, "bbb", ...}},
%%   #tpl_text{text = <<"ccc ">>}
%% ]
-record(tpl_text, {text :: binary()}).
-record(tpl_placeholder, {placeholder :: placeholder()}).

%% Represent resolved parts of a template string,
%% e.g. 'aaa ${bbb} ccc' might be pre-rendered as
%% [
%%   #part_text{text = <<"aaa ">>},
%%   #part_value{value = BBBValue},
%%   #part_text{text = <<"ccc ">>}
%% ]
-record(part_text, {text :: binary()}).
-record(part_value, {value :: term()}).

-type template_part() :: #tpl_text{} | #tpl_placeholder{}.
-type part() :: #part_text{} | #part_value{}.

%% Pre-rendered parts of a full SQL statement
-record(raw, {sql :: binary()}).
-record(value, {placeholder :: placeholder()}).
-record(varchar_string, {parts :: [template_part()]}).
-record(nvarchar_string, {parts :: [template_part()]}).

-type render_op() :: #raw{} | #value{} | #varchar_string{} | #nvarchar_string{}.
-type render_plan() :: [render_op()].

-record(sqlserver_plan, {insert_prefix :: binary(), plan :: render_plan()}).

-define(BATCH_SEPARATOR, <<", ">>).

-opaque plan() :: #sqlserver_plan{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL0) ->
    SQL = unicode:characters_to_binary(SQL0),
    try
        case emqx_bridge_sqlserver_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_bridge_sqlserver_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_sqlserver_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_sqlserver_insert_template, Reason}}
        end
    catch
        Class:CatchReason -> {error, {invalid_sqlserver_insert_template, {Class, CatchReason}}}
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#sqlserver_plan{insert_prefix = Prefix, plan = Plan}, Data, Opts) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(#sqlserver_plan{insert_prefix = Prefix, plan = Plan}, DataList, Opts) when
    %% https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql#limitations
    length(DataList) =< 1000
->
    case render_batch_units(DataList, Plan, Opts, _Index = 1, _Acc = []) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end;
render_batch(_Plan, _DataList, _Opts) ->
    {error, sqlserver_values_row_limit_exceeded}.

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
%% Private funs
%%------------------------------------------------------------------------------

render_batch_units([Data | Rest], Unit, Opts, Index, Acc) ->
    case render_unit(Unit, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Unit, Opts, Index + 1, [Rendered | Acc]);
        {error, Reason} ->
            {error, {sqlserver_template_render_failed, #{batch_index => Index, reason => Reason}}}
    end;
render_batch_units([], _Unit, _Opts, _Index, Acc) ->
    {ok, lists:join(?BATCH_SEPARATOR, lists:reverse(Acc))}.

render_unit(Unit, Data, Opts) ->
    render_plan(Unit, Data, Opts, #{}, []).

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
render_plan([#varchar_string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(varchar, Parts, Rest, Data, Opts, Cache0, Acc);
render_plan([#nvarchar_string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(nvarchar, Parts, Rest, Data, Opts, Cache0, Acc);
render_plan([], _Data, _Opts, _Cache, Acc) ->
    {ok, lists:reverse(Acc)}.

render_template_op(Style, Parts, Rest, Data, Opts, Cache0, Acc) ->
    case resolve_template_parts(Parts, Data, Cache0, []) of
        {ok, Resolved, Cache} ->
            case
                encode_result(fun() -> encode_string(render_text_parts(Resolved, Opts), Style) end)
            of
                {ok, Encoded} -> render_plan(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} -> {error, render_template_error(Parts, Reason)}
            end;
        {error, Placeholder, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end.

encode_result(Encoder) ->
    try
        {ok, Encoder()}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

resolve_template_parts([#tpl_text{text = Text} | Rest], Data, Cache, Acc) ->
    resolve_template_parts(Rest, Data, Cache, [#part_text{text = Text} | Acc]);
resolve_template_parts([#tpl_placeholder{placeholder = Placeholder} | Rest], Data, Cache0, Acc) ->
    case resolve_placeholder(Placeholder, Data, Cache0) of
        {ok, Value, Cache} ->
            resolve_template_parts(Rest, Data, Cache, [#part_value{value = Value} | Acc]);
        {error, Reason} ->
            {error, Placeholder, Reason}
    end;
resolve_template_parts([], _Data, Cache, Acc) ->
    {ok, lists:reverse(Acc), Cache}.

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

%% Encoding fails after all string parts are combined, so no single part owns the error.
%% Report one placeholder with the regular error shape.
%% Report every unique placeholder when the template contains several,
%% and return the reason directly when it contains none.
render_template_error(Parts, Reason) ->
    Placeholders = lists:usort([Placeholder || #tpl_placeholder{placeholder = Placeholder} <- Parts]),
    case Placeholders of
        [Placeholder] ->
            render_error(Placeholder, Reason);
        [] ->
            {invalid_sql_template_value, Reason};
        _ ->
            Names = [Name || {var, Name, _Accessor} <- Placeholders],
            {invalid_sql_template_value, #{placeholders => Names, reason => Reason}}
    end.

compile_ast({insert, #{target := Target, columns := Columns, row := Row}}) ->
    TargetSQL = iolist_to_binary(lists:join($., [serialize_target_part(I) || I <- Target])),
    ColumnsSQL = serialize_columns(Columns),
    Head = <<"INSERT INTO ", TargetSQL/binary, ColumnsSQL/binary, " VALUES ">>,
    Unit = merge_render_ops(compile_row(Row)),
    {ok, #sqlserver_plan{insert_prefix = Head, plan = Unit}}.

serialize_target_part(empty) ->
    <<>>;
serialize_target_part(Identifier) ->
    serialize_identifier(Identifier).

serialize_columns(undefined) ->
    <<>>;
serialize_columns(Identifiers) ->
    Content = lists:join(<<", ">>, [serialize_identifier(I) || I <- Identifiers]),
    iolist_to_binary([" (", Content, ")"]).

serialize_identifier({identifier, bare, Name}) ->
    quote_identifier(Name);
serialize_identifier({identifier, double, Source}) ->
    ok = assert_no_identifier_placeholder(Source),
    Name = decode_doubled(strip_quotes(Source), $", []),
    ok = assert_no_nul(Name),
    quote_identifier(Name);
serialize_identifier({identifier, bracket, Source}) ->
    ok = assert_no_identifier_placeholder(Source),
    Body = binary:part(Source, 1, byte_size(Source) - 2),
    Name = decode_bracket(Body, []),
    ok = assert_no_nul(Name),
    quote_identifier(Name).

assert_no_identifier_placeholder(Source) ->
    case binary:match(Source, <<"${">>) of
        nomatch -> ok;
        _ -> error(dynamic_identifier_not_allowed)
    end.

quote_identifier(Name) ->
    Escaped = binary:replace(Name, <<"]">>, <<"]]">>, [global]),
    <<"[", Escaped/binary, "]">>.

decode_doubled(<<Quote, Quote, Rest/binary>>, Quote, Acc) ->
    decode_doubled(Rest, Quote, [Quote | Acc]);
decode_doubled(<<Char, Rest/binary>>, Quote, Acc) ->
    decode_doubled(Rest, Quote, [Char | Acc]);
decode_doubled(<<>>, _Quote, Acc) ->
    iolist_to_binary(lists:reverse(Acc)).

decode_bracket(<<"]]", Rest/binary>>, Acc) ->
    decode_bracket(Rest, [$] | Acc]);
decode_bracket(<<Char, Rest/binary>>, Acc) ->
    decode_bracket(Rest, [Char | Acc]);
decode_bracket(<<>>, Acc) ->
    iolist_to_binary(lists:reverse(Acc)).

compile_row({row, Expressions}) ->
    [#raw{sql = <<"(">>} | join_ops(<<", ">>, [compile_expression(E) || E <- Expressions])] ++
        [#raw{sql = <<")">>}].

compile_expression({var, Placeholder}) ->
    [#value{placeholder = Placeholder}];
compile_expression({string, Style, Source}) ->
    Parts = parse_string(Source, Style),
    case lists:any(fun is_variable_part/1, Parts) of
        true ->
            [compile_string_op(Style, Parts)];
        false ->
            [#tpl_text{text = Text}] = Parts,
            [#raw{sql = iolist_to_binary(encode_string(Text, Style))}]
    end;
compile_expression({number, Number}) ->
    [#raw{sql = Number}];
compile_expression({hex, Hex}) ->
    [#raw{sql = Hex}];
compile_expression(null) ->
    [#raw{sql = <<"NULL">>}];
compile_expression(default) ->
    [#raw{sql = <<"DEFAULT">>}];
compile_expression(current_timestamp) ->
    [#raw{sql = <<"CURRENT_TIMESTAMP">>}];
compile_expression({identifier_value, Name}) ->
    [#raw{sql = Name}];
compile_expression({call, Name, Args}) ->
    FunctionName = iolist_to_binary(lists:join($., [serialize_function_part(I) || I <- Name])),
    [
        #raw{sql = <<FunctionName/binary, "(">>}
        | join_ops(<<", ">>, [compile_expression(E) || E <- Args])
    ] ++
        [#raw{sql = <<")">>}];
compile_expression({group, Expression}) ->
    [#raw{sql = <<"(">>} | compile_expression(Expression)] ++ [#raw{sql = <<")">>}];
compile_expression({unary, Operator, Expression}) ->
    [#raw{sql = <<(atom_to_binary(Operator))/binary, "(">>} | compile_expression(Expression)] ++
        [#raw{sql = <<")">>}];
compile_expression({is_null, Expression, Negated}) ->
    Suffix =
        case Negated of
            true -> <<" IS NOT NULL">>;
            false -> <<" IS NULL">>
        end,
    compile_expression(Expression) ++ [#raw{sql = Suffix}];
compile_expression({case_expression, Operand, Whens, Else}) ->
    [#raw{sql = <<"CASE">>}] ++
        compile_case_operand(Operand) ++
        lists:append([compile_when_clause(Clause) || Clause <- Whens]) ++
        compile_case_else(Else) ++
        [#raw{sql = <<" END">>}];
compile_expression({binary, Operator, Left, Right}) ->
    compile_expression(Left) ++
        [#raw{sql = <<" ", (atom_to_binary(Operator))/binary, " ">>}] ++
        compile_expression(Right).

compile_case_operand(undefined) ->
    [];
compile_case_operand(Expression) ->
    [#raw{sql = <<" ">>} | compile_expression(Expression)].

compile_when_clause({'when', Condition, Result}) ->
    [#raw{sql = <<" WHEN ">>} | compile_expression(Condition)] ++
        [#raw{sql = <<" THEN ">>} | compile_expression(Result)].

compile_case_else(undefined) ->
    [];
compile_case_else(Expression) ->
    [#raw{sql = <<" ELSE ">>} | compile_expression(Expression)].

compile_string_op(varchar, Parts) ->
    #varchar_string{parts = Parts};
compile_string_op(nvarchar, Parts) ->
    #nvarchar_string{parts = Parts}.

serialize_function_part({identifier, bare, Name}) ->
    Name;
serialize_function_part(Identifier) ->
    serialize_identifier(Identifier).

parse_string(Source, nvarchar) ->
    %% SQL Server keeps doubled apostrophes inside one string token:
    %% https://github.com/microsoft/SqlScriptDOM/commit/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a
    %% SqlScriptDom/Parser/TSql/TSql160.g lines 34233-34259.
    %% Skip the two-byte N' prefix and exclude the closing apostrophe.
    Body = binary:part(Source, 2, byte_size(Source) - 3),
    parse_string_body(Body, [], []);
parse_string(Source, varchar) ->
    parse_string_body(strip_quotes(Source), [], []).

parse_string_body(<<>>, Text, Parts) ->
    finish_parts(Text, Parts);
parse_string_body(<<$', $', Rest/binary>>, Text, Parts) ->
    parse_string_body(Rest, [$' | Text], Parts);
parse_string_body(<<"${$}", Rest/binary>>, Text, Parts) ->
    parse_string_body(Rest, [$$ | Text], Parts);
parse_string_body(<<"${", _/binary>> = Bin, Text, Parts) ->
    {Placeholder, Rest} = take_placeholder(Bin),
    parse_string_body(
        Rest,
        [],
        [#tpl_placeholder{placeholder = Placeholder} | flush_text(Text, Parts)]
    );
parse_string_body(<<Char, Rest/binary>>, Text, Parts) ->
    parse_string_body(Rest, [Char | Text], Parts).

%% Restrict emqx_template's envelope to nonempty dotted paths.
%% Retain `${}` and `${.}`.
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
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

flush_text([], Parts) -> Parts;
flush_text(Text, Parts) -> [#tpl_text{text = iolist_to_binary(lists:reverse(Text))} | Parts].

finish_parts(Text, Parts) ->
    case lists:reverse(flush_text(Text, Parts)) of
        [] -> [#tpl_text{text = <<>>}];
        Result -> Result
    end.

strip_quotes(Source) ->
    binary:part(Source, 1, byte_size(Source) - 2).

is_variable_part(#tpl_placeholder{}) -> true;
is_variable_part(_) -> false.

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

encode_value(undefined, #{undefined_vars_as_null := false}) ->
    encode_string(<<"undefined">>, varchar);
encode_value(undefined, _Opts) ->
    <<"NULL">>;
encode_value(null, _Opts) ->
    <<"NULL">>;
encode_value(true, _Opts) ->
    encode_string(<<"true">>, varchar);
encode_value(false, _Opts) ->
    encode_string(<<"false">>, varchar);
encode_value(Value, _Opts) when is_integer(Value) -> integer_to_binary(Value);
encode_value(Value, _Opts) when is_float(Value) ->
    emqx_template:to_string(Value);
encode_value(<<"0x", Rest/binary>> = Value, _Opts) ->
    case Rest =/= <<>> andalso re:run(Rest, <<"^[0-9A-Fa-f]+$">>, [{capture, none}]) =:= match of
        true -> Value;
        false -> encode_string(Value, varchar)
    end;
encode_value(Value, _Opts) ->
    encode_string(to_text(Value), varchar).

-spec render_text_parts([part()], map()) -> binary().
render_text_parts(Parts, Opts) ->
    iolist_to_binary([
        case Part of
            #part_text{text = Text} ->
                Text;
            #part_value{value = undefined} ->
                case maps:get(undefined_vars_as_null, Opts, true) of
                    true -> <<"null">>;
                    false -> <<"undefined">>
                end;
            #part_value{value = Value} ->
                to_text(Value)
        end
     || Part <- Parts
    ]).

to_text(Value) when is_binary(Value) -> Value;
to_text(Value) -> iolist_to_binary(emqx_template:to_string(Value)).

encode_string(Text, Style) ->
    %% OTP ODBC uses SQL_NTS. An embedded NUL would truncate the statement:
    %% https://github.com/erlang/otp/blob/OTP-26.2.5.14/lib/odbc/c_src/odbcserver.c#L620-L642
    %% https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/using-length-and-indicator-values
    ok = assert_no_nul(Text),
    Escaped = binary:replace(Text, <<"'">>, <<"''">>, [global]),
    Prefix =
        case Style of
            nvarchar -> <<"N">>;
            varchar -> <<>>
        end,
    [Prefix, $', Escaped, $'].

assert_no_nul(Text) ->
    case binary:match(Text, <<0>>) of
        nomatch -> ok;
        _ -> error(nul_character_not_allowed)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Checks that compilation merges adjacent static SQL around a value placeholder.
compile_merges_raw_segments_test() ->
    {ok, Placeholder} = parse_placeholder(<<"${value}">>),
    {ok, #sqlserver_plan{plan = [#raw{sql = <<"(1, 2)">>}]}} =
        compile(<<"INSERT INTO t VALUES (1, 2)">>),
    {ok, #sqlserver_plan{plan = Plan}} =
        compile(<<"INSERT INTO t VALUES (1, ${value}, 2)">>),
    ?assertEqual(
        [
            #raw{sql = <<"(1, ">>},
            #value{placeholder = Placeholder},
            #raw{sql = <<", 2)">>}
        ],
        Plan
    ).

%% Checks nested SQL Server function calls containing placeholders.
function_expression_test() ->
    SQL = <<
        "INSERT INTO TransactionLog(MessageId, DateStamp) VALUES ("
        "${id}, DATEADD(MS, ${ms_shift}, DATEADD(S, ${s_shift}, '19700101 00:00:00:000')))"
    >>,
    {ok, Plan} = compile(SQL),
    ?assertMatch(
        {ok, _},
        render(Plan, #{id => <<"m1">>, ms_shift => 1, s_shift => 2}, null_opts())
    ).

%% Checks rendering of CASE, IIF, comparisons, null predicates, and logical expressions.
conditional_expression_test() ->
    SQL = <<
        "INSERT INTO t(a, b, c) VALUES ("
        "CASE WHEN ${v} >= 10 AND ${v} IS NOT NULL THEN N'large' ELSE N'small' END, "
        "CASE ${v} WHEN 10 THEN 1 ELSE 0 END, "
        "IIF(NOT ${v} = 10 OR ${v} < 0, 1, 0))"
    >>,
    {ok, Plan} = compile(SQL),
    ?assertEqual(
        {ok, <<
            "INSERT INTO [t] ([a], [b], [c]) VALUES ("
            "CASE WHEN 10 >= 10 AND 10 IS NOT NULL THEN N'large' ELSE N'small' END, "
            "CASE 10 WHEN 10 THEN 1 ELSE 0 END, "
            "IIF(NOT(10 = 10) OR 10 < 0, 1, 0))"
        >>},
        rendered_binary(render(Plan, #{v => 10}, null_opts()))
    ).

%% Checks timestamps, grouped values, null predicates, DEFAULT, functions, and quoted identifiers.
timestamp_default_and_grouped_expression_forms_test() ->
    SQL = <<
        "INSERT INTO \"a\"\"b\"([c]]d]) VALUES ("
        "CURRENT_TIMESTAMP, (1), ${v} IS NULL, "
        "CASE WHEN 1 = 1 THEN DEFAULT END, GETDATE(), '')"
    >>,
    {ok, Plan} = compile(SQL),
    ?assertEqual(
        {ok, <<
            "INSERT INTO [a\"b] ([c]]d]) VALUES ("
            "CURRENT_TIMESTAMP, (1), 1 IS NULL, "
            "CASE WHEN 1 = 1 THEN DEFAULT END, GETDATE(), '')"
        >>},
        rendered_binary(render(Plan, #{v => 1}, null_opts()))
    ).

%% Checks booleans, floats, numbers, and literal dollar text inside and outside strings.
scalar_template_value_types_test() ->
    SQL = <<
        "INSERT INTO t VALUES ("
        "'pre${$}${text}post', ${yes}, ${no}, ${float}, ${hexish}, '${number}')"
    >>,
    {ok, Plan} = compile(SQL),
    ?assertEqual(
        {ok, <<
            "INSERT INTO [t] VALUES ("
            "'pre$okpost', 'true', 'false', 1.5, '0xyz', '42')"
        >>},
        rendered_binary(
            render(
                Plan,
                #{
                    text => <<"ok">>,
                    yes => true,
                    no => false,
                    float => 1.5,
                    hexish => <<"0xyz">>,
                    number => 42
                },
                null_opts()
            )
        )
    ).

%% Checks missing string values with both undefined-value policies.
undefined_string_template_values_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t VALUES ('${missing}')">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] VALUES ('null')">>},
        rendered_binary(render(Plan, #{}, null_opts()))
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] VALUES ('undefined')">>},
        rendered_binary(
            render(Plan, #{}, #{undefined_vars_as_null => false})
        )
    ).

%% Checks that a placeholder with a leading dot resolves against the input map.
leading_dot_placeholder_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${.payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES ('hello')">>},
        rendered_binary(render(Plan, #{payload => <<"hello">>}, null_opts()))
    ).

%% Checks that an injection payload remains valid SQL inside an interpolated Unicode string.
quoted_string_boundary_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (N'pre${payload}post')">>),
    Attack = <<"'); DROP TABLE t; --\\">>,
    {ok, Rendered} = render(Plan, #{payload => Attack}, null_opts()),
    ?assertMatch({ok, _}, compile(Rendered)).

%% Checks batch escaping, indexed NUL errors, and the SQL Server row limit.
batch_shape_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${payload})">>),
    {ok, Rendered} = render_batch(
        Plan,
        [#{payload => <<"a');--">>}, #{payload => <<"b">>}],
        null_opts()
    ),
    ?assert(is_list(Rendered)),
    ?assertEqual(
        <<"INSERT INTO [t] ([v]) VALUES ('a'');--'), ('b')">>,
        iolist_to_binary(Rendered)
    ),
    ?assertEqual(
        {error,
            {sqlserver_template_render_failed, #{
                batch_index => 2,
                reason =>
                    {invalid_sql_template_value, #{
                        placeholder => "payload", reason => {error, nul_character_not_allowed}
                    }}
            }}},
        render_batch(
            Plan,
            [#{payload => <<"ok">>}, #{payload => <<"a", 0, "b">>}],
            null_opts()
        )
    ),
    Rows = lists:duplicate(1000, #{payload => <<"ok">>}),
    ?assertMatch({ok, _}, render_batch(Plan, Rows, null_opts())),
    ?assertEqual(
        {error, sqlserver_values_row_limit_exceeded},
        render_batch(Plan, [#{payload => <<"extra">>} | Rows], null_opts())
    ).

%% Checks rendering of arbitrary binary and Unicode values in SQL Server strings.
binary_and_unicode_value_test() ->
    {ok, ValuePlan} = compile(<<"INSERT INTO t(v) VALUES (${payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES ('", 16#FF, "')">>},
        rendered_binary(render(ValuePlan, #{payload => <<16#FF>>}, null_opts()))
    ),
    Unicode = <<"你好😀"/utf8>>,
    {ok, UnicodePlan} = compile(<<"INSERT INTO t(v) VALUES (N'pre${payload}post')">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES (N'pre", Unicode/binary, "post')">>},
        rendered_binary(render(UnicodePlan, #{payload => Unicode}, null_opts()))
    ).

%% Checks that a hexadecimal binary literal renders without string quoting.
binary_literal_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES (0x0010)">>},
        rendered_binary(render(Plan, #{payload => <<"0x0010">>}, null_opts()))
    ).

%% Checks that the null input value renders as SQL NULL.
null_value_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES (NULL)">>},
        rendered_binary(render(Plan, #{payload => null}, null_opts()))
    ).

%% Checks qualified object and function names and rejects excessive qualification.
qualified_names_test() ->
    {ok, Plan} = compile(<<"INSERT INTO mqtt..t(v) VALUES (dbo.fn(${payload}))">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [mqtt]..[t] ([v]) VALUES (dbo.fn(1))">>},
        rendered_binary(render(Plan, #{payload => 1}, null_opts()))
    ),
    {ok, OmittedPlan} = compile(<<"INSERT INTO server...t(v) VALUES (${payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO [server]...[t] ([v]) VALUES (1)">>},
        rendered_binary(render(OmittedPlan, #{payload => 1}, null_opts()))
    ),
    {ok, QuotedFunctionPlan} = compile(
        <<"INSERT INTO t(v) VALUES ([sys].[fn_varbintohexstr](0x01))">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO [t] ([v]) VALUES ([sys].[fn_varbintohexstr](0x01))">>},
        rendered_binary(render(QuotedFunctionPlan, #{}, null_opts()))
    ),
    ?assertMatch({error, _}, compile(<<"INSERT INTO a.b.c.d.e(v) VALUES (1)">>)).

%% Checks rejection of comments, dynamic targets, partial placeholders, and multiple rows.
reject_unsupported_syntax_test() ->
    ?assertMatch({error, _}, compile(<<"INSERT INTO t VALUES (1) --(* comment">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO ${table}(v) VALUES (1)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO [${table}](v) VALUES (1)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t(v) VALUES (${value}0)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t(v) VALUES (1), (2)">>)).

%% Checks identifier, whitespace, expression, length, and NUL lexical boundaries.
lexical_boundary_test() ->
    Accepted = [
        <<"INSERT INTO @t(v) VALUES (1)">>,
        <<"INSERT INTO #t(v) VALUES (1)">>,
        <<"INSERT INTO \"t\"(v) VALUES (1)">>,
        <<"INSERT", 11, "INTO t(v) VALUES (1)">>,
        <<"INSERT INTO t(v) VALUES (DEFAULT + 1)">>,
        iolist_to_binary([<<"INSERT INTO [">>, binary:copy(<<"a">>, 129), <<"](v) VALUES (1)">>])
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({ok, _}, compile(SQL)) end, Accepted),
    Rejected = [
        <<"INSERT INTO [](v) VALUES (1)">>,
        <<"INSERT INTO t(v) VALUES (0x)">>,
        <<"INSERT INTO t(v) VALUES (1e)">>,
        <<"INSERT INTO t(v) VALUES (1e+)">>,
        <<"INSERT INTO таблица(v) VALUES (1)"/utf8>>,
        <<"INSERT INTO foo", 16#C2, 16#A0, "bar(v) VALUES (1)">>,
        iolist_to_binary([<<"INSERT INTO [a">>, 0, <<"b](v) VALUES (1)">>]),
        iolist_to_binary([<<"INSERT INTO \"a">>, 0, <<"b\"(v) VALUES (1)">>]),
        iolist_to_binary([<<"INSERT INTO t(v) VALUES ('a">>, 0, <<"b')">>])
    ],
    lists:foreach(fun(SQL) -> ?assertMatch({error, _}, compile(SQL)) end, Rejected),
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${payload})">>),
    ?assertMatch({error, _}, render(Plan, #{payload => <<"a", 0, "b">>}, null_opts())).

rendered_binary({ok, SQL}) ->
    ?assert(is_list(SQL)),
    {ok, iolist_to_binary(SQL)}.

null_opts() ->
    #{undefined_vars_as_null => true}.

-endif.
