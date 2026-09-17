%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted Doris INSERT INTO VALUES templates.
-module(emqx_doris_sql).

-behaviour(emqx_sql_plan).

-export([compile/1, render/3, render_batch/3]).
-export_type([plan/0]).

-type placeholder() :: emqx_template:placeholder().

%% Represent string templates, e.g. 'aaa ${bbb} ccc' as
%% [
%%   #string_raw{sql = <<"aaa ">>},
%%   #string_placeholder{placeholder = {var, "bbb", ...}},
%%   #string_raw{sql = <<"ccc ">>}
%% ]
-record(string_raw, {sql :: binary()}).
-record(string_placeholder, {placeholder :: placeholder()}).
-type string_part() :: #string_raw{} | #string_placeholder{}.
-record(string_template, {parts :: [string_part()]}).

%% Pre-rendered parts of a full SQL statement
-record(raw, {sql :: binary()}).
-record(value, {placeholder :: placeholder()}).

-type render_op() :: #raw{} | #value{} | #string_template{}.
-type render_plan() :: [render_op()].

%% Doris has no row alias or ON DUPLICATE KEY UPDATE suffix.
%% See README.md, Maintenance: Upstream parser.
-record(doris_plan, {
    insert_prefix :: binary(),
    row_plan :: render_plan()
}).

-opaque plan() :: #doris_plan{}.

-define(BATCH_SEPARATOR, <<", ">>).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL) when is_binary(SQL) ->
    try
        case emqx_doris_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_doris_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_doris_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_doris_insert_template, Reason}}
        end
    catch
        Class:CatchReason -> {error, {invalid_doris_insert_template, {Class, CatchReason}}}
    end;
compile(SQL) ->
    compile(unicode:characters_to_binary(SQL)).

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#doris_plan{insert_prefix = Prefix, row_plan = Plan}, Data, Opts) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(#doris_plan{insert_prefix = Prefix, row_plan = Plan}, DataList, Opts) ->
    case render_batch_units(DataList, Plan, Opts, 1, []) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

render_batch_units([Data | Rest], Plan, Opts, Index, Acc) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Plan, Opts, Index + 1, [Rendered | Acc]);
        {error, Reason} ->
            {error, {doris_template_render_failed, #{batch_index => Index, reason => Reason}}}
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
    try
        {ok, Encoder()}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

compile_ast({insert, Target, Columns, Row}) ->
    TargetSQL = serialize_target(Target),
    ColumnsSQL = serialize_columns(Columns),
    Prefix = <<"INSERT INTO ", TargetSQL/binary, ColumnsSQL/binary, " VALUES ">>,
    RowPlan = merge_render_ops(compile_row(Row)),
    {ok, #doris_plan{insert_prefix = Prefix, row_plan = RowPlan}}.

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
    Source.

serialize_reference_path(Identifiers) ->
    iolist_to_binary(lists:join($., [serialize_reference_part(I) || I <- Identifiers])).

%% The restricted lexer leaves unselected Doris keywords as bare identifiers.
%% Keep quoting reference and function names as well as targets.
%% See README.md, Maintenance: Upstream lexer, lines 93-558.
serialize_reference_part({identifier, bare, Name}) ->
    quote_identifier(Name);
serialize_reference_part({identifier, backtick, Source}) ->
    ok = assert_no_placeholder(Source),
    Source.

quote_identifier(Name) ->
    Escaped = binary:replace(Name, <<"`">>, <<"``">>, [global]),
    <<"`", Escaped/binary, "`">>.

assert_no_placeholder(Source) ->
    case binary:match(Source, <<"${">>) of
        nomatch -> ok;
        _ -> error(dynamic_identifier_not_allowed)
    end.

compile_row(Expressions) ->
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
compile_expression(null) ->
    [#raw{sql = <<"NULL">>}];
compile_expression(true) ->
    [#raw{sql = <<"TRUE">>}];
compile_expression(false) ->
    [#raw{sql = <<"FALSE">>}];
compile_expression(default) ->
    [#raw{sql = <<"DEFAULT">>}];
compile_expression({builtin_expression, SQL}) ->
    [#raw{sql = SQL}];
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

%% Raw STRING_LITERAL bodies have no escape sequences or doubled delimiters.
%% See README.md, Maintenance: Upstream lexer, lines 599-604.
%% Normalize raw bodies because visitStringLiteral strips only the first character
%% and decodes backslashes even for R-prefixed tokens in Doris 2.1.9.
%% See README.md, Maintenance: LogicalPlanBuilder, lines 2391-2405.
parse_sql_string(<<R, _/binary>> = Source) when R =:= $R; R =:= $r ->
    Body = binary:part(Source, 2, byte_size(Source) - 3),
    parse_sql_string_body(Body, raw, [], []);
parse_sql_string(Source) ->
    Delimiter = binary:first(Source),
    Body = binary:part(Source, 1, byte_size(Source) - 2),
    parse_sql_string_body(Body, Delimiter, [], []).

parse_sql_string_body(<<>>, Delimiter, Text, Parts) ->
    case lists:reverse(flush_string_text(Text, Delimiter, Parts)) of
        [] when Delimiter =:= raw ->
            {static, iolist_to_binary(encode_string(<<>>))};
        [] ->
            {static, <<Delimiter, Delimiter>>};
        [#string_raw{sql = SQL}] ->
            {static, SQL};
        FinalParts ->
            ok = assert_safe_string_split(Text, Delimiter),
            {dynamic, FinalParts}
    end;
parse_sql_string_body(<<"${$}", Rest/binary>>, Delimiter, Text, Parts) ->
    parse_sql_string_body(Rest, Delimiter, [$$ | Text], Parts);
parse_sql_string_body(<<"${", _/binary>> = Bin, Delimiter, Text, Parts) ->
    ok = assert_safe_string_split(Text, Delimiter),
    {Placeholder, Rest} = take_placeholder(Bin),
    PartsNext = [
        #string_placeholder{placeholder = Placeholder}
        | flush_string_text(Text, Delimiter, Parts)
    ],
    parse_sql_string_body(Rest, Delimiter, [], PartsNext);
parse_sql_string_body(<<Char, Rest/binary>>, Delimiter, Text, Parts) ->
    parse_sql_string_body(Rest, Delimiter, [Char | Text], Parts).

flush_string_text([], _Delimiter, Parts) ->
    Parts;
flush_string_text(Text, raw, Parts) ->
    Body = iolist_to_binary(lists:reverse(Text)),
    [#string_raw{sql = iolist_to_binary(encode_string(Body))} | Parts];
flush_string_text(Text, Delimiter, Parts) ->
    Body = iolist_to_binary(lists:reverse(Text)),
    [#string_raw{sql = <<Delimiter, Body/binary, Delimiter>>} | Parts].

assert_safe_string_split(_ReversedText, raw) ->
    ok;
assert_safe_string_split(ReversedText, _Delimiter) ->
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

take_placeholder(Bin) ->
    case binary:match(Bin, <<"}">>) of
        {End, 1} ->
            Size = End + 1,
            Source = binary:part(Bin, 0, Size),
            Rest = binary:part(Bin, Size, byte_size(Bin) - Size),
            case emqx_sql_plan:parse_placeholder(Source) of
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
    emqx_template:to_string(Value);
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

%% Doris accepts NUL as \0 and uses UNHEX instead of MySQL hex literals.
%% See README.md, Maintenance: Upstream lexer, lines 599-604.
encode_string(Text) ->
    case unicode:characters_to_list(Text) of
        Chars when is_list(Chars) ->
            [<<"'">>, escape_string(Text), <<"'">>];
        _ ->
            encode_hex_string(Text)
    end.

encode_hex_string(Text) ->
    Hex = binary:encode_hex(Text),
    <<"UNHEX('", Hex/binary, "')">>.

escape_string(Text) ->
    escape_string(Text, []).

%% Match LogicalPlanBuilderAssistant.escapeBackSlash in the pinned Doris source.
%% See README.md, Maintenance: LogicalPlanBuilderAssistant.
%% The shared connector clears NO_BACKSLASH_ESCAPES and ANSI_QUOTES per session.
escape_string(<<>>, Acc) ->
    lists:reverse(Acc);
escape_string(<<0, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\0">> | Acc]);
escape_string(<<$\b, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\b">> | Acc]);
escape_string(<<$\t, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\t">> | Acc]);
escape_string(<<$\n, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\n">> | Acc]);
escape_string(<<$\r, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\r">> | Acc]);
escape_string(<<16#1A, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\Z">> | Acc]);
escape_string(<<$", Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\\"">> | Acc]);
escape_string(<<$', Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\'">> | Acc]);
escape_string(<<$\\, Rest/binary>>, Acc) ->
    escape_string(Rest, [<<"\\\\">> | Acc]);
escape_string(<<Char, Rest/binary>>, Acc) ->
    escape_string(Rest, [Char | Acc]).
