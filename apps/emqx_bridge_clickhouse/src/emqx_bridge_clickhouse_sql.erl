%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted ClickHouse INSERT templates.
-module(emqx_bridge_clickhouse_sql).

-export([compile/1, render/3, render_batch/3, parse_placeholder/1]).
-export_type([plan/0]).

-type placeholder() :: emqx_template:placeholder().

-record(tpl_text, {text :: binary()}).
-record(tpl_placeholder, {placeholder :: placeholder()}).
-record(part_text, {text :: binary()}).
-record(part_value, {value :: term()}).

-type template_part() :: #tpl_text{} | #tpl_placeholder{}.
-type part() :: #part_text{} | #part_value{}.

-record(raw, {sql :: binary()}).
-record(sql_value, {placeholder :: placeholder()}).
-record(json_value, {placeholder :: placeholder()}).
-record(sql_string, {parts :: [template_part()]}).
-record(json_string, {parts :: [template_part()]}).

-type sql_render_op() :: #raw{} | #sql_value{} | #sql_string{}.
-type json_render_op() :: #raw{} | #json_value{} | #json_string{}.
-type sql_render_plan() :: [sql_render_op()].
-type json_render_plan() :: [json_render_op()].

-record(clickhouse_values_plan, {
    insert_prefix :: binary(),
    plan :: sql_render_plan()
}).
-record(clickhouse_json_plan, {
    insert_prefix :: binary(),
    plan :: json_render_plan()
}).

-opaque plan() :: #clickhouse_values_plan{} | #clickhouse_json_plan{}.

-define(VALUES_BATCH_SEPARATOR, <<", ">>).
-define(JSON_BATCH_SEPARATOR, <<>>).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL0) ->
    SQL = unicode:characters_to_binary(SQL0),
    try
        case emqx_bridge_clickhouse_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_bridge_clickhouse_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_clickhouse_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_clickhouse_insert_template, Reason}}
        end
    catch
        Class:CatchReason -> {error, {invalid_clickhouse_insert_template, {Class, CatchReason}}}
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#clickhouse_values_plan{insert_prefix = Prefix, plan = Plan}, Data, Opts) ->
    render_insert(Prefix, Plan, Data, Opts);
render(#clickhouse_json_plan{insert_prefix = Prefix, plan = Plan}, Data, Opts) ->
    render_insert(Prefix, Plan, Data, Opts).

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(
    #clickhouse_values_plan{insert_prefix = Prefix, plan = Plan}, DataList, Opts
) ->
    render_batch_insert(Prefix, Plan, DataList, Opts, ?VALUES_BATCH_SEPARATOR);
render_batch(
    #clickhouse_json_plan{insert_prefix = Prefix, plan = Plan}, DataList, Opts
) ->
    render_batch_insert(Prefix, Plan, DataList, Opts, ?JSON_BATCH_SEPARATOR).

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

render_insert(Prefix, Template, Data, Opts) ->
    case render_unit(Template, Data, Opts) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

render_batch_insert(Prefix, Plan, DataList, Opts, Separator) ->
    case render_batch_units(DataList, Plan, Opts, _Index = 1, _Acc = [], Separator) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

render_batch_units([Data | Rest], Unit, Opts, Index, Acc, Separator) ->
    case render_unit(Unit, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Unit, Opts, Index + 1, [Rendered | Acc], Separator);
        {error, Reason} ->
            {error, {clickhouse_template_render_failed, #{batch_index => Index, reason => Reason}}}
    end;
render_batch_units([], _Unit, _Opts, _Index, Acc, Separator) ->
    {ok, lists:join(Separator, lists:reverse(Acc))}.

render_unit(Unit, Data, Opts) ->
    render_plan(Unit, Data, Opts, #{}, []).

render_plan([#raw{sql = SQL} | Rest], Data, Opts, Cache, Acc) ->
    render_plan(Rest, Data, Opts, Cache, [SQL | Acc]);
render_plan([#sql_value{placeholder = Placeholder} | Rest], Data, Opts, Cache0, Acc) ->
    render_value_op(
        Placeholder,
        fun(Value) -> encode_value(Value, Opts) end,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([#json_value{placeholder = Placeholder} | Rest], Data, Opts, Cache0, Acc) ->
    render_value_op(
        Placeholder,
        fun(Value) -> encode_json_value(Value, Opts) end,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([#sql_string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(
        Parts,
        fun(Resolved) -> encode_sql_string(render_text_parts(Resolved, Opts)) end,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([#json_string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(
        Parts,
        fun(Resolved) -> emqx_utils_json:encode(render_text_parts(Resolved, Opts)) end,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([], _Data, _Opts, _Cache, Acc) ->
    {ok, lists:reverse(Acc)}.

render_value_op(Placeholder, Encoder, Rest, Data, Opts, Cache0, Acc) ->
    case resolve_placeholder(Placeholder, Data, Cache0) of
        {ok, Value, Cache} ->
            case encode_result(fun() -> Encoder(Value) end) of
                {ok, Encoded} -> render_plan(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} -> {error, render_error(Placeholder, Reason)}
            end;
        {error, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end.

render_template_op(Parts, Encoder, Rest, Data, Opts, Cache0, Acc) ->
    case resolve_template_parts(Parts, Data, Cache0, []) of
        {ok, Resolved, Cache} ->
            case encode_result(fun() -> Encoder(Resolved) end) of
                {ok, Encoded} -> render_plan(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} -> {error, render_template_error(Parts, Reason)}
            end;
        {error, Placeholder, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end.

encode_result(Encoder) ->
    try Encoder() of
        {ok, _} = Ok -> Ok;
        {error, _} = Error -> Error;
        Encoded -> {ok, Encoded}
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

compile_ast({insert, #{target := Target, columns := Columns, source := Source}}) ->
    TargetSQL = serialize_target(Target),
    ColumnsSQL = serialize_columns(Columns),
    case Source of
        {values, Rows} ->
            compile_rows(<<"INSERT INTO ", TargetSQL/binary, ColumnsSQL/binary, " VALUES ">>, Rows);
        {format_rows, Format, Rows} ->
            case string:lowercase(Format) of
                <<"values">> ->
                    Head =
                        <<"INSERT INTO ", TargetSQL/binary, ColumnsSQL/binary, " FORMAT Values ">>,
                    compile_rows(Head, Rows);
                _ ->
                    {error, {unsupported_clickhouse_format, Format}}
            end;
        {format_json, Format, JSON} ->
            case string:lowercase(Format) of
                <<"jsoncompacteachrow">> ->
                    Head = <<
                        "INSERT INTO ",
                        TargetSQL/binary,
                        ColumnsSQL/binary,
                        " FORMAT JSONCompactEachRow "
                    >>,
                    {ok, #clickhouse_json_plan{
                        insert_prefix = Head,
                        plan = merge_json_render_ops(compile_json(JSON))
                    }};
                _ ->
                    {error, {unsupported_clickhouse_format, Format}}
            end
    end.

compile_rows(Head, Rows) ->
    Unit = merge_sql_render_ops(join_ops(<<", ">>, [compile_row(Row) || Row <- Rows])),
    {ok, #clickhouse_values_plan{insert_prefix = Head, plan = Unit}}.

serialize_target(Identifiers) ->
    iolist_to_binary(
        lists:join($., [serialize_identifier(Identifier) || Identifier <- Identifiers])
    ).

serialize_columns(undefined) ->
    <<>>;
serialize_columns(Columns) ->
    Content = [serialize_column(Column) || Column <- Columns],
    iolist_to_binary([" (", lists:join(<<", ">>, Content), ")"]).

serialize_column(star) ->
    <<"*">>;
serialize_column({except, Identifiers}) ->
    [
        <<"* EXCEPT (">>,
        lists:join(<<", ">>, [serialize_identifier(I) || I <- Identifiers]),
        <<")">>
    ];
serialize_column(Identifier) ->
    serialize_identifier(Identifier).

serialize_identifier({identifier, bare, Name}) ->
    quote_identifier(Name);
serialize_identifier({identifier, Style, Source}) when Style =:= double; Style =:= backtick ->
    quote_identifier(decode_identifier(Source, Style)).

quote_identifier(Name) ->
    Escaped = binary:replace(
        Name,
        [<<"\\">>, <<"`">>],
        <<"\\">>,
        [global, {insert_replaced, 1}]
    ),
    <<"`", Escaped/binary, "`">>.

decode_identifier(Source, double) ->
    ok = assert_no_identifier_placeholder(Source),
    decode_quoted_identifier(strip_quotes(Source), $", []);
decode_identifier(Source, backtick) ->
    ok = assert_no_identifier_placeholder(Source),
    decode_quoted_identifier(strip_quotes(Source), $`, []).

assert_no_identifier_placeholder(Source) ->
    case binary:match(Source, <<"${">>) of
        nomatch -> ok;
        _ -> error(dynamic_identifier_not_allowed)
    end.

decode_quoted_identifier(<<Quote, Quote, Rest/binary>>, Quote, Acc) ->
    decode_quoted_identifier(Rest, Quote, [Quote | Acc]);
decode_quoted_identifier(<<$\\, $x, High, Low, Rest/binary>>, Quote, Acc) ->
    decode_quoted_identifier(Rest, Quote, [hex_byte(High, Low) | Acc]);
decode_quoted_identifier(<<$\\, $N, Rest/binary>>, Quote, Acc) ->
    decode_quoted_identifier(Rest, Quote, Acc);
decode_quoted_identifier(<<$\\, Escaped, Rest/binary>>, Quote, Acc) ->
    decode_quoted_identifier(Rest, Quote, [decode_sql_escape(Escaped) | Acc]);
decode_quoted_identifier(<<Char, Rest/binary>>, Quote, Acc) ->
    decode_quoted_identifier(Rest, Quote, [Char | Acc]);
decode_quoted_identifier(<<>>, _Quote, Acc) ->
    iolist_to_binary(lists:reverse(Acc)).

compile_row({row, Expressions}) ->
    [#raw{sql = <<"(">>} | join_ops(<<", ">>, [compile_expression(E) || E <- Expressions])] ++
        [#raw{sql = <<")">>}].

compile_expression({var, Placeholder}) ->
    [#sql_value{placeholder = Placeholder}];
compile_expression({string, Source}) ->
    Parts = parse_sql_string(Source),
    compile_sql_string(Parts);
compile_expression({number, Number}) ->
    [#raw{sql = Number}];
compile_expression(null) ->
    [#raw{sql = <<"NULL">>}];
compile_expression(true) ->
    [#raw{sql = <<"true">>}];
compile_expression(false) ->
    [#raw{sql = <<"false">>}];
compile_expression({identifier_value, Name}) ->
    [#raw{sql = Name}];
compile_expression({call, Name, Args}) ->
    [#raw{sql = <<Name/binary, "(">>} | join_ops(<<", ">>, [compile_expression(E) || E <- Args])] ++
        [#raw{sql = <<")">>}];
compile_expression({group, Expression}) ->
    [#raw{sql = <<"(">>} | compile_expression(Expression)] ++ [#raw{sql = <<")">>}];
compile_expression({array, Expressions}) ->
    [#raw{sql = <<"[">>} | join_ops(<<", ">>, [compile_expression(E) || E <- Expressions])] ++
        [#raw{sql = <<"]">>}];
compile_expression({unary, Operator, Expression}) ->
    [#raw{sql = <<(atom_to_binary(Operator))/binary, "(">>} | compile_expression(Expression)] ++
        [#raw{sql = <<")">>}];
compile_expression({binary, Operator, Left, Right}) ->
    compile_expression(Left) ++
        [#raw{sql = <<" ", (atom_to_binary(Operator))/binary, " ">>}] ++
        compile_expression(Right).

compile_json({json_array, Values}) ->
    [#raw{sql = <<"[">>} | join_ops(<<",">>, [compile_json(V) || V <- Values])] ++
        [#raw{sql = <<"]">>}];
compile_json({json_object, Members}) ->
    Compiled = [compile_json_member(Member) || Member <- Members],
    [#raw{sql = <<"{">>} | join_ops(<<",">>, Compiled)] ++ [#raw{sql = <<"}">>}];
compile_json({json_var, Placeholder}) ->
    [#json_value{placeholder = Placeholder}];
compile_json({json_string, Source}) ->
    Parts = parse_json_string(Source),
    compile_json_string(Parts);
compile_json({json_number, Number}) ->
    case
        re:run(
            Number,
            <<"^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$">>,
            [{capture, none}]
        )
    of
        match -> [#raw{sql = Number}];
        nomatch -> error({invalid_json_number, Number})
    end;
compile_json(true) ->
    [#raw{sql = <<"true">>}];
compile_json(false) ->
    [#raw{sql = <<"false">>}];
compile_json(null) ->
    [#raw{sql = <<"null">>}].

compile_json_member({KeySource, Value}) ->
    Key = json_parts_to_static(parse_json_string(KeySource)),
    [#raw{sql = emqx_utils_json:encode(Key)}, #raw{sql = <<":">>} | compile_json(Value)].

json_parts_to_static([#tpl_text{text = Text}]) ->
    Text;
json_parts_to_static(_) ->
    error(dynamic_json_object_key_not_allowed).

compile_sql_string([#tpl_text{text = Text}]) ->
    [#raw{sql = encode_sql_string(Text)}];
compile_sql_string(Parts) ->
    [#sql_string{parts = Parts}].

compile_json_string([#tpl_text{text = Text}]) ->
    [#raw{sql = emqx_utils_json:encode(Text)}];
compile_json_string(Parts) ->
    [#json_string{parts = Parts}].

join_ops(_Separator, []) ->
    [];
join_ops(Separator, [Ops | Rest]) ->
    lists:foldl(fun(Next, Acc) -> Acc ++ [#raw{sql = Separator} | Next] end, Ops, Rest).

-spec merge_sql_render_ops(sql_render_plan()) -> sql_render_plan().
merge_sql_render_ops(Ops) ->
    merge_render_ops(Ops).

-spec merge_json_render_ops(json_render_plan()) -> json_render_plan().
merge_json_render_ops(Ops) ->
    merge_render_ops(Ops).

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

parse_sql_string(Source) ->
    parse_sql_string_body(strip_quotes(Source), [], []).

parse_sql_string_body(<<>>, Text, Parts) ->
    finish_parts(Text, Parts);
parse_sql_string_body(<<$\\, $x, High, Low, Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, [hex_byte(High, Low) | Text], Parts);
parse_sql_string_body(<<$\\, $N, Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, Text, Parts);
parse_sql_string_body(<<$\\, Escaped, Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, [decode_sql_escape(Escaped) | Text], Parts);
parse_sql_string_body(<<$', $', Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, [$' | Text], Parts);
parse_sql_string_body(<<"${$}", Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, [$$ | Text], Parts);
parse_sql_string_body(<<"${", _/binary>> = Bin, Text, Parts) ->
    {Placeholder, Rest} = take_placeholder(Bin),
    parse_sql_string_body(
        Rest,
        [],
        [#tpl_placeholder{placeholder = Placeholder} | flush_text(Text, Parts)]
    );
parse_sql_string_body(<<Char, Rest/binary>>, Text, Parts) ->
    parse_sql_string_body(Rest, [Char | Text], Parts).

decode_sql_escape($a) ->
    7;
decode_sql_escape($n) ->
    $\n;
decode_sql_escape($r) ->
    $\r;
decode_sql_escape($t) ->
    $\t;
decode_sql_escape($b) ->
    $\b;
decode_sql_escape($f) ->
    $\f;
decode_sql_escape($e) ->
    27;
decode_sql_escape($v) ->
    11;
decode_sql_escape($0) ->
    0;
decode_sql_escape(Char) when
    Char =:= $\\; Char =:= $'; Char =:= $"; Char =:= $`; Char =:= $/; Char =:= $=
->
    Char;
decode_sql_escape(Char) ->
    <<$\\, Char>>.

parse_json_string(Source) ->
    parse_json_string_body(strip_quotes(Source), [], []).

parse_json_string_body(<<>>, Text, Parts) ->
    finish_parts(Text, Parts);
parse_json_string_body(<<$\\, $u, H1, H2, H3, H4, Rest/binary>>, Text, Parts) ->
    Codepoint = hex_codepoint(H1, H2, H3, H4),
    case Codepoint >= 16#D800 andalso Codepoint =< 16#DBFF of
        true ->
            case Rest of
                <<$\\, $u, L1, L2, L3, L4, Tail/binary>> ->
                    Low = hex_codepoint(L1, L2, L3, L4),
                    case Low >= 16#DC00 andalso Low =< 16#DFFF of
                        true ->
                            Combined =
                                16#10000 +
                                    ((Codepoint - 16#D800) bsl 10) +
                                    (Low - 16#DC00),
                            parse_json_string_body(
                                Tail,
                                [unicode:characters_to_binary([Combined]) | Text],
                                Parts
                            );
                        false ->
                            error(invalid_json_surrogate_pair)
                    end;
                _ ->
                    error(invalid_json_surrogate_pair)
            end;
        false when Codepoint >= 16#DC00, Codepoint =< 16#DFFF ->
            error(invalid_json_surrogate);
        false ->
            parse_json_string_body(Rest, [unicode:characters_to_binary([Codepoint]) | Text], Parts)
    end;
parse_json_string_body(<<$\\, Escaped, Rest/binary>>, Text, Parts) ->
    parse_json_string_body(Rest, [decode_json_escape(Escaped) | Text], Parts);
parse_json_string_body(<<"${$}", Rest/binary>>, Text, Parts) ->
    parse_json_string_body(Rest, [$$ | Text], Parts);
parse_json_string_body(<<"${", _/binary>> = Bin, Text, Parts) ->
    {Placeholder, Rest} = take_placeholder(Bin),
    parse_json_string_body(
        Rest,
        [],
        [#tpl_placeholder{placeholder = Placeholder} | flush_text(Text, Parts)]
    );
parse_json_string_body(<<Char, _/binary>>, _Text, _Parts) when Char < 16#20; Char =:= $" ->
    error(invalid_json_string);
parse_json_string_body(<<Char, Rest/binary>>, Text, Parts) ->
    parse_json_string_body(Rest, [Char | Text], Parts).

decode_json_escape($") -> $";
decode_json_escape($\\) -> $\\;
decode_json_escape($/) -> $/;
decode_json_escape($b) -> $\b;
decode_json_escape($f) -> $\f;
decode_json_escape($n) -> $\n;
decode_json_escape($r) -> $\r;
decode_json_escape($t) -> $\t;
decode_json_escape(Char) -> error({unsupported_json_escape, Char}).

hex_byte(High, Low) ->
    (hex_digit(High) bsl 4) bor hex_digit(Low).

hex_codepoint(H1, H2, H3, H4) ->
    (hex_digit(H1) bsl 12) bor
        (hex_digit(H2) bsl 8) bor
        (hex_digit(H3) bsl 4) bor
        hex_digit(H4).

hex_digit(Char) when Char >= $0, Char =< $9 -> Char - $0;
hex_digit(Char) when Char >= $A, Char =< $F -> Char - $A + 10;
hex_digit(Char) when Char >= $a, Char =< $f -> Char - $a + 10;
hex_digit(Char) -> error({invalid_hex_digit, Char}).

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

flush_text([], Parts) ->
    Parts;
flush_text(Text, Parts) ->
    [#tpl_text{text = iolist_to_binary(lists:reverse(Text))} | Parts].

finish_parts(Text, Parts) ->
    case lists:reverse(flush_text(Text, Parts)) of
        [] -> [#tpl_text{text = <<>>}];
        Result -> Result
    end.

strip_quotes(Source) ->
    binary:part(Source, 1, byte_size(Source) - 2).

encode_json_value(undefined, #{undefined_vars_as_null := false}) ->
    emqx_utils_json:encode(<<"undefined">>);
encode_json_value(undefined, _Opts) ->
    <<"null">>;
encode_json_value(Value, _Opts) ->
    emqx_utils_json:encode(Value).

encode_value(undefined, #{undefined_vars_as_null := false}) ->
    encode_sql_string(<<"undefined">>);
encode_value(undefined, _Opts) ->
    <<"NULL">>;
encode_value(null, _Opts) ->
    <<"NULL">>;
encode_value(true, _Opts) ->
    <<"true">>;
encode_value(false, _Opts) ->
    <<"false">>;
encode_value(Value, _Opts) when is_integer(Value) ->
    integer_to_binary(Value);
encode_value(Value, _Opts) when is_float(Value) ->
    case Value =:= Value of
        true -> float_to_binary(Value, [compact]);
        false -> {error, non_finite_number}
    end;
encode_value(Value, _Opts) ->
    encode_sql_string(to_text(Value)).

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

encode_sql_string(Text) ->
    Escaped = binary:replace(Text, [<<"\\">>, <<"'">>], <<"\\">>, [global, {insert_replaced, 1}]),
    <<"'", Escaped/binary, "'">>.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_insert_sql_template_test() ->
    Accepted = [
        {<<"insert into tag_VALUES(tag_values,Timestamp) values (${tagvalues},${date})"/utf8>>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"tagvalues">>)},
                #raw{sql = <<", ">>},
                #sql_value{placeholder = test_placeholder(<<"date">>)},
                #raw{sql = <<")">>}
            ]},
        {<<"INSERT INTO Values_таблица (идентификатор, имя, возраст)   VALUES \t (${id}, 'Иван', 25)  "/utf8>>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"id">>)},
                #raw{sql = <<", 'Иван', 25)"/utf8>>}
            ]},
        {<<"INSERT INTO Values_таблица (идентификатор, имя, возраст)   VALUES \t (${id}, 'Иван', 25);  "/utf8>>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"id">>)},
                #raw{sql = <<", 'Иван', 25)"/utf8>>}
            ]},
        {<<"  inSErt into 表格(标识,名字,年龄)values(${id},'李四', 35) ; "/utf8>>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_value{placeholder = test_placeholder(<<"id">>)},
            #raw{sql = <<", '李四', 35)"/utf8>>}
        ]},
        {<<"INSERT INTO mqtt_test(payload, arrived) VALUES (${payload}, FROM_UNIXTIME((${timestamp}/1000)))">>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"payload">>)},
                #raw{sql = <<", FROM_UNIXTIME((">>},
                #sql_value{placeholder = test_placeholder(<<"timestamp">>)},
                #raw{sql = <<" / 1000)))">>}
            ]},
        {<<"insert into таблица (идентификатор,имя,возраст) VALUES(${id},'Алексей',30)"/utf8>>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"id">>)},
                #raw{sql = <<", 'Алексей', 30)"/utf8>>}
            ]},
        {<<"INSERT into 表格 (标识, 名字, 年龄) VALUES (${id}, '张三', 22)"/utf8>>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_value{placeholder = test_placeholder(<<"id">>)},
            #raw{sql = <<", '张三', 22)"/utf8>>}
        ]},
        {<<"  inSErt into 表格(标识,名字,年龄)values(${id},'李四', 35)"/utf8>>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_value{placeholder = test_placeholder(<<"id">>)},
            #raw{sql = <<", '李四', 35)"/utf8>>}
        ]},
        {<<"inSErt  INTO  table75 (column1, column2, column3) values (${one}, ${two},${three})"/utf8>>,
            <<", ">>, [
                #raw{sql = <<"(">>},
                #sql_value{placeholder = test_placeholder(<<"one">>)},
                #raw{sql = <<", ">>},
                #sql_value{placeholder = test_placeholder(<<"two">>)},
                #raw{sql = <<", ">>},
                #sql_value{placeholder = test_placeholder(<<"three">>)},
                #raw{sql = <<")">>}
            ]},
        {<<"INSERT Into some_table      values\t(${tag1},   ${tag2}  )">>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_value{placeholder = test_placeholder(<<"tag1">>)},
            #raw{sql = <<", ">>},
            #sql_value{placeholder = test_placeholder(<<"tag2">>)},
            #raw{sql = <<")">>}
        ]},
        {<<"INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2)">>, <<", ">>, [
            #raw{sql = <<"(2, 2)">>}
        ]},
        {<<"INSERT INTO insert_select_testtable (* EXCEPT(b))Values(2, 2), (3, ${five})">>,
            <<", ">>, [
                #raw{sql = <<"(2, 2), (3, ">>},
                #sql_value{placeholder = test_placeholder(<<"five">>)},
                #raw{sql = <<")">>}
            ]},
        {<<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values (v11, v12, v13), (v21, v22, v23)">>,
            <<", ">>, [#raw{sql = <<"(v11, v12, v13), (v21, v22, v23)">>}]},
        {<<"INSERT INTO mqtt_test(key, data, arrived) FORMAT Values (1, 'a', 2)">>, <<", ">>, [
            #raw{sql = <<"(1, 'a', 2)">>}
        ]},
        {<<"INSERT INTO mqtt_test(data) VALUES ('xxx${var}yyy')">>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_string{
                parts = [
                    #tpl_text{text = <<"xxx">>},
                    test_tpl_placeholder(<<"var">>),
                    #tpl_text{text = <<"yyy">>}
                ]
            },
            #raw{sql = <<")">>}
        ]},
        {<<"INSERT INTO mqtt_test(data) VALUES ('${var}')">>, <<", ">>, [
            #raw{sql = <<"(">>},
            #sql_string{parts = [test_tpl_placeholder(<<"var">>)]},
            #raw{sql = <<")">>}
        ]},
        {<<"INSERT INTO mqtt_test(key, data, arrived) FORMAT JSONCompactEachRow [${key}, \"${data}\", ${timestamp}]">>,
            <<>>, [
                #raw{sql = <<"[">>},
                #json_value{placeholder = test_placeholder(<<"key">>)},
                #raw{sql = <<",">>},
                #json_string{parts = [test_tpl_placeholder(<<"data">>)]},
                #raw{sql = <<",">>},
                #json_value{placeholder = test_placeholder(<<"timestamp">>)},
                #raw{sql = <<"]">>}
            ]},
        {<<"INSERT INTO mqtt_test(data) FORMAT JSONCompactEachRow [\"xxx${var}xxx\"]">>, <<>>, [
            #raw{sql = <<"[">>},
            #json_string{
                parts = [
                    #tpl_text{text = <<"xxx">>},
                    test_tpl_placeholder(<<"var">>),
                    #tpl_text{text = <<"xxx">>}
                ]
            },
            #raw{sql = <<"]">>}
        ]}
    ],
    lists:foreach(
        fun({SQL, ExpectedSeparator, ExpectedTemplate}) ->
            {ok, Plan} = compile(SQL),
            {Separator, Template} = test_plan_template(Plan),
            ?assertEqual(ExpectedSeparator, Separator),
            ?assertEqual(ExpectedTemplate, Template)
        end,
        Accepted
    ),
    Rejected = [
        %% SQL comments are not allowed.
        <<"INSERT INTO mqtt_test VALUES (1) -- comment">>,
        %% Target placeholders are not allowed.
        <<"INSERT INTO ${table} VALUES (1)">>,
        %% A placeholder must occupy a complete value token.
        <<"INSERT INTO mqtt_test VALUES (${value}0)">>,
        %% A target can contain at most database and table components.
        <<"insert into PI.dbo.tags(tag_values,Timestamp) values (${tagvalues},${date}  )"/utf8>>,
        %% A target can contain at most database and table components.
        <<"insert into PI.dbo.tags( tag_value,Timestamp)  VALUES\t\t(   ${tagvalues},   ${date} )"/utf8>>,
        %% A target can contain at most database and table components.
        <<"insert into PI.dbo.tags(tag_value , Timestamp )vALues(${tagvalues},${date})"/utf8>>,
        %% JSONCompactEachRow data must be valid JSON.
        <<"INSERT INTO mqtt_test(key, data, arrived)",
            " FORMAT JSONCompactEachRow [(${key}, \"${data}\", ${timestamp})]">>,
        %% AnyFORMAT is not a supported format.
        <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT AnyFORMAT  👋    .."/utf8>>,
        %% FORMAT Values requires at least one row.
        <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values">>,
        %% FORMAT Values requires at least one row.
        <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values  ">>,
        %% AnyFORMAT is not a supported format.
        <<"INSERT INTO mqtt_test FORMAT AnyFORMAT payload">>,
        %% FORMAT Values requires at least one row.
        <<"INSERT INTO mqtt_test FORMAT Values">>
    ],
    lists:foreach(
        fun(SQL) ->
            ?assertMatch({error, _}, compile(SQL))
        end,
        Rejected
    ).

test_placeholder(Name) ->
    {ok, Placeholder} = parse_placeholder(<<"${", Name/binary, "}">>),
    Placeholder.

test_tpl_placeholder(Name) ->
    #tpl_placeholder{placeholder = test_placeholder(Name)}.

test_plan_template(#clickhouse_values_plan{plan = Plan}) ->
    {<<", ">>, Plan};
test_plan_template(#clickhouse_json_plan{plan = Plan}) ->
    {<<>>, Plan}.

compile_merges_raw_segments_test() ->
    Placeholder = test_placeholder(<<"value">>),
    {ok, #clickhouse_values_plan{plan = [#raw{sql = <<"(1, 2)">>}]}} =
        compile(<<"INSERT INTO t VALUES (1, 2)">>),
    {ok, #clickhouse_values_plan{plan = SQLPlan}} =
        compile(<<"INSERT INTO t VALUES (1, ${value}, 2)">>),
    ?assertEqual(
        [
            #raw{sql = <<"(1, ">>},
            #sql_value{placeholder = Placeholder},
            #raw{sql = <<", 2)">>}
        ],
        SQLPlan
    ),
    {ok, #clickhouse_json_plan{plan = JSONPlan}} =
        compile(<<"INSERT INTO t FORMAT JSONCompactEachRow [1, ${value}, 2]">>),
    ?assertEqual(
        [
            #raw{sql = <<"[1,">>},
            #json_value{placeholder = Placeholder},
            #raw{sql = <<",2]">>}
        ],
        JSONPlan
    ).

values_compile_and_render_test() ->
    {ok, Plan} = compile(
        <<"INSERT INTO mqtt_test(key, data, arrived) VALUES (${key}, '${data}', ${timestamp})">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `mqtt_test` (`key`, `data`, `arrived`) VALUES (1, 'hello', 2)">>},
        rendered_binary(render(Plan, #{key => 1, data => <<"hello">>, timestamp => 2}, null_opts()))
    ).

leading_dot_placeholder_test() ->
    {ok, Plan} = compile(<<"INSERT INTO mqtt_test(payload) VALUES (${.payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `mqtt_test` (`payload`) VALUES ('hello')">>},
        rendered_binary(render(Plan, #{payload => <<"hello">>}, null_opts()))
    ).

json_compile_and_render_test() ->
    {ok, Plan} = compile(
        <<"INSERT INTO mqtt_test(key, data) FORMAT JSONCompactEachRow [${key}, \"${data}\"]">>
    ),
    ?assertEqual(
        {ok,
            <<"INSERT INTO `mqtt_test` (`key`, `data`) FORMAT JSONCompactEachRow [1,\"a\\\"b\"]">>},
        rendered_binary(render(Plan, #{key => 1, data => <<"a\"b">>}, null_opts()))
    ).

static_escape_boundary_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES ('\\\\${v}')">>),
    Attack = <<"'); INSERT INTO t(v) VALUES ('owned'); --">>,
    {ok, Rendered} = render(Plan, #{v => Attack}, null_opts()),
    ?assertMatch({ok, _}, compile(Rendered)).

batch_shape_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t(v) VALUES (${v})">>),
    {ok, Rendered} = render_batch(Plan, [#{v => <<"a');--">>}, #{v => <<"b">>}], null_opts()),
    ?assert(is_list(Rendered)),
    ?assertMatch({ok, _}, compile(Rendered)).

reject_unsupported_syntax_test() ->
    ?assertMatch({error, _}, compile(<<"INSERT INTO t VALUES (1) -- comment">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO ${table} VALUES (1)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO `${table}` VALUES (1)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t VALUES (${value}0)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t FORMAT CSV ${payload}">>)).

identifier_escape_test() ->
    ?assertEqual(<<"`a\\\\\\`b`">>, quote_identifier(<<"a\\`b">>)),
    ?assertEqual(<<"`a\\\\`">>, quote_identifier(<<"a\\">>)).

rendered_binary({ok, SQL}) ->
    ?assert(is_list(SQL)),
    {ok, iolist_to_binary(SQL)}.

null_opts() ->
    #{undefined_vars_as_null => true}.

-endif.
