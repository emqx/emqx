%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted QuasarDB INSERT INTO VALUES templates.
-module(emqx_bridge_quasardb_sql).

-behaviour(emqx_sql_plan).

-on_load(on_load/0).

-export([compile/1, render/3, render_batch/3]).
-export_type([plan/0]).

-type placeholder() :: emqx_template:placeholder().

-record(tpl_text, {text :: binary()}).
-record(tpl_placeholder, {placeholder :: placeholder()}).
-record(part_text, {text :: binary()}).
-record(part_value, {value :: term()}).

-type template_part() :: #tpl_text{} | #tpl_placeholder{}.
-type part() :: #part_text{} | #part_value{}.

-record(raw, {sql :: binary()}).
-record(value, {placeholder :: placeholder()}).
-record(string, {parts :: [template_part()]}).

-type render_op() :: #raw{} | #value{} | #string{}.
-type render_plan() :: [render_op()].

-record(quasardb_plan, {
    insert_prefix :: binary(),
    row_plan :: render_plan()
}).

-opaque plan() :: #quasardb_plan{}.

-define(BATCH_SEPARATOR, <<", ">>).
-define(UNQUOTED_TIMESTAMP_RE_PT_KEY, emqx_bridge_quasardb_sql_unquoted_timestamp_re_pt_key).
-define(UNQUOTED_TIMESTAMP_RE,
    ~b"""
    ^
    (?i:
        # A year with an optional date and time.
        [0-9]{4}
        (?:
            -[0-9]{2}-[0-9]{2}
            (?:
                T[0-9]{2}:[0-9]{2}
                (?:
                    :[0-9]{2}(?:\\.[0-9]{1,9})?
                )?
                Z?
            )?
        )?
        |
        # Special timestamp values with optional parentheses.
        (?:now|today|yesterday|tomorrow|epoch|end_of_time)(?:\\(\\))?
    )
    \\z
    """
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL0) ->
    try
        SQL = unicode:characters_to_binary(SQL0),
        ok = assert_no_nul(SQL),
        case emqx_bridge_quasardb_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_bridge_quasardb_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_quasardb_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_quasardb_insert_template, Reason}}
        end
    catch
        Class:CatchReason ->
            {error, {invalid_quasardb_insert_template, {Class, CatchReason}}}
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#quasardb_plan{insert_prefix = Prefix, row_plan = Plan}, Data, Opts) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(#quasardb_plan{insert_prefix = Prefix, row_plan = Plan}, DataList, Opts) ->
    case render_batch_units(DataList, Plan, Opts, 1, []) of
        {ok, Rendered} -> {ok, [Prefix, Rendered]};
        {error, _} = Error -> Error
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

on_load() ->
    {ok, MP} = re:compile(?UNQUOTED_TIMESTAMP_RE, [extended]),
    persistent_term:put(?UNQUOTED_TIMESTAMP_RE_PT_KEY, MP).

render_batch_units([Data | Rest], Plan, Opts, Index, Acc) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Plan, Opts, Index + 1, [Rendered | Acc]);
        {error, Reason} ->
            {error, {quasardb_template_render_failed, #{batch_index => Index, reason => Reason}}}
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
render_plan([#string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    case resolve_template_parts(Parts, Data, Cache0, []) of
        {ok, Resolved, Cache} ->
            case encode_result(fun() -> encode_string_parts(Resolved, Opts) end) of
                {ok, Encoded} -> render_plan(Rest, Data, Opts, Cache, [Encoded | Acc]);
                {error, Reason} -> {error, render_template_error(Parts, Reason)}
            end;
        {error, Placeholder, Reason} ->
            {error, render_error(Placeholder, Reason)}
    end;
render_plan([], _Data, _Opts, _Cache, Acc) ->
    {ok, lists:reverse(Acc)}.

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

encode_result(Encoder) ->
    try
        {ok, Encoder()}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

compile_ast({insert, Target, Columns, Row}) ->
    case validate_insert(Columns, Row) of
        ok ->
            TargetSQL = serialize_identifier(Target),
            ColumnsSQL = lists:join(<<", ">>, [serialize_identifier(I) || I <- Columns]),
            Prefix = iolist_to_binary([
                <<"INSERT INTO ">>, TargetSQL, <<" (">>, ColumnsSQL, <<") VALUES ">>
            ]),
            RowPlan = merge_render_ops(compile_row(Row)),
            {ok, #quasardb_plan{insert_prefix = Prefix, row_plan = RowPlan}};
        {error, _} = Error ->
            Error
    end.

validate_insert(Columns, Row) ->
    Names = [identifier_name(I) || I <- Columns],
    case Names of
        [<<"$timestamp">>, _ | _] ->
            case length(Names) =:= length(lists:usort(Names)) of
                false -> {error, duplicate_quasardb_column};
                true when length(Names) =:= length(Row) -> ok;
                true -> {error, quasardb_column_value_count_mismatch}
            end;
        _ ->
            {error, quasardb_timestamp_column_must_be_first}
    end.

serialize_identifier({identifier, bare, Name}) ->
    Name;
serialize_identifier({identifier, special, Name}) ->
    Name;
serialize_identifier({identifier, double, Source}) ->
    ok = assert_no_placeholder(Source),
    Name = decode_escaped(strip_quotes(Source), []),
    Escaped = escape_identifier(Name),
    <<$", Escaped/binary, $">>.

identifier_name({identifier, bare, Name}) ->
    Name;
identifier_name({identifier, special, Name}) ->
    Name;
identifier_name({identifier, double, Source}) ->
    ok = assert_no_placeholder(Source),
    decode_escaped(strip_quotes(Source), []).

assert_no_placeholder(Source) ->
    case binary:match(Source, <<"${">>) of
        nomatch -> ok;
        _ -> error(dynamic_identifier_not_allowed)
    end.

escape_identifier(Text) ->
    escape_bytes(Text, $", []).

compile_row(Values) ->
    [#raw{sql = <<"(">>} | join_ops(<<", ">>, [compile_value(Value) || Value <- Values])] ++
        [#raw{sql = <<")">>}].

compile_value({var, Placeholder}) ->
    [#value{placeholder = Placeholder}];
compile_value({string, Source}) ->
    case parse_string(Source) of
        [#tpl_text{text = Text}] -> [#raw{sql = encode_string(Text)}];
        Parts -> [#string{parts = Parts}]
    end;
compile_value({number, Number}) ->
    [#raw{sql = Number}];
compile_value({timestamp, Timestamp}) ->
    [#raw{sql = Timestamp}];
compile_value({timestamp_name, Name, bare}) ->
    [#raw{sql = Name}];
compile_value({timestamp_name, Name, call}) ->
    [#raw{sql = <<Name/binary, "()">>}];
compile_value(null) ->
    [#raw{sql = <<"NULL">>}].

parse_string(Source) ->
    parse_string_body(strip_quotes(Source), [], []).

parse_string_body(<<>>, Text, Parts) ->
    finish_parts(Text, Parts);
parse_string_body(<<$\\, Escaped, Rest/binary>>, Text, Parts) ->
    parse_string_body(Rest, [Escaped | Text], Parts);
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

decode_escaped(<<$\\, Escaped, Rest/binary>>, Acc) ->
    decode_escaped(Rest, [Escaped | Acc]);
decode_escaped(<<Char, Rest/binary>>, Acc) ->
    decode_escaped(Rest, [Char | Acc]);
decode_escaped(<<>>, Acc) ->
    iolist_to_binary(lists:reverse(Acc)).

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
    encode_string(<<"undefined">>);
encode_value(undefined, _Opts) ->
    <<"NULL">>;
encode_value(null, _Opts) ->
    <<"NULL">>;
encode_value(Value, _Opts) when is_integer(Value) ->
    integer_to_binary(Value);
encode_value(Value, _Opts) when is_float(Value) ->
    float_to_binary(Value, [short]);
encode_value(Value, _Opts) ->
    Text = to_text(Value),
    case is_unquoted_timestamp(Text) of
        true -> Text;
        false -> encode_string(Text)
    end.

encode_string_parts([#part_value{value = undefined}], #{undefined_vars_as_null := false}) ->
    encode_string(<<"undefined">>);
encode_string_parts([#part_value{value = undefined}], _Opts) ->
    <<"NULL">>;
encode_string_parts([#part_value{value = null}], _Opts) ->
    <<"NULL">>;
encode_string_parts(Parts, Opts) ->
    encode_string(render_text_parts(Parts, Opts)).

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

to_text(Value) when is_binary(Value) ->
    Value;
to_text(Value) ->
    case unicode:characters_to_binary(emqx_template:to_string(Value)) of
        Text when is_binary(Text) -> Text;
        Error -> error({invalid_unicode, Error})
    end.

is_unquoted_timestamp(Text) ->
    MP = persistent_term:get(?UNQUOTED_TIMESTAMP_RE_PT_KEY),
    re:run(Text, MP, [{capture, none}]) =:= match.

%% QuasarDB 3.14.1 uses backslash escaping for strings. The public INSERT
%% documentation shows single-quoted BLOB values:
%% https://doc.quasar.ai/3.14.1/queries/insert.html#examples
encode_string(Text) ->
    ok = assert_no_nul(Text),
    Escaped = escape_bytes(Text, $', []),
    <<$', Escaped/binary, $'>>.

escape_bytes(<<$\\, Rest/binary>>, Quote, Acc) ->
    escape_bytes(Rest, Quote, [<<"\\\\">> | Acc]);
escape_bytes(<<Quote, Rest/binary>>, Quote, Acc) ->
    escape_bytes(Rest, Quote, [<<$\\, Quote>> | Acc]);
escape_bytes(<<Char, Rest/binary>>, Quote, Acc) ->
    escape_bytes(Rest, Quote, [Char | Acc]);
escape_bytes(<<>>, _Quote, Acc) ->
    iolist_to_binary(lists:reverse(Acc)).

%% qdb_query consumes a null-terminated UTF-8 query. NUL would truncate SQL:
%% https://doc.quasar.ai/3.14.1/cdoc/group__query.html#gabea1cc60780c4ff27ef8acef98dc3dbc
assert_no_nul(Text) ->
    case binary:match(Text, <<0>>) of
        nomatch -> ok;
        _ -> error(nul_character_not_allowed)
    end.
