%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Compile and render restricted TDengine multi-table INSERT templates.
-module(emqx_bridge_tdengine_sql).

-export([compile/1, render/3, render_batch/3, parse_placeholder/1, parse_identifier_parts/2]).
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
-record(identifier, {parts :: [template_part()]}).

-type render_op() :: #raw{} | #value{} | #string{} | #identifier{}.
-type render_plan() :: [render_op()].

-record(tdengine_plan, {plan :: render_plan()}).

-define(BATCH_SEPARATOR, <<" ">>).

-opaque plan() :: #tdengine_plan{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(SQL0) ->
    SQL = unicode:characters_to_binary(SQL0),
    try
        case emqx_bridge_tdengine_sql_lexer:string(binary_to_list(SQL)) of
            {ok, Tokens, _EndLine} ->
                case emqx_bridge_tdengine_sql_parser:parse(Tokens) of
                    {ok, AST} -> compile_ast(AST);
                    {error, Reason} -> {error, {invalid_tdengine_insert_template, Reason}}
                end;
            {error, Reason, _EndLine} ->
                {error, {invalid_tdengine_insert_template, Reason}}
        end
    catch
        Class:CatchReason -> {error, {invalid_tdengine_insert_template, {Class, CatchReason}}}
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#tdengine_plan{plan = Plan}, Data, Opts) ->
    case render_unit(Plan, Data, Opts) of
        {ok, Rendered} -> {ok, [<<"INSERT INTO ">>, Rendered]};
        {error, _} = Error -> Error
    end.

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(#tdengine_plan{plan = Plan}, DataList, Opts) ->
    case render_batch_units(DataList, Plan, Opts, 1, []) of
        {ok, Rendered} -> {ok, [<<"INSERT INTO ">>, Rendered]};
        {error, _} = Error -> Error
    end.

-spec parse_identifier_parts(binary(), bare | backtick) ->
    {ok, [template_part()]} | {error, term()}.
parse_identifier_parts(Source, Style) ->
    try
        {ok, parse_identifier_parts(Source, Style, [], [])}
    catch
        error:Reason -> {error, Reason}
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
%% Private funs
%%------------------------------------------------------------------------------

render_batch_units([Data | Rest], Template, Opts, Index, Acc) ->
    case render_unit(Template, Data, Opts) of
        {ok, Rendered} ->
            render_batch_units(Rest, Template, Opts, Index + 1, [Rendered | Acc]);
        {error, Reason} ->
            {error, {tdengine_template_render_failed, #{batch_index => Index, reason => Reason}}}
    end;
render_batch_units([], _Template, _Opts, _Index, Acc) ->
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
render_plan([#string{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(
        Parts,
        fun(Resolved) -> encode_string(render_text_parts(Resolved, Opts)) end,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([#identifier{parts = Parts} | Rest], Data, Opts, Cache0, Acc) ->
    render_template_op(
        Parts,
        fun encode_identifier/1,
        Rest,
        Data,
        Opts,
        Cache0,
        Acc
    );
render_plan([], _Data, _Opts, _Cache, Acc) ->
    {ok, lists:reverse(Acc)}.

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

compile_ast({insert, #{clauses := Clauses}}) ->
    case compile_clauses(Clauses, []) of
        {ok, ClauseOps} ->
            Unit = merge_render_ops(join_ops(<<" ">>, lists:reverse(ClauseOps))),
            {ok, #tdengine_plan{plan = Unit}};
        {error, _} = Error ->
            Error
    end.

compile_clauses([Clause | Rest], Acc) ->
    case compile_clause(Clause) of
        {ok, Ops} -> compile_clauses(Rest, [Ops | Acc]);
        {error, _} = Error -> Error
    end;
compile_clauses([], Acc) ->
    {ok, Acc}.

compile_clause({table_clause, Clause}) ->
    Before = maps:get(columns_before_using, Clause),
    Using = maps:get(using, Clause),
    After = maps:get(columns_after_using, Clause),
    case valid_column_positions(Before, Using, After) of
        true ->
            Ops =
                compile_target(maps:get(target, Clause)) ++
                    compile_columns(Before) ++
                    compile_using(Using) ++
                    compile_columns(After) ++
                    [#raw{sql = <<" VALUES ">>}] ++
                    join_ops(<<" ">>, [compile_row(Row) || Row <- maps:get(rows, Clause)]),
            {ok, Ops};
        false ->
            {error, invalid_tdengine_column_positions}
    end.

valid_column_positions(_Before, undefined, undefined) -> true;
valid_column_positions(_Before, undefined, _After) -> false;
valid_column_positions(undefined, _Using, _After) -> true;
valid_column_positions(_Before, _Using, undefined) -> true;
valid_column_positions(_Before, _Using, _After) -> false.

compile_target({target, Database, Component}) ->
    DatabaseOps =
        case Database of
            undefined -> [];
            _ -> [#raw{sql = <<(serialize_identifier(Database))/binary, ".">>}]
        end,
    DatabaseOps ++ compile_target_component(Component).

compile_target_component({identifier_parts, Parts} = Identifier) ->
    case lists:any(fun is_variable_part/1, Parts) of
        true -> [#identifier{parts = Parts}];
        false -> [#raw{sql = serialize_identifier(Identifier)}]
    end;
compile_target_component({identifier_placeholder, Placeholder}) ->
    [#identifier{parts = [#tpl_placeholder{placeholder = Placeholder}]}];
compile_target_component(Identifier) ->
    [#raw{sql = serialize_identifier(Identifier)}].

compile_columns(undefined) ->
    [];
compile_columns(Identifiers) ->
    SQL = lists:join(<<", ">>, [serialize_identifier(I) || I <- Identifiers]),
    [#raw{sql = iolist_to_binary([" (", SQL, ")"])}].

compile_using(undefined) ->
    [];
compile_using(#{stable := Stable, tag_columns := TagColumns, tags := Tags}) ->
    StableSQL = serialize_qualified_identifier(Stable),
    [#raw{sql = <<" USING ", StableSQL/binary>>}] ++
        compile_columns(TagColumns) ++
        [#raw{sql = <<" TAGS (">>}] ++
        join_ops(<<", ">>, [compile_value(Value) || Value <- Tags]) ++
        [#raw{sql = <<")">>}].

compile_row({row, Values}) ->
    [#raw{sql = <<"(">>} | join_ops(<<", ">>, [compile_value(Value) || Value <- Values])] ++
        [#raw{sql = <<")">>}].

compile_value({var, Placeholder}) ->
    [#value{placeholder = Placeholder}];
compile_value({string, Style, Source}) ->
    Parts = parse_string(Source, Style),
    case lists:any(fun is_variable_part/1, Parts) of
        true ->
            [#string{parts = Parts}];
        false ->
            [#tpl_text{text = Text}] = Parts,
            [#raw{sql = encode_string(Text)}]
    end;
compile_value({number, Number}) ->
    [#raw{sql = Number}];
compile_value(null) ->
    [#raw{sql = <<"NULL">>}];
compile_value(true) ->
    [#raw{sql = <<"true">>}];
compile_value(false) ->
    [#raw{sql = <<"false">>}];
compile_value(now) ->
    [#raw{sql = <<"NOW">>}];
compile_value(today) ->
    [#raw{sql = <<"TODAY">>}];
compile_value({time_arithmetic, Base, Ops}) ->
    compile_value(Base) ++
        [
            #raw{sql = <<(atom_to_binary(Operator))/binary, Duration/binary>>}
         || {Operator, Duration} <- Ops
        ].

serialize_identifier({identifier, bare, Name}) ->
    Name;
serialize_identifier({identifier_parts, Parts}) ->
    case Parts of
        [#tpl_text{text = Text}] -> quote_identifier(Text);
        _ -> error(dynamic_identifier_not_allowed)
    end.

serialize_qualified_identifier(Identifiers) ->
    iolist_to_binary(lists:join($., [serialize_identifier(I) || I <- Identifiers])).

quote_identifier(Name) ->
    Escaped = binary:replace(Name, <<"`">>, <<"``">>, [global]),
    <<"`", Escaped/binary, "`">>.

parse_string(Source, single) ->
    parse_string_body(strip_quotes(Source), $', [], []);
parse_string(Source, double) ->
    parse_string_body(strip_quotes(Source), $", [], []).

parse_string_body(<<>>, _Quote, Text, Parts) ->
    finish_parts(Text, Parts);
parse_string_body(<<$\\, Escaped, Rest/binary>>, Quote, Text, Parts) ->
    parse_string_body(Rest, Quote, [decode_escape(Escaped) | Text], Parts);
parse_string_body(<<Quote, Quote, Rest/binary>>, Quote, Text, Parts) ->
    parse_string_body(Rest, Quote, [Quote | Text], Parts);
parse_string_body(<<"${$}", Rest/binary>>, Quote, Text, Parts) ->
    parse_string_body(Rest, Quote, [$$ | Text], Parts);
parse_string_body(<<"${", _/binary>> = Bin, Quote, Text, Parts) ->
    {Placeholder, Rest} = take_placeholder(Bin),
    parse_string_body(
        Rest,
        Quote,
        [],
        [#tpl_placeholder{placeholder = Placeholder} | flush_text(Text, Parts)]
    );
parse_string_body(<<Char, Rest/binary>>, Quote, Text, Parts) ->
    parse_string_body(Rest, Quote, [Char | Text], Parts).

parse_identifier_parts(<<>>, _Style, Text, Parts) ->
    lists:reverse(flush_text(Text, Parts));
parse_identifier_parts(<<$`, $`, Rest/binary>>, backtick, Text, Parts) ->
    parse_identifier_parts(Rest, backtick, [$` | Text], Parts);
parse_identifier_parts(<<"${", _/binary>> = Bin, Style, Text, Parts) ->
    {Placeholder, Rest} = take_placeholder(Bin),
    parse_identifier_parts(
        Rest,
        Style,
        [],
        [#tpl_placeholder{placeholder = Placeholder} | flush_text(Text, Parts)]
    );
parse_identifier_parts(<<Char, Rest/binary>>, Style, Text, Parts) ->
    parse_identifier_parts(Rest, Style, [Char | Text], Parts).

decode_escape($n) -> $\n;
decode_escape($r) -> $\r;
decode_escape($t) -> $\t;
decode_escape($%) -> <<"\\%">>;
decode_escape($_) -> <<"\\_">>;
decode_escape(Char) -> Char.

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

encode_identifier(Parts) ->
    case render_identifier_parts(Parts) of
        {ok, Identifier} -> quote_identifier(Identifier);
        {error, _} = Error -> Error
    end.

encode_value(undefined, #{undefined_vars_as_null := false}) ->
    encode_string(<<"undefined">>);
encode_value(undefined, _Opts) ->
    <<"NULL">>;
encode_value(null, _Opts) ->
    <<"NULL">>;
encode_value(true, _Opts) ->
    <<"true">>;
encode_value(false, _Opts) ->
    <<"false">>;
encode_value(Value, _Opts) when is_integer(Value) -> integer_to_binary(Value);
encode_value(Value, _Opts) when is_float(Value) ->
    case Value =:= Value of
        true -> float_to_binary(Value, [compact]);
        false -> {error, non_finite_number}
    end;
encode_value(Value, _Opts) ->
    encode_string(to_text(Value)).

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

render_identifier_parts(Parts) ->
    try
        Identifier = iolist_to_binary([
            case Part of
                #part_text{text = Text} ->
                    Text;
                #part_value{value = undefined} ->
                    error(undefined_identifier);
                #part_value{value = null} ->
                    error(null_identifier);
                #part_value{value = Value} when
                    is_binary(Value); is_atom(Value); is_number(Value)
                ->
                    to_text(Value);
                #part_value{value = Value} when is_list(Value) ->
                    identifier_text(Value);
                #part_value{} ->
                    error(invalid_identifier_type)
            end
         || Part <- Parts
        ]),
        case validate_dynamic_identifier(Identifier) of
            ok -> {ok, Identifier};
            {error, ValidationReason} -> {error, {invalid_tdengine_identifier, ValidationReason}}
        end
    catch
        error:Reason -> {error, Reason}
    end.

validate_dynamic_identifier(<<>>) ->
    {error, empty};
validate_dynamic_identifier(Identifier) when byte_size(Identifier) > 192 ->
    {error, too_long};
validate_dynamic_identifier(Identifier) ->
    case binary:match(Identifier, <<".">>) of
        nomatch -> ok;
        _ -> {error, contains_dot}
    end.

to_text(Value) when is_binary(Value) -> Value;
to_text(Value) -> iolist_to_binary(emqx_template:to_string(Value)).

identifier_text(Value) ->
    case io_lib:printable_unicode_list(Value) of
        true -> unicode:characters_to_binary(Value);
        false -> error(invalid_identifier_type)
    end.

encode_string(Text) ->
    Escaped = binary:replace(Text, [<<"\\">>, <<"'">>], <<"\\">>, [global, {insert_replaced, 1}]),
    <<"'", Escaped/binary, "'">>.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

compile_merges_raw_segments_test() ->
    {ok, Placeholder} = parse_placeholder(<<"${value}">>),
    {ok, #tdengine_plan{plan = [#raw{sql = <<"t VALUES (1, 2)">>}]}} =
        compile(<<"INSERT INTO t VALUES (1, 2)">>),
    {ok, #tdengine_plan{plan = Plan}} =
        compile(<<"INSERT INTO t VALUES (1, ${value}, 2)">>),
    ?assertEqual(
        [
            #raw{sql = <<"t VALUES (1, ">>},
            #value{placeholder = Placeholder},
            #raw{sql = <<", 2)">>}
        ],
        Plan
    ).

multi_table_compile_and_render_test() ->
    SQL = <<
        "INSERT INTO ${clientid} USING s_tab TAGS ('${clientid}') "
        "VALUES (${timestamp}, '${payload}') "
        "test_${clientid} USING s_tab TAGS ('${clientid}') "
        "VALUES (${second_ts}, '${payload}')"
    >>,
    {ok, Plan} = compile(SQL),
    {ok, Rendered} = render(
        Plan,
        #{
            clientid => <<"client-1">>,
            timestamp => 1,
            second_ts => 2,
            payload => <<"hello">>
        },
        null_opts()
    ),
    ?assert(is_list(Rendered)),
    ?assertEqual(
        <<
            "INSERT INTO `client-1` USING s_tab TAGS ('client-1') VALUES (1, 'hello') "
            "`test_client-1` USING s_tab TAGS ('client-1') VALUES (2, 'hello')"
        >>,
        iolist_to_binary(Rendered)
    ).

leading_dot_placeholder_test() ->
    {ok, Plan} = compile(<<"INSERT INTO test_${.clientid} VALUES (${.payload})">>),
    ?assertEqual(
        {ok, <<"INSERT INTO `test_client-1` VALUES ('hello')">>},
        rendered_binary(
            render(Plan, #{clientid => <<"client-1">>, payload => <<"hello">>}, null_opts())
        )
    ).

identifier_boundary_test() ->
    {ok, Plan} = compile(<<"INSERT INTO test_${clientid} VALUES (${timestamp}, ${payload})">>),
    {ok, Rendered} = render(
        Plan,
        #{clientid => <<"t`; DROP TABLE t; --">>, timestamp => 1, payload => <<"text">>},
        null_opts()
    ),
    ?assertEqual(
        <<"INSERT INTO `test_t``; DROP TABLE t; --` VALUES (1, 'text')">>,
        iolist_to_binary(Rendered)
    ),
    ?assertMatch({ok, _}, compile(Rendered)).

identifier_template_test() ->
    {ok, BarePlan} = compile(
        <<"INSERT INTO pre_${tenant}_${clientid} VALUES (1)">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `pre_acme_client-1` VALUES (1)">>},
        rendered_binary(
            render(BarePlan, #{tenant => <<"acme">>, clientid => <<"client-1">>}, null_opts())
        )
    ),
    {ok, BacktickPlan} = compile(
        <<"INSERT INTO `test_``${clientid}` VALUES (1)">>
    ),
    ?assertEqual(
        {ok, <<"INSERT INTO `test_``client-1` VALUES (1)">>},
        rendered_binary(render(BacktickPlan, #{clientid => <<"client-1">>}, null_opts()))
    ).

static_escape_boundary_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t VALUES (1, '\\\\${payload}')">>),
    Attack = <<"') t VALUES (2, 'owned'); --">>,
    {ok, Rendered} = render(Plan, #{payload => Attack}, null_opts()),
    ?assertMatch({ok, _}, compile(Rendered)).

batch_shape_test() ->
    {ok, Plan} = compile(<<"INSERT INTO ${clientid} VALUES (${timestamp}, '${payload}')">>),
    {ok, Rendered} = render_batch(
        Plan,
        [
            #{clientid => <<"a">>, timestamp => 1, payload => <<"x');--">>},
            #{clientid => <<"b">>, timestamp => 2, payload => <<"y">>}
        ],
        null_opts()
    ),
    ?assert(is_list(Rendered)),
    ?assertMatch({ok, _}, compile(Rendered)).

reject_unsupported_syntax_test() ->
    ?assertMatch({error, _}, compile(<<"INSERT INTO t VALUES (1) -- comment">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t FILE 'rows.csv'">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO t VALUES (${value}0)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO test_ ${clientid} VALUES (1)">>)),
    ?assertMatch({error, _}, compile(<<"INSERT INTO ${clientid} _suffix VALUES (1)">>)),
    ?assertMatch(
        {error, _},
        compile(<<"INSERT INTO t USING stable_${tenant} TAGS (1) VALUES (1)">>)
    ).

comma_separated_table_clauses_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t VALUES (1), u VALUES (2)">>),
    ?assertEqual(
        {ok, <<"INSERT INTO t VALUES (1) u VALUES (2)">>},
        rendered_binary(render(Plan, #{}, null_opts()))
    ).

row_and_table_clause_commas_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t VALUES (1), (2), u VALUES (3), (4)">>),
    ?assertEqual(
        {ok, <<"INSERT INTO t VALUES (1) (2) u VALUES (3) (4)">>},
        rendered_binary(render(Plan, #{}, null_opts()))
    ),
    {ok, MixedPlan} = compile(<<"INSERT INTO t VALUES (1) (2), u VALUES (3), (4)">>),
    ?assertEqual(
        {ok, <<"INSERT INTO t VALUES (1) (2) u VALUES (3) (4)">>},
        rendered_binary(render(MixedPlan, #{}, null_opts()))
    ).

decimal_forms_test() ->
    {ok, Plan} = compile(<<"INSERT INTO t VALUES (.5, 1., -.5, -1.)">>),
    ?assertEqual(
        {ok, <<"INSERT INTO t VALUES (.5, 1., -.5, -1.)">>},
        rendered_binary(render(Plan, #{}, null_opts()))
    ).

identifier_error_placeholder_test() ->
    {ok, Plan} = compile(<<"INSERT INTO ${clientid} VALUES (1)">>),
    ?assertEqual(
        {error,
            {invalid_sql_template_value, #{
                placeholder => "clientid", reason => invalid_identifier_type
            }}},
        render(Plan, #{clientid => #{}}, null_opts())
    ).

invalid_dynamic_identifier_test() ->
    {ok, Plan} = compile(<<"INSERT INTO ${clientid} VALUES (1)">>),
    ?assertMatch(
        {error,
            {invalid_sql_template_value, #{
                placeholder := "clientid", reason := {invalid_tdengine_identifier, empty}
            }}},
        render(Plan, #{clientid => <<>>}, null_opts())
    ),
    ?assertMatch(
        {error,
            {invalid_sql_template_value, #{
                placeholder := "clientid", reason := {invalid_tdengine_identifier, contains_dot}
            }}},
        render(Plan, #{clientid => <<"a.b">>}, null_opts())
    ),
    ?assertMatch(
        {error,
            {invalid_sql_template_value, #{
                placeholder := "clientid", reason := {invalid_tdengine_identifier, too_long}
            }}},
        render(Plan, #{clientid => binary:copy(<<"a">>, 193)}, null_opts())
    ),
    {ok, PrefixedPlan} = compile(<<"INSERT INTO test_${clientid} VALUES (1)">>),
    ?assertMatch(
        {error,
            {invalid_sql_template_value, #{
                placeholder := "clientid", reason := {invalid_tdengine_identifier, contains_dot}
            }}},
        render(PrefixedPlan, #{clientid => <<"a.b">>}, null_opts())
    ),
    ?assertMatch(
        {error,
            {invalid_sql_template_value, #{
                placeholder := "clientid", reason := {invalid_tdengine_identifier, too_long}
            }}},
        render(PrefixedPlan, #{clientid => binary:copy(<<"a">>, 188)}, null_opts())
    ).

rendered_binary({ok, SQL}) ->
    ?assert(is_list(SQL)),
    {ok, iolist_to_binary(SQL)}.

null_opts() ->
    #{undefined_vars_as_null => true}.

-endif.
