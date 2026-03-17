%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_subscription_filter).

-moduledoc """
Utilities for parsing and evaluating subscription message filters appended to
topic filters with `?`.

For the accepted filter-expression syntax, see
`apps/emqx/doc/subscription-message-filter-bnf.md`.

This module is used on the durable-session replay path. The AST shape produced
by this module and the semantics of `match_message/2` therefore must remain
deterministic and stable across releases for stored subscription filters.
""".

-include("emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-export([
    split_topic_filter/1,
    validate_subscription/1,
    normalize_topic_filter/1,
    match_message/2
]).

-type op() :: eq | gt | lt | gte | lte.
-type clause() :: {op(), binary(), binary() | number()}.
-type ast() :: [clause()].

-export_type([ast/0]).

-doc """
Split a topic filter at the first `?`.

Returns the base topic filter together with either `no_filter` or the raw filter
expression suffix.
""".
-spec split_topic_filter(
    emqx_types:topic() | emqx_types:share()
) -> {emqx_types:topic() | emqx_types:share(), no_filter | {filter, binary()}}.
split_topic_filter(#share{topic = TopicFilter} = S) ->
    {BaseTopic, Filter} = split_topic_filter(TopicFilter),
    {S#share{topic = BaseTopic}, Filter};
split_topic_filter(TopicFilter) when is_binary(TopicFilter) ->
    case binary:match(TopicFilter, <<"?">>) of
        nomatch ->
            {TopicFilter, no_filter};
        {Pos, 1} ->
            {
                binary:part(TopicFilter, 0, Pos),
                {filter, binary:part(TopicFilter, Pos + 1, byte_size(TopicFilter) - Pos - 1)}
            }
    end.

-doc """
Validate and parse a subscription filter.

Returns `{ok, no_filter}` when no filter suffix is present. Returns the parsed
base topic and filter AST when a filter suffix is present and valid.
""".
-spec validate_subscription(emqx_types:topic() | emqx_types:share()) ->
    {ok, no_filter}
    | {ok, #{
        base_topic := emqx_types:topic() | emqx_types:share(),
        raw_expr := binary(),
        ast := ast()
    }}
    | {error, malformed_filter}.
validate_subscription(TopicFilter) ->
    case split_topic_filter(TopicFilter) of
        {_BaseTopic, no_filter} ->
            {ok, no_filter};
        {BaseTopic, {filter, RawExpr}} ->
            case parse(RawExpr) of
                {ok, AST} ->
                    {ok, #{base_topic => BaseTopic, raw_expr => RawExpr, ast => AST}};
                {error, malformed_filter} ->
                    {error, malformed_filter}
            end
    end.

-doc """
Return the topic filter with any `?filter-expression` suffix removed.
""".
-spec normalize_topic_filter(
    emqx_types:topic() | emqx_types:share()
) -> emqx_types:topic() | emqx_types:share().
normalize_topic_filter(TopicFilter) ->
    {Base, _Filter} = split_topic_filter(TopicFilter),
    Base.

-doc """
Evaluate a parsed filter AST against MQTT 5 `User-Property` values on a message.

This function is used during durable-session replay, so its behavior must stay
deterministic and compatible with the persisted filter AST representation.
""".
-spec match_message(ast(), emqx_types:message()) -> boolean().
match_message(AST, Msg) ->
    Properties = emqx_message:get_header(properties, Msg, #{}),
    UserProperties = normalize_user_properties(maps:get('User-Property', Properties, [])),
    lists:all(fun(Clause) -> match_clause(Clause, UserProperties) end, AST).

-spec parse(binary()) -> {ok, ast()} | {error, malformed_filter}.
parse(Expr0) when is_binary(Expr0) ->
    Expr = trim(Expr0),
    case Expr of
        <<>> ->
            {error, malformed_filter};
        _ ->
            parse_clauses(binary:split(Expr, <<"&">>, [global]), [])
    end.

parse_clauses([], Acc) ->
    {ok, lists:reverse(Acc)};
parse_clauses([Clause | More], Acc) ->
    case parse_clause(trim(Clause)) of
        {ok, Parsed} ->
            parse_clauses(More, [Parsed | Acc]);
        {error, malformed_filter} ->
            {error, malformed_filter}
    end.

parse_clause(<<>>) ->
    {error, malformed_filter};
parse_clause(Clause) ->
    parse_clause(Clause, [<<">=">>, <<"<=">>, <<">">>, <<"<">>, <<"=">>]).

parse_clause(_Clause, []) ->
    {error, malformed_filter};
parse_clause(Clause, [Separator | More]) ->
    case binary:split(Clause, Separator, [global]) of
        [Key, Value] ->
            mk_clause(Separator, trim(Key), trim(Value));
        _ ->
            parse_clause(Clause, More)
    end.

mk_clause(_Separator, <<>>, _Value) ->
    {error, malformed_filter};
mk_clause(_Separator, _Key, <<>>) ->
    {error, malformed_filter};
mk_clause(<<"=">>, Key, Value) ->
    {ok, {eq, Key, Value}};
mk_clause(<<">">>, Key, Value) ->
    mk_numeric_clause(gt, Key, Value);
mk_clause(<<"<">>, Key, Value) ->
    mk_numeric_clause(lt, Key, Value);
mk_clause(<<">=">>, Key, Value) ->
    mk_numeric_clause(gte, Key, Value);
mk_clause(<<"<=">>, Key, Value) ->
    mk_numeric_clause(lte, Key, Value).

mk_numeric_clause(Op, Key, Value) ->
    case maybe_number(Value) of
        {ok, Number} ->
            {ok, {Op, Key, Number}};
        {error, bad_number} ->
            {error, malformed_filter}
    end.

normalize_user_properties(UserProperties) when is_list(UserProperties) ->
    UserProperties;
normalize_user_properties({Key, Value}) ->
    [{Key, Value}];
normalize_user_properties(_) ->
    [].

match_clause({eq, Key, Expected}, UserProperties) ->
    lists:any(
        fun
            ({UserKey, Value}) when UserKey =:= Key, is_binary(Value) ->
                Value =:= Expected;
            ({UserKey, Value}) when UserKey =:= Key ->
                emqx_utils_conv:bin(Value) =:= Expected;
            (_) ->
                false
        end,
        UserProperties
    );
match_clause({Op, Key, Expected}, UserProperties) ->
    lists:any(
        fun
            ({UserKey, Value}) when UserKey =:= Key ->
                case maybe_number(Value) of
                    {ok, Actual} -> compare(Op, Actual, Expected);
                    {error, bad_number} -> false
                end;
            (_) ->
                false
        end,
        UserProperties
    ).

compare(gt, Actual, Expected) -> Actual > Expected;
compare(lt, Actual, Expected) -> Actual < Expected;
compare(gte, Actual, Expected) -> Actual >= Expected;
compare(lte, Actual, Expected) -> Actual =< Expected.

maybe_number(Value) when is_integer(Value); is_float(Value) ->
    {ok, Value};
maybe_number(Value) when is_binary(Value) ->
    try emqx_utils_conv:float(trim(Value)) of
        Number ->
            {ok, Number}
    catch
        error:badarg ->
            {error, bad_number}
    end;
maybe_number(Value) ->
    maybe_number(emqx_utils_conv:bin(Value)).

trim(Bin) when is_binary(Bin) ->
    string:trim(Bin).
