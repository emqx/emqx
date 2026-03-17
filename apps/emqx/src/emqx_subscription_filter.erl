%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_subscription_filter).

-include("emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-export([
    split_topic_filter/2,
    validate_subscription/2,
    normalize_topic_filter/2,
    match_message/2
]).

-type mode() :: enable | disable.
-type op() :: eq | gt | lt | gte | lte.
-type clause() :: {op(), binary(), binary() | number()}.
-type ast() :: [clause()].

-export_type([mode/0, ast/0]).

-spec split_topic_filter(
    emqx_types:topic() | emqx_types:share(), mode()
) -> {emqx_types:topic() | emqx_types:share(), no_filter | {filter, binary()}}.
split_topic_filter(#share{topic = TopicFilter} = S, Mode) ->
    {BaseTopic, Filter} = split_topic_filter(TopicFilter, Mode),
    {S#share{topic = BaseTopic}, Filter};
split_topic_filter(TopicFilter, disable) when is_binary(TopicFilter) ->
    {TopicFilter, no_filter};
split_topic_filter(TopicFilter, enable) when is_binary(TopicFilter) ->
    case binary:match(TopicFilter, <<"?">>) of
        nomatch ->
            {TopicFilter, no_filter};
        {Pos, 1} ->
            {
                binary:part(TopicFilter, 0, Pos),
                {filter, binary:part(TopicFilter, Pos + 1, byte_size(TopicFilter) - Pos - 1)}
            }
    end.

-spec validate_subscription(emqx_types:topic() | emqx_types:share(), mode()) ->
    {ok, #{
        mode := plain | filtered,
        base_topic := emqx_types:topic() | emqx_types:share(),
        raw_expr := binary() | undefined,
        ast := ast() | undefined
    }}
    | {error, malformed_filter}.
validate_subscription(TopicFilter, Mode) ->
    case split_topic_filter(TopicFilter, Mode) of
        {BaseTopic, no_filter} ->
            {ok, #{mode => plain, base_topic => BaseTopic, raw_expr => undefined, ast => undefined}};
        {BaseTopic, {filter, RawExpr}} ->
            case parse(RawExpr) of
                {ok, AST} ->
                    {ok, #{
                        mode => filtered, base_topic => BaseTopic, raw_expr => RawExpr, ast => AST
                    }};
                {error, malformed_filter} ->
                    {error, malformed_filter}
            end
    end.

-spec normalize_topic_filter(
    emqx_types:topic() | emqx_types:share(), mode()
) -> emqx_types:topic() | emqx_types:share().
normalize_topic_filter(TopicFilter, Mode) ->
    element(1, split_topic_filter(TopicFilter, Mode)).

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
