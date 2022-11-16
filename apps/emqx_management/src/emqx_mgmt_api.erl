%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api).

-include_lib("stdlib/include/qlc.hrl").

-elvis([{elvis_style, dont_repeat_yourself, #{min_complexity => 100}}]).

-define(FRESH_SELECT, fresh_select).

-export([
    paginate/3,
    paginate/4
]).

%% first_next query APIs
-export([
    node_query/6,
    cluster_query/5,
    b2i/1
]).

-export([do_query/5]).

paginate(Tables, Params, {Module, FormatFun}) ->
    Qh = query_handle(Tables),
    Count = count(Tables),
    do_paginate(Qh, Count, Params, {Module, FormatFun}).

paginate(Tables, MatchSpec, Params, {Module, FormatFun}) ->
    Qh = query_handle(Tables, MatchSpec),
    Count = count(Tables, MatchSpec),
    do_paginate(Qh, Count, Params, {Module, FormatFun}).

do_paginate(Qh, Count, Params, {Module, FormatFun}) ->
    Page = b2i(page(Params)),
    Limit = b2i(limit(Params)),
    Cursor = qlc:cursor(Qh),
    case Page > 1 of
        true ->
            _ = qlc:next_answers(Cursor, (Page - 1) * Limit),
            ok;
        false ->
            ok
    end,
    Rows = qlc:next_answers(Cursor, Limit),
    qlc:delete_cursor(Cursor),
    #{
        meta => #{page => Page, limit => Limit, count => Count},
        data => [erlang:apply(Module, FormatFun, [Row]) || Row <- Rows]
    }.

query_handle(Table) when is_atom(Table) ->
    qlc:q([R || R <- ets:table(Table)]);
query_handle({Table, Opts}) when is_atom(Table) ->
    qlc:q([R || R <- ets:table(Table, Opts)]);
query_handle([Table]) when is_atom(Table) ->
    qlc:q([R || R <- ets:table(Table)]);
query_handle([{Table, Opts}]) when is_atom(Table) ->
    qlc:q([R || R <- ets:table(Table, Opts)]);
query_handle(Tables) ->
    %
    qlc:append([query_handle(T) || T <- Tables]).

query_handle(Table, MatchSpec) when is_atom(Table) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:q([R || R <- ets:table(Table, Options)]);
query_handle([Table], MatchSpec) when is_atom(Table) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:q([R || R <- ets:table(Table, Options)]);
query_handle(Tables, MatchSpec) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:append([qlc:q([E || E <- ets:table(T, Options)]) || T <- Tables]).

count(Table) when is_atom(Table) ->
    ets:info(Table, size);
count({Table, _}) when is_atom(Table) ->
    ets:info(Table, size);
count([Table]) when is_atom(Table) ->
    ets:info(Table, size);
count([{Table, _}]) when is_atom(Table) ->
    ets:info(Table, size);
count(Tables) ->
    lists:sum([count(T) || T <- Tables]).

count(Table, MatchSpec) when is_atom(Table) ->
    [{MatchPattern, Where, _Re}] = MatchSpec,
    NMatchSpec = [{MatchPattern, Where, [true]}],
    ets:select_count(Table, NMatchSpec);
count([Table], MatchSpec) when is_atom(Table) ->
    count(Table, MatchSpec);
count(Tables, MatchSpec) ->
    lists:sum([count(T, MatchSpec) || T <- Tables]).

page(Params) when is_map(Params) ->
    maps:get(<<"page">>, Params, 1);
page(Params) ->
    proplists:get_value(<<"page">>, Params, <<"1">>).

limit(Params) when is_map(Params) ->
    maps:get(<<"limit">>, Params, emqx_mgmt:max_row_limit());
limit(Params) ->
    proplists:get_value(<<"limit">>, Params, emqx_mgmt:max_row_limit()).

%%--------------------------------------------------------------------
%% Node Query
%%--------------------------------------------------------------------

-type query_params() :: list() | map().

-type query_schema() :: [{Key :: binary(), Type :: atom | integer | timestamp | ip | ip_port}].

-type query_to_match_spec_fun() ::
    fun((list(), list()) -> {ets:match_spec(), fun()}).

-type format_result_fun() ::
    fun((node(), term()) -> term())
    | fun((term()) -> term()).

-type query_return() :: #{meta := map(), data := [term()]}.

-spec node_query(
    node(),
    atom(),
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
node_query(Node, Tab, QString, QSchema, MsFun, FmtFun) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            ResultAcc = init_query_result(),
            QueryState = init_query_state(Meta),
            NResultAcc = do_node_query(
                Node, Tab, NQString, MsFun, QueryState, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_node_query(
    Node,
    Tab,
    QString,
    MsFun,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, Tab, QString, MsFun, QueryState) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Rows, NQueryState = #{continuation := ?FRESH_SELECT}} ->
            {_, NResultAcc} = accumulate_query_rows(Node, Rows, NQueryState, ResultAcc),
            NResultAcc;
        {Rows, NQueryState} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    NResultAcc;
                {more, NResultAcc} ->
                    do_node_query(Node, Tab, QString, MsFun, NQueryState, NResultAcc)
            end
    end.

%%--------------------------------------------------------------------
%% Cluster Query
%%--------------------------------------------------------------------
-spec cluster_query(
    atom(),
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
cluster_query(Tab, QString, QSchema, MsFun, FmtFun) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            Nodes = mria_mnesia:running_nodes(),
            ResultAcc = init_query_result(),
            QueryState = init_query_state(Meta),
            NResultAcc = do_cluster_query(
                Nodes, Tab, NQString, MsFun, QueryState, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_cluster_query([], _Tab, _QString, _MsFun, _QueryState, ResultAcc) ->
    ResultAcc;
do_cluster_query(
    [Node | Tail] = Nodes,
    Tab,
    QString,
    MsFun,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, Tab, QString, MsFun, QueryState) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Rows, NQueryState} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    maybe_collect_total_from_tail_nodes(Tail, Tab, QString, MsFun, NResultAcc);
                {more, NResultAcc} ->
                    NextNodes =
                        case NQueryState of
                            #{continuation := ?FRESH_SELECT} -> Tail;
                            _ -> Nodes
                        end,
                    do_cluster_query(NextNodes, Tab, QString, MsFun, NQueryState, NResultAcc)
            end
    end.

maybe_collect_total_from_tail_nodes([], _Tab, _QString, _MsFun, ResultAcc) ->
    ResultAcc;
maybe_collect_total_from_tail_nodes(Nodes, Tab, QString, MsFun, ResultAcc = #{total := TotalAcc}) ->
    {Ms, FuzzyFun} = erlang:apply(MsFun, [Tab, QString]),
    case is_countable_total(Ms, FuzzyFun) of
        true ->
            %% XXX: badfun risk? if the FuzzyFun is an anonumous func in local node
            case rpc:multicall(Nodes, ?MODULE, apply_total_query, [Tab, Ms, FuzzyFun]) of
                {_, [Node | _]} ->
                    {error, Node, {badrpc, badnode}};
                {ResL0, []} ->
                    ResL = lists:zip(Nodes, ResL0),
                    case lists:filter(fun({_, I}) -> not is_integer(I) end, ResL) of
                        [{Node, {badrpc, Reason}} | _] ->
                            {error, Node, {badrpc, Reason}};
                        [] ->
                            ResultAcc#{total => ResL ++ TotalAcc}
                    end
            end;
        false ->
            ResultAcc
    end.

%%--------------------------------------------------------------------
%% Do Query (or rpc query)
%%--------------------------------------------------------------------

%% QueryState ::
%%  #{continuation := ets:continuation(),
%%    page := pos_integer(),
%%    limit := pos_integer(),
%%    total := [{node(), non_neg_integer()}]
%%    }
init_query_state(_Meta = #{page := Page, limit := Limit}) ->
    #{
        continuation => ?FRESH_SELECT,
        page => Page,
        limit => Limit,
        total => []
    }.

%% @private This function is exempt from BPAPI
do_query(Node, Tab, QString, MsFun, QueryState) when Node =:= node(), is_function(MsFun) ->
    {Ms, FuzzyFun} = erlang:apply(MsFun, [Tab, QString]),
    do_select(Node, Tab, Ms, FuzzyFun, QueryState);
do_query(Node, Tab, QString, MsFun, QueryState) when is_function(MsFun) ->
    case
        rpc:call(
            Node,
            ?MODULE,
            do_query,
            [Node, Tab, QString, MsFun, QueryState],
            50000
        )
    of
        {badrpc, _} = R -> {error, R};
        Ret -> Ret
    end.

do_select(
    Node,
    Tab,
    Ms,
    FuzzyFun,
    QueryState0 = #{continuation := Continuation, limit := Limit}
) ->
    QueryState = maybe_apply_total_query(Node, Tab, Ms, FuzzyFun, QueryState0),
    Result =
        case Continuation of
            ?FRESH_SELECT ->
                ets:select(Tab, Ms, Limit);
            _ ->
                ets:select(ets:repair_continuation(Continuation, Ms))
        end,
    case Result of
        '$end_of_table' ->
            {[], QueryState#{continuation => ?FRESH_SELECT}};
        {Rows, NContinuation} ->
            NRows =
                case is_function(FuzzyFun) of
                    true -> FuzzyFun(Rows);
                    false -> Rows
                end,
            {NRows, QueryState#{continuation => NContinuation}}
    end.

maybe_apply_total_query(Node, Tab, Ms, FuzzyFun, QueryState = #{total := TotalAcc}) ->
    case proplists:get_value(Node, TotalAcc, undefined) of
        undefined ->
            Total = apply_total_query(Tab, Ms, FuzzyFun),
            QueryState#{total := [{Node, Total} | TotalAcc]};
        _ ->
            QueryState
    end.

%% XXX: Calculating the total number of data that match a certain condition under a large table
%% is very expensive because the entire ETS table needs to be scanned.
apply_total_query(Tab, Ms, FuzzyFun) ->
    case is_countable_total(Ms, FuzzyFun) of
        true ->
            ets:info(Tab, size);
        false ->
            %% return a fake total number if the query have any conditions
            0
    end.

is_countable_total(Ms, FuzzyFun) ->
    FuzzyFun =:= undefined andalso is_non_conditions_match_spec(Ms).

is_non_conditions_match_spec([{_MatchHead, _Conds = [], _Return} | More]) ->
    is_non_conditions_match_spec(More);
is_non_conditions_match_spec([{_MatchHead, Conds, _Return} | _More]) when length(Conds) =/= 0 ->
    false;
is_non_conditions_match_spec([]) ->
    true.

%% ResultAcc :: #{count := integer(),
%%                cursor := integer(),
%%                rows  := [{node(), Rows :: list()}],
%%                total := [{node() => integer()}]
%%               }
init_query_result() ->
    #{cursor => 0, count => 0, rows => [], total => []}.

accumulate_query_rows(
    Node,
    Rows,
    _QueryState = #{page := Page, limit := Limit, total := TotalAcc},
    ResultAcc = #{cursor := Cursor, count := Count, rows := RowsAcc}
) ->
    PageStart = (Page - 1) * Limit + 1,
    PageEnd = Page * Limit,
    Len = length(Rows),
    case Cursor + Len of
        NCursor when NCursor < PageStart ->
            {more, ResultAcc#{cursor => NCursor, total => TotalAcc}};
        NCursor when NCursor < PageEnd ->
            {more, ResultAcc#{
                cursor => NCursor,
                count => Count + length(Rows),
                total => TotalAcc,
                rows => [{Node, Rows} | RowsAcc]
            }};
        NCursor when NCursor >= PageEnd ->
            SubRows = lists:sublist(Rows, Limit - Count),
            {enough, ResultAcc#{
                cursor => NCursor,
                count => Count + length(SubRows),
                total => TotalAcc,
                rows => [{Node, SubRows} | RowsAcc]
            }}
    end.

%%--------------------------------------------------------------------
%% Table Select
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

parse_qstring(QString, QSchema) when is_map(QString) ->
    parse_qstring(maps:to_list(QString), QSchema);
parse_qstring(QString, QSchema) ->
    {NQString, FuzzyQString} = do_parse_qstring(QString, QSchema, [], []),
    {length(NQString) + length(FuzzyQString), {NQString, FuzzyQString}}.

do_parse_qstring([], _, Acc1, Acc2) ->
    NAcc2 = [E || E <- Acc2, not lists:keymember(element(1, E), 1, Acc1)],
    {lists:reverse(Acc1), lists:reverse(NAcc2)};
do_parse_qstring([{Key, Value} | RestQString], QSchema, Acc1, Acc2) ->
    case proplists:get_value(Key, QSchema) of
        undefined ->
            do_parse_qstring(RestQString, QSchema, Acc1, Acc2);
        Type ->
            case Key of
                <<Prefix:4/binary, NKey/binary>> when
                    Prefix =:= <<"gte_">>;
                    Prefix =:= <<"lte_">>
                ->
                    OpposeKey =
                        case Prefix of
                            <<"gte_">> -> <<"lte_", NKey/binary>>;
                            <<"lte_">> -> <<"gte_", NKey/binary>>
                        end,
                    case lists:keytake(OpposeKey, 1, RestQString) of
                        false ->
                            do_parse_qstring(
                                RestQString,
                                QSchema,
                                [qs(Key, Value, Type) | Acc1],
                                Acc2
                            );
                        {value, {K2, V2}, NParams} ->
                            do_parse_qstring(
                                NParams,
                                QSchema,
                                [qs(Key, Value, K2, V2, Type) | Acc1],
                                Acc2
                            )
                    end;
                _ ->
                    case is_fuzzy_key(Key) of
                        true ->
                            do_parse_qstring(
                                RestQString,
                                QSchema,
                                Acc1,
                                [qs(Key, Value, Type) | Acc2]
                            );
                        _ ->
                            do_parse_qstring(
                                RestQString,
                                QSchema,
                                [qs(Key, Value, Type) | Acc1],
                                Acc2
                            )
                    end
            end
    end.

qs(K1, V1, K2, V2, Type) ->
    {Key, Op1, NV1} = qs(K1, V1, Type),
    {Key, Op2, NV2} = qs(K2, V2, Type),
    {Key, Op1, NV1, Op2, NV2}.

qs(K, Value0, Type) ->
    try
        qs(K, to_type(Value0, Type))
    catch
        throw:bad_value_type ->
            throw({bad_value_type, {K, Type, Value0}})
    end.

qs(<<"gte_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), '>=', Value};
qs(<<"lte_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), '=<', Value};
qs(<<"like_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), like, Value};
qs(<<"match_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), match, Value};
qs(Key, Value) ->
    {binary_to_existing_atom(Key, utf8), '=:=', Value}.

is_fuzzy_key(<<"like_", _/binary>>) ->
    true;
is_fuzzy_key(<<"match_", _/binary>>) ->
    true;
is_fuzzy_key(_) ->
    false.

format_query_result(_FmtFun, _Meta, Error = {error, _Node, _Reason}) ->
    Error;
format_query_result(
    FmtFun, Meta, _ResultAcc = #{total := TotalAcc, rows := RowsAcc}
) ->
    Total = lists:foldr(fun({_Node, T}, N) -> N + T end, 0, TotalAcc),
    #{
        %% The `count` is used in HTTP API to indicate the total number of
        %% queries that can be read
        meta => Meta#{count => Total},
        data => lists:flatten(
            lists:foldr(
                fun({Node, Rows}, Acc) ->
                    [lists:map(fun(Row) -> exec_format_fun(FmtFun, Node, Row) end, Rows) | Acc]
                end,
                [],
                RowsAcc
            )
        )
    }.

exec_format_fun(FmtFun, Node, Row) ->
    case erlang:fun_info(FmtFun, arity) of
        {arity, 1} -> FmtFun(Row);
        {arity, 2} -> FmtFun(Node, Row)
    end.

parse_pager_params(Params) ->
    Page = b2i(page(Params)),
    Limit = b2i(limit(Params)),
    case Page > 0 andalso Limit > 0 of
        true ->
            #{page => Page, limit => Limit, count => 0};
        false ->
            false
    end.

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

to_type(V, TargetType) ->
    try
        to_type_(V, TargetType)
    catch
        _:_ ->
            throw(bad_value_type)
    end.

to_type_(V, atom) -> to_atom(V);
to_type_(V, integer) -> to_integer(V);
to_type_(V, timestamp) -> to_timestamp(V);
to_type_(V, ip) -> aton(V);
to_type_(V, ip_port) -> to_ip_port(V);
to_type_(V, _) -> V.

to_atom(A) when is_atom(A) ->
    A;
to_atom(B) when is_binary(B) ->
    binary_to_atom(B, utf8).

to_integer(I) when is_integer(I) ->
    I;
to_integer(B) when is_binary(B) ->
    binary_to_integer(B).

to_timestamp(I) when is_integer(I) ->
    I;
to_timestamp(B) when is_binary(B) ->
    binary_to_integer(B).

aton(B) when is_binary(B) ->
    list_to_tuple([binary_to_integer(T) || T <- re:split(B, "[.]")]).

to_ip_port(IPAddress) ->
    [IP0, Port0] = string:tokens(binary_to_list(IPAddress), ":"),
    {ok, IP} = inet:parse_address(IP0),
    Port = list_to_integer(Port0),
    {IP, Port}.

b2i(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
b2i(Any) ->
    Any.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

params2qs_test() ->
    QSchema = [
        {<<"str">>, binary},
        {<<"int">>, integer},
        {<<"atom">>, atom},
        {<<"ts">>, timestamp},
        {<<"gte_range">>, integer},
        {<<"lte_range">>, integer},
        {<<"like_fuzzy">>, binary},
        {<<"match_topic">>, binary}
    ],
    QString = [
        {<<"str">>, <<"abc">>},
        {<<"int">>, <<"123">>},
        {<<"atom">>, <<"connected">>},
        {<<"ts">>, <<"156000">>},
        {<<"gte_range">>, <<"1">>},
        {<<"lte_range">>, <<"5">>},
        {<<"like_fuzzy">>, <<"user">>},
        {<<"match_topic">>, <<"t/#">>}
    ],
    ExpectedQs = [
        {str, '=:=', <<"abc">>},
        {int, '=:=', 123},
        {atom, '=:=', connected},
        {ts, '=:=', 156000},
        {range, '>=', 1, '=<', 5}
    ],
    FuzzyNQString = [
        {fuzzy, like, <<"user">>},
        {topic, match, <<"t/#">>}
    ],
    ?assertEqual({7, {ExpectedQs, FuzzyNQString}}, parse_qstring(QString, QSchema)),

    {0, {[], []}} = parse_qstring([{not_a_predefined_params, val}], QSchema).

-endif.
