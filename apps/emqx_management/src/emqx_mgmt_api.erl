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
-define(LONG_QUERY_TIMEOUT, 50000).

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

-export_type([
    match_spec_and_filter/0
]).

-type query_params() :: list() | map().

-type query_schema() :: [
    {Key :: binary(), Type :: atom | binary | integer | timestamp | ip | ip_port}
].

-type query_to_match_spec_fun() :: fun((list(), list()) -> match_spec_and_filter()).

-type match_spec_and_filter() :: #{match_spec := ets:match_spec(), fuzzy_fun := fuzzy_filter_fun()}.

-type fuzzy_filter_fun() :: undefined | {fun(), list()}.

-type format_result_fun() ::
    fun((node(), term()) -> term())
    | fun((term()) -> term()).

-type query_return() :: #{meta := map(), data := [term()]}.

-export([do_query/2, apply_total_query/1]).

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
            QueryState = init_query_state(Tab, NQString, MsFun, Meta),
            NResultAcc = do_node_query(
                Node, QueryState, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_node_query(
    Node,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, QueryState) of
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
                    do_node_query(Node, NQueryState, NResultAcc)
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
            QueryState = init_query_state(Tab, NQString, MsFun, Meta),
            NResultAcc = do_cluster_query(
                Nodes, QueryState, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_cluster_query([], _QueryState, ResultAcc) ->
    ResultAcc;
do_cluster_query(
    [Node | Tail] = Nodes,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, QueryState) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Rows, NQueryState} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    maybe_collect_total_from_tail_nodes(Tail, NQueryState, NResultAcc);
                {more, NResultAcc} ->
                    NextNodes =
                        case NQueryState of
                            #{continuation := ?FRESH_SELECT} -> Tail;
                            _ -> Nodes
                        end,
                    do_cluster_query(NextNodes, NQueryState, NResultAcc)
            end
    end.

maybe_collect_total_from_tail_nodes([], _QueryState, ResultAcc) ->
    ResultAcc;
maybe_collect_total_from_tail_nodes(Nodes, QueryState, ResultAcc) ->
    case counting_total_fun(QueryState) of
        false ->
            ResultAcc;
        _Fun ->
            collect_total_from_tail_nodes(Nodes, QueryState, ResultAcc)
    end.

collect_total_from_tail_nodes(Nodes, QueryState, ResultAcc = #{total := TotalAcc}) ->
    %% XXX: badfun risk? if the FuzzyFun is an anonumous func in local node
    case rpc:multicall(Nodes, ?MODULE, apply_total_query, [QueryState], ?LONG_QUERY_TIMEOUT) of
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
    end.

%%--------------------------------------------------------------------
%% Do Query (or rpc query)
%%--------------------------------------------------------------------

%% QueryState ::
%%  #{continuation := ets:continuation(),
%%    page := pos_integer(),
%%    limit := pos_integer(),
%%    total := [{node(), non_neg_integer()}],
%%    table := atom(),
%%    qs := {Qs, Fuzzy}   %% parsed query params
%%    msfun := query_to_match_spec_fun()
%%    }
init_query_state(Tab, QString, MsFun, _Meta = #{page := Page, limit := Limit}) ->
    #{match_spec := Ms, fuzzy_fun := FuzzyFun} = erlang:apply(MsFun, [Tab, QString]),
    %% assert FuzzyFun type
    _ =
        case FuzzyFun of
            undefined ->
                ok;
            {NamedFun, Args} ->
                true = is_list(Args),
                {type, external} = erlang:fun_info(NamedFun, type)
        end,
    #{
        page => Page,
        limit => Limit,
        table => Tab,
        qs => QString,
        msfun => MsFun,
        mactch_spec => Ms,
        fuzzy_fun => FuzzyFun,
        total => [],
        continuation => ?FRESH_SELECT
    }.

%% @private This function is exempt from BPAPI
do_query(Node, QueryState) when Node =:= node() ->
    do_select(Node, QueryState);
do_query(Node, QueryState) ->
    case
        rpc:call(
            Node,
            ?MODULE,
            do_query,
            [Node, QueryState],
            ?LONG_QUERY_TIMEOUT
        )
    of
        {badrpc, _} = R -> {error, R};
        Ret -> Ret
    end.

do_select(
    Node,
    QueryState0 = #{
        table := Tab,
        mactch_spec := Ms,
        fuzzy_fun := FuzzyFun,
        continuation := Continuation,
        limit := Limit
    }
) ->
    QueryState = maybe_apply_total_query(Node, QueryState0),
    Result =
        case Continuation of
            ?FRESH_SELECT ->
                ets:select(Tab, Ms, Limit);
            _ ->
                %% XXX: Repair is necessary because we pass Continuation back
                %% and forth through the nodes in the `do_cluster_query`
                ets:select(ets:repair_continuation(Continuation, Ms))
        end,
    case Result of
        '$end_of_table' ->
            {[], QueryState#{continuation => ?FRESH_SELECT}};
        {Rows, NContinuation} ->
            NRows =
                case FuzzyFun of
                    undefined ->
                        Rows;
                    {FilterFun, Args0} when is_function(FilterFun), is_list(Args0) ->
                        lists:filter(
                            fun(E) -> erlang:apply(FilterFun, [E | Args0]) end,
                            Rows
                        )
                end,
            {NRows, QueryState#{continuation => NContinuation}}
    end.

maybe_apply_total_query(Node, QueryState = #{total := TotalAcc}) ->
    case proplists:get_value(Node, TotalAcc, undefined) of
        undefined ->
            Total = apply_total_query(QueryState),
            QueryState#{total := [{Node, Total} | TotalAcc]};
        _ ->
            QueryState
    end.

apply_total_query(QueryState = #{table := Tab}) ->
    case counting_total_fun(QueryState) of
        false ->
            %% return a fake total number if the query have any conditions
            0;
        Fun ->
            Fun(Tab)
    end.

counting_total_fun(_QueryState = #{qs := {[], []}}) ->
    fun(Tab) -> ets:info(Tab, size) end;
counting_total_fun(_QueryState = #{mactch_spec := Ms, fuzzy_fun := undefined}) ->
    %% XXX: Calculating the total number of data that match a certain
    %% condition under a large table is very expensive because the
    %% entire ETS table needs to be scanned.
    %%
    %% XXX: How to optimize it? i.e, using:
    [{MatchHead, Conditions, _Return}] = Ms,
    CountingMs = [{MatchHead, Conditions, [true]}],
    fun(Tab) ->
        ets:select_count(Tab, CountingMs)
    end;
counting_total_fun(_QueryState = #{fuzzy_fun := FuzzyFun}) when FuzzyFun =/= undefined ->
    %% XXX: Calculating the total number for a fuzzy searching is very very expensive
    %% so it is not supported now
    false.

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
%% Internal Functions
%%--------------------------------------------------------------------

parse_qstring(QString, QSchema) when is_map(QString) ->
    parse_qstring(maps:to_list(QString), QSchema);
parse_qstring(QString, QSchema) ->
    {NQString, FuzzyQString} = do_parse_qstring(QString, QSchema, [], []),
    {length(NQString) + length(FuzzyQString), {NQString, FuzzyQString}}.

do_parse_qstring([], _, Acc1, Acc2) ->
    %% remove fuzzy keys if present in accurate query
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
            lists:foldl(
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
