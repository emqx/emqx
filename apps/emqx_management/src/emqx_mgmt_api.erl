%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([paginate/3]).

%% first_next query APIs
-export([ params2qs/2
        , node_query/4
        , node_query/5
        , node_query/6
        , cluster_query/3
        , traverse_table/5
        , select_table/5
        ]).

-export([do_query/5]).

-export([ page/1
        , limit/1
        ]).

paginate(Tables, Params, RowFun) ->
    Qh = query_handle(Tables),
    Count = count(Tables),
    Page = page(Params),
    Limit = limit(Params),
    Cursor = qlc:cursor(Qh),
    case Page > 1 of
        true  ->
            _ = qlc:next_answers(Cursor, (Page - 1) * Limit),
            ok;
        false -> ok
    end,
    Rows = qlc:next_answers(Cursor, Limit),
    qlc:delete_cursor(Cursor),
    #{meta  => #{page => Page, limit => Limit, count => Count},
      data  => [RowFun(Row) || Row <- Rows]}.

query_handle(Table) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table)]);

query_handle({Table, Opts}) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table, Opts)]);

query_handle([Table]) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table)]);

query_handle([{Table, Opts}]) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table, Opts)]);

query_handle(Tables) ->
    Fold = fun({Table, Opts}, Acc) ->
                   Handle = qlc:q([R|| R <- ets:table(Table, Opts)]),
                   [Handle | Acc];
              (Table, Acc) ->
                   Handle = qlc:q([R|| R <- ets:table(Table)]),
                   [Handle | Acc]
            end,
    Handles = lists:foldl(Fold, [], Tables),
    qlc:append(lists:reverse(Handles)).

count_size(Table, undefined) ->
    count(Table);
count_size(_Table, CountFun) ->
    CountFun().

count(Table) when is_atom(Table) ->
    ets:info(Table, size);

count({Table, _Opts}) when is_atom(Table) ->
    ets:info(Table, size);

count([Table]) when is_atom(Table) ->
    ets:info(Table, size);

count([{Table, _Opts}]) when is_atom(Table) ->
    ets:info(Table, size);

count(Tables) ->
    Fold = fun({Table, _Opts}, Acc) ->
                   count(Table) ++ Acc;
              (Table, Acc) ->
                   count(Table) ++ Acc
           end,
    lists:foldl(Fold, 0, Tables).

count(Table, Nodes) ->
    lists:sum([rpc_call(Node, ets, info, [Table, size], 5000) || Node <- Nodes]).

page(Params) ->
    binary_to_integer(proplists:get_value(<<"_page">>, Params, <<"1">>)).

limit(Params) ->
    case proplists:get_value(<<"_limit">>, Params) of
        undefined -> emqx_mgmt:max_row_limit();
        Size      -> binary_to_integer(Size)
    end.

%%--------------------------------------------------------------------
%% Node Query
%%--------------------------------------------------------------------

node_query(Node, Params, {Tab, QsSchema}, QueryFun) ->
    node_query(Node, Params, {Tab, QsSchema}, QueryFun, undefined, undefined).

node_query(Node, Params, {Tab, QsSchema}, QueryFun, SortFun) ->
    node_query(Node, Params, {Tab, QsSchema}, QueryFun, SortFun, undefined).

node_query(Node, Params, {Tab, QsSchema}, QueryFun, SortFun, CountFun) ->
    {CodCnt, Qs} = params2qs(Params, QsSchema),
    Limit = limit(Params),
    Page  = page(Params),
    Start = if Page > 1 -> (Page-1) * Limit;
               true -> 0
            end,
    {_, Rows} = do_query(Node, Qs, QueryFun, Start, Limit+1),
    Meta = #{page => Page, limit => Limit},
    NMeta = case CodCnt =:= 0 of
                true -> Meta#{count => count_size(Tab, CountFun), hasnext => length(Rows) > Limit};
                _ -> Meta#{count => -1, hasnext => length(Rows) > Limit}
            end,
    Data0 = lists:sublist(Rows, Limit),
    Data = case SortFun of
               undefined -> Data0;
               _ -> lists:sort(SortFun, Data0)
           end,
    #{meta => NMeta, data => Data}.

%% @private
do_query(Node, Qs, {M,F}, Start, Limit) when Node =:= node() ->
    M:F(Qs, Start, Limit);
do_query(Node, Qs, QueryFun, Start, Limit) ->
    rpc_call(Node, ?MODULE, do_query, [Node, Qs, QueryFun, Start, Limit], 50000).

%% @private
rpc_call(Node, M, F, A, T) ->
    case rpc:call(Node, M, F, A, T) of
        {badrpc, _} = R -> {error, R};
        Res -> Res
    end.

%%--------------------------------------------------------------------
%% Cluster Query
%%--------------------------------------------------------------------

cluster_query(Params, {Tab, QsSchema}, QueryFun) ->
    {CodCnt, Qs} = params2qs(Params, QsSchema),
    Limit = limit(Params),
    Page  = page(Params),
    Start = if Page > 1 -> (Page-1) * Limit;
               true -> 0
            end,
    Nodes = lists:sort(ekka_mnesia:running_nodes()),
    Rows = do_cluster_query(Nodes, Qs, QueryFun, Start, Limit+1, []),
    Meta = #{page => Page, limit => Limit},
    NMeta = case CodCnt =:= 0 of
                true -> Meta#{count => count(Tab, Nodes), hasnext => length(Rows) > Limit};
                _ -> Meta#{count => -1, hasnext => length(Rows) > Limit}
            end,
    #{meta => NMeta, data => lists:sublist(Rows, Limit)}.

%% @private
do_cluster_query([], _, _, _, _, Acc) ->
    lists:append(lists:reverse(Acc));
do_cluster_query([Node|Nodes], Qs, QueryFun, Start, Limit, Acc) ->
    {NStart, Rows} = do_query(Node, Qs, QueryFun, Start, Limit),
    case Limit - length(Rows) of
        Rest when Rest > 0 ->
            do_cluster_query(Nodes, Qs, QueryFun, NStart, Limit, [Rows|Acc]);
        0 ->
            lists:append(lists:reverse([Rows|Acc]))
    end.

traverse_table(Tab, MatchFun, Start, Limit, FmtFun) ->
    ets:safe_fixtable(Tab, true),
    {NStart, Rows} = traverse_n_by_one(Tab, ets:first(Tab), MatchFun, Start, Limit, []),
    ets:safe_fixtable(Tab, false),
    {NStart, lists:map(FmtFun, Rows)}.

%% @private
traverse_n_by_one(_, '$end_of_table', _, Start, _, Acc) ->
    {Start, lists:flatten(lists:reverse(Acc))};
traverse_n_by_one(_, _, _, Start, _Limit=0, Acc) ->
    {Start, lists:flatten(lists:reverse(Acc))};
traverse_n_by_one(Tab, K, MatchFun, Start, Limit, Acc) ->
    GetRows = fun _GetRows('$end_of_table', _, Ks) ->
                      {'$end_of_table', Ks};
                  _GetRows(Kn,  1, Ks) ->
                      {ets:next(Tab, Kn), [ets:lookup(Tab, Kn) | Ks]};
                  _GetRows(Kn, N, Ks) ->
                      _GetRows(ets:next(Tab, Kn), N-1, [ets:lookup(Tab, Kn) | Ks])
              end,
    {K2, Rows} = GetRows(K, 100, []),
    case MatchFun(lists:flatten(lists:reverse(Rows))) of
        [] ->
            traverse_n_by_one(Tab, K2, MatchFun, Start, Limit, Acc);
        Ls ->
            case Start - length(Ls) of
                N when N > 0 -> %% Skip
                    traverse_n_by_one(Tab, K2, MatchFun, N, Limit, Acc);
                _ ->
                    Got = lists:sublist(Ls, Start+1, Limit),
                    NLimit = Limit - length(Got),
                    traverse_n_by_one(Tab, K2, MatchFun, 0, NLimit, [Got|Acc])
            end
    end.

select_table(Tab, Ms, 0, Limit, FmtFun) ->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {0, []};
        {Rows, _} ->
            {0, lists:map(FmtFun, lists:reverse(Rows))}
    end;

select_table(Tab, Ms, Start, Limit, FmtFun) ->
    {NStart, Rows} = select_n_by_one(ets:select(Tab, Ms, Limit), Start, Limit, []),
    {NStart, lists:map(FmtFun, Rows)}.

select_n_by_one('$end_of_table', Start, _Limit, Acc) ->
    {Start, lists:flatten(lists:reverse(Acc))};
select_n_by_one(_, Start, _Limit = 0, Acc) ->
    {Start, lists:flatten(lists:reverse(Acc))};

select_n_by_one({Rows0, Cons}, Start, Limit, Acc) ->
    Rows = lists:reverse(Rows0),
    case Start - length(Rows) of
        N when N > 0 -> %% Skip
            select_n_by_one(ets:select(Cons), N, Limit, Acc);
        _ ->
            Got = lists:sublist(Rows, Start+1, Limit),
            NLimit = Limit - length(Got),
            select_n_by_one(ets:select(Cons), 0, NLimit, [Got|Acc])
    end.

params2qs(Params, QsSchema) ->
    {Qs, Fuzzy} = pick_params_to_qs(Params, QsSchema, [], []),
    {length(Qs) + length(Fuzzy), {Qs, Fuzzy}}.

%%--------------------------------------------------------------------
%% Intenal funcs

pick_params_to_qs([], _, Acc1, Acc2) ->
    NAcc2 = [E || E <- Acc2, not lists:keymember(element(1, E), 1, Acc1)],
    {lists:reverse(Acc1), lists:reverse(NAcc2)};

pick_params_to_qs([{Key, Value}|Params], QsKits, Acc1, Acc2) ->
    case proplists:get_value(Key, QsKits) of
        undefined -> pick_params_to_qs(Params, QsKits, Acc1, Acc2);
        Type ->
            case Key of
                <<Prefix:5/binary, NKey/binary>>
                  when Prefix =:= <<"_gte_">>;
                       Prefix =:= <<"_lte_">> ->
                    OpposeKey = case Prefix of
                                    <<"_gte_">> -> <<"_lte_", NKey/binary>>;
                                    <<"_lte_">> -> <<"_gte_", NKey/binary>>
                                end,
                    case lists:keytake(OpposeKey, 1, Params) of
                        false ->
                            pick_params_to_qs(Params, QsKits, [qs(Key, Value, Type) | Acc1], Acc2);
                        {value, {K2, V2}, NParams} ->
                            pick_params_to_qs(NParams, QsKits, [qs(Key, Value, K2, V2, Type) | Acc1], Acc2)
                    end;
                _ ->
                    case is_fuzzy_key(Key) of
                        true ->
                            pick_params_to_qs(Params, QsKits, Acc1, [qs(Key, Value, Type) | Acc2]);
                        _ ->
                            pick_params_to_qs(Params, QsKits, [qs(Key, Value, Type) | Acc1], Acc2)

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
        throw : bad_value_type ->
            throw({bad_value_type, {K, Type, Value0}})
    end.

qs(<<"_gte_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), '>=', Value};
qs(<<"_lte_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), '=<', Value};
qs(<<"_like_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), like, Value};
qs(<<"_match_", Key/binary>>, Value) ->
    {binary_to_existing_atom(Key, utf8), match, Value};
qs(Key, Value) ->
    {binary_to_existing_atom(Key, utf8), '=:=', Value}.

is_fuzzy_key(<<"_like_", _/binary>>) ->
    true;
is_fuzzy_key(<<"_match_", _/binary>>) ->
    true;
is_fuzzy_key(_) ->
    false.

%%--------------------------------------------------------------------
%% Types

to_type(V, TargetType) ->
    try
        to_type_(V, TargetType)
    catch
        _ : _ ->
            throw(bad_value_type)
    end.

to_type_(V, atom) -> to_atom(V);
to_type_(V, integer) -> to_integer(V);
to_type_(V, timestamp) -> to_timestamp(V);
to_type_(V, ip) -> aton(V);
to_type_(V, _) -> V.

to_atom(A) when is_atom(A) ->
    A;
to_atom(B) when is_binary(B) ->
    binary_to_atom(B, utf8).

to_integer(I) when is_integer(I) ->
    I;
to_integer(B) when is_binary(B) ->
    binary_to_integer(B).

%% @doc The input timestamp time is in seconds, which needs to be
%% converted to internal milliseconds here
to_timestamp(I) when is_integer(I) ->
    I * 1000;
to_timestamp(B) when is_binary(B) ->
    binary_to_integer(B) * 1000.

aton(B) when is_binary(B) ->
    list_to_tuple([binary_to_integer(T) || T <- re:split(B, "[.]")]).

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

params2qs_test() ->
    Schema = [{<<"str">>, binary},
              {<<"int">>, integer},
              {<<"atom">>, atom},
              {<<"ts">>, timestamp},
              {<<"_gte_range">>, integer},
              {<<"_lte_range">>, integer},
              {<<"_like_fuzzy">>, binary},
              {<<"_match_topic">>, binary}],
    Params = [{<<"str">>, <<"abc">>},
              {<<"int">>, <<"123">>},
              {<<"atom">>, <<"connected">>},
              {<<"ts">>, <<"156000">>},
              {<<"_gte_range">>, <<"1">>},
              {<<"_lte_range">>, <<"5">>},
              {<<"_like_fuzzy">>, <<"user">>},
              {<<"_match_topic">>, <<"t/#">>}],
    ExpectedQs = [{str, '=:=', <<"abc">>},
                  {int, '=:=', 123},
                  {atom, '=:=', connected},
                  {ts, '=:=', 156000000},
                  {range, '>=', 1, '=<', 5}
                 ],
    FuzzyQs = [{fuzzy, like, <<"user">>},
               {topic, match, <<"t/#">>}],
    ?assertEqual({7, {ExpectedQs, FuzzyQs}}, params2qs(Params, Schema)),

    {0, {[], []}} = params2qs([{not_a_predefined_params, val}], Schema).

-endif.
