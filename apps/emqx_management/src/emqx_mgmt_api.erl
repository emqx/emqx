%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(FRESH_SELECT, fresh_select).

-export([ paginate/3
        , paginate/4
        ]).

%% first_next query APIs
-export([ node_query/5
        , cluster_query/4
        , select_table_with_count/5
        ]).

-export([do_query/6]).

paginate(Tables, Params, RowFun) ->
    Qh = query_handle(Tables),
    Count = count(Tables),
    Page = b2i(page(Params)),
    Limit = b2i(limit(Params)),
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

paginate(Tables, MatchSpec, Params, RowFun) ->
    Qh = query_handle(Tables, MatchSpec),
    Count = count(Tables, MatchSpec),
    Page = b2i(page(Params)),
    Limit = b2i(limit(Params)),
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
query_handle([Table]) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table)]);
query_handle(Tables) ->
    qlc:append([qlc:q([E || E <- ets:table(T)]) || T <- Tables]).

query_handle(Table, MatchSpec) when is_atom(Table) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:q([R|| R <- ets:table(Table, Options)]);
query_handle([Table], MatchSpec) when is_atom(Table) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:q([R|| R <- ets:table(Table, Options)]);
query_handle(Tables, MatchSpec) ->
    Options = {traverse, {select, MatchSpec}},
    qlc:append([qlc:q([E || E <- ets:table(T, Options)]) || T <- Tables]).

count(Table) when is_atom(Table) ->
    ets:info(Table, size);
count([Table]) when is_atom(Table) ->
    ets:info(Table, size);
count(Tables) ->
    lists:sum([count(T) || T <- Tables]).

count(Table, MatchSpec) when is_atom(Table) ->
    [{MatchPattern, Where, _Re}] = MatchSpec,
    NMatchSpec = [{MatchPattern, Where, [true]}],
    ets:select_count(Table, NMatchSpec);
count([Table], MatchSpec) when is_atom(Table) ->
    [{MatchPattern, Where, _Re}] = MatchSpec,
    NMatchSpec = [{MatchPattern, Where, [true]}],
    ets:select_count(Table, NMatchSpec);
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

node_query(Node, Params, Tab, QsSchema, QueryFun) ->
    {CodCnt, Qs} = params2qs(Params, QsSchema),
    Limit = b2i(limit(Params)),
    Page  = b2i(page(Params)),
    PageStart = page_start(Page, Limit),
    %% Rows so big, fixme.
    Rows = do_node_query(Node, Tab, Qs, QueryFun, _Continuation = ?FRESH_SELECT, Limit, []),
    Data = lists:sublist(Rows, PageStart, Limit),
    Meta = #{page => Page, limit => Limit},
    NMeta = case CodCnt =:= 0 of
                true -> Meta#{count => count(Tab)};
                _ -> Meta#{count => length(Rows)}
            end,
    #{meta => NMeta, data => Data}.

%% @private
do_node_query(Node, Tab, Qs, QueryFun, Continuation, Limit, Acc) ->
    {Rows, NContinuation} = do_query(Node, Tab, Qs, QueryFun, Continuation, Limit),
    NAcc = [Rows | Acc],
    case NContinuation of
        ?FRESH_SELECT ->
            lists:append(lists:reverse(Acc));
        _ ->
            do_node_query(Node, Tab, Qs, QueryFun, NContinuation, Limit, NAcc)
    end.

%%--------------------------------------------------------------------
%% Cluster Query
%%--------------------------------------------------------------------

cluster_query(Params, Tab, QsSchema, QueryFun) ->
    {CodCnt, Qs} = params2qs(Params, QsSchema),
    Limit = b2i(limit(Params)),
    Page  = b2i(page(Params)),
    PageStart = page_start(Page, Limit),
    Nodes = ekka_mnesia:running_nodes(),
    %% Rows so big, fixme.
    Rows = do_cluster_query(Nodes, Tab, Qs, QueryFun, _Continuation = ?FRESH_SELECT, Limit, []),
    Data = lists:sublist(Rows, PageStart, Limit),
    Meta = #{page => Page, limit => Limit},
    NMeta = case CodCnt =:= 0 of
                true -> Meta#{count => lists:sum([rpc_call(Node, ets, info, [Tab, size], 5000) || Node <- Nodes])};
                _ -> Meta#{count => length(Rows)}
            end,
    #{meta => NMeta, data => Data}.

%% @private
do_cluster_query([], _Tab, _Qs, _QueryFun, _Continuation, _Limit, Acc) ->
    lists:append(lists:reverse(Acc));
do_cluster_query([Node | Nodes], Tab, Qs, QueryFun, Continuation, Limit, Acc) ->
    {Rows, NContinuation} = do_query(Node, Tab, Qs, QueryFun, Continuation, Limit),
    NAcc = [Rows | Acc],
    case NContinuation of
        ?FRESH_SELECT ->
            do_cluster_query(Nodes, Tab, Qs, QueryFun, NContinuation, Limit, NAcc);
        _ ->
            do_cluster_query([Node | Nodes], Tab, Qs, QueryFun, NContinuation, Limit, NAcc)
    end.

%%--------------------------------------------------------------------
%% Do Query (or rpc query)
%%--------------------------------------------------------------------

%% @private
do_query(Node, Tab, Qs, {M,F}, Continuation, Limit) when Node =:= node() ->
    M:F(Tab, Qs, Continuation, Limit);
do_query(Node, Tab, Qs, QueryFun, Continuation, Limit) ->
    rpc_call(Node, ?MODULE, do_query,
             [Node, Tab, Qs, QueryFun, Continuation, Limit], 50000).

%% @private
rpc_call(Node, M, F, A, T) ->
    case rpc:call(Node, M, F, A, T) of
        {badrpc, _} = R -> {error, R};
        Res -> Res
    end.

%%--------------------------------------------------------------------
%% Table Select
%%--------------------------------------------------------------------

select_table_with_count(Tab, {Ms, FuzzyFilterFun}, ?FRESH_SELECT, Limit, FmtFun)
  when is_function(FuzzyFilterFun) andalso Limit > 0 ->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {length(Rows), lists:map(FmtFun, Rows), NContinuation}
    end;
select_table_with_count(_Tab, {_Ms, FuzzyFilterFun}, Continuation, _Limit, FmtFun)
  when is_function(FuzzyFilterFun) ->
    case ets:select(Continuation) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {length(Rows), lists:map(FmtFun, Rows), NContinuation}
    end;
select_table_with_count(Tab, Ms, ?FRESH_SELECT, Limit, FmtFun)
  when Limit > 0  ->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {length(RawResult), lists:map(FmtFun, RawResult), NContinuation}
    end;
select_table_with_count(_Tab, _Ms, Continuation, _Limit, FmtFun) ->
    case ets:select(Continuation) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {length(RawResult), lists:map(FmtFun, RawResult), NContinuation}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

params2qs(Params, QsSchema) when is_map(Params) ->
    params2qs(maps:to_list(Params), QsSchema);
params2qs(Params, QsSchema) ->
    {Qs, Fuzzy} = pick_params_to_qs(Params, QsSchema, [], []),
    {length(Qs) + length(Fuzzy), {Qs, Fuzzy}}.

pick_params_to_qs([], _, Acc1, Acc2) ->
    NAcc2 = [E || E <- Acc2, not lists:keymember(element(1, E), 1, Acc1)],
    {lists:reverse(Acc1), lists:reverse(NAcc2)};

pick_params_to_qs([{Key, Value}|Params], QsKits, Acc1, Acc2) ->
    case proplists:get_value(Key, QsKits) of
        undefined -> pick_params_to_qs(Params, QsKits, Acc1, Acc2);
        Type ->
            case Key of
                <<Prefix:4/binary, NKey/binary>>
                  when Prefix =:= <<"gte_">>;
                       Prefix =:= <<"lte_">> ->
                    OpposeKey = case Prefix of
                                    <<"gte_">> -> <<"lte_", NKey/binary>>;
                                    <<"lte_">> -> <<"gte_", NKey/binary>>
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

page_start(Page, Limit) ->
    if Page > 1 -> (Page-1) * Limit + 1;
       true -> 1
    end.

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

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
              {<<"gte_range">>, integer},
              {<<"lte_range">>, integer},
              {<<"like_fuzzy">>, binary},
              {<<"match_topic">>, binary}],
    Params = [{<<"str">>, <<"abc">>},
              {<<"int">>, <<"123">>},
              {<<"atom">>, <<"connected">>},
              {<<"ts">>, <<"156000">>},
              {<<"gte_range">>, <<"1">>},
              {<<"lte_range">>, <<"5">>},
              {<<"like_fuzzy">>, <<"user">>},
              {<<"match_topic">>, <<"t/#">>}],
    ExpectedQs = [{str, '=:=', <<"abc">>},
                  {int, '=:=', 123},
                  {atom, '=:=', connected},
                  {ts, '=:=', 156000},
                  {range, '>=', 1, '=<', 5}
                 ],
    FuzzyQs = [{fuzzy, like, <<"user">>},
               {topic, match, <<"t/#">>}],
    ?assertEqual({7, {ExpectedQs, FuzzyQs}}, params2qs(Params, Schema)),

    {0, {[], []}} = params2qs([{not_a_predefined_params, val}], Schema).

-endif.


b2i(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
b2i(Any) ->
    Any.
