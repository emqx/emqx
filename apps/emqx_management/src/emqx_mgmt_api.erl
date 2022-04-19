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
    node_query/5,
    cluster_query/4,
    select_table_with_count/5,
    b2i/1
]).

-export([do_query/6]).

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

init_meta(Params) ->
    Limit = b2i(limit(Params)),
    Page = b2i(page(Params)),
    #{
        page => Page,
        limit => Limit,
        count => 0
    }.

%%--------------------------------------------------------------------
%% Node Query
%%--------------------------------------------------------------------

node_query(Node, QString, Tab, QSchema, QueryFun) ->
    {_CodCnt, NQString} = parse_qstring(QString, QSchema),
    page_limit_check_query(
        init_meta(QString),
        {fun do_node_query/5, [Node, Tab, NQString, QueryFun, init_meta(QString)]}
    ).

%% @private
do_node_query(Node, Tab, QString, QueryFun, Meta) ->
    do_node_query(Node, Tab, QString, QueryFun, _Continuation = ?FRESH_SELECT, Meta, _Results = []).

do_node_query(
    Node,
    Tab,
    QString,
    QueryFun,
    Continuation,
    Meta = #{limit := Limit},
    Results
) ->
    case do_query(Node, Tab, QString, QueryFun, Continuation, Limit) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Len, Rows, ?FRESH_SELECT} ->
            {NMeta, NResults} = sub_query_result(Len, Rows, Limit, Results, Meta),
            #{meta => NMeta, data => NResults};
        {Len, Rows, NContinuation} ->
            {NMeta, NResults} = sub_query_result(Len, Rows, Limit, Results, Meta),
            do_node_query(Node, Tab, QString, QueryFun, NContinuation, NMeta, NResults)
    end.

%%--------------------------------------------------------------------
%% Cluster Query
%%--------------------------------------------------------------------

cluster_query(QString, Tab, QSchema, QueryFun) ->
    {_CodCnt, NQString} = parse_qstring(QString, QSchema),
    Nodes = mria_mnesia:running_nodes(),
    page_limit_check_query(
        init_meta(QString),
        {fun do_cluster_query/5, [Nodes, Tab, NQString, QueryFun, init_meta(QString)]}
    ).

%% @private
do_cluster_query(Nodes, Tab, QString, QueryFun, Meta) ->
    do_cluster_query(
        Nodes,
        Tab,
        QString,
        QueryFun,
        _Continuation = ?FRESH_SELECT,
        Meta,
        _Results = []
    ).

do_cluster_query([], _Tab, _QString, _QueryFun, _Continuation, Meta, Results) ->
    #{meta => Meta, data => Results};
do_cluster_query(
    [Node | Tail] = Nodes,
    Tab,
    QString,
    QueryFun,
    Continuation,
    Meta = #{limit := Limit},
    Results
) ->
    case do_query(Node, Tab, QString, QueryFun, Continuation, Limit) of
        {error, {badrpc, R}} ->
            {error, Node, {bar_rpc, R}};
        {Len, Rows, ?FRESH_SELECT} ->
            {NMeta, NResults} = sub_query_result(Len, Rows, Limit, Results, Meta),
            do_cluster_query(Tail, Tab, QString, QueryFun, ?FRESH_SELECT, NMeta, NResults);
        {Len, Rows, NContinuation} ->
            {NMeta, NResults} = sub_query_result(Len, Rows, Limit, Results, Meta),
            do_cluster_query(Nodes, Tab, QString, QueryFun, NContinuation, NMeta, NResults)
    end.

%%--------------------------------------------------------------------
%% Do Query (or rpc query)
%%--------------------------------------------------------------------

%% @private This function is exempt from BPAPI
do_query(Node, Tab, QString, {M, F}, Continuation, Limit) when Node =:= node() ->
    erlang:apply(M, F, [Tab, QString, Continuation, Limit]);
do_query(Node, Tab, QString, QueryFun, Continuation, Limit) ->
    case
        rpc:call(
            Node,
            ?MODULE,
            do_query,
            [Node, Tab, QString, QueryFun, Continuation, Limit],
            50000
        )
    of
        {badrpc, _} = R -> {error, R};
        Ret -> Ret
    end.

sub_query_result(Len, Rows, Limit, Results, Meta) ->
    {Flag, NMeta} = judge_page_with_counting(Len, Meta),
    NResults =
        case Flag of
            more ->
                [];
            cutrows ->
                {SubStart, NeedNowNum} = rows_sub_params(Len, NMeta),
                ThisRows = lists:sublist(Rows, SubStart, NeedNowNum),
                lists:sublist(lists:append(Results, ThisRows), SubStart, Limit);
            enough ->
                lists:sublist(lists:append(Results, Rows), 1, Limit)
        end,
    {NMeta, NResults}.

%%--------------------------------------------------------------------
%% Table Select
%%--------------------------------------------------------------------

select_table_with_count(Tab, {Ms, FuzzyFilterFun}, ?FRESH_SELECT, Limit, FmtFun) when
    is_function(FuzzyFilterFun) andalso Limit > 0
->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {length(Rows), lists:map(FmtFun, Rows), NContinuation}
    end;
select_table_with_count(_Tab, {Ms, FuzzyFilterFun}, Continuation, _Limit, FmtFun) when
    is_function(FuzzyFilterFun)
->
    case ets:select(ets:repair_continuation(Continuation, Ms)) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {length(Rows), lists:map(FmtFun, Rows), NContinuation}
    end;
select_table_with_count(Tab, Ms, ?FRESH_SELECT, Limit, FmtFun) when
    Limit > 0
->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {length(RawResult), lists:map(FmtFun, RawResult), NContinuation}
    end;
select_table_with_count(_Tab, Ms, Continuation, _Limit, FmtFun) ->
    case ets:select(ets:repair_continuation(Continuation, Ms)) of
        '$end_of_table' ->
            {0, [], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {length(RawResult), lists:map(FmtFun, RawResult), NContinuation}
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

page_start(1, _) -> 1;
page_start(Page, Limit) -> (Page - 1) * Limit + 1.

judge_page_with_counting(Len, Meta = #{page := Page, limit := Limit, count := Count}) ->
    PageStart = page_start(Page, Limit),
    PageEnd = Page * Limit,
    case Count + Len of
        NCount when NCount < PageStart ->
            {more, Meta#{count => NCount}};
        NCount when NCount < PageEnd ->
            {cutrows, Meta#{count => NCount}};
        NCount when NCount >= PageEnd ->
            {enough, Meta#{count => NCount}}
    end.

rows_sub_params(Len, _Meta = #{page := Page, limit := Limit, count := Count}) ->
    PageStart = page_start(Page, Limit),
    case (Count - Len) < PageStart of
        true ->
            NeedNowNum = Count - PageStart + 1,
            SubStart = Len - NeedNowNum + 1,
            {SubStart, NeedNowNum};
        false ->
            {_SubStart = 1, _NeedNowNum = Len}
    end.

page_limit_check_query(Meta, {F, A}) ->
    case Meta of
        #{page := Page, limit := Limit} when
            Page < 1; Limit < 1
        ->
            {error, page_limit_invalid};
        _ ->
            erlang:apply(F, A)
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

b2i(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
b2i(Any) ->
    Any.
