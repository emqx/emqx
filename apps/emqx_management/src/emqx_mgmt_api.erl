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
    select_table_with_count/4,
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

%%--------------------------------------------------------------------
%% Node Query
%%--------------------------------------------------------------------

node_query(Node, QString, Tab, QSchema, QueryFun, FmtFun) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            ResultAcc = #{cursor => 0, count => 0, rows => []},
            NResultAcc = do_node_query(
                Node, Tab, NQString, QueryFun, ?FRESH_SELECT, Meta, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_node_query(
    Node,
    Tab,
    QString,
    QueryFun,
    Continuation,
    Meta = #{limit := Limit},
    ResultAcc
) ->
    case do_query(Node, Tab, QString, QueryFun, Continuation, Limit) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Rows, ?FRESH_SELECT} ->
            {_, NResultAcc} = accumulate_query_rows(Node, Rows, ResultAcc, Meta),
            NResultAcc;
        {Rows, NContinuation} ->
            case accumulate_query_rows(Node, Rows, ResultAcc, Meta) of
                {enough, NResultAcc} ->
                    NResultAcc;
                {more, NResultAcc} ->
                    do_node_query(Node, Tab, QString, QueryFun, NContinuation, Meta, NResultAcc)
            end
    end.

%%--------------------------------------------------------------------
%% Cluster Query
%%--------------------------------------------------------------------

cluster_query(QString, Tab, QSchema, QueryFun, FmtFun) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            Nodes = mria_mnesia:running_nodes(),
            ResultAcc = #{cursor => 0, count => 0, rows => []},
            NResultAcc = do_cluster_query(
                Nodes, Tab, NQString, QueryFun, ?FRESH_SELECT, Meta, ResultAcc
            ),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_cluster_query([], _Tab, _QString, _QueryFun, _Continuation, _Meta, ResultAcc) ->
    ResultAcc;
do_cluster_query(
    [Node | Tail] = Nodes,
    Tab,
    QString,
    QueryFun,
    Continuation,
    Meta = #{limit := Limit},
    ResultAcc
) ->
    case do_query(Node, Tab, QString, QueryFun, Continuation, Limit) of
        {error, {badrpc, R}} ->
            {error, Node, {badrpc, R}};
        {Rows, NContinuation} ->
            case accumulate_query_rows(Node, Rows, ResultAcc, Meta) of
                {enough, NResultAcc} ->
                    NResultAcc;
                {more, NResultAcc} ->
                    case NContinuation of
                        ?FRESH_SELECT ->
                            do_cluster_query(
                                Tail, Tab, QString, QueryFun, ?FRESH_SELECT, Meta, NResultAcc
                            );
                        _ ->
                            do_cluster_query(
                                Nodes, Tab, QString, QueryFun, NContinuation, Meta, NResultAcc
                            )
                    end
            end
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

%% ResultAcc :: #{count := integer(),
%%                cursor := integer(),
%%                rows  := [{node(), Rows :: list()}]
%%               }
accumulate_query_rows(
    Node,
    Rows,
    ResultAcc = #{cursor := Cursor, count := Count, rows := RowsAcc},
    _Meta = #{page := Page, limit := Limit}
) ->
    PageStart = (Page - 1) * Limit + 1,
    PageEnd = Page * Limit,
    Len = length(Rows),
    case Cursor + Len of
        NCursor when NCursor < PageStart ->
            {more, ResultAcc#{cursor => NCursor}};
        NCursor when NCursor < PageEnd ->
            {more, ResultAcc#{
                cursor => NCursor,
                count => Count + length(Rows),
                rows => [{Node, Rows} | RowsAcc]
            }};
        NCursor when NCursor >= PageEnd ->
            SubRows = lists:sublist(Rows, Limit - Count),
            {enough, ResultAcc#{
                cursor => NCursor,
                count => Count + length(SubRows),
                rows => [{Node, SubRows} | RowsAcc]
            }}
    end.

%%--------------------------------------------------------------------
%% Table Select
%%--------------------------------------------------------------------

select_table_with_count(Tab, {Ms, FuzzyFilterFun}, ?FRESH_SELECT, Limit) when
    is_function(FuzzyFilterFun) andalso Limit > 0
->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {[], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {Rows, NContinuation}
    end;
select_table_with_count(_Tab, {Ms, FuzzyFilterFun}, Continuation, _Limit) when
    is_function(FuzzyFilterFun)
->
    case ets:select(ets:repair_continuation(Continuation, Ms)) of
        '$end_of_table' ->
            {[], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            Rows = FuzzyFilterFun(RawResult),
            {Rows, NContinuation}
    end;
select_table_with_count(Tab, Ms, ?FRESH_SELECT, Limit) when
    Limit > 0
->
    case ets:select(Tab, Ms, Limit) of
        '$end_of_table' ->
            {[], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {RawResult, NContinuation}
    end;
select_table_with_count(_Tab, Ms, Continuation, _Limit) ->
    case ets:select(ets:repair_continuation(Continuation, Ms)) of
        '$end_of_table' ->
            {[], ?FRESH_SELECT};
        {RawResult, NContinuation} ->
            {RawResult, NContinuation}
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

format_query_result(_FmtFun, _Meta, Error = {error, _Node, _Reason}) ->
    Error;
format_query_result(
    FmtFun, Meta, _ResultAcc = #{count := _Count, cursor := Cursor, rows := RowsAcc}
) ->
    #{
        meta => Meta#{count => Cursor},
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
