%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_mgmt.hrl").

-elvis([{elvis_style, dont_repeat_yourself, #{min_complexity => 100}}]).

-define(LONG_QUERY_TIMEOUT, 50000).

-export([
    paginate/3
]).

%% first_next query APIs
-export([
    node_query/6,
    node_query/7,
    node_query_with_tabs/6,
    node_query_with_tabs/7,
    cluster_query/5,
    cluster_query/6,
    b2i/1
]).

-export([
    parse_pager_params/1,
    parse_cont_pager_params/2,
    parse_qstring/2,
    init_query_result/0,
    init_query_state/5,
    reset_query_state/1,
    accumulate_query_rows/4,
    finalize_query/2,
    mark_complete/2,
    format_query_result/3,
    format_query_result/4,
    maybe_collect_total_from_tail_nodes/2
]).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([paginate_test_format/1]).

-endif.

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

-type query_options() :: #{
    %% Whether to use `ets:info/2` to get the total number of rows when the query conditions are
    %% empty. It can significantly improves the speed of the query when the table stored large
    %% amounts of data.
    %%
    %% However, it may cause the total number of rows to be inaccurate when the table stored in
    %% multiple schemas of data, i.e: Built-in Authorization
    %%
    %% Default: false
    fast_total_counting => boolean()
}.

-type query_return() :: #{meta := map(), data := [term()]}.
-type table_name() :: atom().
-type table_names() :: [table_name()].

-export([do_query/2, apply_total_query/1]).

-spec paginate(table_name() | table_names(), map(), {atom(), atom()}) ->
    #{
        meta => #{page => pos_integer(), limit => pos_integer(), count => pos_integer()},
        data => list(term())
    }.
paginate(Tables, Params, {Module, FormatFun}) ->
    Qh = query_handle(Tables),
    Count = count(Tables),
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

query_handle(Tables) when is_list(Tables) ->
    qlc:append([query_handle(T) || T <- Tables]);
query_handle(Table) ->
    ets:table(Table).

count(Tables) when is_list(Tables) ->
    lists:sum([count(T) || T <- Tables]);
count(Table) ->
    ets:info(Table, size).

page(Params) ->
    maps:get(<<"page">>, Params, 1).

limit(Params) when is_map(Params) ->
    maps:get(<<"limit">>, Params, emqx_mgmt:default_row_limit()).

position(Params, Decoder) ->
    try
        decode_position(maps:get(<<"position">>, Params, none), Decoder)
    catch
        _:_ ->
            error
    end.

decode_position(none, _Decoder) ->
    none;
decode_position(Pos, Decoder) ->
    Decoder(Pos).

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
    node_query(Node, Tab, QString, QSchema, MsFun, FmtFun, #{}).

-spec node_query(
    node(),
    atom(),
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun(),
    query_options()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
node_query(Node, Tab, QString, QSchema, MsFun, FmtFun, Options) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            ResultAcc = init_query_result(),
            QueryState = init_query_state(Tab, NQString, MsFun, Meta, Options),
            NResultAcc = do_node_query(Node, QueryState, ResultAcc),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_node_query(
    Node,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, QueryState) of
        {error, Error} ->
            {error, Node, Error};
        {Rows, NQueryState = #{complete := Complete}} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    finalize_query(NResultAcc, NQueryState);
                {_, NResultAcc} when Complete ->
                    finalize_query(NResultAcc, NQueryState);
                {more, NResultAcc} ->
                    do_node_query(Node, NQueryState, NResultAcc)
            end
    end.

%%--------------------------------------------------------------------
%% Node Query with tables
%%--------------------------------------------------------------------

-spec node_query_with_tabs(
    node(),
    [atom()],
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
node_query_with_tabs(Node, Tabs, QString, QSchema, MsFun, FmtFun) ->
    node_query_with_tabs(Node, Tabs, QString, QSchema, MsFun, FmtFun, #{}).

-spec node_query_with_tabs(
    node(),
    [atom()],
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun(),
    query_options()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
node_query_with_tabs(Node, [Tab | Tabs], QString, QSchema, MsFun, FmtFun, Options) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            {_CodCnt, NQString} = parse_qstring(QString, QSchema),
            ResultAcc = init_query_result(),
            QueryState = init_query_state(Tab, NQString, MsFun, Meta, Options),
            NResultAcc = do_node_query_with_tabs(Node, Tabs, QueryState, ResultAcc),
            format_query_result(FmtFun, Meta, NResultAcc)
    end.

%% @private
do_node_query_with_tabs(
    Node,
    Tabs,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, QueryState) of
        {error, Error} ->
            {error, Node, Error};
        {Rows, NQueryState = #{complete := Complete}} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    FComplete = Complete andalso Tabs =:= [],
                    finalize_query(NResultAcc, mark_complete(NQueryState, FComplete));
                {more, NResultAcc} when not Complete ->
                    do_node_query_with_tabs(Node, Tabs, NQueryState, NResultAcc);
                {more, NResultAcc} when Tabs =/= [] ->
                    [Tab | NTabs] = Tabs,
                    NQueryState2 = reinit_query_state(Tab, NQueryState),
                    do_node_query_with_tabs(Node, NTabs, NQueryState2, NResultAcc);
                {more, NResultAcc} ->
                    finalize_query(NResultAcc, NQueryState)
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
    cluster_query(Tab, QString, QSchema, MsFun, FmtFun, #{}).

-spec cluster_query(
    atom(),
    query_params(),
    query_schema(),
    query_to_match_spec_fun(),
    format_result_fun(),
    query_options()
) -> {error, page_limit_invalid} | {error, atom(), term()} | query_return().
cluster_query(Tab, QString, QSchema, MsFun, FmtFun, Options) ->
    case parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta ->
            try
                {_CodCnt, NQString} = parse_qstring(QString, QSchema),
                Nodes = emqx:running_nodes(),
                ResultAcc = init_query_result(),
                QueryState = init_query_state(Tab, NQString, MsFun, Meta, Options),
                NResultAcc = do_cluster_query(
                    Nodes, QueryState, ResultAcc
                ),
                format_query_result(FmtFun, Meta, NResultAcc)
            catch
                throw:{bad_value_type, {Key, ExpectedType, AcutalValue}} ->
                    {error, invalid_query_string_param, {Key, ExpectedType, AcutalValue}}
            end
    end.

%% @private
do_cluster_query(
    [Node | Tail] = Nodes,
    QueryState,
    ResultAcc
) ->
    case do_query(Node, QueryState) of
        {error, Error} ->
            {error, Node, Error};
        {Rows, NQueryState = #{complete := Complete}} ->
            case accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    FQueryState = maybe_collect_total_from_tail_nodes(Tail, NQueryState),
                    FComplete = Complete andalso Tail =:= [],
                    finalize_query(NResultAcc, mark_complete(FQueryState, FComplete));
                {more, NResultAcc} when not Complete ->
                    do_cluster_query(Nodes, NQueryState, NResultAcc);
                {more, NResultAcc} when Tail =/= [] ->
                    do_cluster_query(Tail, reset_query_state(NQueryState), NResultAcc);
                {more, NResultAcc} ->
                    finalize_query(NResultAcc, NQueryState)
            end
    end.

maybe_collect_total_from_tail_nodes([], QueryState) ->
    QueryState;
maybe_collect_total_from_tail_nodes(Nodes, QueryState = #{total := _}) ->
    collect_total_from_tail_nodes(Nodes, QueryState);
maybe_collect_total_from_tail_nodes(_Nodes, QueryState) ->
    QueryState.

collect_total_from_tail_nodes(Nodes, QueryState = #{total := TotalAcc}) ->
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
                    NTotalAcc = maps:merge(TotalAcc, maps:from_list(ResL)),
                    QueryState#{total := NTotalAcc}
            end
    end.

%%--------------------------------------------------------------------
%% Do Query (or rpc query)
%%--------------------------------------------------------------------

%% QueryState ::
%%  #{continuation => ets:continuation(),
%%    page := pos_integer(),
%%    limit := pos_integer(),
%%    total => #{node() => non_neg_integer()},
%%    table := atom(),
%%    qs := {Qs, Fuzzy},  %% parsed query params
%%    msfun := query_to_match_spec_fun(),
%%    complete := boolean(),
%%    options := query_options()
%%    }
init_query_state(Tab, QString, MsFun, _Meta = #{page := Page, limit := Limit}, Options) ->
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
    QueryState = #{
        page => Page,
        limit => Limit,
        table => Tab,
        qs => QString,
        msfun => MsFun,
        match_spec => Ms,
        fuzzy_fun => FuzzyFun,
        complete => false,
        options => Options
    },
    case counting_total_fun(QueryState) of
        false ->
            QueryState;
        Fun when is_function(Fun) ->
            QueryState#{total => #{}}
    end.

reinit_query_state(Tab, #{qs := QString, msfun := MsFun} = QueryState) ->
    #{match_spec := Ms, fuzzy_fun := FuzzyFun} = erlang:apply(MsFun, [Tab, QString]),
    _ =
        case FuzzyFun of
            undefined ->
                ok;
            {NamedFun, Args} ->
                true = is_list(Args),
                {type, external} = erlang:fun_info(NamedFun, type)
        end,

    QueryState2 = reset_query_state(QueryState),

    QueryState2#{
        table := Tab,
        match_spec := Ms,
        fuzzy_fun := FuzzyFun
    }.

reset_query_state(QueryState) ->
    maps:remove(continuation, mark_complete(QueryState, false)).

mark_complete(QueryState) ->
    mark_complete(QueryState, true).

mark_complete(QueryState, Complete) ->
    QueryState#{complete => Complete}.

%% @private This function is exempt from BPAPI
do_query(Node, QueryState) when Node =:= node() ->
    do_select(Node, QueryState);
do_query(Node, QueryState) ->
    case
        catch rpc:call(
            Node,
            ?MODULE,
            do_query,
            [Node, QueryState],
            ?LONG_QUERY_TIMEOUT
        )
    of
        {badrpc, _} = R -> {error, R};
        {'EXIT', _} = R -> {error, R};
        Ret -> Ret
    end.

do_select(
    Node,
    QueryState0 = #{
        table := Tab,
        match_spec := Ms,
        limit := Limit,
        complete := false
    }
) ->
    QueryState = maybe_apply_total_query(Node, QueryState0),
    Result =
        try
            case maps:get(continuation, QueryState, undefined) of
                undefined ->
                    ets:select_reverse(Tab, Ms, Limit);
                Continuation ->
                    %% XXX: Repair is necessary because we pass Continuation back
                    %% and forth through the nodes in the `do_cluster_query`
                    ets:select_reverse(ets:repair_continuation(Continuation, Ms))
            end
        catch
            exit:_ = Exit ->
                {error, Exit};
            Type:Reason:Stack ->
                {error, #{exception => Type, reason => Reason, stacktrace => Stack}}
        end,
    case Result of
        {error, _} ->
            {[], mark_complete(QueryState)};
        {Rows, '$end_of_table'} ->
            NRows = maybe_apply_fuzzy_filter(Rows, QueryState),
            {NRows, mark_complete(QueryState)};
        {Rows, NContinuation} ->
            NRows = maybe_apply_fuzzy_filter(Rows, QueryState),
            {NRows, QueryState#{continuation => NContinuation}};
        '$end_of_table' ->
            {[], mark_complete(QueryState)}
    end.

maybe_apply_fuzzy_filter(Rows, #{fuzzy_fun := undefined}) ->
    Rows;
maybe_apply_fuzzy_filter(Rows, #{fuzzy_fun := {FilterFun, Args}}) ->
    lists:filter(
        fun(E) -> erlang:apply(FilterFun, [E | Args]) end,
        Rows
    ).

maybe_apply_total_query(Node, QueryState = #{total := Acc}) ->
    case Acc of
        #{Node := _} ->
            QueryState;
        #{} ->
            NodeTotal = apply_total_query(QueryState),
            QueryState#{total := Acc#{Node => NodeTotal}}
    end;
maybe_apply_total_query(_Node, QueryState = #{}) ->
    QueryState.

apply_total_query(QueryState = #{table := Tab}) ->
    case counting_total_fun(QueryState) of
        false ->
            %% return a fake total number if the query have any conditions
            0;
        Fun ->
            Fun(Tab)
    end.

counting_total_fun(_QueryState = #{qs := {[], []}, options := #{fast_total_counting := true}}) ->
    fun(Tab) -> ets:info(Tab, size) end;
counting_total_fun(_QueryState = #{match_spec := Ms, fuzzy_fun := undefined}) ->
    %% XXX: Calculating the total number of data that match a certain
    %% condition under a large table is very expensive because the
    %% entire ETS table needs to be scanned.
    %%
    %% XXX: How to optimize it? i.e, using:
    [{MatchHead, Conditions, _Return}] = Ms,
    CountingMs = [{MatchHead, Conditions, [true]}],
    fun(Tab) ->
        try
            ets:select_count(Tab, CountingMs)
        catch
            _Type:_Reason -> 0
        end
    end;
counting_total_fun(_QueryState = #{fuzzy_fun := FuzzyFun}) when FuzzyFun =/= undefined ->
    %% XXX: Calculating the total number for a fuzzy searching is very very expensive
    %% so it is not supported now
    false.

%% ResultAcc :: #{count := integer(),
%%                cursor := integer(),
%%                rows  := [{node(), Rows :: list()}],
%%                overflow := boolean(),
%%                hasnext => boolean()
%%               }
init_query_result() ->
    #{cursor => 0, count => 0, rows => [], overflow => false}.

accumulate_query_rows(
    Node,
    Rows,
    _QueryState = #{page := Page, limit := Limit},
    ResultAcc = #{cursor := Cursor, count := Count, rows := RowsAcc}
) ->
    PageStart = (Page - 1) * Limit + 1,
    PageEnd = Page * Limit,
    Len = length(Rows),
    case Cursor + Len of
        NCursor when NCursor < PageStart ->
            %% Haven't reached the required page.
            {more, ResultAcc#{cursor => NCursor}};
        NCursor when NCursor < PageEnd ->
            %% Rows overlap with the page start
            %% Throw away rows in the beginning belonging to the previous page(s).
            SubRows = lists:nthtail(max(0, PageStart - Cursor - 1), Rows),
            {more, ResultAcc#{
                cursor => NCursor,
                count => Count + length(SubRows),
                rows => [{Node, SubRows} | RowsAcc]
            }};
        NCursor when NCursor >= PageEnd ->
            %% Rows overlap with the page end (and potentially with the page start).
            %% Throw away rows in the beginning belonging to the previous page(s).
            %% Then throw away rows in the tail belonging to the next page(s).
            PageRows = lists:nthtail(max(0, PageStart - Cursor - 1), Rows),
            SubRows = lists:sublist(PageRows, Limit - Count),
            {enough, ResultAcc#{
                cursor => NCursor,
                count => Count + length(SubRows),
                rows => [{Node, SubRows} | RowsAcc],
                % there are more rows than can fit in the page
                overflow => (Limit - Count) < Len
            }}
    end.

finalize_query(Result = #{overflow := Overflow}, QueryState = #{complete := Complete}) ->
    HasNext = Overflow orelse not Complete,
    maybe_accumulate_totals(Result#{hasnext => HasNext}, QueryState).

maybe_accumulate_totals(Result, #{total := TotalAcc}) ->
    AccFun = fun
        (_Node, NodeTotal, AccIn) when is_number(NodeTotal) -> AccIn + NodeTotal;
        (_Node, _, AccIn) -> AccIn
    end,
    QueryTotal = maps:fold(AccFun, 0, TotalAcc),
    Result#{total => QueryTotal};
maybe_accumulate_totals(Result, _QueryState) ->
    Result.

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

format_query_result(FmtFun, MetaIn, ResultAcc) ->
    format_query_result(FmtFun, MetaIn, ResultAcc, #{}).

format_query_result(_FmtFun, _MetaIn, Error = {error, _Node, _Reason}, _Opts) ->
    Error;
format_query_result(
    FmtFun, MetaIn, ResultAcc = #{hasnext := HasNext, rows := RowsAcc}, Opts
) ->
    Meta =
        case ResultAcc of
            #{total := QueryTotal} ->
                %% The `count` is used in HTTP API to indicate the total number of
                %% queries that can be read
                MetaIn#{hasnext => HasNext, count => QueryTotal};
            #{} ->
                MetaIn#{hasnext => HasNext}
        end,
    #{
        meta => Meta,
        data => format_query_data(FmtFun, RowsAcc, Opts)
    }.

format_query_data(FmtFun, RowsAcc, Opts) ->
    %% NOTE: `RowsAcc` is reversed in the node-order, `lists:foldl/3` is correct here.
    lists:foldl(
        fun({Node, Rows}, Acc) ->
            [exec_format_fun(FmtFun, Node, R, Opts) || R <- Rows] ++ Acc
        end,
        [],
        RowsAcc
    ).

exec_format_fun(FmtFun, Node, Row, Opts) ->
    case erlang:fun_info(FmtFun, arity) of
        {arity, 1} -> FmtFun(Row);
        {arity, 2} -> FmtFun(Node, Row);
        {arity, 3} -> FmtFun(Node, Row, Opts)
    end.

parse_pager_params(Params) ->
    Page = b2i(page(Params)),
    Limit = b2i(limit(Params)),
    case Page > 0 andalso Limit > 0 of
        true ->
            #{page => Page, limit => Limit};
        false ->
            false
    end.

-spec parse_cont_pager_params(map(), fun((binary()) -> term())) ->
    #{limit := pos_integer(), position := none | term()} | false.
parse_cont_pager_params(Params, PositionDecoder) ->
    Pos = position(Params, PositionDecoder),
    Limit = b2i(limit(Params)),
    case Limit > 0 andalso Pos =/= error of
        true ->
            #{position => Pos, limit => Limit};
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

to_type_(V, atom) ->
    to_atom(V);
to_type_(V, integer) ->
    to_integer(V);
to_type_(V, timestamp) ->
    to_timestamp(V);
to_type_(V, ip) ->
    to_ip(V);
to_type_(V, ip_port) ->
    to_ip_port(V);
to_type_(V, Fun) when is_function(Fun, 1) ->
    Fun(V);
to_type_(V, _) ->
    V.

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

to_ip(IP0) when is_binary(IP0) ->
    ensure_ok(inet:parse_address(binary_to_list(IP0))).

to_ip_port(IPAddress) ->
    ensure_ok(emqx_schema:to_ip_port(IPAddress)).

ensure_ok({ok, V}) ->
    V;
ensure_ok({error, _R} = E) ->
    throw(E).

b2i(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
b2i(Any) ->
    Any.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).

params2qs_test_() ->
    QSchema = [
        {<<"str">>, binary},
        {<<"int">>, integer},
        {<<"binatom">>, atom},
        {<<"atom">>, atom},
        {<<"ts">>, timestamp},
        {<<"gte_range">>, integer},
        {<<"lte_range">>, integer},
        {<<"like_fuzzy">>, binary},
        {<<"match_topic">>, binary},
        {<<"ip">>, ip},
        {<<"ip_port">>, ip_port}
    ],
    QString = [
        {<<"str">>, <<"abc">>},
        {<<"int">>, <<"123">>},
        {<<"binatom">>, <<"connected">>},
        {<<"atom">>, ok},
        {<<"ts">>, <<"156000">>},
        {<<"gte_range">>, <<"1">>},
        {<<"lte_range">>, <<"5">>},
        {<<"like_fuzzy">>, <<"user">>},
        {<<"match_topic">>, <<"t/#">>},
        {<<"ip">>, <<"127.0.0.1">>},
        {<<"ip_port">>, <<"127.0.0.1:8888">>}
    ],
    ExpectedQs = [
        {str, '=:=', <<"abc">>},
        {int, '=:=', 123},
        {binatom, '=:=', connected},
        {atom, '=:=', ok},
        {ts, '=:=', 156000},
        {range, '>=', 1, '=<', 5},
        {ip, '=:=', {127, 0, 0, 1}},
        {ip_port, '=:=', {{127, 0, 0, 1}, 8888}}
    ],
    FuzzyNQString = [
        {fuzzy, like, <<"user">>},
        {topic, match, <<"t/#">>}
    ],

    [
        ?_assertEqual({10, {ExpectedQs, FuzzyNQString}}, parse_qstring(QString, QSchema)),
        ?_assertEqual({0, {[], []}}, parse_qstring([{not_a_predefined_params, val}], QSchema)),
        ?_assertEqual(
            {1, {[{ip, '=:=', {0, 0, 0, 0, 0, 0, 0, 1}}], []}},
            parse_qstring([{<<"ip">>, <<"::1">>}], QSchema)
        ),
        ?_assertEqual(
            {1, {[{ip_port, '=:=', {{0, 0, 0, 0, 0, 0, 0, 1}, 8888}}], []}},
            parse_qstring([{<<"ip_port">>, <<"::1:8888">>}], QSchema)
        ),
        ?_assertThrow(
            {bad_value_type, {<<"ip">>, ip, <<"helloworld">>}},
            parse_qstring([{<<"ip">>, <<"helloworld">>}], QSchema)
        ),
        ?_assertThrow(
            {bad_value_type, {<<"ip_port">>, ip_port, <<"127.0.0.1">>}},
            parse_qstring([{<<"ip_port">>, <<"127.0.0.1">>}], QSchema)
        ),
        ?_assertThrow(
            {bad_value_type, {<<"ip_port">>, ip_port, <<"helloworld:abcd">>}},
            parse_qstring([{<<"ip_port">>, <<"helloworld:abcd">>}], QSchema)
        )
    ].

paginate_test_format(Row) ->
    Row.

paginate_test_() ->
    _ = ets:new(?MODULE, [named_table]),
    Size = 1000,
    MyLimit = 10,
    ets:insert(?MODULE, [{I, foo} || I <- lists:seq(1, Size)]),
    DefaultLimit = emqx_mgmt:default_row_limit(),
    NoParamsResult = paginate(?MODULE, #{}, {?MODULE, paginate_test_format}),
    PaginateResults = [
        paginate(
            ?MODULE, #{<<"page">> => I, <<"limit">> => MyLimit}, {?MODULE, paginate_test_format}
        )
     || I <- lists:seq(1, floor(Size / MyLimit))
    ],
    [
        ?_assertMatch(
            #{meta := #{count := Size, page := 1, limit := DefaultLimit}}, NoParamsResult
        ),
        ?_assertEqual(DefaultLimit, length(maps:get(data, NoParamsResult))),
        ?_assertEqual(
            #{data => [], meta => #{count => Size, limit => DefaultLimit, page => 100}},
            paginate(?MODULE, #{<<"page">> => <<"100">>}, {?MODULE, paginate_test_format})
        )
    ] ++ assert_paginate_results(PaginateResults, Size, MyLimit).

assert_paginate_results(Results, Size, Limit) ->
    AllData = lists:flatten([Data || #{data := Data} <- Results]),
    [
        begin
            Result = lists:nth(I, Results),
            [
                ?_assertMatch(#{meta := #{count := Size, limit := Limit, page := I}}, Result),
                ?_assertEqual(Limit, length(maps:get(data, Result)))
            ]
        end
     || I <- lists:seq(1, floor(Size / Limit))
    ] ++
        [
            ?_assertEqual(floor(Size / Limit), length(Results)),
            ?_assertEqual(Size, length(AllData)),
            ?_assertEqual(Size, sets:size(sets:from_list(AllData)))
        ].

accumulate_prop_test() ->
    ?assert(proper:quickcheck(accumulate_prop(), [{numtests, 1000}])).

accumulate_prop() ->
    ?FORALL(
        #{page := Page, limit := Limit, noderows := NodeRows},
        emqx_proper_types:fixedmap(#{
            page => page_t(),
            limit => limit_t(),
            noderows => noderows_t()
        }),
        begin
            {Status, QRows} = accumulate_page_rows(Page, Limit, NodeRows),
            {_Status, QRowsNext} = accumulate_page_rows(Page + 1, Limit, NodeRows),
            measure(
                #{
                    "Limit" => Limit,
                    "Page" => Page,
                    "NRows" => length(QRows),
                    "Complete" => emqx_utils_conv:int(Status == enough)
                },
                %% Verify page is non-empty if accumulation is complete.
                accumulate_assert_nonempty(Status, Limit, QRows) and
                    %% Verify rows across 2 consective pages form continuous sequence.
                    accumulate_assert_continuous(QRows ++ QRowsNext)
            )
        end
    ).

accumulate_page_rows(Page, Limit, NodeRows) ->
    QState = #{page => Page, limit => Limit},
    {Status, #{rows := QRowsAcc}} = lists:foldl(
        fun
            ({Node, Rows}, {more, QRAcc}) ->
                accumulate_query_rows(Node, Rows, QState, QRAcc);
            (_NodeRows, {enough, QRAcc}) ->
                {enough, QRAcc}
        end,
        {more, init_query_result()},
        NodeRows
    ),
    QRows = format_query_data(fun(N, R) -> {N, R} end, QRowsAcc, #{}),
    {Status, QRows}.

accumulate_assert_nonempty(enough, Limit, QRows) ->
    length(QRows) =:= Limit;
accumulate_assert_nonempty(more, _Limit, _QRows) ->
    true.

accumulate_assert_continuous([{N, R1} | Rest = [{N, R2} | _]]) ->
    (R2 - R1 =:= 1) andalso accumulate_assert_continuous(Rest);
accumulate_assert_continuous([{_N1, _} | Rest = [{_N2, R} | _]]) ->
    (R =:= 1) andalso accumulate_assert_continuous(Rest);
accumulate_assert_continuous([_]) ->
    true;
accumulate_assert_continuous([]) ->
    true.

page_t() ->
    pos_integer().

limit_t() ->
    emqx_proper_types:scaled(0.6, pos_integer()).

noderows_t() ->
    ?LET(
        {Nodes, PageSize},
        {pos_integer(), limit_t()},
        [{N, lists:seq(1, PageSize)} || N <- lists:seq(1, Nodes)]
    ).

measure(NamedSamples, Test) ->
    maps:fold(fun(Name, Sample, Acc) -> measure(Name, Sample, Acc) end, Test, NamedSamples).

-endif.
