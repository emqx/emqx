%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 06. 3月 2023 下午1:54
%%%-------------------------------------------------------------------
-module(alinkalarm_product_rules).

-behaviour(gen_server).


-export([
    load_rules/0,
    load_rules/1,
    get_rule_ids/1,
    update_rules_by_product/1,
    update_rules_by_id/1
]).

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-define(RULE_TAB, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
load_rules() ->
    gen_server:call(?SERVER, {load_rules, all}).

load_rules(ProductId) ->
    gen_server:call(?SERVER, {local_rules, ProductId}).

get_rule_ids(ProductId) ->
    case ets:lookup(?RULE_TAB, ProductId) of
        RawRules when is_list(RawRules) ->
            lists:map(fun({_, RawRule}) -> RawRule end, RawRules);
        {error, Reason} ->
            {error, Reason}
    end.


update_rules_by_product(ProductId) ->
    gen_server:cast(?SERVER, {update_rules_by_product, ProductId}).


update_rules_by_id(RuleId) ->
    gen_server:cast(?SERVER, {update_rules, RuleId}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?RULE_TAB, [bag, public, named_table, {read_concurrency, true}]),
    do_load_rules(all),
    {ok, #state{}}.

handle_call({load_rules, all}, _From, State) ->
    do_load_rules(all),
    {reply, ok, State};
handle_call({load_rules, ProductId}, _From, State) ->
    do_load_rules(ProductId),
    {reply, ok, State};
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.


handle_cast({update_rules_by_product, ProductId}, State) ->
    do_load_rules(ProductId),
    {noreply, State};
handle_cast({update_rules_by_id, RuleId}, State) ->
    ProductIds = lists:flatten(ets:match(?RULE_TAB, {'$1', RuleId})),
    lists:foreach(fun do_load_rules/1, ProductIds),
    {noreply, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_load_rules(ProductId) ->
    case catch do_load_rules_1(ProductId) of
        {'EXIT', Reason} ->
            logger:error("load rule ~p get unexcepted error:~p", [ProductId, Reason]);
        _ ->
            ok
    end.

do_load_rules_1(all) ->
    case alinkdata_dao:query_no_count('QUERY_rule', #{}) of
        {ok, Rules} ->
            ets:delete_all_objects(?RULE_TAB),
            lists:foreach(
                fun(#{<<"product">> := ProductId, <<"id">> := RuleId}) ->
                    ets:insert(?RULE_TAB, {ProductId, RuleId})
                end, Rules);
        {error, Reason} ->
            logger:error("load product rules failed ~p", [Reason])
    end;
do_load_rules_1(ProductId) ->
    case alinkdata_dao:query_no_count('QUERY_rule', #{<<"product">> => ProductId}) of
        {ok, Rules} ->
            ets:delete(?RULE_TAB, product),
            lists:foreach(
                fun(#{<<"id">> := RuleId}) ->
                    ets:insert(?RULE_TAB, {ProductId, RuleId})
                end, Rules);
        {error, Reason} ->
            logger:error("load product rule with product Id ~p failed:~p", [ProductId, Reason])
    end.




