%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(alinkalarm_cache).

-behaviour(gen_server).


-export([
    clear/2,
    clear_failed_count/2,
    add_failed_count/2,
    get_product/1,
    get_device/1,
    get_rule/1,
    trigger/3,
    recover/2
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


-record(state, {}).
%%%===================================================================
%%% API
%%%===================================================================
clear(Table, Id) ->
    gen_server:cast(?SERVER, {clear, Table, Id}).


clear_failed_count(Addr, RuleId) ->
    ets:delete(?SERVER, {operate_failed, Addr, RuleId}).

add_failed_count(Addr, RuleId) ->
    ets:update_counter(?MODULE, {operate_failed, Addr, RuleId}, {2, 1}, {{operate_failed, Addr, RuleId}, 0}).

trigger(Addr, RuleId, TriggerExpire) ->
    Now = erlang:system_time(second),
    case ets:lookup(?SERVER, {trigger_rule, RuleId, Addr}) of
        [{_, Expire}] when Now > Expire ->
            ets:insert(?SERVER, {{trigger_rule, RuleId, Addr}, Now + TriggerExpire}),
            {ok, trigger};
        [] ->
            ets:insert(?SERVER, {{trigger_rule, RuleId, Addr}, Now + TriggerExpire}),
            {ok, trigger};
        [_] ->
            {error, not_expired};
        {error, Reason} ->
            {error, Reason}
    end.

recover(Addr, RuleId) ->
    case ets:lookup(?SERVER, {trigger_rule, RuleId, Addr}) of
        [_] ->
            ets:delete(?SERVER, {trigger_rule, RuleId, Addr}),
            {ok, recover};
        [] ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.


get_product(RProductId) ->
    ProductId = alinkutil_type:to_integer(RProductId),
    case ets:lookup(?SERVER, {product, ProductId}) of
        [{_, #{<<"status">> := <<"0">>,
               <<"thing">> := Thing} = ProductInfo}] when is_map(Thing) ->
            ProductInfo;
        [{_, _ProductInfo}] ->
            undefined;
        [] ->
            case alinkdata_dao:query('QUERY_product', #{<<"id">> => ProductId}) of
                {ok, [ProductInfo]} ->
                    Thing = maps:get(<<"thing">>, ProductInfo, <<>>),
                    {NProductInfo, Return} =
                        case (maps:get(<<"status">>, ProductInfo, <<"1">>) =:= <<"0">>)
                            andalso (Thing =/= <<>>) of
                            true ->
                                ThingMap = thing2kv(Thing),
                                {ProductInfo#{<<"thing">> => ThingMap},
                                    ProductInfo#{<<"thing">> => ThingMap}};
                            _ ->
                                {ProductInfo, undefined}
                        end,
                    ets:insert(?SERVER, {{product, ProductId}, NProductInfo}),
                    Return;
                {ok, []} ->
                    undefined;
                {error, Reason} ->
                    logger:error("query product get error ~p", [Reason]),
                    undefined
            end
    end.


get_device(RDeviceAddr) ->
    DeviceAddr = alinkutil_type:to_binary(RDeviceAddr),
    case ets:lookup(?SERVER, {device, DeviceAddr}) of
        [{_, #{<<"status">> := <<"0">>} = DeviceInfo}] ->
            DeviceInfo;
        [{_, _ProductInfo}] ->
            undefined;
        [] ->
            case alinkdata_dao:query('QUERY_device', #{<<"addr">> => DeviceAddr}) of
                {ok, [DeviceInfo]} ->
                    ets:insert(?SERVER, {{device, DeviceAddr}, DeviceInfo}),
                    case maps:get(<<"status">>, DeviceInfo, <<"1">>) of
                        <<"0">> ->
                            DeviceInfo;
                        _ ->
                            undefined
                    end;
                {ok, []} ->
                    undefined;
                {error, Reason} ->
                    logger:error("query DeviceInfo get error ~p", [Reason]),
                    undefined
            end
    end.



get_rule(RRuleId) ->
    RuleId = alinkutil_type:to_integer(RRuleId),
    case ets:lookup(?SERVER, {rule, RuleId}) of
        [{_, Rule}] ->
            Rule;
        [] ->
            case alinkdata_dao:query('QUERY_rule', #{<<"id">> => RuleId}) of
                {ok, [Rule]} ->
                    RuleDetail = maps:get(<<"rule">>, Rule, <<>>),
                    RuleDetailMap =
                        case lists:member(RuleDetail, [undefined, null, <<>>]) of
                            true ->
                                RuleDetail;
                            false ->
                                jiffy:decode(RuleDetail, [return_maps])
                        end,
                    ets:insert(?SERVER, {{rule, RuleId}, Rule#{<<"rule">> => RuleDetailMap}}),
                    Rule#{<<"rule">> => RuleDetailMap};
                {ok, []} ->
                    undefined;
                {error, Reason} ->
                    logger:error("query rule get error ~p", [Reason]),
                    undefined
            end
    end.
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?MODULE, [set, public, named_table, {read_concurrency, true}]),
    {ok, #state{}}.


handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast({clear, Table, Id}, State) ->
    ets:delete(?SERVER, {Table, Id}),
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
%%% Internal functions``
%%%===================================================================
thing2kv(null) ->
    #{};
thing2kv(<<>>) ->
    #{};
thing2kv(Thing) ->
    lists:foldl(
        fun(#{<<"name">> := Name} = M, Acc) ->
            Acc#{Name => M}
    end, #{}, jiffy:decode(Thing, [return_maps])).