%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(alinkalarm_handler).

-behaviour(gen_server).

-export([
    handle/1,
    just_check_alarm/1
]).

-export([
    start_link/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {pool, id}).
%%%===================================================================
%%% API
%%%===================================================================
handle(MsgStat) ->
    gen_server:cast(pick(), {check_alarm, MsgStat}).

just_check_alarm(MsgStat) ->
    gen_server:cast(pick(), {just_check_alarm, MsgStat}).

%% Pick a broker
pick() ->
    gproc_pool:pick_worker(alinkalarm_handler_pool).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Pool, Id) ->
    gen_server:start_link({local, list_to_atom(lists:concat([?MODULE, "_", Id]))}, ?MODULE, [Pool, Id], []).

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.


handle_cast({check_alarm, MsgStat}, State) ->
    erlang:erase(alink_msg_type),
    erlang:put(alink_msg_type, data),
    case catch do_check_alarm(MsgStat) of
        {'EXIT', Reason} ->
            logger:error("check alarm with device stat ~p failed: ~p", [MsgStat, Reason]);
        _ ->
            ok
    end,
    erlang:erase(alink_msg_type),
    {noreply, State};
handle_cast({just_check_alarm, MsgStat}, State) ->
    erlang:erase(alink_msg_type),
    erlang:put(alink_msg_type, notify),
    case catch do_check_alarm(MsgStat) of
        {'EXIT', Reason} ->
            logger:error("check alarm with device stat ~p failed: ~p", [MsgStat, Reason]);
        _ ->
            ok
    end,
    erlang:erase(alink_msg_type),
    {noreply, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{pool = Pool, id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}),
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_check_alarm(MsgStat) ->
    #{<<"deviceAddr">> := DeviceAddr, <<"productId">> := ProductId, <<"stat">> := Data} = MsgStat,
    DeviceInfo = alinkalarm_cache:get_device(DeviceAddr),
    ProductInfo = alinkalarm_cache:get_product(ProductId),
    case lists:member(undefined, [DeviceInfo, ProductInfo]) of
        true ->
            maybe_publish_to_db(ProductId, DeviceAddr, Data),
            logger:debug("check alarm find DeviceInfo ~p PorductInfo", [DeviceInfo, ProductInfo]);
        false ->
            get_and_check_rules(DeviceInfo, ProductInfo, MsgStat)
    end.


get_and_check_rules(DeviceInfo, #{<<"id">> := ProductId} = ProductInfo, DeviceStat) ->
    RuleIds = alinkalarm_product_rules:get_rule_ids(ProductId),
    Stat =
        #{
            <<"device_info">> => DeviceInfo,
            <<"product_info">> => ProductInfo,
            <<"device_stat">> => DeviceStat
        },
    #{
        <<"stat">> := Data,
        <<"deviceAddr">> := DeviceAddr,
        <<"productId">> := PId
    } = DeviceStat,
    case RuleIds of
        [] ->
            maybe_publish_to_db(PId, DeviceAddr, Data);
        _ ->
            AlarmRuleIdList =
                lists:foldl(
                    fun(RuleId, Acc) ->
                        case alinkalarm_cache:get_rule(RuleId) of
                            #{
                                <<"rule">> := Rule,
                                <<"id">> := RuleId,
                                <<"groupId">> := GroupId,
                                <<"notifyType">> := NotifyType} = RuleInfo when is_map(Rule) ->
                                case do_check_rule(Stat, RuleId, GroupId, Rule, RuleInfo, NotifyType) of
                                    true ->
                                        [RuleId|Acc];
                                    _ ->
                                        Acc
                                end;
                            _ ->
                                logger:debug("rule may not enable: ~p", [RuleId]),
                                Acc
                        end
                    end, [], RuleIds),
            NData =
                case AlarmRuleIdList of
                    [] ->
                        Data;
                    _ ->
                        AlarmRule =
                            lists:foldl(
                                fun(I, <<>>) ->
                                    IB = alinkutil_type:to_binary(I),
                                    IB;
                                   (I, Acc) ->
                                    IB = alinkutil_type:to_binary(I),
                                    <<Acc/binary, ",", IB/binary>>
                            end, <<>>, AlarmRuleIdList),
                        Data#{<<"alarm_rule">> => AlarmRule}
                end,
            maybe_publish_to_db(PId, DeviceAddr, NData)
    end.


do_check_rule(Stat, RuleId, GroupId, Rule, RuleInfo, NotifyType) ->
    #{<<"device_stat">> := #{<<"deviceAddr">> := Addr}} = Stat,
    erlang:erase({alinkalarm, RuleId}),
    erlang:erase({alinkalarm, RuleId, ignore}),
    ThisMsgType = erlang:get(alink_msg_type),
    TriggerExpire = application:get_env(alinkalarm, trigger_expire, 300),
    CheckRules = alinkalarm_check:check_rule(Stat, RuleId, Rule),
    WrongInfos = erlang:get({alinkalarm, RuleId}),
    IsIgnore = erlang:get({alinkalarm, RuleId, ignore}),
    erlang:erase({alinkalarm, RuleId}),
    erlang:erase({alinkalarm, RuleId, ignore}),
    case CheckRules of
        true ->
            case alinkalarm_cache:trigger(Addr, RuleId, TriggerExpire) of
                        {ok, trigger} ->
                            alinkalarm_alert:alert(Stat#{<<"groupId">> => GroupId}, WrongInfos, RuleInfo, NotifyType);
                        {error, not_expired} ->
                            ok;
                        {error, Reason} ->
                            logger:error("trigger cached with ruleid ~p failed:~p", [RuleId, Reason])
                    end,
                    true;
        false when IsIgnore =:= true orelse ThisMsgType =:= notify ->
            false;
        _ ->
            case alinkalarm_cache:recover(Addr, RuleId) of
                {ok, recover} ->
                    alinkalarm_alert:recover(Stat#{<<"groupId">> => GroupId}, RuleInfo, NotifyType);
                {error, not_found} ->
                    ok;
                {error, Reason} ->
                    logger:error("recover cached with ruleid ~p failed:~p", [RuleId, Reason])
            end,
            false
    end.



maybe_publish_to_db(PId, DeviceAddr, Data) ->
    case erlang:get(alink_msg_type) of
        notify ->
            ok;
        _ ->
            alinkiot_tdengine_subscribe:data_publish(PId, DeviceAddr, Data)
    end.
