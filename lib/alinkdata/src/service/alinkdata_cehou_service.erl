%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 12月 2023 上午11:05
%%%-------------------------------------------------------------------
-module(alinkdata_cehou_service).
-author("yqfclid").

%% API
-export([
    analyze/4
]).

%%%===================================================================
%%% API
%%%===================================================================
analyze(_OperationID, #{<<"addr">> := Addr} = Args, _Context, Req) ->
    InitCeHou = get_config_init_value(Addr),
    Res =
        case alinkiot_tdengine:do_query_history(Args#{<<"orderBy[desc]">> => <<"createTime">>}) of
            {ok, []} ->
                Ret = #{<<"prediction">> => [], <<"last_cehou">> => 0, <<"init_cehou">> => InitCeHou},
                alinkdata_ajax_result:success_result(Ret);
            {ok, [#{<<"houdu">> := LastHoudu}|_] = History} ->
                Ret = #{<<"prediction">> => prediction(History), <<"last_cehou">> => LastHoudu, <<"init_cehou">> => InitCeHou},
                alinkdata_ajax_result:success_result(Ret);
            {error, Reason} ->
                logger:error("query cehou analyze failed ~p ~p", [Addr, Reason]),
                alinkdata_ajax_result:error_result(<<"Interrnal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_config_init_value(Addr) ->
    case alinkdata_device_config:get_config(Addr) of
        Configs when is_list(Configs) ->
            lists:foldl(
                fun(#{<<"name">> := Name, <<"value">> := Value}, Acc) ->
                    case Name =:= <<"init_cehou">> of
                        true ->
                            Value;
                        _ ->
                            Acc
                    end
            end, 0, Configs);
        Err ->
            logger:error("query device config ~p error ~p", [Addr, Err]),
            0
    end.

prediction([]) ->
    [];
prediction([#{<<"createtime">> := Time1} = A1, #{<<"createtime">> := Time2} = A2, A3, A4|_]) ->
    Source =
        lists:map(
            fun(#{<<"createtime">> := Time, <<"houdu">> := Houdu}) ->
                {Time, Houdu}
        end, [A1, A2, A3, A4]),
    Prediction = do_prediction(Source),
    NextTime = alinkdata_wechat:timestamp2localtime_str(Time1 + Time1 - Time2),
    [#{<<"createtime">> => NextTime, <<"houdu">> => Prediction}];
prediction(History) ->
    Source =
        lists:map(
            fun(#{<<"houdu">> := Houdu}) ->
                Houdu
            end, History),
    Prediction = do_prediction(Source),
    NextTime =
        case History of
            [#{<<"createtime">> := Time1}, #{<<"createtime">> := Time2}|_] ->
                alinkdata_wechat:timestamp2localtime_str(Time1 + Time1 - Time2);
            [#{<<"createtime">> := Time1}] ->
                alinkdata_wechat:timestamp2localtime_str(Time1 + 30 * 60)
        end,
    [#{<<"createtime">> => NextTime, <<"houdu">> => Prediction}].

do_prediction(Source) ->
    NSource =
        lists:map(
            fun(D) when is_integer(D) orelse is_float(D)->
                D;
                (_) ->
                    0
            end, Source),
    do_prediction_1(NSource).


do_prediction_1([A1]) ->
    A1;
do_prediction_1([A1|RemainSource]) ->
    A1 * 0.6 + (lists:sum(RemainSource) / length(RemainSource)) * 0.4.