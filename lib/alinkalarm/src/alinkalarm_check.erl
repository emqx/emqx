%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 11. 3月 2023 下午3:11
%%%-------------------------------------------------------------------
-module(alinkalarm_check).
-author("yqfclid").

%% API
-export([
    check_rule/3
]).

%%%===================================================================
%%% API
%%%===================================================================

check_rule(Stat, RuleId, #{<<"relation">> := <<"and">>, <<"conditions">> := Conditions, <<"children">> := Children}) ->
    CheckConditionsF =
        fun() ->
            lists:all(
                fun(Condition) ->
                    check_condition(Stat, RuleId, Condition)
                end, Conditions)
        end,
    CheckChildrenF =
        fun() ->
            lists:all(
                fun(Rule) ->
                    check_rule(Stat, RuleId, Rule)
                end, Children)
        end,
    CheckConditionsF() andalso CheckChildrenF();
check_rule(Stat, RuleId, #{<<"relation">> := <<"or">>, <<"conditions">> := Conditions, <<"children">> := Children}) ->
    CheckConditionsF =
        fun() ->
            lists:any(
                fun(Condition) ->
                    check_condition(Stat, RuleId, Condition)
                end, Conditions)
        end,
    CheckChildrenF =
        fun() ->
            lists:any(
                fun(Rule) ->
                    check_rule(Stat, RuleId, Rule)
                end, Children)
        end,
    CheckConditionsF() orelse CheckChildrenF().
%%%===================================================================
%%% Application callbacks
%%%===================================================================
check_condition(Stat0, RuleId, #{<<"name">> := Name, <<"operate">> := Operate, <<"value">> := Value} = Condition) ->
    #{
        <<"device_stat">> := #{<<"deviceAddr">> := Addr},
        <<"product_info">> := #{<<"thing">> := Thing0},
        <<"device_stat">> := #{<<"stat">> := Stat}} = Stat0,
    Thing = add_base_features(Thing0),
    case maps:get(Name, Thing, undefined) of
        #{<<"type">> := Type} = ThingConfig->
            case maps:get(Name, Stat, undefined) of
                undefined ->
                    false;
                #{<<"value">> := <<"alink_ignore_", _/binary>> = NameV} when Operate =:= <<"fail">> ->
                    case compare_value(Type, Operate, NameV, Value) of
                        true ->
                            ValueI = alinkutil_type:to_integer(Value),
                            case alinkalarm_cache:add_failed_count(Addr, RuleId) of
                                FailedCount when FailedCount >= ValueI ->
                                    WrongStatInfo =
                                        #{
                                            <<"name">> => Name,
                                            <<"thing_config">> => ThingConfig,
                                            <<"stat">> => #{<<"value">> => {<<"alink_ignore_sync_data_timeout">>, FailedCount}},
                                            <<"condition">> => Condition
                                        },
                                    add_wrong_info(RuleId, WrongStatInfo),
                                    true;
                                _ ->
                                    false
                            end;
                        false ->
                            false
                    end;
                #{<<"value">> := <<"alink_ignore_", _/binary>>} ->
                    erlang:put({alinkalarm, RuleId, ignore}, true),
                    false;
                #{<<"value">> := NameV} = NameStat when Operate =/= <<"fail">>->
                    case compare_value(Type, Operate, NameV, Value) of
                        true ->
                            WrongStatInfo =
                                #{
                                    <<"name">> => Name,
                                    <<"thing_config">> => ThingConfig,
                                    <<"stat">> => NameStat,
                                    <<"condition">> => Condition
                                },
                            add_wrong_info(RuleId, WrongStatInfo),
                            true;
                        false ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.


compare_value(_Type, <<"fail">>, <<"alink_ignore_sync_data_timeout">>, _Value) ->
    true;
compare_value(_Type, <<"fail">>, _NameV, _Value) ->
    false;
compare_value(_Type, _, <<"alink_ignore", _/binary>>, _Value) ->
    false;
compare_value(_Type, <<"==">>, NameV, Value) ->
    Value == alinkutil_type:to_binary(NameV);
compare_value(Type, <<">=">>, NameV, Value) ->
    NameV >= to_number(Type, Value);
compare_value(Type, <<"<=">>, NameV, Value) ->
    NameV =< to_number(Type, Value);
compare_value(Type, <<">">>, NameV, Value) ->
    NameV > to_number(Type, Value);
compare_value(Type, <<"<">>, NameV, Value) ->
    NameV < to_number(Type, Value);
compare_value(_Type, <<"regex">>, NameV, Value) ->
    case re:run(Value, NameV, [global, {capture, all, list}]) of
        nomatch ->
            false;
        {error, Reason} ->
            logger:error("regex error:~p ~p ~p", [NameV, Value, Reason]),
            false;
        _ ->
            true
    end.

to_number(<<"integer">>, V) ->
    alinkutil_type:to_integer(V);
to_number(<<"int">>, V) ->
    alinkutil_type:to_integer(V);
to_number(<<"float">>, V) when is_binary(V) ->
    case binary:match(V, <<".">>) of
        nomatch ->
            alinkutil_type:to_integer(V);
        _ ->
            alinkutil_type:to_float(V)
    end;
to_number(<<"string">>, V) ->
    alinkutil_type:to_binary(V);
to_number(_, V) ->
    V.


add_wrong_info(RuleId, WrongStatInfo) ->
    case erlang:get({alinkalarm, RuleId}) of
        WrongStatInfos when is_list(WrongStatInfos)->
            erlang:put({alinkalarm, RuleId}, [WrongStatInfo|WrongStatInfos]);
        _ ->
            erlang:put({alinkalarm, RuleId}, [WrongStatInfo])
    end.


add_base_features(Thing) ->
    Thing#{
        <<"addr">> => #{
            <<"title">> => <<"设备"/utf8>>,
            <<"name">> => <<"addr">>,
            <<"type">> => <<"string">>,
            <<"unit">> => <<>>
        },
        <<"productId">> => #{
            <<"title">> => <<"产品"/utf8>>,
            <<"name">> => <<"productId">>,
            <<"type">> => <<"string">>,
            <<"unit">> => <<>>
        },
        <<"status">> => #{
            <<"title">> => <<"设备状态"/utf8>>,
            <<"name">> => <<"status">>,
            <<"type">> => <<"string">>,
            <<"unit">> => <<>>
        }
    }.