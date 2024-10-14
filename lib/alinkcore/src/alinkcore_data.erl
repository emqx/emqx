-module(alinkcore_data).

%% API
-export([handle/3, format_data/4]).


handle(ProductId, Addr, OrigData) ->
    case alinkcore_cache:query_product(ProductId) of
        {error, Reason} ->
            {error, Reason};
        {ok, #{<<"thing">> := Things} = ProductInfo} ->
            case alinkcore_cache:query_device(Addr) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Device} ->
                    Data0 = format_data(Things, OrigData, Device, ProductInfo),
                    Data = alinkformula:execute(ProductInfo, Device, Data0),
                    publish_data_by_mqtt(Device, ProductId, Addr, Data),
                    alinkdata_hooks:run('data.publish', [ProductId, Addr, Data]),
                    run_scene_hook(ProductId, Addr, Data)
            end
    end.

run_scene_hook(ProductId, Addr, Data) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"id">> := DeviceId}} ->
            case alinkcore_cache:query_device_scene(DeviceId) of
                {ok, Scenes} ->
                    lists:foreach(
                        fun(#{<<"scene">> := SceneId}) ->
                            alinkdata_hooks:run('data.scene_publish', [SceneId, ProductId, Addr, Data])
                        end, Scenes);
                _ ->
                    ok
            end;
        _ ->
            skip
    end.

publish_data_by_mqtt(#{<<"id">> := DeviceId}, ProductId, Addr, Data) ->
    TransFun =
        fun(D) ->
            maps:fold(
                fun(K, V, Acc) ->
                    case V of
                        #{<<"value">> := <<"alink_ignore", _/binary>>} ->
                            Acc;
                        _ ->
                            Acc#{K => V}
                    end
            end, #{}, D)
        end,
    case TransFun(Data) of
        #{<<"ts">> := _} = NData ->
            case maps:size(NData) of
                Size when Size > 1 ->
                    ProductIdB = alinkutil_type:to_binary(ProductId),
                    Topic0 = <<"p/thing/", ProductIdB/binary, "/", Addr/binary>>,
                    emqx:publish(emqx_message:make(Topic0, jiffy:encode(Data))),
                    Topic01 = <<"p/out/", Addr/binary>>,
                    emqx:publish(emqx_message:make(Topic01, jiffy:encode(Data))),
                    case alinkcore_cache:query_device(Addr) of
                        {ok, #{<<"project">> := Project}} when Project =/= 0 ->
                            ProjectB = alinkutil_type:to_binary(Project),
                            case alinkcore_cache:query_device_scene(DeviceId) of
                                {ok, Scenes} ->
                                    lists:foreach(
                                        fun(#{<<"scene">> := Scene}) ->
                                            SceneB = alinkutil_type:to_binary(Scene),
                                            Topic1 = <<"s/out/", ProjectB/binary, "/", SceneB/binary, "/", Addr/binary>>,
                                            Msg1 =
                                                #{
                                                    <<"msgType">> => <<"deviceData">>,
                                                    <<"data">> => #{
                                                        <<"addr">> => Addr,
                                                        <<"time">> => maps:get(<<"ts">>, Data, erlang:system_time(second)),
                                                        <<"data">> => Data
                                                    }
                                                },
                                            emqx:publish(emqx_message:make(Topic1, jiffy:encode(Msg1)))
                                    end, Scenes);
                                _ ->
                                    ok
                            end;
                        _ ->
                            skip
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

format_data(Things, OrigData, Device, ProductInfo) ->
    Time =
        case maps:get(<<"ts">>, OrigData, <<>>) of
            T when is_integer(T) ->
                T;
            _ ->
                os:system_time(second)
        end,
    Handles = [
        fun format_by_thing/5,
        fun format_precision/5
    ],
    exec_handle(Things, OrigData, Device, ProductInfo, Handles, #{<<"ts">> => Time}).

exec_handle(_Things, _OrigData, _Device, _ProductInfo, [], Acc) -> Acc;
exec_handle(Things, OrigData, Device, ProductInfo, [Fun | OtherFun], Acc) ->
    Acc1 =
        lists:foldl(
            fun(Thing, Accc) ->
                Fun(Thing, OrigData, Device, ProductInfo, Accc)
            end, Acc, Things),
    exec_handle(Things, OrigData, Device, ProductInfo, OtherFun, Acc1).

format_by_thing(#{<<"name">> := Name} = Thing, OrigData, _Device, _ProductInfo, Acc) ->
    case maps:get(Name, OrigData, undefined) of
        undefined -> Acc;
        #{<<"value">> := <<"alink_ignore", _/binary>>} = Data ->
            Acc#{
                Name => Data
            };
        #{<<"value">> := Value} = Data ->
            Format = create_format(Thing),
            Acc#{
                Name => Data#{
                    <<"value">> => Format(Value)
                }
            }
    end.


format_precision(#{ <<"name">> := Name, <<"precision">> := Precision }, _OrigData, _, _, Acc)
    when is_integer(Precision) ->
    case maps:get(Name, Acc, <<>>) of
        #{<<"value">> := <<"alink_ignore", _/binary>>} = Data ->
            Acc#{
                Name => Data
            };
        #{<<"value">> := V} = Data when is_float(V)->
            Acc#{
                Name => Data#{
                    <<"value">> => float(V, Precision)
                }
            };
        _ ->
            Acc
    end;
format_precision(_, _, _, _, Acc) ->
    Acc.

create_format(#{<<"type">> := <<"string">>}) ->
    fun(Value) -> Value end;
create_format(Thing) ->
    Precision = maps:get(<<"precision">>, Thing, undefined),
    Rate = maps:get(<<"rate">>, Thing, 1),
    Offset = maps:get(<<"offset">>, Thing, 0),
    fun(Value) ->
        Value1 = Value * Rate + Offset,
        float(Value1, Precision)
    end.

float(Value, undefined) -> Value;
float(Value, Precision) when not is_integer(Precision) -> Value;
float(Value, 0) when is_integer(Value) -> Value;
float(Value, Precision) ->
    N = math:pow(10, Precision),
    round(Value * N) / N.

