%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 08. 8月 2023 下午4:21
%%%-------------------------------------------------------------------
-module(gnss_server).
-author("yqfclid").

-protocol(<<"GNSS">>).

-include("alinkcore.hrl").
%% API
-export([
    init/1,
    handle_in/3,
    handle_out/3,
    build_data/1
]).


%%%===================================================================
%%% API
%%%===================================================================
init([Addr, _ConnInfo]) ->
    case alinkcore_cache:query_children(Addr) of
        {ok, Children} ->
            Self = self(),
            lists:foreach(
                fun(#{<<"addr">> := ChildAddr, <<"subAddr">> := SubAddr}) ->
                    case alinkcore_cache:query_device(ChildAddr) of
                        {ok, #{<<"product">> := ProductId} = Map} ->
                            DevConfig = maps:get(<<"config">>, Map, []),
                            case alinkcore_cache:query_product(ProductId) of
                                {ok, #{<<"config">> := ProdConfig}} ->
                                    Config = maps:merge(DevConfig, ProdConfig),
                                    gnss_sub_device:start_link(Self, ProductId, ChildAddr, SubAddr, Config);
                                {error, Reason} ->
                                    logger:error("query ~p device failed ~p", [Addr, Reason])
                            end;
                        {error, Reason} ->
                            logger:error("query ~p device failed ~p", [Addr, Reason])
                    end
            end, Children);
        {error, Reason} ->
            logger:error("gnss query ~p children failed:~p", [Addr, Reason])
    end.


handle_in(ProductId, Addr, #{payload := <<_:3/binary, Payload/binary>>, topic := <<"$dp">>, from := From}) ->
    case jiffy:decode(Payload, [return_maps]) of
        #{From := RData} ->
            case build_data(RData) of
                {ok, Data} ->
                    alinkcore_data:handle(ProductId, Addr, Data);
                {error, Reason} ->
                    logger:error("build param ~p to data failed:~p", [RData, Reason])
            end;
        _ ->
            ok
    end;
handle_in(_, _, _) ->
    ok.



handle_out(_ProductId, _Addr, #{payload := _Payload} = Message) ->
    Message.

%%%===================================================================
%%% Internal functions
%%%===================================================================
build_data(Rdata) ->
    try
        Now = erlang:system_time(second),
        Data =
            maps:fold(
                fun(K, V, Acc) ->
                    [Type, Code, _] = binary:split(K, <<"_">>, [global]),
                    build_type_data(Type, Code, Now, V, Acc)
            end, #{<<"ts">> => Now}, Rdata),
        {ok, Data}
    catch
        E:R:ST ->
            logger:error("build gnss data error:~p ~p ~p", [E, R, ST]),
            {error, build_error}
    end.


build_type_data(<<"S1">>, <<"ZT">>, Time, V, Acc) ->
    maps:fold(
        fun(TypeK, TypeV, Acc1) ->
            case lists:member(TypeK, type_code_fields(<<"S1">>, <<"ZT">>)) of
                true ->
                    Acc1#{TypeK => #{<<"value">> => TypeV, <<"ts">> => Time}};
                _ ->
                    Acc1
            end
    end, Acc, V);
build_type_data(<<"L1">>, <<"LF">>, Time, V, Acc) ->
    Acc#{<<"l1_lf">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}};
build_type_data(<<"L1">>, <<"GP">>, Time, V, Acc) ->
    [GpsInitial, GpsTotalX, GpsTotalY, GpsTotalZ] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l1_gpsinitial">> => #{<<"value">> => alinkutil_type:to_float(GpsInitial), <<"ts">> => Time},
        <<"l1_gpstotalx">> => #{<<"value">> => alinkutil_type:to_float(GpsTotalX), <<"ts">> => Time},
        <<"l1_gpstotaly">> => #{<<"value">> => alinkutil_type:to_float(GpsTotalY), <<"ts">> => Time},
        <<"l1_gpstotalz">> => #{<<"value">> => alinkutil_type:to_float(GpsTotalZ), <<"ts">> => Time}
    };
build_type_data(<<"L1">>, <<"SW">>, Time, V, Acc) ->
    [DispsX, DispsY] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l1_dispsx">> => #{<<"value">> => alinkutil_type:to_float(DispsX), <<"ts">> => Time},
        <<"l1_dispsy">> => #{<<"value">> => alinkutil_type:to_float(DispsY), <<"ts">> => Time}
    };
build_type_data(<<"L1">>, <<"JS">>, Time, V, Acc) ->
    [GX, GY, GZ] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l1_gx">> => #{<<"value">> => alinkutil_type:to_float(GX), <<"ts">> => Time},
        <<"l1_gy">> => #{<<"value">> => alinkutil_type:to_float(GY), <<"ts">> => Time},
        <<"l1_gz">> => #{<<"value">> => alinkutil_type:to_float(GZ), <<"ts">> => Time}
    };
build_type_data(<<"L1">>, <<"QJ">>, Time, V, Acc) ->
    case binary:split(V, <<",">>, [global]) of
        [X, Y, Z, Angle, AZI] ->
            Acc#{
                <<"l1_x">> => #{<<"value">> => alinkutil_type:to_float(X), <<"ts">> => Time},
                <<"l1_y">> => #{<<"value">> => alinkutil_type:to_float(Y), <<"ts">> => Time},
                <<"l1_z">> => #{<<"value">> => alinkutil_type:to_float(Z), <<"ts">> => Time},
                <<"l1_angle">> => #{<<"value">> => alinkutil_type:to_float(Angle), <<"ts">> => Time},
                <<"l1_azi">> => #{<<"value">> => alinkutil_type:to_float(AZI), <<"ts">> => Time}
            };
        [PLX, PLY, PLZ, PLValue, SJX, SJY, SJZ, SJValue] ->
            Acc#{
                <<"l1_plx">> => #{<<"value">> => alinkutil_type:to_float(PLX), <<"ts">> => Time},
                <<"l1_ply">> => #{<<"value">> => alinkutil_type:to_float(PLY), <<"ts">> => Time},
                <<"l1_plz">> => #{<<"value">> => alinkutil_type:to_float(PLZ), <<"ts">> => Time},
                <<"l1_plvalue">> => #{<<"value">> => alinkutil_type:to_float(PLValue), <<"ts">> => Time},
                <<"l1_sjx">> => #{<<"value">> => alinkutil_type:to_float(SJX), <<"ts">> => Time},
                <<"l1_sjy">> => #{<<"value">> => alinkutil_type:to_float(SJY), <<"ts">> => Time},
                <<"l1_sjz">> => #{<<"value">> => alinkutil_type:to_float(SJZ), <<"ts">> => Time},
                <<"l1_sjvalue">> => #{<<"value">> => alinkutil_type:to_float(SJValue), <<"ts">> => Time}
            }
    end;

build_type_data(<<"L2">>, <<"YL">>, Time, V, Acc) ->
    Acc#{<<"l2_yl">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}};
build_type_data(<<"L2">>, <<"TY">>, Time, V, Acc) ->
    Acc#{<<"l2_ty">> => #{<<"value">> => V, <<"ts">> => Time}};
build_type_data(<<"L2">>, <<"CS">>, Time, V, Acc) ->
    [OSP, VSP, Freq] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l2_cs_osp">> => #{<<"value">> => alinkutil_type:to_float(OSP), <<"ts">> => Time},
        <<"l2_cs_vsp">> => #{<<"value">> => alinkutil_type:to_float(VSP), <<"ts">> => Time},
        <<"l2_cs_freq">> => #{<<"value">> => alinkutil_type:to_float(Freq), <<"ts">> => Time}
    };
build_type_data(<<"L2">>, <<"DS">>, Time, V, Acc) ->
    [OSP, VSP, Freq] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l2_ds_osp">> => #{<<"value">> => alinkutil_type:to_float(OSP), <<"ts">> => Time},
        <<"l2_ds_vsp">> => #{<<"value">> => alinkutil_type:to_float(VSP), <<"ts">> => Time},
        <<"l2_ds_freq">> => #{<<"value">> => alinkutil_type:to_float(Freq), <<"ts">> => Time}
    };


build_type_data(<<"L3">>, <<"YL">>, Time, V, Acc) ->
    [YL, Total] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l3_yl">> => #{<<"value">> => alinkutil_type:to_float(YL), <<"ts">> => Time},
        <<"l3_yl_total">> => #{<<"value">> => alinkutil_type:to_float(Total), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"QW">>, Time, V, Acc) ->
    Acc#{<<"l3_qw">> => #{<<"value">> => V, <<"ts">> => Time}};
build_type_data(<<"L3">>, <<"TW">>, Time, V, Acc) ->
    Acc#{
        <<"l3_tw">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"HS">>, Time, V, Acc) ->
    Acc#{
        <<"l3_hs">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"DB">>, Time, V, Acc) ->
    [Temp, Value] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l3_db_temp">> => #{<<"value">> => alinkutil_type:to_float(Temp), <<"ts">> => Time},
        <<"l3_db_value">> => #{<<"value">> => alinkutil_type:to_float(Value), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"DX">>, Time, V, Acc) ->
    [Temp, Value] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l3_dx_temp">> => #{<<"value">> => alinkutil_type:to_float(Temp), <<"ts">> => Time},
        <<"l3_dx_value">> => #{<<"value">> => alinkutil_type:to_float(Value), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"SY">>, Time, V, Acc) ->
    [Temp, Value] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l3_sy_temp">> => #{<<"value">> => alinkutil_type:to_float(Temp), <<"ts">> => Time},
        <<"l3_sy_value">> => #{<<"value">> => alinkutil_type:to_float(Value), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"ST">>, Time, V, Acc) ->
    Acc#{
        <<"l3_st">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"LS">>, Time, V, Acc) ->
    Acc#{
        <<"l3_ls">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"CJ">>, Time, V, Acc) ->
    Acc#{
        <<"l3_cj">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };
build_type_data(<<"L3">>, <<"QY">>, Time, V, Acc) ->
    Acc#{
        <<"l3_qj">> => #{<<"value">> => alinkutil_type:to_float(V), <<"ts">> => Time}
    };



build_type_data(<<"L4">>, <<"SP">>, Time, V, Acc) ->
    Acc#{<<"l4_sp">> => #{<<"value">> => V, <<"ts">> => Time}};
build_type_data(<<"L4">>, <<"NW">>, Time, V, Acc) ->
    Acc#{<<"l4_ty">> => #{<<"value">> => V, <<"ts">> => Time}};
build_type_data(<<"L4">>, <<"LD">>, Time, V, Acc) ->
    [X, Y, Z, Speed] = binary:split(V, <<",">>, [global]),
    Acc#{
        <<"l4_ld_x">> => #{<<"value">> => alinkutil_type:to_float(X), <<"ts">> => Time},
        <<"l4_ld_y">> => #{<<"value">> => alinkutil_type:to_float(Y), <<"ts">> => Time},
        <<"l4_ld_z">> => #{<<"value">> => alinkutil_type:to_float(Z), <<"ts">> => Time},
        <<"l4_ld_speed">> => #{<<"value">> => alinkutil_type:to_float(Speed), <<"ts">> => Time}
    };
build_type_data(<<"L4">>, <<"LB">>, Time, V, Acc) ->
    Acc#{<<"yl">> => #{<<"value">> => V, <<"ts">> => Time}};

build_type_data(_, _, _, _, Acc) ->
    Acc.



type_code_fields(<<"S1">>, <<"ZT">>) ->
    [
        <<"ext_power_volt">>,
        <<"solar_volt">>,
        <<"battery_dump_energy">>,
        <<"temp">>,
        <<"humidity">>,
        <<"lon">>,
        <<"lat">>,
        <<"on_4g">>,
        <<"signal_4g">>,
        <<"signal_NB">>,
        <<"signal_bd">>,
        <<"sw_version">>
    ].