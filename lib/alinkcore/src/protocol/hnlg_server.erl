%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%        河南理工大学mqtt设备协议接入
%%% @end
%%% Created : 21. 9月 2023 下午7:54
%%%-------------------------------------------------------------------
-module(hnlg_server).
-author("yqfclid").

%% API
-export([
    init/1,
    handle_in/3,
    handle_out/3,
    handle_close/3
]).

-export([
    test_send/2
]).

-protocol(<<"hnlg-protocol">>).

-include("alinkcore.hrl").

%%%===================================================================
%%% API
%%%===================================================================
init([_Addr, _ConnInfo]) ->
    erlang:put(alinkcore_hnlg_sub_sessions, []),
    ok.


handle_in(_ProductId, Addr, #{payload := Payload, topic := <<"d/out/", Addr/binary>>}) ->
    Data =  jiffy:decode(Payload, [return_maps]),
    ReplyData = handle_data(Addr, Data),
    reply(Addr, ReplyData);
handle_in(_, _, _) ->
    ok.



handle_out(_ProductId, Addr, #{payload := Payload} = Message) ->
    case jiffy:decode(Payload, [return_maps]) of
        #{<<"name">> := <<"interval_time">>, <<"value">> := Value} ->
            Id = get_seq(),
            put(interval_time, Value),
            NPayload = jiffy:encode(#{<<"id">> => Id, <<"cmd">> => 4, <<"address">> => 0, <<"deviceSet">> => [Value]}),
            insert_cache(Id, NPayload),
            emqx_message:from_map(Message#{payload => NPayload, topic => <<"d/in/", Addr/binary>>});
        #{<<"cmd">> := 5} ->
            Id = get_seq(),
            NPayload = read_device_cmd(Id),
            insert_cache(Id, NPayload),
            emqx_message:from_map(Message#{payload => NPayload, topic => <<"d/in/", Addr/binary>>});
        #{<<"cmd">> := 6} ->
            Id = get_seq(),
            NPayload = set_device_cmd(Id),
            insert_cache(Id, NPayload),
            emqx_message:from_map(Message#{payload => NPayload, topic => <<"d/in/", Addr/binary>>});
        #{<<"cmd">> := 20} ->
            Id = get_seq(),
            NPayload = clear_cache_cmd(Id),
            insert_cache(Id, NPayload),
            emqx_message:from_map(Message#{payload => NPayload, topic => <<"d/in/", Addr/binary>>});
        _ ->
            emqx_message:from_map(Message)
    end.


handle_close(_ProductId, Addr, _Session) ->
    {ok, Chilren} = alinkcore_cache:query_children(Addr),
    lists:foreach(
        fun(#{<<"addr">> := A}) ->
            alinkcore_cache:delete(session, A)
    end, Chilren),
    erlang:erase(alinkcore_hnlg_sub_sessions),
    erlang:erase(alinkcore_hnlg_data_cache),
    erlang:erase(alinkcore_hnlg_gateway_config_data),
    ok.


handle_data(Addr, #{<<"id">> := Id, <<"cmd">> := 2, <<"timesTamp">> := Ts, <<"dataArray">> := EquDataArray}) ->
    deal_gateway_data_array(Addr, EquDataArray, Ts),
    jiffy:encode(#{<<"id">> => Id, <<"address">> => 0, <<"state">> => 1, <<"cmd">> => 2});
handle_data(Addr, #{<<"id">> := Id, <<"address">> := Address, <<"cmd">> := 3, <<"timesTamp">> := Ts, <<"offline_flag">> := OfflineFlag, <<"dataArray">> := SubDataArray}) ->
    SubSessions = erlang:get(alinkcore_hnlg_sub_sessions),
    Children =
        case alinkcore_cache:query_children(Addr) of
            {ok, Cs} ->
                Cs;
            {error, Reason} ->
                logger:error("query ~p children failed:~p", [Addr, Reason]),
                []
        end,
    case lists:keyfind(Address, 1, SubSessions) of
        false when OfflineFlag =:= 1 ->
            ok;
        {SubAddress, SubAddr, SubSession} when OfflineFlag =:= 1 ->
            alinkcore_cache:delete_object(session, SubAddr, SubSession),
            del_dict_sesison({SubAddress, SubAddr, SubSession});
        false ->
            case get_sub_addr(Address, Children) of
                {ok, SubAddr} ->
                    SubSession = #session{node = node(), pid = self(), connect_time = Ts},
                    add_dict_sesison({Address, SubAddr, SubSession}),
                    alinkcore_cache:save(session, SubAddr, SubSession),
                    deal_data_array(SubAddr, SubDataArray, Ts);
                false ->
                    ok
            end;
        {SubAddress, SubAddr, SubSession}  ->
            case get_sub_addr(SubAddress, Children) of
                {ok, SubAddr} ->
                    deal_data_array(SubAddr, SubDataArray, Ts),
                    ok;
                {ok, AnotherSubAddr} ->
                    alinkcore_cache:delete_object(session, SubAddr, SubSession),
                    del_dict_sesison({SubAddress, SubAddr, SubSession}),
                    add_dict_sesison({Address, AnotherSubAddr, SubSession}),
                    alinkcore_cache:save(session, AnotherSubAddr, SubSession),
                    deal_data_array(AnotherSubAddr, SubDataArray, Ts)
            end

    end,
    jiffy:encode(#{<<"id">> => Id, <<"address">> => Address, <<"state">> => 1, <<"cmd">> => 3});
handle_data(Addr, #{<<"IntervalTime">> := IntervalTime, <<"address">> := 0, <<"id">> := Id, <<"cmd">> := 1}) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            set_config_data(<<"interval_time">>, IntervalTime),
            Ts = erlang:system_time(second),
            Data =
                #{
                    <<"ts">> => Ts,
                    <<"interval_time">> => #{
                        <<"value">> => IntervalTime,
                        <<"ts">> => Ts
                    }
                },
            Topic01 = <<"p/out/", Addr/binary>>,
            emqx:publish(emqx_message:make(Topic01, jiffy:encode(Data))),
            ProductIdB = alinkutil_type:to_binary(ProductId),
            Topic = <<"p/thing/", ProductIdB/binary, "/", Addr/binary>>,
            emqx:publish(emqx_message:make(Topic, jiffy:encode(Data)));
        {error, Reason} ->
            logger:error("query ~p product failed:~p", [Addr, Reason])
    end,
    jiffy:encode(#{<<"id">> => Id, <<"address">> => 0, <<"state">> => 1, <<"cmd">> => 1});
handle_data(Addr, #{<<"id">> := _Id, <<"cmd">> :=4, <<"address">> := 0, <<"state">> := 1}) ->
    Ts = erlang:system_time(second),
    IntervalTime = get(interval_time),
    case IntervalTime =/= undefined of
        false ->
            ignore;
        true ->
            Data =
                #{
                    <<"ts">> => Ts,
                    <<"interval_time">> => #{
                        <<"value">> => IntervalTime,
                        <<"ts">> => Ts
                    }
                },
            Topic01 = <<"p/out/", Addr/binary>>,
            emqx:publish(emqx_message:make(Topic01, jiffy:encode(Data)))
    end;
handle_data(_Addr, #{<<"id">> := _Id, <<"address">> := 0, <<"state">> := 1}) ->
    ignore;
handle_data(_Addr, #{<<"id">> := _Id, <<"cmd">> := 6, <<"state">> := 1}) ->
    ignore;
handle_data(_Addr, #{<<"id">> := _Id, <<"cmd">> := 4, <<"state">> := 1}) ->
    ignore;
handle_data(Addr, #{<<"id">> := _Id, <<"cmd">> := 5, <<"dataArray">> := DataArray, <<"state">> := 1}) ->
    Device485 = maps:get(<<"RS485">>, DataArray, []),
    deal_device_stat(Addr, Device485);
handle_data(_Addr, #{<<"id">> := _Id, <<"cmd">> := 20, <<"state">> := 1}) ->
    ignore;
handle_data(_Addr, #{<<"id">> := Id, <<"address">> := 0, <<"state">> := 0}) ->
    retry(Id).
%%%===================================================================
%%% Internal functions
%%%===================================================================
test_send(Addr, Data) ->
    emqx:publish(emqx_message:make(<<"p/in/", Addr/binary>>, jiffy:encode(Data))).


clear_cache_cmd(SeqId) ->
    jiffy:encode(#{
        <<"id">> => SeqId,
        <<"cmd">> => 20,
        <<"address">> => 0,
        <<"data">> => 1
    }).


read_device_cmd(SeqId) ->
    jiffy:encode(#{
        <<"id">> => SeqId,
        <<"cmd">> => 5,
        <<"address">> => 0
    }).


set_device_cmd(SeqId) ->
    jiffy:encode(#{
        <<"id">> => SeqId,
        <<"cmd">> => 6,
        <<"address">> => 0,
        <<"dataArray">> => #{
            <<"RS485">> => []
        }
    }).

deal_device_stat(Addr, Device485s) when is_list(Device485s)->
    lists:foreach(
        fun(Device485) ->
            #{
                <<"BaudRate">> := BaudRate,
                <<"address">> := StartInt,
                <<"Feature">> := Feature,
                <<"startReg">> := StartReg,
                <<"numReg">> := NumReg
            } = Device485,
            UpdateConfig = #{
                <<"BaudRate">> => BaudRate,
                <<"Feature">> => Feature,
                <<"startReg">> => StartReg,
                <<"numReg">> => NumReg
            },
            case StartInt of
                0 ->
                    case alinkdata:query_mysql_format_map(default, <<"select config from sys_device where addr = '", Addr/binary, "'">>) of
                        {ok, [#{<<"id">> := DeviceId, <<"config">> := Config}]} when is_binary(Config) ->
                            ConfigMap =
                                case Config of
                                    <<>> ->
                                        [];
                                    _ ->
                                        jiffy:decode(Config, [return_maps])
                                end,
                            NewConfigMap =
                                maps:fold(
                                    fun(K, V, Acc) ->
                                        insert_into_config_map(Acc, K, V, [])
                                end, ConfigMap, UpdateConfig),
                            ok = alinkdata_dao:query_no_count('PUT_device', #{<<"id">> => DeviceId, <<"config">> => jiffy:encode(NewConfigMap)}),
                            alinkcore_cache:delete(device, Addr);
                        _ ->
                            ignore
                    end;
                _ ->
                    case alinkcore_cache:query_children(Addr) of
                        {ok, Children} ->
                            case get_device_from_children(<<StartInt>>, Children) of
                                {ok, #{<<"addr">> := ChildAddr}} ->
                                    case alinkdata:query_mysql_format_map(default, <<"select config from sys_device where addr = '", ChildAddr/binary, "'">>) of
                                        {ok, [#{<<"id">> := DeviceId, <<"config">> := Config}]} when is_binary(Config) ->
                                            ConfigMap =
                                                case Config of
                                                    <<>> ->
                                                        [];
                                                    _ ->
                                                        jiffy:decode(Config, [return_maps])
                                                end,
                                            NewConfigMap =
                                                maps:fold(
                                                    fun(K, V, Acc) ->
                                                        insert_into_config_map(Acc, K, V, [])
                                                    end, ConfigMap, UpdateConfig),
                                            ok = alinkdata_dao:query_no_count('PUT_device', #{<<"id">> => DeviceId, <<"config">> => jiffy:encode(NewConfigMap)}),
                                            alinkcore_cache:delete(device, Addr);
                                        _ ->
                                            ignore
                                    end;
                                _ ->
                                    ignore
                            end;
                        {error, Reason} ->
                            logger:error("~p error ~p", [Addr, Reason])
                    end
            end
    end, Device485s);
deal_device_stat(Addr, Device485s)->
    logger:error("wrong data ~p ~p", [Addr, Device485s]).



get_device_from_children(_SubAddr, []) ->
    not_found;
get_device_from_children(SubAddr, [#{<<"subAddr">> := SubAddrHex} = N|T]) ->
    case binary:decode_hex(SubAddrHex) of
        SubAddr ->
            {ok, N};
        _ ->
            get_device_from_children(SubAddr, T)
    end.

insert_into_config_map([], _K, _V, Return) ->
    Return;
insert_into_config_map([#{<<"name">> := K} = OldKV|T], K, V, Return) ->
    insert_into_config_map(T, K, V, [OldKV#{<<"name">> => K, <<"value">> => V, <<"type">> => <<"int">>}|Return]);
insert_into_config_map([OldKV|T], K, V, Return) ->
    insert_into_config_map(T, K, V, [OldKV|Return]).



reply(_Addr, ignore) ->
    ok;
reply(Addr, ReturnData) ->
    Msg = emqx_message:make(<<"ALINKIOT_SYSTEM">>, <<"d/in/", Addr/binary>>, ReturnData),
    emqx:publish(Msg).




del_dict_sesison(SubSess) ->
    SubSessions = erlang:get(alinkcore_hnlg_sub_sessions),
    NSubSessions =
        lists:foldl(
            fun(SubS, Acc) ->
                case SubS =:= SubSess of
                    true ->
                        Acc;
                    _ ->
                        [SubS|Acc]
                end
        end, [], SubSessions),
    erlang:put(alinkcore_hnlg_sub_sessions, NSubSessions).


add_dict_sesison(SubSess) ->
    SubSessions = erlang:get(alinkcore_hnlg_sub_sessions),
    erlang:put(alinkcore_hnlg_sub_sessions, [SubSess|SubSessions]).


get_sub_addr(_Address, []) ->
    false;
get_sub_addr(Address, [#{<<"subAddr">> := SubAddress, <<"addr">> := Addr}|T]) ->
    case binary:decode_hex(SubAddress) =:= <<Address>> of
        true ->
            {ok, Addr};
        false ->
            get_sub_addr(Address, T)
    end.


deal_gateway_data_array(Addr, DataArray, Ts) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            {Data, _} =
                lists:foldl(
                    fun(D, {Acc, Seq}) ->
                        NSeq = Seq + 1,
                        NSeqB = alinkutil_type:to_binary(NSeq),
                        Name = <<"d", NSeqB/binary>>,
                        NAcc = Acc#{
                            Name => #{
                                <<"value">> => D,
                                <<"ts">> => Ts
                            }
                        },
                        {NAcc, NSeq}
                end, {#{<<"ts">> => Ts}, 0}, DataArray),
            alinkcore_data:handle(ProductId, Addr, append_config_data(Data, Ts));
        {error, Reason} ->
            logger:error("query device ~p failed:~p", [Addr, Reason])
    end.


deal_data_array(Addr, DataArray, Ts) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            case alinkcore_cache:query_product(ProductId) of
                {ok, #{<<"thing">> := Thing}} ->
                    Data = build_data_from_data_array(DataArray, Thing, Ts),
                    alinkcore_data:handle(ProductId, Addr, Data);
                {error, Reason} ->
                    logger:error("query product ~p failed ~p", [ProductId, Reason])
            end;
        {error, Reason} ->
            logger:error("query device ~p failed:~p", [Addr, Reason])
    end.


retry(Id) ->
    case erlang:get(alinkcore_hnlg_data_cache) of
        undefined ->
            ignore;
        DataCache ->
            case proplists:get_value(Id, DataCache) of
                undefined ->
                    ignore;
                Data ->
                    Data
            end
    end.


get_seq() ->
    case erlang:get(alinkcore_hnlg_data_seq) of
        undefined ->
            erlang:put(alinkcore_hnlg_data_seq, 1),
            1;
        OldSeq ->
            erlang:put(alinkcore_hnlg_data_seq, OldSeq + 1),
            OldSeq + 1
    end.


insert_cache(Id, Data) ->
    case erlang:get(alinkcore_hnlg_data_cache) of
        undefined ->
            erlang:put(alinkcore_hnlg_data_cache, [{Id, Data}]);
        DataCache ->
            NDataCache = [{Id, Data}|DataCache],
            case length(NDataCache) > 3 of
                true ->
                    [D1, D2, D3|_] = NDataCache,
                    erlang:put(alinkcore_hnlg_data_cache, [D1, D2, D3]);
                false ->
                    erlang:put(alinkcore_hnlg_data_cache, NDataCache)
            end
    end.

set_config_data(K, V) ->
    case erlang:erase(alinkcore_hnlg_gateway_config_data) of
        undefined ->
            erlang:put(alinkcore_hnlg_gateway_config_data, #{K => V});
        ConfigData ->
            erlang:put(alinkcore_hnlg_gateway_config_data, ConfigData#{K => V})
    end.


append_config_data(Acc, Ts) ->
    case erlang:get(alinkcore_hnlg_gateway_config_data) of
        undefined ->
            Acc;
        ConfigData ->
            maps:fold(
                fun(K, V, Acc1) ->
                    Acc1#{
                        K => #{
                            <<"value">> => V,
                            <<"ts">> => Ts
                        }
                    }
            end, Acc, ConfigData)
    end.


build_data_from_data_array(DataArray, Thing, Ts) ->
    DataBin = binary:decode_hex(DataArray),
    lists:foldl(
        fun(#{
            <<"name">> := Name,
            <<"access">> := Access,
            <<"type">> := Type,
            <<"modbus">> := #{
                <<"start">> := StartB,
                <<"quantity">> := Quantity,
                <<"signed">> := Signed,
                <<"endianness">> := Endianness}}, Acc) when Access =:= <<"read">> orelse Access =:= <<"rw">> ->
            Start = binary_to_integer(StartB, 16),
            DBin = binary:part(DataBin, Start, Quantity),
            FormatType = format_type(Type, byte_size(DBin), Signed, Endianness),
            D = format_zone(DBin, FormatType),
            Acc#{
                Name =>#{
                    <<"value">> => D,
                    <<"ts">> => Ts
                }
            };
           (_, Acc) ->
            Acc
     end, #{<<"ts">> => Ts}, Thing).




format_zone(<<V:64/big-signed-float>>, <<"64/big-signed-float">>) -> V;
format_zone(<<V:64/little-signed-float>>, <<"64/little-signed-float">>) -> V;
format_zone(<<V:64/big-signed-integer>>, <<"64/big-signed-integer">>) -> V;
format_zone(<<V:64/little-signed-integer>>, <<"64/little-signed-integer">>) -> V;
format_zone(<<V:64/big-unsigned-float>>, <<"64/big-unsigned-float">>) -> V;
format_zone(<<V:64/little-unsigned-float>>, <<"64/little-unsigned-float">>) -> V;
format_zone(<<V:64/big-unsigned-integer>>, <<"64/big-unsigned-integer">>) -> V;
format_zone(<<V:64/little-unsigned-integer>>, <<"64/little-unsigned-integer">>) -> V;

format_zone(<<V:32/big-signed-float>>, <<"32/big-signed-float">>) -> V;
format_zone(<<V:32/little-signed-float>>, <<"32/little-signed-float">>) -> V;
format_zone(<<V:32/big-signed-integer>>, <<"32/big-signed-integer">>) -> V;
format_zone(<<V:32/little-signed-integer>>, <<"32/little-signed-integer">>) -> V;
format_zone(<<V:32/big-unsigned-float>>, <<"32/big-unsigned-float">>) -> V;
format_zone(<<V:32/little-unsigned-float>>, <<"32/little-unsigned-float">>) -> V;
format_zone(<<V:32/big-unsigned-integer>>, <<"32/big-unsigned-integer">>) -> V;
format_zone(<<V:32/little-unsigned-integer>>, <<"32/little-unsigned-integer">>) -> V;

format_zone(<<V:16/big-signed-integer>>, <<"16/big-signed-integer">>) -> V;
format_zone(<<V:16/little-signed-integer>>, <<"16/little-signed-integer">>) -> V;
format_zone(<<V:16/big-signed-float>>, <<"16/big-signed-float">>) -> V;
format_zone(<<V:16/little-signed-float>>, <<"16/little-signed-float">>) -> V;
format_zone(<<V:16/big-unsigned-integer>>, <<"16/big-unsigned-integer">>) -> V;
format_zone(<<V:16/little-unsigned-integer>>, <<"16/little-unsigned-integer">>) -> V;
format_zone(<<V:16/big-unsigned-float>>, <<"16/big-unsigned-float">>) -> V;
format_zone(<<V:16/little-unsigned-float>>, <<"16/little-unsigned-float">>) -> V;

format_zone(<<V:8/signed-integer>>, <<"8/signed-integer">>) -> V;
format_zone(<<V:8/unsigned-integer>>, <<"8/unsigned-integer">>) -> V;
format_zone(<<V:8/signed-integer>>, <<"8/signed-float">>) -> V;
format_zone(<<V:8/unsigned-integer>>, <<"8/unsigned-float">>) -> V;
format_zone(Value, _) -> binary:encode_hex(Value).


format_type(<<"string">>, _, _, _) ->
    <<"string">>;
format_type(Type, Length, Signed, Endianness) ->
    Len = Length * 8,
    case Len > 8 of
        true ->
            L = binary_to_list(<<"/", Endianness/binary, "-", Signed/binary, "-", Type/binary>>),
            list_to_binary(lists:concat([Len, L]));
        false ->
            L = binary_to_list(<<"/", Signed/binary, "-", Type/binary>>),
            list_to_binary(lists:concat([Len, L]))
    end.