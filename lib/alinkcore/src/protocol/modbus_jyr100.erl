%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%      神奇的modbus协议，静位水准仪，勾八设备不知道为什么不用标准的
%%% @end
%%% Created : 10. 6月 2023 上午12:30
%%%-------------------------------------------------------------------
-module(modbus_jyr100).


-protocol(<<"Modbus-JY-R100">>).
-include("alinkcore.hrl").
%% API
-behavior(gen_server).
-export([kick/1, start_sub_device/1, crc16/1]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2, code_change/3]).


-record(state, { addr, productId, tasks}).

-record(sub_device_state, {addr, product_id, tasks, sub_addr = <<>>, gateway, session}).

-record(device_info, {addr, product_id, sub_addr = <<>>}).

-define(TOPIC(Addr), <<"p/in/", Addr/binary>>).


kick(Addr) ->
    case alinkcore_cache:lookup(session, Addr) of
        {ok, #session{node = Node}} when Node =/= node() ->
            {error, {other_node, Node}};
        {ok, #session{pid = Pid}} ->
            gen_server:cast(Pid, kick);
        _ ->
            {error, not_found}
    end.


start_sub_device(#{
    <<"subAddr">> := SubDevAddr,
    <<"addr">> := Addr
}) ->
    Pid = self(),
    gen_server:start_monitor(?MODULE, [Pid, Addr, SubDevAddr], []).

%%作为子设备进程调用
init([GatewayPid, Addr, SubDevAddr]) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, Device} ->
            ProductId = maps:get(<<"product">>, Device),
            case alinkcore_cache:query_product(ProductId) of
                {ok, Product} ->
                    Tasks = create_task(Product),
%%                    self() ! init_device,
                    create_timer(Tasks),
                    Session = #session{node = node(), pid = self(), connect_time = erlang:system_time(second)},
                    alinkcore_cache:save(session, Addr, Session),
                    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"login">>}]),
                    {ok, #sub_device_state{
                        addr = Addr,
                        product_id = ProductId,
                        tasks = Tasks,
                        sub_addr = SubDevAddr,
                        gateway = GatewayPid,
                        session = Session
                    }};
                {error, Reason} ->
                    logger:error("find product ~p error", [ProductId, Reason]),
                    {error, product_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end;


%%作为网关进程调用
init([Addr, TcpState]) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, Device} ->
%%            emqx:subscribe(?TOPIC(Addr), #{ qos => 0 }),
            ProductId = maps:get(<<"product">>, Device),
            case alinkcore_cache:query_product(ProductId) of
                {ok, Product} ->
                    Tasks = create_task(Product),
%%                    self() ! init_device,
                    create_timer(Tasks),
                    Session = #session{node = node(), pid = self(), connect_time = erlang:system_time(second)},
                    alinkcore_cache:save(session, Addr, Session),
                    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"login">>}]),
                    {ok, TcpState#tcp_state{state = #state{
                        addr = Addr,
                        productId = ProductId,
                        tasks = Tasks
                    }, session = Session}};
                {error, Reason} ->
                    logger:error("find product ~p error", [ProductId, Reason]),
                    {error, product_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%作为子设备调用
handle_info(init_device, State) when is_record(State, sub_device_state)->
    #sub_device_state{
        sub_addr = SubDevAddr,
        addr = Addr,
        product_id = ProductId,
        gateway = GatewayPid
    } = State,
    DeviceInfo =
        #device_info{
            addr = Addr,
            sub_addr = SubDevAddr,
            product_id = ProductId
        },
    GatewayPid ! {help_init_device, DeviceInfo},
    {noreply, State};


%%作为网关进程调用
handle_info({help_init_device, DeviceInfo}, TcpState) when is_record(TcpState, tcp_state)->
    #device_info{
        addr = Addr,
        product_id = ProductId,
        sub_addr = SubAddr
    } = DeviceInfo,
    case init_jyr100_device(SubAddr, Addr, ProductId, TcpState) of
        ok ->
            {noreply, TcpState};
        error ->
            {stop, normal, TcpState}
    end;
handle_info(init_device, TcpState) when is_record(TcpState, tcp_state)->
    #state{
        addr = Addr,
        productId = ProductId
    } = TcpState#tcp_state.state,
    case init_jyr100_device(<<>>, Addr, ProductId, TcpState) of
        ok ->
            {noreply, TcpState};
        error ->
            {stop, normal, TcpState}
    end;

%%作为网关进程调用
handle_info({help_run_tasks, DeviceInfo, TaskThings}, TcpState) ->
    #device_info{addr = Addr, product_id = ProductId} = DeviceInfo,
    case run_task(DeviceInfo, TaskThings, TcpState, #{}) of
        {error, timeout} ->
            {noreply, TcpState};
        {error, Reason} ->
            {stop, Reason, TcpState};
        {Acc, NewTcpState} ->
            case maps:size(Acc) == 0 of
                true -> ok;
                false -> alinkcore_data:handle(ProductId, Addr, Acc)
            end,
            {noreply, NewTcpState}
    end;

%%作为网关进程调用
handle_info({help_write, DeviceInfo, Message}, TcpState) ->
    help_handle_message(Message, DeviceInfo, TcpState),
    {noreply, TcpState};

%%作为网关进程调用
handle_info({task, [Time | Times]}, #tcp_state{ state =#state{ tasks = Tasks} } = TcpState) ->
    #state{
        addr = Addr,
        productId = ProductId
    } = TcpState#tcp_state.state,
    I = lists:last([Time | Times]),
    case Time > I of
        true ->
            erlang:send_after((Time - I) * 1000, self(), {task, Times ++ [Time]});
        false ->
            erlang:send_after(Time * 1000, self(), {task, Times ++ [Time]})
    end,
    Things = maps:get(I, Tasks),
    DeviceInfo =
        #device_info{
            addr = Addr,
            product_id = ProductId,
            sub_addr = <<>>
        },
    case run_task(DeviceInfo, Things, TcpState, #{}) of
        {error, timeout} ->
            {noreply, TcpState};
        {error, Reason} ->
            {stop, Reason, TcpState};
        {Acc, NewTcpState} ->
            case maps:size(Acc) == 0 of
                true -> ok;
                false ->
                    alinkcore_data:handle(ProductId, Addr, Acc)
            end,
            {noreply, NewTcpState}
    end;

%%作为子设备进程调用
handle_info({task, [Time | Times]}, #sub_device_state{ tasks = Tasks,
    sub_addr = SubDevAddr,
    addr = Addr,
    product_id = ProductId,
    gateway = GatewayPid} = State) ->
    I = lists:last([Time | Times]),
    case Time > I of
        true ->
            erlang:send_after((Time - I) * 1000, self(), {task, Times ++ [Time]});
        false ->
            erlang:send_after(Time * 1000, self(), {task, Times ++ [Time]})
    end,
    TaskThings = maps:get(I, Tasks),
    DeviceInfo = #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr},
    GatewayPid ! {help_run_tasks, DeviceInfo, TaskThings},
    {noreply, State};
%%作为网关进程调用
handle_info({tcp_closed, Reason}, TcpState) ->
    {stop, Reason, TcpState};

%%作为网关进程调用
handle_info({deliver, ?TOPIC(Addr), Packet}, #tcp_state{ state = #state{ addr = Addr } } = TcpState) ->
    Payload = Packet#message.payload,
    Message = jiffy:decode(Payload, [return_maps]),
    {noreply, handle_message(Message, TcpState)};

%%作为子设备进程调用
handle_info({deliver, ?TOPIC(Addr), Packet}, #sub_device_state{ addr = Addr,
    product_id = ProductId,
    sub_addr = SubDevAddr,
    gateway = GatewayPid} = State) ->
    Payload = Packet#message.payload,
    Message = jiffy:decode(Payload, [return_maps]),
    DeviceInfo = #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr},
    GatewayPid ! {help_write, DeviceInfo, Message},
    {noreply, State};

handle_info(stop, State) ->
    {stop, normal, State};


handle_info({'DOWN', _Ref, _Msg, _PID, _Reason}, State) ->
    {stop, normal, State};

handle_info(_Info, TcpState) ->
    alinkcore_logger:log(warning, "handle_info ~p~n", [_Info]),
    {noreply, TcpState}.


handle_call(kick, _From, TcpState) ->
    {stop, kick, TcpState};
handle_call(_Msg, _From, TcpState) ->
    {reply, ok, TcpState}.

%%作为网关进程调用
handle_cast(kick, #tcp_state{addr = Addr} = TcpState) ->
    logger:info("kick Addr ~p offline", [Addr]),
    {stop, normal, TcpState};
handle_cast(_Msg, TcpState) ->
    {noreply, TcpState}.

%%作为网关进程调用
terminate(Reason, #tcp_state{ state = #state{ addr = Addr, productId = ProductId},
                            children = Children, ip = Ip,
                            session = Session}) ->
    lists:foreach(fun stop_child/1, Children),
    alinkcore_cache:delete_object(session, Addr, Session),
    alinkcore_device_log:disconnected(Addr, ProductId, Reason, Ip),
    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"logout">>}]),
    ok;
%%作为子设备进程调用
terminate(Reason, #sub_device_state{ addr = Addr, product_id = ProductId, session = Session}) ->
    alinkcore_cache:delete_object(session, Addr, Session),
    alinkcore_device_log:disconnected(Addr, ProductId, Reason, <<>>),
    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"logout">>}]),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @doc 写数据
handle_message(#{
    <<"type">> := <<"write">>,
    <<"name">> := Name,
    <<"value">> := Value
} = Message, #tcp_state{state = #state{productId = ProductId, addr = Addr}} = TcpState) ->
    case get_modbus_by_name(Name, ProductId) of
        {error, Reason} ->
            logger:error("handle error, ~p, ~p", [Message, Reason]),
            TcpState;
        {ok, Thing} ->
            STing =
                case Name of
                    <<"x0">> ->
                        Thing#{<<"type">> => <<"write">>, <<"value">> => Value, <<"init">> => true};
                    _ ->
                        Thing#{<<"type">> => <<"write">>, <<"value">> => Value}
                end,
            case send_to_device(STing, TcpState) of
                {ok, Zone} ->
                    logger:debug("recv ~p write response value ~p", [Addr, Zone]);
                {error, Reason} ->
                    logger:error("handle error, ~p, ~p", [Message, Reason])
            end,
            TcpState
    end.


%% @doc 帮助子设备写数据
help_handle_message(#{
    <<"type">> := <<"write">>,
    <<"name">> := Name,
    <<"value">> := Value
} = Message, DeviceInfo, TcpState) ->
    #device_info{product_id = ProductId} = DeviceInfo,
    case get_modbus_by_name(Name, ProductId) of
        {error, Reason} ->
            logger:error("handle error, ~p, ~p", [Message, Reason]);
        {ok, Thing} ->
            STing =
                case Name of
                    <<"x0">> ->
                        Thing#{<<"type">> => <<"write">>, <<"value">> => Value, <<"init">> => true};
                    _ ->
                        Thing#{<<"type">> => <<"write">>, <<"value">> => Value}
                end,
            case send_to_device(DeviceInfo, STing, TcpState) of
                {ok, Zone} ->
                    logger:debug("handle_message ~p~n", [Zone]);
                {error, Reason} ->
                    logger:error("handle error, ~p, ~p", [Message, Reason])
            end
    end.

%% @doc 任务读数据
run_task(_DeviceInfo, [], TcpState, Acc) -> {Acc, TcpState};
run_task(DeviceInfo, [Thing | Things], TcpState, Acc) ->
    case send_to_device(DeviceInfo, Thing#{ <<"type">> => <<"read">> }, TcpState) of
%%        {ok, Zone} ->
%%            Type = format_type(byte_size(Zone), Thing),
%%            Name = maps:get(<<"name">>, Thing),
%%            NAcc =
%%                case format_zone(Zone, Type) of
%%                    <<"FFFF">> ->
%%                        Acc;
%%                    Value ->
%%                        Acc#{ Name => #{
%%                            <<"value">> => Value,
%%                            <<"ts">> => os:system_time(second)
%%                        }}
%%                end,
%%            run_task(DeviceInfo, Things, TcpState, NAcc);
        {ok, ZonesBin} ->
            Now = os:system_time(second),
            {_, NAcc} =
                lists:foldl(
                    fun(Zone, {Seq, Acc1}) ->
                        Type = format_type(byte_size(Zone), Thing),
                        Num = ((Seq - 1) div 3) + 1,
                        NameNum = integer_to_binary(Num),
                        NamePrefix =
                            case Seq rem 3 of
                                0 ->
                                    <<"z">>;
                                1 ->
                                    <<"x">>;
                                2 ->
                                    <<"y">>
                            end,
                        Name = <<NamePrefix/binary, NameNum/binary>>,
                        NAcc =
                            case format_zone(Zone, Type) of
                                <<"FFFF">> ->
                                    Acc;
                                Value ->
                                    Acc1#{ Name => #{
                                        <<"value">> => Value,
                                        <<"ts">> => Now
                                    }}
                            end,
                        {Seq + 1, NAcc}
                end, {1, Acc}, zones_from_zonesbin(ZonesBin)),
            run_task(DeviceInfo, Things, TcpState, NAcc);
        {error, Reason} ->
            {error, Reason}
    end.


zones_from_zonesbin(ZoneBin) ->
    zones_from_zonesbin(ZoneBin, []).


zones_from_zonesbin(<<>>, Acc) ->
    Acc;
zones_from_zonesbin(ZoneBin, Acc) when byte_size(ZoneBin) < 4 ->
    Acc ++ [ZoneBin];
zones_from_zonesbin(<<Zone:4/binary, Tail/binary>>, Acc)->
    zones_from_zonesbin(Tail, Acc ++ [Zone]).



send_to_device(Thing, TcpState) ->
    #tcp_state{state = #state{addr = Addr, productId = ProductId}} = TcpState,
    DeviceInfo = #device_info{addr = Addr, product_id = ProductId, sub_addr = <<>>},
    send_to_device(DeviceInfo, Thing, TcpState).



send_to_device(DeviceInfo, #{<<"type">> := <<"write">>, <<"init">> := true} = Thing, #tcp_state{ip = Ip} = TcpState) ->
    #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr} = DeviceInfo,
    Payload = encode_frame(SubDevAddr, Thing),
    alinkcore_device_log:down_data(Addr, ProductId, Payload, Ip, hex),
    FilterFun =
        fun(<<"ping\\r\\n">>) ->
            self() ! {tcp, <<"ping\\r\\n">>},
            continue;
            (<<"ping\\r\\n", RecvData/binary>>) ->
                self() ! {tcp, <<"ping\\r\\n">>},
                case RecvData =:= Payload of
                    true ->
                        continue;
                    false ->
                        {break, RecvData}
                end;
            (RecvData) ->
                case RecvData =:= Payload of
                    true ->
                        alinkcore_device_log:up_data(Addr, ProductId, RecvData, Ip, hex),
                        continue;
                    false ->
                        break
                end
        end,
    case alinkcore_tcp:sync_send(TcpState, Payload, 10000, FilterFun) of
        {ok, Data} ->
            alinkcore_device_log:up_data(Addr, ProductId, Data, Ip, hex),
            decode_frame(SubDevAddr, Data, Thing);
        {error, Reason} ->
            {error, Reason}
    end;
send_to_device(DeviceInfo, Thing,  #tcp_state{ip = Ip} = TcpState) ->
    #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr} = DeviceInfo,
    Payload = encode_frame(SubDevAddr, Thing),
    alinkcore_device_log:down_data(Addr, ProductId, Payload, Ip, hex),
    FilterFun =
        fun(<<"ping\\r\\n">>) ->
            self() ! {tcp, <<"ping\\r\\n">>},
            continue;
            (<<"ping\\r\\n", RecvData/binary>>) ->
                self() ! {tcp, <<"ping\\r\\n">>},
                {break, RecvData};
            (_RecvData) ->
                break
        end,
    case alinkcore_tcp:sync_send(TcpState, Payload, 10000, FilterFun) of
        {ok, Data} ->
            alinkcore_device_log:up_data(Addr, ProductId, Data, Ip, hex),
            decode_frame(SubDevAddr, Data, Thing);
        {error, Reason} ->
            {error, Reason}
    end.


decode_frame(SubDevAddr, <<SlaveId:8, _/binary>> = Bin, Thing) ->
    Data = binary:part(Bin, 0, byte_size(Bin) - 2),
    case crc16(Data) == binary:part(Bin, byte_size(Bin) - 2, 2) of
        true ->
            ConfigSlaveId =
                case lists:member(SubDevAddr, [null, undefined, <<>>]) of
                    true ->
                        maps:get(<<"slaveId">>, Thing, <<"01">>);
                    _ ->
                        SubDevAddr
                end,
            case binary_to_integer(ConfigSlaveId, 16) of
                SlaveId ->
                    case Data of
                        <<SlaveId:8, _Code:8, Len:16, Zone:Len/bytes>> ->
                            {ok, Zone};
                        <<SlaveId:8, _Code:8, Zone/binary>> ->
                            {ok, Zone}
                    end;
                _ ->
                    {error, id_error}
            end;
        false ->
            {error, crc_error}
    end.


encode_frame(SubDevAddr, #{ <<"type">> := <<"read">>, <<"rFunCode">> := FunCode,
    <<"quantity">> := Quantity } = Thing) ->
    encode_frame(SubDevAddr, FunCode, Quantity, Thing);

encode_frame(SubDevAddr, #{ <<"type">> := <<"write">>,
    <<"wFunCode">> := FunCode, <<"value">> := Value } = Thing) ->
    encode_frame(SubDevAddr, FunCode, Value, Thing).

encode_frame(SubDevAddr, HxFunCode, Zoom, #{<<"start">> := HxStart} = Thing) ->
    HxSlave =
        case lists:member(SubDevAddr, [undefined, <<>>, null]) of
            true ->
                maps:get(<<"slaveId">>, Thing, <<"01">>);
            _ ->
                SubDevAddr
        end,
    Slave = binary_to_integer(HxSlave, 16),
    Start = binary_to_integer(HxStart, 16),
    FunCode = binary_to_integer(HxFunCode, 16),
    Bin = <<Slave:8, FunCode:8, Start:16, Zoom:8>>,
    CRC = crc16(Bin), <<Bin/binary, CRC/binary>>.


get_modbus_by_name(Name, ProductId) ->
    case alinkcore_cache:query_product(ProductId) of
        {error, Reason} ->
            {error, Reason};
        {ok, #{ <<"thing">> := Things }} ->
            case get_thing_by_name(Name, Things) of
                undefined ->
                    {error, notfound};
                #{ <<"modbus">> := Modbus } ->
                    {ok, Modbus}
            end
    end.

get_thing_by_name(_Name, []) -> undefined;
get_thing_by_name(Name, [Thing | Things]) ->
    case maps:get(<<"name">>, Thing) == Name andalso
        lists:member(maps:get(<<"access">>, Thing), [<<"rw">>, <<"write">>]) of
        true -> Thing;
        false -> get_thing_by_name(Name, Things)
    end.


format_type(Length, Thing) ->
    case maps:get(<<"type">>, Thing, <<"string">>) of
        <<"string">> ->
            <<"string">>;
        Type ->
            Signed = maps:get(<<"signed">>, Thing, <<"signed">>),
            Endianness = maps:get(<<"endianness">>, Thing, <<"little">>),
            Len = Length * 8,
            case Len > 8 of
                true ->
                    L = binary_to_list(<<"/", Endianness/binary, "-", Signed/binary, "-", Type/binary>>),
                    list_to_binary(lists:concat([Len, L]));
                false ->
                    L = binary_to_list(<<"/", Signed/binary, "-", Type/binary>>),
                    list_to_binary(lists:concat([Len, L]))
            end
    end.

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


crc16(Buff) -> crc16(Buff, 16#FFFF).
crc16(<<>>, Crc) ->
    <<A:8, B:8>> = <<Crc:16>>,
    <<B:8, A:8>>;
crc16(<<B:8, Other/binary>>, Crc) ->
    NewCrc =
        lists:foldl(
            fun(_, Acc) ->
                Odd = Acc band 16#0001,
                New = Acc bsr 1,
                case Odd of
                    1 ->
                        New bxor 16#A001;
                    0 ->
                        New
                end
            end, Crc bxor B, lists:seq(1, 8)),
    crc16(Other, NewCrc).

create_task(#{<<"thing">> := [ThingG|_]}) ->
    lists:foldl(
        fun(Thing, Acc) ->
            create_task_by_thing(Thing, Acc)
        end, #{}, [ThingG]);
create_task(#{<<"thing">> := []}) ->
    #{}.


create_task_by_thing(#{<<"name">> := Name, <<"type">> := Type, <<"modbus">> := Opts} = Thing, Acc) ->
    case maps:get(<<"access">>, Thing, <<"read">>) of
        <<"write">> ->
            Acc;
        _ ->
            Freq = case maps:get(<<"freq">>, Opts, 0) of
                       0 -> 30;
                       V -> V
                   end,
            Tasks = maps:get(Freq, Acc, []),
            Acc#{
                Freq => [Opts#{
                    <<"name">> => Name,
                    <<"type">> => Type
                } | Tasks]
            }
    end;
create_task_by_thing(_, Acc) ->
    Acc.


create_timer(Tasks) ->
    case lists:sort(maps:keys(Tasks)) of
        [] ->
            ok;
        [Time | Times] ->
            erlang:send_after(Time * 1000, self(), {task, Times ++ [Time]})
    end.


stop_child(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! stop;
        _ ->
            ok
    end.


init_jyr100_device(SubAddr, Addr, ProductId, #tcp_state{ip = Ip} = TcpState)->
    HxSlave =
        case lists:member(SubAddr, [undefined, <<>>, null]) of
            true ->
                <<"01">>;
            _ ->
                SubAddr
        end,
    Slave = binary_to_integer(HxSlave, 16),
    Start = binary_to_integer(<<"0003">>, 16),
    FunCode = binary_to_integer(<<"06">>, 8),
    Zoom = binary_to_integer(<<"01">>, 8),
    Bin = <<Slave:8, FunCode:8, Start:16, Zoom:8>>,
    CRC = crc16(Bin),
    Payload = <<Bin/binary, CRC/binary>>,
    FilterFun =
        fun(<<"ping\\r\\n">>) ->
            self() ! {tcp, <<"ping\\r\\n">>},
            continue;
            (<<"ping\\r\\n", RecvData/binary>>) ->
                self() ! {tcp, <<"ping\\r\\n">>},
                {break, RecvData};
            (_RecvData) ->
                break
        end,
    alinkcore_device_log:down_data(Addr, ProductId, Payload, Ip, hex),
    case alinkcore_tcp:sync_send(TcpState, Payload, 60000, FilterFun) of
        {ok, <<Slave:8, _/binary>> = DataBin} ->
            alinkcore_device_log:up_data(Addr, ProductId, DataBin, Ip, hex),
            Data = binary:part(DataBin, 0, byte_size(DataBin) - 2),
            case crc16(Data) == binary:part(DataBin, byte_size(DataBin) - 2, 2) of
                true ->
                    ok;
                false ->
                    logger:error("addr ~p init failed: ~p crc error", [Addr, DataBin]),
                    error
            end;
        {error, Reason} ->
            logger:error("addr ~p init failed:~p", [Addr, Reason]),
            error
    end.


