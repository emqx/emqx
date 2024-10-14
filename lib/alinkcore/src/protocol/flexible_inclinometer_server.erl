%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%         柔性测斜仪协议
%%% @end
%%% Created : 14. 9月 2023 上午10:29
%%%-------------------------------------------------------------------
-module(flexible_inclinometer_server).
-author("yqfclid").

-protocol(<<"rouxingcexie-huasi">>).
-include("alinkcore.hrl").
%% API
-behavior(gen_server).
-export([kick/1, start_sub_device/1, kick_call/1, checksum/1, send_data/2]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2, code_change/3]).

-define(DATA_INTERVAL, 60 * 1000).


-record(state, { addr, productId, tasks, device_id}).

-record(sub_device_state, {addr, product_id, tasks, sub_addr = <<>>, gateway, session, device_id}).

-record(device_info, {addr, product_id, sub_addr = <<>>}).

-define(TOPIC(Addr), <<"p/in/", Addr/binary>>).

send_data(Addr, Data) ->
    case alinkcore_cache:lookup(session, Addr) of
        {ok, #session{node = Node}} when Node =/= node() ->
            {error, {other_node, Node}};
        {ok, #session{pid = Pid}} ->
            Pid ! {trace_send_data, Data};
        _ ->
            {error, not_found}
    end.

kick_call(Addr) ->
    case alinkcore_cache:lookup(session, Addr) of
        {ok, #session{node = Node}} when Node =/= node() ->
            {error, {other_node, Node}};
        {ok, #session{pid = Pid}} ->
            gen_server:call(Pid, kick, 30000);
        _ ->
            {error, not_found}
    end.

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
            DeviceId =
                case alinkcore_cache:query_device(Addr) of
                    {ok, #{<<"config">> := Config}} ->
                        lists:foldl(
                            fun(#{<<"name">> := <<"device_id">>, <<"value">> := DId}, _Acc) ->
                                DId;
                               (_, Acc) ->
                                Acc
                        end, <<>>, Config);
                    {error, Re} ->
                        logger:error("find device ~p error", [Addr, Re]),
                        <<>>
                end,
            case alinkcore_cache:query_product(ProductId) of
                {ok, Product} ->
                    Tasks = thing(Product),
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
                        session = Session,
                        device_id = DeviceId
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
            DeviceId =
                case alinkcore_cache:query_device(Addr) of
                    {ok, #{<<"config">> := Config}} ->
                        lists:foldl(
                            fun(#{<<"name">> := <<"device_id">>, <<"value">> := DId}, _Acc) ->
                                DId;
                                (_, Acc) ->
                                    Acc
                            end, <<>>, Config);
                    {error, Re} ->
                        logger:error("find device ~p error", [Addr, Re]),
                        <<>>
                end,
            ProductId = maps:get(<<"product">>, Device),
            case alinkcore_cache:query_product(ProductId) of
                {ok, Product} ->
                    Tasks = thing(Product),
                    create_timer(Tasks),
                    Session = #session{node = node(), pid = self(), connect_time = erlang:system_time(second)},
                    alinkcore_cache:save(session, Addr, Session),
                    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"login">>}]),
                    {ok, TcpState#tcp_state{state = #state{
                        addr = Addr,
                        productId = ProductId,
                        tasks = Tasks,
                        device_id = DeviceId
                    }, session = Session}};
                {error, Reason} ->
                    logger:error("find product ~p error", [ProductId, Reason]),
                    {error, product_error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%作为网关进程调用
handle_info({help_run_tasks_with_fun, DeviceInfo, TaskThings, Fun}, TcpState) ->
    #device_info{addr = Addr, product_id = ProductId} = DeviceInfo,
    case Fun(DeviceInfo, TaskThings, TcpState, #{<<"ts">> => os:system_time(second)}) of
        {error, timeout} ->
            {noreply, TcpState};
        {error, Reason} ->
            {stop, Reason, TcpState};
        {Acc, NewTcpState} ->
            case maps:size(Acc) == 1 of
                true -> ok;
                false -> alinkcore_data:handle(ProductId, Addr, Acc)
            end,
            {noreply, NewTcpState}
    end;

%%作为网关进程调用
handle_info({help_write_with_fun, DeviceInfo, Message, Fun}, TcpState) ->
    Fun(Message, DeviceInfo, TcpState),
    {noreply, TcpState};

%%作为网关进程调用
handle_info(get_data, #tcp_state{ state =#state{ tasks = Tasks, device_id = DeviceId} } = TcpState) ->
    #state{
        addr = Addr,
        productId = ProductId
    } = TcpState#tcp_state.state,
    erlang:send_after(?DATA_INTERVAL, self(), get_data),
    DeviceInfo =
        #device_info{
            addr = Addr,
            product_id = ProductId,
            sub_addr = <<>>
        },
    case run_task(DeviceId, DeviceInfo, Tasks, TcpState, #{<<"ts">> => os:system_time(second)}) of
        {error, timeout} ->
            {noreply, TcpState};
        {error, Reason} ->
            {stop, Reason, TcpState};
        {Acc, NewTcpState} ->
            case maps:size(Acc) == 1 of
                true -> ok;
                false ->
                    alinkcore_data:handle(ProductId, Addr, Acc)
            end,
            {noreply, NewTcpState}
    end;
handle_info({trace_send_data, Data}, #tcp_state{} = TcpState) ->
    case alinkcore_tcp:sync_send(TcpState, Data, 10000, undefined) of
        {ok, Data} ->
            ok;
        {error, _Reason} ->
            ok
    end,
    {noreply, TcpState};

%%作为子设备进程调用
handle_info(get_data, #sub_device_state{ tasks = Tasks,
    device_id = DeviceId,
    sub_addr = SubDevAddr,
    addr = Addr,
    product_id = ProductId,
    gateway = GatewayPid} = State) ->
    erlang:send_after(?DATA_INTERVAL, self(), get_data),
    DeviceInfo = #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr},
    F = fun(DInfo, Things, TcpState, Acc) -> run_task(DeviceId, DInfo, Things, TcpState, Acc) end,
    GatewayPid ! {help_run_tasks_with_fun, DeviceInfo, Tasks, F},
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


handle_call(kick, _From, TcpState) when is_record(TcpState, tcp_state)->
    {stop, kick, TcpState};
handle_call(kick, _From, TcpState) ->
    {stop, kick, ok, TcpState};
handle_call(_Msg, _From, TcpState) ->
    {reply, ok, TcpState}.

%%作为网关进程调用
handle_cast(kick, #tcp_state{addr = Addr} = TcpState) ->
    logger:info("kick Addr ~p offline", [Addr]),
    {stop, normal, TcpState};
handle_cast(_Msg, TcpState) ->
    {noreply, TcpState}.

%%作为网关进程调用
terminate(Reason, #tcp_state{ state = #state{ addr = Addr, productId = ProductId}, children = Children, ip = Ip, session = Session}) ->
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


%% @doc 任务读数据
run_task(DeviceId, DeviceInfo, Things, TcpState, Acc) ->
    case catch send_to_device(DeviceId, DeviceInfo, Things, TcpState, Acc) of
        {ok, NAcc} ->
            {NAcc, TcpState};
        {error, Reason} ->
            logger:error("run task failed ~p ~p", [DeviceInfo, Reason]),
            {Acc, TcpState};
        {'EXIT', Reason} ->
            logger:error("run task failed ~p ~p", [DeviceInfo, Reason]),
            {Acc, TcpState}
    end.


send_to_device(DeviceId, DeviceInfo, Thing, #tcp_state{ip = Ip} = TcpState, Acc) ->
    #device_info{addr = Addr, product_id = ProductId, sub_addr = SubDevAddr} = DeviceInfo,
    Payload = encode_frame(SubDevAddr, Thing, DeviceId),
    alinkcore_device_log:down_data(Addr, ProductId, Payload, Ip, hex),
    case alinkcore_tcp:sync_send(TcpState, Payload, 10000, undefined) of
        {ok, Data} ->
            alinkcore_device_log:up_data(Addr, ProductId, Data, Ip, hex),
            catch case decode_frame(SubDevAddr, Data, Thing, Acc) of
                      {'EXIT', Reason} ->
                          {error, Reason};
                      Other ->
                          Other
                  end;
        {error, Reason} ->
            {error, Reason}
    end.


encode_frame(_, _Thing, DeviceId) ->
    Data = <<"$HUASI,GET,DATA,", DeviceId/binary, "*">>,
    CheckSum = checksum(Data),
    <<Data/binary, CheckSum/binary, "\r\n">>.



decode_frame(_SubDevAddr, Data0, _Thing, Acc) ->
    Data = binary:replace(Data0, <<"\r\n">>, <<>>, [global]),
    case binary:split(Data, <<"*">>, [global]) of
        [RealData0, CheckSum] ->
            RealData = <<RealData0/binary, "*">>,
            case checksum(RealData) of
                CheckSum ->
                    decode_data(RealData, Acc);
                _ ->
                    {error, checksum_error}
            end;
        _ ->
            {error, checksum_error}
    end.



decode_data(<<"$HUASI,GET,DATA,", Data0/binary>>, Acc) ->
    Ts = maps:get(<<"ts">>, Acc, erlang:system_time(second)),
    Data = binary:replace(Data0, <<"*">>, <<>>, [global]),
    [DeviceId, NodeNum|DataList] = binary:split(Data, <<",">>, [global]),
    NAcc = Acc#{
        <<"device_id">> => #{
            <<"value">> => DeviceId,
            <<"ts">> => Ts
        },
        <<"node_num">> => #{
            <<"value">> => binary_to_integer(NodeNum),
            <<"ts">> => Ts
        }
    },
    {_, DecodedData} =
        lists:foldl(
            fun(D, {Seq, Acc1}) ->
                {Name, Type} = get_key_name(Seq),
                {Seq + 1, Acc1#{
                    Name => #{
                        <<"value">> => format_type(D, Type),
                        <<"ts">> => Ts
                    }
                }}
        end, {1, NAcc}, DataList),
    {ok, DecodedData}.



get_key_name(Seq) ->
    {SeqRem, SeqDiv} =
        case {Seq rem 10, Seq div 10} of
            {0, S} ->
                {0, S - 1};
            {SR, SD} ->
                {SR, SD}
        end,
    NumB = alinkutil_type:to_binary(SeqDiv),
    {Name, Type} = get_name(SeqRem),
    {<<Name/binary, NumB/binary>>, Type}.


get_name(1) -> {<<"node">>, string};
get_name(2) -> {<<"voltage">>, float};
get_name(3) -> {<<"temp">>, float};
get_name(4) -> {<<"accx">>, float};
get_name(5) -> {<<"accy">>, float};
get_name(6) -> {<<"accz">>, float};
get_name(7) -> {<<"locationx">>, float};
get_name(8) -> {<<"locationy">>, float};
get_name(9) -> {<<"locationz">>, float};
get_name(0) -> {<<"rotation_angle">>, float}.

format_type(V, string) ->
    V;
format_type(V, int) ->
    binary_to_integer(V);
format_type(V, float) ->
    binary_to_float(V).





checksum(Data) ->
    do_checksum(Data, false, undefined).


do_checksum(<<$*, _/binary>>, true, CryptoBin) ->
    binary:encode_hex(<<CryptoBin>>);
do_checksum(<<$$, Remain/binary>>, false, CryptoBin) ->
    do_checksum(Remain, true, CryptoBin);
do_checksum(<<Bin:8, Remain/binary>>, true, undefined) ->
    do_checksum(Remain, true, Bin);
do_checksum(<<Bin:8, Remain/binary>>, true, CryptoBin) ->
    NBin = CryptoBin bxor Bin,
    do_checksum(Remain, true, NBin);
do_checksum(<<_Bin:8, Remain/binary>>, false, CryptoBin) ->
    do_checksum(Remain, false, CryptoBin).







thing(#{<<"thing">> := Things}) ->
    Things.





create_timer(_Tasks) ->
    erlang:send_after(?DATA_INTERVAL, self(), get_data).


stop_child(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! stop;
        _ ->
            ok
    end.

handle_message(_Message, TcpState) ->
    TcpState.

