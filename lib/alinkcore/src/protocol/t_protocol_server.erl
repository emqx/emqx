%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%         T系列产品  主动上报
%%% @end
%%% Created : 04. 6月 2024 上午10:03
%%%-------------------------------------------------------------------
-module(t_protocol_server).
-author("yqfclid").
-protocol(<<"protocol_t">>).
-include("alinkcore.hrl").

%% API
-behavior(gen_server).
-export([kick_call/1]).
-export([start_link/2]).
-export([init/1, handle_call/3, handle_info/2, handle_cast/2, terminate/2, code_change/3]).

-export([decode_frame/1]).


-record(state, {transport, sock, addr, product_id, session, product, ip = <<>>}).

kick_call(Addr) ->
    case alinkcore_cache:lookup(session, Addr) of
        {ok, #session{node = Node}} when Node =/= node() ->
            {error, {other_node, Node}};
        {ok, #session{pid = Pid}} ->
            case erlang:is_process_alive(Pid) of
                true ->
                    gen_server:call(Pid, kick, 30000);
                false ->
                    {error, not_found}
            end;
        _ ->
            {error, not_found}
    end.

start_link(Transport, Sock) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Sock]])}.

%%作为网关进程调用
init([Transport, Sock]) ->
    case Transport:wait(Sock) of
        {ok, NewSock} ->
            case alinkdata_dao:query_no_count('QUERY_product', #{<<"protocol">> => <<"protocol_t">>}) of
                {ok, [#{<<"id">> := Id} = Product|_]} ->
                    activate_socket(Transport, Sock),
                    {ok, {RemoteIp, _RemotePort}} = Transport:peername(NewSock),
                    RemoteIpB = alinkutil_type:to_binary(inet:ntoa(RemoteIp)),
                    State = #state{product_id = Id, product = Product, sock = NewSock, transport = Transport, ip = RemoteIpB},
                    gen_server:enter_loop(?MODULE, [], State);
                {ok, []} ->
                    {error, not_found};
                {error, Reason} ->
                    {error, Reason}
            end;
        Error -> Error
    end.


handle_info({stop, Reason}, State) ->
    {stop, Reason, State};
handle_info({tcp, _Sock, Bin}, #state{product_id = ProductId, addr = Addr, session = Session, product = Product, transport = Transport, sock = Socket, ip = Ip} = State) ->
    activate_socket(Transport, Socket),
    Data = decode_frame(Bin),
    NewAddr = maps:get(<<"addr">>, Data, Addr),
    alinkcore_data:handle(ProductId, NewAddr, Data),
    alinkcore_device_log:up_data(NewAddr, ProductId, Bin, Ip, hex),
    NewSession =
        case Session of
            undefined ->
                kick_call(NewAddr),
                check_register(Product, NewAddr),
                S = #session{node = node(), pid = self(), connect_time = erlang:system_time(second)},
                alinkcore_cache:save(session, NewAddr, S);
            _ ->
                Session
        end,
    {noreply, State#state{session = NewSession, addr = NewAddr}};
handle_info({tcp_closed, Reason}, State) ->
    {stop, Reason, State};

handle_info(stop, State) ->
    {stop, normal, State};


handle_info({'DOWN', _Ref, _Msg, _PID, _Reason}, State) ->
    {stop, normal, State};

handle_info(_Info, TcpState) ->
    alinkcore_logger:log(warning, "handle_info ~p~n", [_Info]),
    {noreply, TcpState}.


handle_call(kick, _From, State) ->
    {stop, kick, State};
handle_call(_Msg, _From, TcpState) ->
    {reply, ok, TcpState}.

%%作为网关进程调用
handle_cast(_Msg, TcpState) ->
    {noreply, TcpState}.

%%作为网关进程调用
terminate(_Reason, #state{ addr = Addr, session = Session}) when is_record(Session, session)->
    alinkcore_cache:delete_object(session, Addr, Session),
    ok;
terminate(_Reason, _State)->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



decode_frame(<<16#A5, 16#5A, 16#00, Len:16/big-signed-integer, Addr:12/binary, DianliangStatus:8/big-signed-integer,
    XinhaoStatus:8/big-signed-integer, RecordInterval:16/big-signed-integer, CacheDataLen:16/big-signed-integer,
    AddrType:8/big-signed-integer, AlertStatus:8/big-signed-integer, WeiyiAlertMin:16/big-signed-integer,
    WeiyiAlertMax:16/big-signed-integer, WenduAlertMin:16/big-signed-integer, WenduAlertMax:16/big-signed-integer,
    WeiyiData:16/big-signed-integer, WenduData:16/big-signed-integer, NowTime:32/big-signed-integer,
    _CacheData1:CacheDataLen/binary, _CacheData2:CacheDataLen/binary, _CacheData3:CacheDataLen/binary,
    _CacheData4:CacheDataLen/binary,Tail/binary>> = Frame) ->
    <<Crc1:1/binary, Crc2:1/binary, 16#55, 16#AA>> = Tail,
    CrcData = binary:part(Frame, 0, byte_size(Frame) - byte_size(Tail)),
    TargetCrc = <<Crc2/binary, Crc1/binary>>,
    TargetCrc = crc16(CrcData),
    Len = byte_size(Frame),
    RealDianliangStatus = (((DianliangStatus div 16) rem 2) * 16 + (DianliangStatus rem 16)) * 5,
    #{
        <<"addr">> => Addr,
        <<"dianliang_status">> => #{
            <<"value">> => RealDianliangStatus,
            <<"ts">> => NowTime
        },
        <<"xinhao_status">> => #{
            <<"value">> => XinhaoStatus,
            <<"ts">> => NowTime
        },
        <<"record_interval">> => #{
            <<"value">> => RecordInterval,
            <<"ts">> => NowTime
        },
        <<"device_type">> => #{
            <<"value">> => AddrType,
            <<"ts">> => NowTime
        },
        <<"alert_status">> => #{
            <<"value">> => AlertStatus,
            <<"ts">> => NowTime
        },
        <<"weiyi_alert_min">> => #{
            <<"value">> => WeiyiAlertMin,
            <<"ts">> => NowTime
        },
        <<"weiyi_alert_max">> => #{
            <<"value">> => WeiyiAlertMax,
            <<"ts">> => NowTime
        },
        <<"wendu_alert_min">> => #{
            <<"value">> => WenduAlertMin,
            <<"ts">> => NowTime
        },
        <<"wendu_alert_max">> => #{
            <<"value">> => WenduAlertMax,
            <<"ts">> => NowTime
        },
        <<"weiyi">> => #{
            <<"value">> => WeiyiData,
            <<"ts">> => NowTime
        },
        <<"wendu">> => #{
            <<"value">> => WenduData,
            <<"ts">> => NowTime
        },
        <<"ts">> => NowTime
    }.



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


check_register(#{
    <<"autoRegister">> := AutoRegister,
    <<"owner">> := Owner,
    <<"ownerDept">> := OwnerDept,
    <<"id">> := ProductId
}, Addr) ->
    case alinkcore_cache:query_device(Addr) of
        {error, notfound} when AutoRegister =:= <<"1">> ->
            Info = #{
                <<"addr">> => Addr,
                <<"owner">> => Owner,
                <<"owner_dept">> => OwnerDept,
                <<"product">> => ProductId,
                <<"name">> => Addr
            },
            catch alinkdata_dao:query_no_count('POST_device', Info),
            true;
        {ok, #{<<"product">> := ProductId}} ->
            true;
        _ ->
            false
    end;
check_register(_, _) ->
    skip.


activate_socket(Transport, Socket) ->
    case Transport:setopts(Socket, [{active, 10}]) of
        ok -> ok;
        {error, Reason} ->
            self() ! {stop, Reason},
            ok
    end.