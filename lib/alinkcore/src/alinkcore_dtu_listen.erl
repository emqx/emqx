-module(alinkcore_dtu_listen).
-include("alinkcore.hrl").
-behavior(gen_server).
-export([start/2, init/1, handle_info/2, handle_cast/2, handle_call/3, terminate/2, code_change/3]).

start(Name, Port) ->
    alinkcore_tcp:start_listen(Name, ?MODULE, Port).

init(TcpState) ->
    {ok, TcpState#tcp_state{ mod = undefined }}.



handle_info(D, TcpState) ->
    case alinkcore_healthy:check_oom() of
        shutdown ->
            logger:warning(" ~p exit because of oom", [self()]),
            {stop, normal, TcpState};
        _ ->
            do_handle_info(D,TcpState)
    end.

do_handle_info({tcp, Bin}, #tcp_state{ip = Ip} = TcpState) ->
    alinkcore_logger:log(info, "Recv:~p", [binary:encode_hex(Bin)]),
    alinkcore_device_log:up_data(TcpState#tcp_state.addr, TcpState#tcp_state.product_id, Bin, Ip, hex),
    Buff = TcpState#tcp_state.buff,
    TcpState1 = TcpState#tcp_state{buff = <<>>},
    handle_frame(<<Buff/binary, Bin/binary>>, TcpState1);

do_handle_info(Info, #tcp_state{ mod = Mod } = TcpState) ->
    Mod:handle_info(Info, TcpState).


handle_cast(Msg, #tcp_state{ mod = Mod } = TcpState) ->
    Mod:handle_cast(Msg, TcpState).

handle_call(Msg, From, #tcp_state{ mod = Mod } = TcpState) ->
    Mod:handle_call(Msg, From, TcpState).


terminate(Reason, #tcp_state{ mod = undefined }) ->
    logger:error("exit with reason ~p, and protocol mod not found", [Reason]);
terminate(Reason, #tcp_state{ mod = Mod } = TcpState) ->
    Mod:terminate(Reason, TcpState).

code_change(_OldVsn, TcpState, _Extra) ->
    {ok, TcpState}.

handle_register(#{<<"addr">> := Addr} = Message, #tcp_state{mod = undefined} = TcpState) ->
    case do_register(Message) andalso alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            case alinkcore_cache:query_product(ProductId) of
                {ok, #{<<"protocol">> := Protocol, <<"node_type">> := NodeType}} ->
                    case alinkcore_protocol:get_mod(Protocol) of
                        undefined ->
                            {error, protocol_notfound};
                        Mod when NodeType =/= <<"gateway">> ->
                            Mod:init([Addr, TcpState#tcp_state{mod = Mod, addr = Addr, product_id = ProductId}]);
                        Mod ->
                            NMod = alinkcore_protocol:get_childeren_mod(Addr, Protocol),
                            Children = start_sub_devices(NMod, Addr),
                            NMod:init([Addr, TcpState#tcp_state{
                                mod = NMod,
                                addr = Addr,
                                product_id = ProductId,
                                children = Children}])
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            {error, auth_failed};
        {error, Reason} ->
            {error, Reason}
    end;
handle_register(_Message, TcpState) ->
    {ok, TcpState}.

do_register(#{
    <<"msgType">> := <<"auth_by_device">>,
    <<"addr">> := Addr,
    <<"dk">> := Dk,
    <<"ds">> := Ds
}) ->
    alinkcore_cache:auth_by_device(Addr, Dk, Ds);
do_register(#{
    <<"msgType">> := <<"auth_by_product">>,
    <<"addr">> := Addr,
    <<"pk">> := Pk,
    <<"ps">> := Ps
}) ->
    alinkcore_cache:auth_by_product(Addr, Pk, Ps).


handle_frame(Bin, TcpState) when byte_size(Bin) < 3 ->
    {noreply, TcpState#tcp_state{buff = Bin}};
handle_frame(<<"reg", Bin/binary>> = RegBin, #tcp_state{ip = Ip} = TcpState) ->
    case parse_reg_frame(Bin, <<"reg">>) of
        empty ->
            {noreply, TcpState#tcp_state{buff = <<"reg", Bin/binary>>}};
        {#{<<"addr">> := Addr} = RegFrame, Last} ->
            case handle_register(RegFrame, TcpState) of
                {ok, #tcp_state{product_id = ProductId} = NewTcpState} ->
                    alinkcore_device_log:auth(Addr, ProductId, RegBin, <<"1">>, <<>>, Ip),
                    handle_frame(Last, NewTcpState);
                {error, Reason} ->
                    alinkcore_device_log:auth(Addr, 0, RegBin, <<"0">>, Reason, Ip),
                    alinkcore_tcp:send(TcpState, encode_reason(Reason)),
                    logger:error("reg frame ~p register error:~p", [RegFrame, Reason]),
                    {stop, Reason, TcpState}
            end
    end;
handle_frame(<<"ping\\r\\n", Bin/binary>>, TcpState) ->
    handle_frame(Bin, TcpState);
%%柔性测斜仪心跳
handle_frame(<<"$HB", Bin/binary>>, TcpState) ->
    [_, RemainBin] = binary:split(Bin, <<"*HB">>),
    handle_frame(RemainBin, TcpState);
handle_frame(Bin, TcpState) ->
    case TcpState#tcp_state.mod of
        undefined ->
            {stop, protocol_notfound, TcpState};
        Mod ->
            Mod:handle_info({tcp, Bin}, TcpState)
    end.


%% uzy 网关注册协议
parse_reg_frame(<<>>, _Last) -> empty;
parse_reg_frame(<<"\\r\\n", Last/binary>>, Buff) ->
    Msg = case re:split(Buff, <<"[,|/]">>) of
              [<<"reg">>, <<"1">>, Addr, Dk, Ds] ->
                  #{
                      <<"msgType">> => <<"auth_by_device">>,
                      <<"addr">> => Addr,
                      <<"dk">> => Dk,
                      <<"ds">> => Ds
                  };
              [<<"reg">>, <<"2">>, Addr, Pk, Ps] ->
                  #{
                      <<"msgType">> => <<"auth_by_product">>,
                      <<"addr">> => Addr,
                      <<"pk">> => Pk,
                      <<"ps">> => Ps
                  }
          end,
    {Msg, Last};
parse_reg_frame(<<B:8, Last/binary>>, Buff) ->
    parse_reg_frame(Last, <<Buff/binary, B:8>>).


encode_reason(auth_failed) ->
    <<"01">>;
encode_reason(protocol_notfound) ->
    <<"02">>;
encode_reason(Reason) ->
    logger:error("get unknown reason ~p", [Reason]),
    <<"99">>.


start_sub_devices(Mod, Addr) ->
    case alinkcore_cache:query_children(Addr) of
        {ok, Children} ->
            lists:foldl(
                fun(Child, Acc) ->
                    case Mod:start_sub_device(Child) of
                        {ok, {ChildPid, _}} ->
                            [ChildPid|Acc];
                        {ok, ChildPid} ->
                            [ChildPid|Acc];
                        {error, Reason} ->
                            logger:error("start child ~p failed ~p", [Child, Reason]),
                            Acc
                    end
            end, [], Children);
        {error, Reason} ->
            logger:error("lookup ~p sub devices failed:~p", [Addr, Reason]),
            []
    end.