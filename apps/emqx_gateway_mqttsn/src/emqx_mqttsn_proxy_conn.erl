%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqttsn_proxy_conn).

-behaviour(esockd_udp_proxy_connection).

-include("emqx_mqttsn.hrl").

-export([
    initialize/1,
    find_or_create/4,
    find_or_create/5,
    get_connection_id/4,
    dispatch/3,
    detach/2,
    detach/3,
    close/2,
    close/3
]).

-define(GATEWAY, mqttsn).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

initialize(Opts) ->
    FrameOpts = emqx_gateway_utils:frame_options(Opts),
    #{
        parse_state => emqx_mqttsn_frame:initial_parse_state(FrameOpts),
        cid => undefined,
        packet_type => undefined
    }.

find_or_create(CId, Transport, Peer, Opts) ->
    find_or_create(CId, Transport, Peer, Opts, #{}).

find_or_create({peer, _Peer}, Transport, Peer, Opts, _State) ->
    emqx_gateway_conn:start_link(Transport, Peer, Opts);
find_or_create(ClientId, Transport, Peer, Opts, State) when is_binary(ClientId) ->
    ReusableStates =
        case maps:get(packet_type, State, undefined) of
            connect -> [asleep, awake];
            pingreq -> [asleep, awake];
            _Other -> [connected, asleep, awake]
        end,
    case find_reusable_channel(ClientId, ReusableStates) of
        {ok, Pid} ->
            {ok, Pid};
        false ->
            emqx_gateway_conn:start_link(Transport, Peer, Opts)
    end;
find_or_create(_CId, Transport, Peer, Opts, _State) ->
    emqx_gateway_conn:start_link(Transport, Peer, Opts).

get_connection_id(_Transport, Peer, State, Data) ->
    {ParseState, BoundCId} = split_state(State),
    case parse_incoming(Data, [], ParseState) of
        {[Packet | _] = Packets, NParseState} ->
            {CId, NBoundCId, PacketType} = choose_cid(Packet, BoundCId, Peer),
            {ok, CId, Packets, merge_state(NParseState, NBoundCId, PacketType)};
        {[], NParseState} ->
            {ok, peer_id(Peer), [], merge_state(NParseState, BoundCId, undefined)}
    end.

dispatch(Pid, _State, Packet) ->
    erlang:send(Pid, Packet),
    ok.

detach(Pid, _State) ->
    erlang:send(Pid, udp_proxy_detached),
    ok.

detach(Pid, ProxyId, _State) ->
    erlang:send(Pid, {udp_proxy_detached, ProxyId}),
    ok.

close(Pid, _State) ->
    erlang:send(Pid, udp_proxy_closed),
    ok.

close(Pid, ProxyId, _State) ->
    erlang:send(Pid, {udp_proxy_closed, ProxyId}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

find_reusable_channel(ClientId, ReusableStates) ->
    Pids = emqx_gateway_cm_registry:lookup_channels(?GATEWAY, ClientId),
    pick_reusable_channel(ClientId, ReusableStates, lists:reverse(Pids)).

pick_reusable_channel(_ClientId, _ReusableStates, []) ->
    false;
pick_reusable_channel(ClientId, ReusableStates, [Pid | Rest]) ->
    case channel_conn_state(ClientId, Pid) of
        ConnState when
            ConnState == connected; ConnState == asleep; ConnState == awake
        ->
            case lists:member(ConnState, ReusableStates) of
                true ->
                    {ok, Pid};
                false ->
                    pick_reusable_channel(ClientId, ReusableStates, Rest)
            end;
        _Other ->
            pick_reusable_channel(ClientId, ReusableStates, Rest)
    end.

channel_conn_state(ClientId, Pid) ->
    case safe_chan_info(ClientId, Pid) of
        #{conn_state := ConnState} ->
            ConnState;
        _ ->
            undefined
    end.

safe_chan_info(ClientId, Pid) ->
    case safe_gateway_chan_info(ClientId, Pid) of
        Info = #{} ->
            Info;
        _ ->
            case safe_gateway_conn_info(Pid) of
                Info = #{} -> Info;
                _ -> undefined
            end
    end.

safe_gateway_chan_info(ClientId, Pid) ->
    try emqx_gateway_cm:get_chan_info(?GATEWAY, ClientId, Pid) of
        Info -> Info
    catch
        _:_ -> undefined
    end.

safe_gateway_conn_info(Pid) ->
    try emqx_gateway_conn:info(Pid) of
        Info -> Info
    catch
        _:_ -> undefined
    end.

split_state(#{parse_state := ParseState, cid := BoundCId}) ->
    {ParseState, BoundCId};
split_state(#{parse_state := ParseState}) ->
    {ParseState, undefined};
split_state(ParseState) ->
    {ParseState, undefined}.

merge_state(ParseState, BoundCId, PacketType) ->
    #{parse_state => ParseState, cid => BoundCId, packet_type => PacketType}.

choose_cid(Packet, BoundCId, Peer) ->
    {ReqCId, PacketType} = packet_cid(Packet),
    {CId, NBoundCId} = select_cid(PacketType, ReqCId, BoundCId, Peer),
    {CId, NBoundCId, PacketType}.

packet_cid(?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, ClientId)) ->
    {normalize_clientid(ClientId), connect};
packet_cid(?SN_PINGREQ_MSG(ClientId)) ->
    {normalize_clientid(ClientId), pingreq};
packet_cid(_Packet) ->
    {undefined, undefined}.

normalize_clientid(ClientId) when ClientId == undefined; ClientId == <<>> ->
    undefined;
normalize_clientid(ClientId) when is_binary(ClientId) ->
    ClientId;
normalize_clientid(_ClientId) ->
    undefined.

select_cid(_PacketType, undefined, undefined, Peer) ->
    {peer_id(Peer), undefined};
select_cid(_PacketType, undefined, BoundCId, _Peer) ->
    {BoundCId, BoundCId};
select_cid(pingreq, ReqCId, BoundCId, Peer) ->
    select_pingreq_cid(ReqCId, BoundCId, Peer);
select_cid(_PacketType, ReqCId, undefined, _Peer) ->
    {ReqCId, ReqCId};
select_cid(_PacketType, ReqCId, {peer, _Peer}, _Peer0) ->
    {ReqCId, ReqCId};
select_cid(_PacketType, ReqCId, _BoundCId, _Peer) ->
    {ReqCId, ReqCId}.

select_pingreq_cid(ReqCId, ReqCId, _Peer) ->
    {ReqCId, ReqCId};
select_pingreq_cid(ReqCId, _BoundCId, Peer) ->
    case find_reusable_channel(ReqCId, [asleep, awake]) of
        {ok, _Pid} ->
            {ReqCId, ReqCId};
        false ->
            {peer_id(Peer), undefined}
    end.

peer_id(Peer) ->
    {peer, Peer}.

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};
parse_incoming(Data, Packets, State) ->
    try emqx_mqttsn_frame:parse(Data, State) of
        {ok, Packet, Rest, NParseState} ->
            parse_incoming(Rest, [Packet | Packets], NParseState)
    catch
        error:Reason ->
            {[{frame_error, Reason} | Packets], State}
    end.
