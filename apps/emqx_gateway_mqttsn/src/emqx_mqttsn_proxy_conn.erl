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

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

initialize(Opts) ->
    FrameOpts = emqx_gateway_utils:frame_options(Opts),
    #{
        parse_state => emqx_mqttsn_frame:initial_parse_state(FrameOpts),
        cid => undefined
    }.

find_or_create(_CId, Transport, Peer, Opts) ->
    emqx_gateway_conn:start_link(Transport, Peer, Opts).

find_or_create(CId, Transport, Peer, Opts, _State) ->
    find_or_create(CId, Transport, Peer, Opts).

get_connection_id(_Transport, Peer, State, Data) ->
    {ParseState, BoundCId} = split_state(State),
    case parse_incoming(Data, [], ParseState) of
        {[Packet | _] = Packets, NParseState} ->
            {CId, NBoundCId} = choose_cid(Packet, BoundCId, Peer),
            {ok, proxy_connection_id(CId, Peer), Packets, merge_state(NParseState, NBoundCId)};
        {[], NParseState} ->
            {ok, proxy_connection_id(peer_id(Peer), Peer), [], merge_state(NParseState, BoundCId)}
    end.

dispatch(Pid, _State, Packet) ->
    erlang:send(Pid, Packet),
    ok.

%% The legacy callback has no proxy owner, so acting on it could detach a
%% channel which has already moved to a newer proxy.
detach(_Pid, _State) ->
    ok.

detach(Pid, ProxyId, _State) ->
    erlang:send(Pid, {udp_proxy_detached, ProxyId}),
    ok.

%% See detach/2. esockd 5.17.1 uses the owner-aware close/3 callback.
close(_Pid, _State) ->
    ok.

close(Pid, ProxyId, _State) ->
    erlang:send(Pid, {udp_proxy_closed, ProxyId}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

split_state(#{parse_state := ParseState, cid := BoundCId}) ->
    {ParseState, BoundCId};
split_state(#{parse_state := ParseState}) ->
    {ParseState, undefined};
split_state(ParseState) ->
    {ParseState, undefined}.

merge_state(ParseState, BoundCId) ->
    #{parse_state => ParseState, cid => BoundCId}.

choose_cid(Packet, BoundCId, Peer) ->
    {ReqCId, PacketType} = packet_cid(Packet),
    select_cid(PacketType, ReqCId, BoundCId, Peer).

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
select_cid(connect, ReqCId, _BoundCId, _Peer) ->
    {ReqCId, ReqCId};
select_cid(pingreq, _ReqCId, undefined, Peer) ->
    {peer_id(Peer), undefined};
select_cid(pingreq, _ReqCId, BoundCId, _Peer) ->
    {BoundCId, BoundCId};
select_cid(_PacketType, _ReqCId, BoundCId, _Peer) ->
    {BoundCId, BoundCId}.

peer_id(Peer) ->
    {peer, Peer}.

proxy_connection_id({peer, _} = CId, _Peer) ->
    CId;
proxy_connection_id(CId, Peer) ->
    {mqttsn_udp_proxy, Peer, CId}.

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
