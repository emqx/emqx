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
-define(CHAN_INFO_TIMEOUT, 5000).

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

find_or_create(ClientId, _Transport, _Peer, _Opts, #{reusable_channel := {ClientId, Pid}}) when
    is_binary(ClientId), is_pid(Pid)
->
    {ok, Pid};
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
            {CId, NBoundCId, PacketType, ReusableChannel} = choose_cid(Packet, BoundCId, Peer),
            {ok, CId, Packets, merge_state(NParseState, NBoundCId, PacketType, ReusableChannel)};
        {[], NParseState} ->
            {ok, peer_id(Peer), [], merge_state(NParseState, BoundCId, undefined, undefined)}
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

find_reusable_channel(ClientId, ReusableStates) ->
    Pids = emqx_gateway_cm_registry:lookup_channels(?GATEWAY, ClientId),
    case
        lists:search(
            fun(Pid) ->
                lists:member(channel_conn_state(ClientId, Pid), ReusableStates)
            end,
            lists:reverse(Pids)
        )
    of
        {value, Pid} -> {ok, Pid};
        false -> false
    end.

channel_conn_state(ClientId, Pid) ->
    %% Registry and info ETS are updated separately. If the pid is visible before
    %% its info snapshot, do not call the live process from UDP proxy routing.
    %% Treat it as not reusable; CONNECT can take over later, and PINGREQ can retry.
    case safe_gateway_chan_info(ClientId, Pid) of
        #{conn_state := ConnState} ->
            ConnState;
        _ ->
            undefined
    end.

safe_gateway_chan_info(ClientId, Pid) ->
    try emqx_gateway_cm:get_chan_info(?GATEWAY, ClientId, Pid, ?CHAN_INFO_TIMEOUT) of
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

merge_state(ParseState, BoundCId, PacketType, undefined) ->
    %% Rebuild state on each datagram so a reusable_channel hint is single-use.
    #{parse_state => ParseState, cid => BoundCId, packet_type => PacketType};
merge_state(ParseState, BoundCId, PacketType, ReusableChannel) ->
    (merge_state(ParseState, BoundCId, PacketType, undefined))#{
        reusable_channel => ReusableChannel
    }.

choose_cid(Packet, BoundCId, Peer) ->
    {ReqCId, PacketType} = packet_cid(Packet),
    {CId, NBoundCId, ReusableChannel} = select_cid(PacketType, ReqCId, BoundCId, Peer),
    {CId, NBoundCId, PacketType, ReusableChannel}.

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
    {peer_id(Peer), undefined, undefined};
select_cid(_PacketType, undefined, BoundCId, _Peer) ->
    {BoundCId, BoundCId, undefined};
select_cid(pingreq, ReqCId, BoundCId, Peer) ->
    select_pingreq_cid(ReqCId, BoundCId, Peer);
select_cid(_PacketType, ReqCId, _BoundCId, _Peer) ->
    {ReqCId, ReqCId, undefined}.

select_pingreq_cid(ReqCId, ReqCId, _Peer) ->
    {ReqCId, ReqCId, undefined};
select_pingreq_cid(ReqCId, _BoundCId, Peer) ->
    case find_reusable_channel(ReqCId, [asleep, awake]) of
        {ok, Pid} ->
            {ReqCId, ReqCId, {ReqCId, Pid}};
        false ->
            {peer_id(Peer), undefined, undefined}
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
