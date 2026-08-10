%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_proxy_conn).

-behaviour(esockd_udp_proxy_connection).

-include("emqx_coap.hrl").

-export([initialize/1, find_or_create/4, get_connection_id/4, dispatch/3, close/2, close/3]).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
initialize(_Opts) ->
    emqx_coap_frame:initial_parse_state(#{}).

find_or_create(ProxyCId, Transport, Peer, Opts) ->
    CId = logical_connection_id(ProxyCId),
    case emqx_gateway_cm_registry:lookup_channels(coap, CId) of
        [Pid] ->
            {ok, Pid};
        [] ->
            emqx_gateway_conn:start_link(Transport, Peer, Opts)
    end.

get_connection_id(_Transport, Peer, State, Data) ->
    {ParseState, BoundCId} = split_state(State),
    case parse_incoming(Data, [], ParseState) of
        {[Msg | _] = Packets, NState} ->
            case Msg of
                #coap_message{} ->
                    {CId, NBoundCId} = choose_cid(Msg, BoundCId, Peer),
                    {ok, proxy_connection_id(CId, Peer), Packets, merge_state(NState, NBoundCId)};
                {coap_ignore, _} ->
                    %% RFC 7252 Section 3: unknown versions must be silently ignored.
                    CId = route_cid(BoundCId, Peer),
                    {ok, proxy_connection_id(CId, Peer), Packets, merge_state(NState, BoundCId)};
                {coap_format_error, Type, MsgId, _} ->
                    %% RFC 7252 Section 4.2: reject CON format errors with Reset.
                    _ = {Type, MsgId},
                    %% RFC 7252 Section 4.2: per-message errors must not stop the listener.
                    CId = route_cid(BoundCId, Peer),
                    {ok, proxy_connection_id(CId, Peer), Packets, merge_state(NState, BoundCId)};
                {coap_request_error, Req, Error} ->
                    %% RFC 7252 Section 5.4.1/5.8: reply with a 4.xx error for bad requests.
                    _ = {Req, Error},
                    %% RFC 7252 Section 4.2: per-message errors must not stop the listener.
                    CId = route_cid(BoundCId, Peer),
                    {ok, proxy_connection_id(CId, Peer), Packets, merge_state(NState, BoundCId)}
            end;
        _Error ->
            invalid
    end.

peer_id(Peer) ->
    {peer, Peer}.

proxy_connection_id({peer, _} = CId, _Peer) ->
    CId;
proxy_connection_id(CId, Peer) ->
    %% esockd takes over an existing proxy as soon as the same connection ID
    %% is attached.  Keep candidates isolated until CoAP validates the token.
    {coap_udp_proxy, Peer, CId}.

logical_connection_id({coap_udp_proxy, _Peer, CId}) ->
    CId;
logical_connection_id(CId) ->
    CId.

split_state(#{parse_state := ParseState, cid := BoundCId}) ->
    {ParseState, BoundCId};
split_state(#{parse_state := ParseState}) ->
    {ParseState, undefined};
split_state(ParseState) ->
    {ParseState, undefined}.

merge_state(ParseState, BoundCId) ->
    #{parse_state => ParseState, cid => BoundCId}.

route_cid(undefined, Peer) ->
    peer_id(Peer);
route_cid(BoundCId, _Peer) ->
    BoundCId.

choose_cid(Msg, BoundCId, Peer) ->
    URIQuery = emqx_coap_message:extract_uri_query(Msg),
    ReqClientId = normalize_clientid(extract_clientid(URIQuery)),
    select_cid(ReqClientId, BoundCId, Peer).

extract_clientid(URIQuery) ->
    case maps:find(<<"clientid">>, URIQuery) of
        {ok, ClientId} ->
            ClientId;
        error ->
            maps:get("clientid", URIQuery, undefined)
    end.

normalize_clientid(undefined) ->
    undefined;
normalize_clientid(ClientId) when is_binary(ClientId) ->
    ClientId;
normalize_clientid(ClientId) when is_list(ClientId) ->
    list_to_binary(ClientId);
normalize_clientid(_Other) ->
    undefined.

select_cid(undefined, undefined, Peer) ->
    {peer_id(Peer), undefined};
select_cid(undefined, BoundCId, _Peer) ->
    {BoundCId, BoundCId};
select_cid(ReqClientId, undefined, _Peer) ->
    {ReqClientId, ReqClientId};
select_cid(_ReqClientId, BoundCId, _Peer) ->
    {BoundCId, BoundCId}.

dispatch(Pid, _State, Packet) ->
    erlang:send(Pid, Packet).

close(Pid, _State) ->
    erlang:send(Pid, udp_proxy_closed),
    ok.

close(Pid, ProxyId, _State) ->
    erlang:send(Pid, {udp_proxy_closed, ProxyId}),
    ok.

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};
parse_incoming(Data, Packets, State) ->
    {ok, Packet, Rest, NParseState} = emqx_coap_frame:parse(Data, State),
    parse_incoming(Rest, [Packet | Packets], NParseState).
