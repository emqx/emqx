%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_proxy_conn).

-behaviour(esockd_udp_proxy_connection).

-include("emqx_coap.hrl").

-export([initialize/1, find_or_create/4, get_connection_id/4, dispatch/3, close/2]).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
initialize(_Opts) ->
    emqx_coap_frame:initial_parse_state(#{}).

find_or_create(CId, Transport, Peer, Opts) ->
    case emqx_gateway_cm_registry:lookup_channels(coap, CId) of
        [Pid] ->
            {ok, Pid};
        [] ->
            emqx_gateway_conn:start_link(Transport, Peer, Opts)
    end.

get_connection_id(_Transport, Peer, State, Data) ->
    case parse_incoming(Data, [], State) of
        {[Msg | _] = Packets, NState} ->
            case Msg of
                #coap_message{} ->
                    case emqx_coap_message:extract_uri_query(Msg) of
                        #{
                            <<"clientid">> := ClientId
                        } ->
                            {ok, ClientId, Packets, NState};
                        _ ->
                            %% RFC 7252 Section 4.2: per-message errors must not stop the listener.
                            {ok, peer_id(Peer), Packets, NState}
                    end;
                {coap_ignore, _} ->
                    %% RFC 7252 Section 3: unknown versions must be silently ignored.
                    {ok, peer_id(Peer), Packets, NState};
                {coap_format_error, Type, MsgId, _} ->
                    %% RFC 7252 Section 4.2: reject CON format errors with Reset.
                    _ = {Type, MsgId},
                    %% RFC 7252 Section 4.2: per-message errors must not stop the listener.
                    {ok, peer_id(Peer), Packets, NState};
                {coap_request_error, Req, Error} ->
                    %% RFC 7252 Section 5.4.1/5.8: reply with a 4.xx error for bad requests.
                    _ = {Req, Error},
                    %% RFC 7252 Section 4.2: per-message errors must not stop the listener.
                    {ok, peer_id(Peer), Packets, NState}
            end;
        _Error ->
            invalid
    end.

peer_id(Peer) ->
    {peer, Peer}.

dispatch(Pid, _State, Packet) ->
    erlang:send(Pid, Packet).

close(Pid, _State) ->
    erlang:send(Pid, udp_proxy_closed).

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};
parse_incoming(Data, Packets, State) ->
    {ok, Packet, Rest, NParseState} = emqx_coap_frame:parse(Data, State),
    parse_incoming(Rest, [Packet | Packets], NParseState).
