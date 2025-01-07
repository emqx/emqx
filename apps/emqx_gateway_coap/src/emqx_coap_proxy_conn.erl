%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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

get_connection_id(_Transport, _Peer, State, Data) ->
    case parse_incoming(Data, [], State) of
        {[Msg | _] = Packets, NState} ->
            case emqx_coap_message:extract_uri_query(Msg) of
                #{
                    <<"clientid">> := ClientId
                } ->
                    {ok, ClientId, Packets, NState};
                _ ->
                    ErrMsg = <<"Missing token or clientid in connection mode">>,
                    Reply = emqx_coap_message:piggyback({error, bad_request}, ErrMsg, Msg),
                    Bin = emqx_coap_frame:serialize_pkt(Reply, emqx_coap_frame:serialize_opts()),
                    {error, Bin}
            end;
        _Error ->
            invalid
    end.

dispatch(Pid, _State, Packet) ->
    erlang:send(Pid, Packet).

close(Pid, _State) ->
    erlang:send(Pid, udp_proxy_closed).

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};
parse_incoming(Data, Packets, State) ->
    {ok, Packet, Rest, NParseState} = emqx_coap_frame:parse(Data, State),
    parse_incoming(Rest, [Packet | Packets], NParseState).
