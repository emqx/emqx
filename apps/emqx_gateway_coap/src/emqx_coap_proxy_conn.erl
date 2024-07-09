%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([initialize/1, create/3, get_connection_id/4, dispatch/3, close/2]).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
initialize(_Opts) ->
    emqx_coap_frame:initial_parse_state(#{}).

create(Transport, Peer, Opts) ->
    emqx_gateway_conn:start_link(Transport, Peer, Opts).

get_connection_id(_Transport, _Peer, State, Data) ->
    case parse_incoming(Data, [], State) of
        {[Msg | _] = Packets, NState} ->
            case emqx_coap_message:extract_uri_query(Msg) of
                #{
                    <<"clientid">> := ClientId
                } ->
                    {ok, ClientId, Packets, NState};
                _ ->
                    invalid
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
