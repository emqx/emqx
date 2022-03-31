%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mqtt_test_client).

-behaviour(gen_server).

-include_lib("emqx/include/emqx_mqtt.hrl").

%% API
-export([
    start_link/2,
    stop/1
]).

-export([send/2]).

%% gen_server callbacks

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TIMEOUT, 1000).
-define(TCP_OPTIONS, [
    binary,
    {packet, raw},
    {active, once},
    {nodelay, true}
]).

-define(PARSE_OPTIONS, #{
    strict_mode => false,
    max_size => ?MAX_PACKET_SIZE,
    version => ?MQTT_PROTO_V5
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [Host, Port, self()], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Packet) ->
    gen_server:call(Pid, {send, Packet}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Host, Port, Owner]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, ?TCP_OPTIONS, ?TIMEOUT),
    {ok, #{
        owner => Owner,
        socket => Socket,
        parse_state => emqx_frame:initial_parse_state(?PARSE_OPTIONS)
    }}.

handle_info(
    {tcp, _Sock, Data},
    #{
        parse_state := PSt,
        owner := Owner,
        socket := Socket
    } = St
) ->
    {NewPSt, Packets} = process_incoming(PSt, Data, []),
    ok = deliver(Owner, Packets),
    ok = run_sock(Socket),
    {noreply, St#{parse_state => NewPSt}};
handle_info({tcp_closed, _Sock}, St) ->
    {stop, normal, St}.

handle_call({send, Packet}, _From, #{socket := Socket} = St) ->
    ok = gen_tcp:send(Socket, emqx_frame:serialize(Packet, ?MQTT_PROTO_V5)),
    {reply, ok, St};
handle_call(stop, _From, #{socket := Socket} = St) ->
    ok = gen_tcp:close(Socket),
    {stop, normal, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

process_incoming(PSt, Data, Packets) ->
    case emqx_frame:parse(Data, PSt) of
        {more, NewPSt} ->
            {NewPSt, lists:reverse(Packets)};
        {ok, Packet, Rest, NewPSt} ->
            process_incoming(NewPSt, Rest, [Packet | Packets])
    end.

deliver(_Owner, []) ->
    ok;
deliver(Owner, [Packet | Packets]) ->
    Owner ! {packet, Packet},
    deliver(Owner, Packets).

run_sock(Socket) ->
    inet:setopts(Socket, [{active, once}]).
