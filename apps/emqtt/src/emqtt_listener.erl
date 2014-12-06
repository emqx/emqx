%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%

-module(emqtt_listener).

-include("emqtt.hrl").

-define(MQTT_SOCKOPTS, [
	binary,
	{packet,        raw},
	{reuseaddr,     true},
	{backlog,       128},
	{nodelay,       false}
]).

-export([start/1]).

start(Listeners) ->
	todo.
	

spec({Listener, SockOpts}, Callback) ->
    [tcp_listener_spec(emqtt_tcp_listener_sup, Address, SockOpts,
		 mqtt, "TCP Listener", Callback) || Address <- tcp_listener_addresses(Listener)].

tcp_listener_spec(NamePrefix, {IPAddress, Port, Family}, SocketOpts,
                  Protocol, Label, OnConnect) ->
    {emqtt_net:tcp_name(NamePrefix, IPAddress, Port),
     {tcp_listener_sup, start_link,
      [IPAddress, Port, [Family | SocketOpts],
       {?MODULE, listener_started, [Protocol]},
       {?MODULE, listener_stopped, [Protocol]},
       OnConnect, Label]},
     transient, infinity, supervisor, [tcp_listener_sup]}.

tcp_listener_addresses(Port) when is_integer(Port) ->
    tcp_listener_addresses_auto(Port);
tcp_listener_addresses({"auto", Port}) ->
    %% Variant to prevent lots of hacking around in bash and batch files
    tcp_listener_addresses_auto(Port);
tcp_listener_addresses({Host, Port}) ->
    %% auto: determine family IPv4 / IPv6 after converting to IP address
    tcp_listener_addresses({Host, Port, auto});
tcp_listener_addresses({Host, Port, Family0})
  when is_integer(Port) andalso (Port >= 0) andalso (Port =< 65535) ->
    [{IPAddress, Port, Family} ||
        {IPAddress, Family} <- emqtt_net:getaddr(Host, Family0)];
tcp_listener_addresses({_Host, Port, _Family0}) ->
    ?ERROR("invalid port ~p - not 0..65535~n", [Port]),
    throw({error, {invalid_port, Port}}).

tcp_listener_addresses_auto(Port) ->
    lists:append([tcp_listener_addresses(Listener) ||
                     Listener <- emqtt_net:port_to_listeners(Port)]).

%--------------------------------------------
%TODO: callback 
%--------------------------------------------
listener_started(Protocol, IPAddress, Port) ->
    %% We need the ip to distinguish e.g. 0.0.0.0 and 127.0.0.1
    %% We need the host so we can distinguish multiple instances of the above
    %% in a cluster.
	?INFO("tcp listener started: ~p ~p:~p", [Protocol, IPAddress, Port]).

listener_stopped(Protocol, IPAddress, Port) ->
	?INFO("tcp listener stopped: ~p ~p:~p", [Protocol, IPAddress, Port]).

