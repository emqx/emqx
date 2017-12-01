%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_ws_client_sup).

-author("Feng Lee <feng@emqtt.io>").

-behavior(supervisor).

-export([start_link/0, start_client/3]).

-export([init/1]).

%% @doc Start websocket client supervisor
-spec(start_link() -> {ok, pid()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a WebSocket Connection.
-spec(start_client(pid(), mochiweb_request:request(), fun()) -> {ok, pid()}).
start_client(WsPid, Req, ReplyChannel) ->
    supervisor:start_child(?MODULE, [WsPid, Req, ReplyChannel]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    Env = lists:append(emqttd:env(client, []),
                       emqttd:env(protocol, []) ++ forwarded_header()),
    {ok, {{simple_one_for_one, 0, 1},
           [{ws_client, {emqttd_ws_client, start_link, [Env]},
             temporary, 5000, worker, [emqttd_ws_client]}]}}.

forwarded_header() ->
    Env = [{Proto, Opts} || {Proto, _, Opts} <- emqttd:env(listeners, []), Proto == ws orelse Proto == wss],
    lists:foldl(fun({Proto, Opts}, Acc) ->
                  Proto1 = case Proto of
                               ws  -> tcp;
                               wss -> ssl
                           end,
                  case {proplists:get_value(proxy_ipaddress_header, Opts),
                          proplists:get_value(proxy_port_header, Opts)} of
                      {undefined, _}           -> Acc;
                      {AddrHeader, undefined}  -> [{Proto1, [{proxy_ipaddress_header, AddrHeader}]} | Acc];
                      {AddrHeader, PortHeader} -> [{Proto1, [{proxy_ipaddress_header, AddrHeader},
                                                             {proxy_port_header, PortHeader}]} | Acc]
                  end
              end, [], Env).

