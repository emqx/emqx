%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc start/stop MQTT listeners.
-module(emqx_listeners).

-include("emqx_mqtt.hrl").

-export([start_all/0, restart_all/0, stop_all/0]).
-export([start_listener/1, stop_listener/1, restart_listener/1]).

-type(listener() :: {atom(), esockd:listen_on(), [esockd:option()]}).

%% @doc Start all listeners
-spec(start_all() -> ok).
start_all() ->
    lists:foreach(fun start_listener/1, emqx_config:get_env(listeners, [])).

%% Start MQTT/TCP listener
-spec(start_listener(listener()) -> {ok, pid()} | {error, term()}).
start_listener({tcp, ListenOn, Options}) ->
    start_mqtt_listener('mqtt:tcp', ListenOn, Options);
%% Start MQTT/TLS listener
start_listener({Proto, ListenOn, Options}) when Proto == ssl; Proto == tls ->
    start_mqtt_listener('mqtt:tls', ListenOn, Options);
%% Start MQTT/WS listener
start_listener({Proto, ListenOn, Options}) when Proto == http; Proto == ws ->
    start_http_listener('mqtt:ws', ListenOn, Options);
%% Start MQTT/WSS listener
start_listener({Proto, ListenOn, Options}) when Proto == https; Proto == wss ->
    start_http_listener('mqtt:wss', ListenOn, Options).

start_mqtt_listener(Name, ListenOn, Options) ->
    SockOpts = esockd:parse_opt(Options),
    MFA = {emqx_connection, start_link, [Options -- SockOpts]},
    {ok, _} = esockd:open(Name, ListenOn, merge_default(SockOpts), MFA).

start_http_listener(Name, ListenOn, Options) ->
    SockOpts = esockd:parse_opt(Options),
    MFA = {emqx_ws, handle_request, [Options -- SockOpts]},
    {ok, _} = mochiweb:start_http(Name, ListenOn, SockOpts, MFA).

%% @doc Restart all listeners
-spec(restart_all() -> ok).
restart_all() ->
    lists:foreach(fun restart_listener/1, emqx_config:get_env(listeners, [])).

-spec(restart_listener(listener()) -> any()).
restart_listener({tcp, ListenOn, _Opts}) ->
    esockd:reopen('mqtt:tcp', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == ssl; Proto == tls ->
    esockd:reopen('mqtt:ssl', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:restart_http('mqtt:ws', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:restart_http('mqtt:wss', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) ->
    esockd:reopen(Proto, ListenOn).

%% @doc Stop all listeners
-spec(stop_all() -> ok).
stop_all() ->
    lists:foreach(fun stop_listener/1, emqx_config:get_env(listeners, [])).

-spec(stop_listener(listener()) -> ok | {error, any()}).
stop_listener({tcp, ListenOn, _Opts}) ->
    esockd:close('mqtt:tcp', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == ssl; Proto == tls ->
    esockd:close('mqtt:ssl', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:stop_http('mqtt:ws', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:stop_http('mqtt:wss', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) ->
    esockd:close(Proto, ListenOn).

merge_default(Options) ->
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(?MQTT_SOCKOPTS, TcpOpts)} | Options1];
        false ->
            [{tcp_options, ?MQTT_SOCKOPTS} | Options]
    end.

