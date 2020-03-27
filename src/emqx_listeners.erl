%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Start/Stop MQTT listeners.
-module(emqx_listeners).

-include("emqx_mqtt.hrl").

%% APIs
-export([ start/0
        , restart/0
        , stop/0
        ]).

-export([ start_listener/1
        , start_listener/3
        , stop_listener/1
        , stop_listener/3
        , restart_listener/1
        , restart_listener/3
        ]).

-type(listener() :: {esockd:proto(), esockd:listen_on(), [esockd:option()]}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start all listeners.
-spec(start() -> ok).
start() ->
    lists:foreach(fun start_listener/1, emqx:get_env(listeners, [])).

-spec(start_listener(listener()) -> {ok, pid()} | {error, term()}).
start_listener({Proto, ListenOn, Options}) ->
    StartRet = start_listener(Proto, ListenOn, Options),
    case StartRet of
        {ok, _} -> io:format("Start mqtt:~s listener on ~s successfully.~n",
                             [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start mqtt:~s listener on ~s - ~0p~n!",
                      [Proto, format(ListenOn), Reason])
    end,
    StartRet.

%% Start MQTT/TCP listener
-spec(start_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> {ok, pid()} | {error, term()}).
start_listener(tcp, ListenOn, Options) ->
    start_mqtt_listener('mqtt:tcp', ListenOn, Options);

%% Start MQTT/TLS listener
start_listener(Proto, ListenOn, Options) when Proto == ssl; Proto == tls ->
    start_mqtt_listener('mqtt:ssl', ListenOn, Options);

%% Start MQTT/WS listener
start_listener(Proto, ListenOn, Options) when Proto == http; Proto == ws ->
    start_http_listener(fun cowboy:start_clear/3, 'mqtt:ws', ListenOn,
                        ranch_opts(Options), ws_opts(Options));

%% Start MQTT/WSS listener
start_listener(Proto, ListenOn, Options) when Proto == https; Proto == wss ->
    start_http_listener(fun cowboy:start_tls/3, 'mqtt:wss', ListenOn,
                        ranch_opts(Options), ws_opts(Options)).

start_mqtt_listener(Name, ListenOn, Options) ->
    SockOpts = esockd:parse_opt(Options),
    esockd:open(Name, ListenOn, merge_default(SockOpts),
                {emqx_connection, start_link, [Options -- SockOpts]}).

start_http_listener(Start, Name, ListenOn, RanchOpts, ProtoOpts) ->
    Start(ws_name(Name, ListenOn), with_port(ListenOn, RanchOpts), ProtoOpts).

mqtt_path(Options) ->
    proplists:get_value(mqtt_path, Options, "/mqtt").

ws_opts(Options) ->
    WsPaths = [{mqtt_path(Options), emqx_ws_connection, Options}],
    Dispatch = cowboy_router:compile([{'_', WsPaths}]),
    ProxyProto = proplists:get_value(proxy_protocol, Options, false),
    #{env => #{dispatch => Dispatch}, proxy_header => ProxyProto}.

ranch_opts(Options) ->
    NumAcceptors = proplists:get_value(acceptors, Options, 4),
    MaxConnections = proplists:get_value(max_connections, Options, 1024),
    TcpOptions = proplists:get_value(tcp_options, Options, []),
    RanchOpts = #{num_acceptors => NumAcceptors,
                  max_connections => MaxConnections,
                  socket_opts => TcpOptions},
    case proplists:get_value(ssl_options, Options) of
        undefined  -> RanchOpts;
        SslOptions -> RanchOpts#{socket_opts => TcpOptions ++ SslOptions}
    end.

with_port(Port, Opts = #{socket_opts := SocketOption}) when is_integer(Port) ->
    Opts#{socket_opts => [{port, Port}| SocketOption]};
with_port({Addr, Port}, Opts = #{socket_opts := SocketOption}) ->
    Opts#{socket_opts => [{ip, Addr}, {port, Port}| SocketOption]}.

%% @doc Restart all listeners
-spec(restart() -> ok).
restart() ->
    lists:foreach(fun restart_listener/1, emqx:get_env(listeners, [])).

-spec(restart_listener(listener()) -> any()).
restart_listener({Proto, ListenOn, Options}) ->
    restart_listener(Proto, ListenOn, Options).

-spec(restart_listener(esockd:proto(), esockd:listen_on(), [esockd:option()]) -> any()).
restart_listener(tcp, ListenOn, _Options) ->
    esockd:reopen('mqtt:tcp', ListenOn);
restart_listener(Proto, ListenOn, _Options) when Proto == ssl; Proto == tls ->
    esockd:reopen('mqtt:ssl', ListenOn);
restart_listener(Proto, ListenOn, Options) when Proto == http; Proto == ws ->
    cowboy:stop_listener(ws_name('mqtt:ws', ListenOn)),
    start_listener(Proto, ListenOn, Options);
restart_listener(Proto, ListenOn, Options) when Proto == https; Proto == wss ->
    cowboy:stop_listener(ws_name('mqtt:wss', ListenOn)),
    start_listener(Proto, ListenOn, Options);
restart_listener(Proto, ListenOn, _Opts) ->
    esockd:reopen(Proto, ListenOn).

%% @doc Stop all listeners.
-spec(stop() -> ok).
stop() ->
    lists:foreach(fun stop_listener/1, emqx:get_env(listeners, [])).

-spec(stop_listener(listener()) -> ok | {error, term()}).
stop_listener({Proto, ListenOn, Opts}) ->
    StopRet = stop_listener(Proto, ListenOn, Opts),
    case StopRet of
        ok -> io:format("Stop mqtt:~s listener on ~s successfully.~n",
                        [Proto, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop mqtt:~s listener on ~s - ~p~n.",
                      [Proto, format(ListenOn), Reason])
    end,
    StopRet.

-spec(stop_listener(esockd:proto(), esockd:listen_on(), [esockd:option()])
      -> ok | {error, term()}).
stop_listener(tcp, ListenOn, _Opts) ->
    esockd:close('mqtt:tcp', ListenOn);
stop_listener(Proto, ListenOn, _Opts) when Proto == ssl; Proto == tls ->
    esockd:close('mqtt:ssl', ListenOn);
stop_listener(Proto, ListenOn, _Opts) when Proto == http; Proto == ws ->
    cowboy:stop_listener(ws_name('mqtt:ws', ListenOn));
stop_listener(Proto, ListenOn, _Opts) when Proto == https; Proto == wss ->
    cowboy:stop_listener(ws_name('mqtt:wss', ListenOn));
stop_listener(Proto, ListenOn, _Opts) ->
    esockd:close(Proto, ListenOn).

merge_default(Options) ->
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(?MQTT_SOCKOPTS, TcpOpts)} | Options1];
        false ->
            [{tcp_options, ?MQTT_SOCKOPTS} | Options]
    end.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).

ws_name(Name, {_Addr, Port}) ->
    ws_name(Name, Port);
ws_name(Name, Port) ->
    list_to_atom(lists:concat([Name, ":", Port])).
