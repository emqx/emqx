%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

%% APIs
-export([ list/0
        , start/0
        , restart/0
        , stop/0
        , is_running/1
        ]).

-export([ start_listener/1
        , start_listener/3
        , stop_listener/1
        , stop_listener/3
        , restart_listener/1
        , restart_listener/3
        , has_enabled_listener_conf_by_type/1
        ]).

-export([ listener_id/2
        , parse_listener_id/1
        ]).

%% @doc List configured listeners.
-spec(list() -> [{ListenerId :: atom(), ListenerConf :: map()}]).
list() ->
    [{listener_id(ZoneName, LName), LConf} || {ZoneName, LName, LConf} <- do_list()].

do_list() ->
    Listeners = maps:to_list(emqx:get_config([listeners], #{})),
    lists:append([list(Type, maps:to_list(Conf)) || {Type, Conf} <- Listeners]).

list(Type, Conf) ->
    [begin
        Running = is_running(Type, listener_id(Type, LName), LConf),
        {Type, LName, maps:put(running, Running, LConf)}
     end || {LName, LConf} <- Conf, is_map(LConf)].

-spec is_running(ListenerId :: atom()) -> boolean() | {error, no_found}.
is_running(ListenerId) ->
    case lists:filtermap(fun({_Zone, Id, #{running := IsRunning}}) ->
                                 Id =:= ListenerId andalso {true, IsRunning}
                         end, do_list()) of
        [IsRunning] -> IsRunning;
        [] -> {error, not_found}
    end.

is_running(Type, ListenerId, #{bind := ListenOn}) when Type =:= tcp; Type =:= ssl ->
    try esockd:listener({ListenerId, ListenOn}) of
        Pid when is_pid(Pid)->
            true
    catch _:_ ->
        false
    end;

is_running(Type, ListenerId, _Conf) when Type =:= ws; Type =:= wss ->
    try
        Info = ranch:info(ListenerId),
        proplists:get_value(status, Info) =:= running
    catch _:_ ->
        false
    end;

is_running(quic, _ListenerId, _Conf)->
    %% TODO: quic support
    {error, no_found}.

%% @doc Start all listeners.
-spec(start() -> ok).
start() ->
    foreach_listeners(fun start_listener/3).

-spec start_listener(atom()) -> ok | {error, term()}.
start_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun start_listener/3).

-spec start_listener(atom(), atom(), map()) -> ok | {error, term()}.
start_listener(Type, ListenerName, #{bind := Bind} = Conf) ->
    case do_start_listener(Type, ListenerName, Conf) of
        {ok, {skipped, Reason}} when Reason =:= listener_disabled;
                                     Reason =:= quic_app_missing ->
            console_print("- Skip - starting listener ~s on ~s ~n due to ~p",
                          [listener_id(Type, ListenerName), format_addr(Bind), Reason]);
        {ok, _} ->
            console_print("Start listener ~s on ~s successfully.~n",
                [listener_id(Type, ListenerName), format_addr(Bind)]);
        {error, {already_started, Pid}} ->
            {error, {already_started, Pid}};
        {error, Reason} ->
            ?ELOG("Failed to start listener ~s on ~s: ~0p~n",
                  [listener_id(Type, ListenerName), format_addr(Bind), Reason]),
            error(Reason)
    end.

%% @doc Restart all listeners
-spec(restart() -> ok).
restart() ->
    foreach_listeners(fun restart_listener/3).

-spec(restart_listener(atom()) -> ok | {error, term()}).
restart_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun restart_listener/3).

-spec(restart_listener(atom(), atom(), map()) -> ok | {error, term()}).
restart_listener(Type, ListenerName, Conf) ->
    case stop_listener(Type, ListenerName, Conf) of
        ok -> start_listener(Type, ListenerName, Conf);
        Error -> Error
    end.

%% @doc Stop all listeners.
-spec(stop() -> ok).
stop() ->
    foreach_listeners(fun stop_listener/3).

-spec(stop_listener(atom()) -> ok | {error, term()}).
stop_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun stop_listener/3).

-spec(stop_listener(atom(), atom(), map()) -> ok | {error, term()}).
stop_listener(Type, ListenerName, #{bind := ListenOn}) when Type == tcp; Type == ssl ->
    esockd:close(listener_id(Type, ListenerName), ListenOn);
stop_listener(Type, ListenerName, _Conf) when Type == ws; Type == wss ->
    cowboy:stop_listener(listener_id(Type, ListenerName));
stop_listener(quic, ListenerName, _Conf) ->
    quicer:stop_listener(listener_id(quic, ListenerName)).

-ifndef(TEST).
console_print(Fmt, Args) -> ?ULOG(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.

%% Start MQTT/TCP listener
-spec(do_start_listener(atom(), atom(), map())
      -> {ok, pid() | {skipped, atom()}} | {error, term()}).
do_start_listener(_Type, _ListenerName, #{enabled := false}) ->
    {ok, {skipped, listener_disabled}};
do_start_listener(Type, ListenerName, #{bind := ListenOn} = Opts)
        when Type == tcp; Type == ssl ->
    esockd:open(listener_id(Type, ListenerName), ListenOn, merge_default(esockd_opts(Type, Opts)),
                {emqx_connection, start_link,
                    [#{listener => {Type, ListenerName},
                       zone => zone(Opts)}]});

%% Start MQTT/WS listener
do_start_listener(Type, ListenerName, #{bind := ListenOn} = Opts)
        when Type == ws; Type == wss ->
    Id = listener_id(Type, ListenerName),
    RanchOpts = ranch_opts(Type, ListenOn, Opts),
    WsOpts = ws_opts(Type, ListenerName, Opts),
    case Type of
        ws -> cowboy:start_clear(Id, RanchOpts, WsOpts);
        wss -> cowboy:start_tls(Id, RanchOpts, WsOpts)
    end;

%% Start MQTT/QUIC listener
do_start_listener(quic, ListenerName, #{bind := ListenOn} = Opts) ->
    case [ A || {quicer, _, _} = A<-application:which_applications() ] of
        [_] ->
            DefAcceptors = erlang:system_info(schedulers_online) * 8,
            ListenOpts = [ {cert, maps:get(certfile, Opts)}
                         , {key, maps:get(keyfile, Opts)}
                         , {alpn, ["mqtt"]}
                         , {conn_acceptors, maps:get(acceptors, Opts, DefAcceptors)}
                         , {idle_timeout_ms, emqx_config:get_zone_conf(zone(Opts),
                                                [mqtt, idle_timeout])}
                         ],
            ConnectionOpts = #{ conn_callback => emqx_quic_connection
                              , peer_unidi_stream_count => 1
                              , peer_bidi_stream_count => 10
                              , zone => zone(Opts)
                              , listener => {quic, ListenerName}
                              },
            StreamOpts = [{stream_callback, emqx_quic_stream}],
            quicer:start_listener(listener_id(quic, ListenerName),
                                  port(ListenOn), {ListenOpts, ConnectionOpts, StreamOpts});
        [] ->
            {ok, {skipped, quic_app_missing}}
    end.

esockd_opts(Type, Opts0) ->
    Opts1 = maps:with([acceptors, max_connections, proxy_protocol, proxy_protocol_timeout], Opts0),
    Opts2 = case emqx_config:get_zone_conf(zone(Opts0), [rate_limit, max_conn_rate]) of
        infinity -> Opts1;
        Rate -> Opts1#{max_conn_rate => Rate}
    end,
    Opts3 = Opts2#{access_rules => esockd_access_rules(maps:get(access_rules, Opts0, []))},
    maps:to_list(case Type of
        tcp -> Opts3#{tcp_options => tcp_opts(Opts0)};
        ssl -> Opts3#{ssl_options => ssl_opts(Opts0), tcp_options => tcp_opts(Opts0)}
    end).

ws_opts(Type, ListenerName, Opts) ->
    WsPaths = [{maps:get(mqtt_path, Opts, "/mqtt"), emqx_ws_connection,
        #{zone => zone(Opts), listener => {Type, ListenerName}}}],
    Dispatch = cowboy_router:compile([{'_', WsPaths}]),
    ProxyProto = maps:get(proxy_protocol, Opts, false),
    #{env => #{dispatch => Dispatch}, proxy_header => ProxyProto}.

ranch_opts(Type, ListenOn, Opts) ->
    NumAcceptors = maps:get(acceptors, Opts, 4),
    MaxConnections = maps:get(max_connections, Opts, 1024),
    SocketOpts = case Type of
        wss -> tcp_opts(Opts) ++ proplists:delete(handshake_timeout, ssl_opts(Opts));
        ws -> tcp_opts(Opts)
    end,
    #{num_acceptors => NumAcceptors,
      max_connections => MaxConnections,
      handshake_timeout => maps:get(handshake_timeout, Opts, 15000),
      socket_opts => ip_port(ListenOn) ++
            %% cowboy don't allow us to set 'reuseaddr'
            proplists:delete(reuseaddr, SocketOpts)}.

ip_port(Port) when is_integer(Port) ->
    [{port, Port}];
ip_port({Addr, Port}) ->
    [{ip, Addr}, {port, Port}].

port(Port) when is_integer(Port) -> Port;
port({_Addr, Port}) when is_integer(Port) -> Port.

esockd_access_rules(StrRules) ->
    Access = fun(S) ->
        [A, CIDR] = string:tokens(S, " "),
        {list_to_atom(A), case CIDR of "all" -> all; _ -> CIDR end}
    end,
    [Access(R) || R <- StrRules].

merge_default(Options) ->
    case lists:keytake(tcp_options, 1, Options) of
        {value, {tcp_options, TcpOpts}, Options1} ->
            [{tcp_options, emqx_misc:merge_opts(?MQTT_SOCKOPTS, TcpOpts)} | Options1];
        false ->
            [{tcp_options, ?MQTT_SOCKOPTS} | Options]
    end.

format_addr(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format_addr({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format_addr({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).

listener_id(Type, ListenerName) ->
    list_to_atom(lists:append([atom_to_list(Type), ":", atom_to_list(ListenerName)])).

parse_listener_id(Id) ->
    try
        [Zone, Listen] = string:split(atom_to_list(Id), ":", leading),
        {list_to_existing_atom(Zone), list_to_existing_atom(Listen)}
    catch
        _ : _ -> error({invalid_listener_id, Id})
    end.

zone(Opts) ->
    maps:get(zone, Opts, undefined).

ssl_opts(Opts) ->
    maps:to_list(
        emqx_tls_lib:drop_tls13_for_old_otp(
            maps:without([enable],
                maps:get(ssl, Opts, #{})))).

tcp_opts(Opts) ->
    maps:to_list(
        maps:without([active_n],
            maps:get(tcp, Opts, #{}))).

foreach_listeners(Do) ->
    lists:foreach(
        fun({ZoneName, LName, LConf}) ->
                Do(ZoneName, LName, LConf)
        end, do_list()).

has_enabled_listener_conf_by_type(Type) ->
    lists:any(
        fun({Type0, _LName, LConf}) when is_map(LConf) ->
                Type =:= Type0 andalso maps:get(enabled, LConf, true)
        end, do_list()).

apply_on_listener(ListenerId, Do) ->
    {Type, ListenerName} = parse_listener_id(ListenerId),
    case emqx_config:find_listener_conf(Type, ListenerName, []) of
        {not_found, _, _} -> error({listener_config_not_found, Type, ListenerName});
        {ok, Conf} -> Do(Type, ListenerName, Conf)
    end.
