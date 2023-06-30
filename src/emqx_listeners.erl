%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , ensure_all_started/0
        , restart/0
        , stop/0
        ]).

-export([ start_listener/1
        , start_listener/3
        , stop_listener/1
        , update_listeners_env/2
        , restart_listener/1
        , restart_listener/3
        ]).

-export([ find_id_by_listen_on/1
        , find_by_listen_on/1
        , find_by_id/1
        , identifier/1
        , format_listen_on/1
        ]).

-type(listener_name() :: binary()).
-type(listener_id() :: binary()).
-type(listener() :: #{ name := listener_name()
                     , proto := esockd:proto()
                     , listen_on := esockd:listen_on()
                     , opts := [esockd:option()]
                     , any() => term()
                     }).

%% @doc Find listener identifier by listen-on.
%% Return empty string (binary) if listener is not found in config.
-spec(find_id_by_listen_on(esockd:listen_on()) -> binary() | false).
find_id_by_listen_on(ListenOn) ->
    case find_by_listen_on(ListenOn) of
        false -> false;
        L -> identifier(L)
    end.

%% @doc Find listener by listen-on.
%% Return 'false' if not found.
-spec(find_by_listen_on(esockd:listen_on()) -> listener() | false).
find_by_listen_on(ListenOn) ->
    find_by_listen_on(ListenOn, emqx:get_env(listeners, [])).

%% @doc Find listener by identifier.
%% Return 'false' if not found.
-spec(find_by_id(string() | binary()) -> listener() | false).
find_by_id(Id) ->
    find_by_id(iolist_to_binary(Id), emqx:get_env(listeners, [])).

%% @doc Return the ID of the given listener.
-spec identifier(listener()) -> listener_id().
identifier(#{proto := Proto, name := Name}) ->
    identifier(Proto, Name).

%% @doc Start all listeners.
-spec(start() -> ok).
start() ->
    lists:foreach(fun start_listener/1, emqx:get_env(listeners, [])).

%% @doc Ensure all configured listeners are started.
%% Raise exception if any of them failed to start.
-spec(ensure_all_started() -> ok).
ensure_all_started() ->
    ensure_all_started(emqx:get_env(listeners, []), []).

ensure_all_started([], []) -> ok;
ensure_all_started([], Failed) -> error(Failed);
ensure_all_started([L | Rest], Results) ->
    #{proto := Proto, listen_on := ListenOn, opts := Options0} = L,
    Options = [{listener_id, identifier(L)} | Options0],
    NewResults =
        case start_listener(Proto, ListenOn, Options) of
            {ok, _Pid} ->
                Results;
            {error, {already_started, _Pid}} ->
                Results;
            {error, Reason} ->
                [{identifier(L), Reason} | Results]
        end,
    ensure_all_started(Rest, NewResults).

%% @doc Format address:port for logging.
-spec(format_listen_on(esockd:listen_on()) -> [char()]).
format_listen_on(ListenOn) -> format(ListenOn).

-spec(start_listener(listener()) -> ok).
start_listener(#{proto := Proto, name := Name, listen_on := ListenOn, opts := Opts0}) ->
    ID = identifier(Proto, Name),
    Opts = [{listener_id, ID} | Opts0],
    case start_listener(Proto, ListenOn, Opts) of
        {ok, _} ->
            console_print("Start ~s listener on ~s successfully.~n", [ID, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start mqtt listener ~s on ~s: ~0p~n",
                      [ID, format(ListenOn), Reason]),
            error(Reason)
    end.

-ifndef(TEST).
console_print(Fmt, Args) ->
    io:format(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.

%% Start MQTT/TCP listener
-spec(start_listener(esockd:proto(), esockd:listen_on(), [ esockd:option()
                                                         | {listener_id, binary()}])
      -> {ok, pid()} | {error, term()}).
start_listener(tcp, ListenOn, Options) ->
    start_mqtt_listener('mqtt:tcp', ListenOn, Options);

%% Start MQTT/TLS listener
start_listener(Proto, ListenOn, Options0) when Proto == ssl; Proto == tls ->
    ListenerID = proplists:get_value(listener_id, Options0),
    Options1 = proplists:delete(listener_id, Options0),
    Options2 = emqx_ocsp_cache:inject_sni_fun(ListenerID, Options1),
    Options3 = emqx_tls_lib:inject_root_fun(Options2),
    Options4 = emqx_tls_lib:inject_verify_fun(Options3),
    Options = emqx_tls_lib:maybe_drop_incompatible_options(Options4),
    ok = maybe_register_crl_urls(Options),
    start_mqtt_listener('mqtt:ssl', ListenOn, Options);

%% Start MQTT/WS listener
start_listener(Proto, ListenOn, Options) when Proto == http; Proto == ws ->
    start_http_listener(fun cowboy:start_clear/3, 'mqtt:ws', ListenOn,
                        ranch_opts(Options), ws_opts(Options));

%% Start MQTT/WSS listener
start_listener(Proto, ListenOn, Options) when Proto == https; Proto == wss ->
    start_http_listener(fun cowboy:start_tls/3, 'mqtt:wss', ListenOn,
                        ranch_opts(Options), ws_opts(Options)).

replace(Opts, Key, Value) -> [{Key, Value} | proplists:delete(Key, Opts)].

drop_tls13_for_old_otp(Options) ->
    case proplists:get_value(ssl_options, Options) of
        undefined -> Options;
        SslOpts ->
            SslOpts1 = emqx_tls_lib:drop_tls13_for_old_otp(SslOpts),
            replace(Options, ssl_options, SslOpts1)
    end.

start_mqtt_listener(Name, ListenOn, Options0) ->
    Options = drop_tls13_for_old_otp(Options0),
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

ranch_opts(Options0) ->
    Options = drop_tls13_for_old_otp(Options0),
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

update_listeners_env(Action, NewConf = #{name := NewName, proto := NewProto}) ->
    Listener = emqx:get_env(listeners, []),
    Listener1 = lists:filter(
        fun(#{name := Name, proto := Proto}) ->
            not (Name =:= NewName andalso Proto =:= NewProto)
        end, Listener),
    Listener2 =
        case Action of
            update -> [NewConf | Listener1];
            delete -> Listener1
        end,
    application:set_env(emqx, listeners, Listener2),
    ok.

%% @doc Restart all listeners
-spec(restart() -> ok).
restart() ->
    lists:foreach(fun restart_listener/1, emqx:get_env(listeners, [])).

-spec(restart_listener(listener() | string() | binary()) -> ok | {error, any()}).
restart_listener(#{proto := Proto, listen_on := ListenOn, opts := Options}) ->
    restart_listener(Proto, ListenOn, Options);
restart_listener(Identifier) ->
    case emqx_listeners:find_by_id(Identifier) of
        false    -> {error, {no_such_listener, Identifier}};
        Listener -> restart_listener(Listener)
    end.

-spec(restart_listener(esockd:proto(), esockd:listen_on(), [esockd:option()]) ->
    ok | {error, any()}).
restart_listener(tcp, ListenOn, _Options) ->
    esockd:reopen('mqtt:tcp', ListenOn);
restart_listener(Proto, ListenOn, _Options) when Proto == ssl; Proto == tls ->
    esockd:reopen('mqtt:ssl', ListenOn);
restart_listener(Proto, ListenOn, Options) when Proto == http; Proto == ws ->
    _ = cowboy:stop_listener(ws_name('mqtt:ws', ListenOn)),
    ok(start_listener(Proto, ListenOn, Options));
restart_listener(Proto, ListenOn, Options) when Proto == https; Proto == wss ->
    _ = cowboy:stop_listener(ws_name('mqtt:wss', ListenOn)),
    ok(start_listener(Proto, ListenOn, Options));
restart_listener(Proto, ListenOn, _Opts) ->
    esockd:reopen(Proto, ListenOn).

ok({ok, _}) -> ok;
ok(Other) -> Other.

%% @doc Stop all listeners.
-spec(stop() -> ok).
stop() ->
    lists:foreach(fun stop_listener/1, emqx:get_env(listeners, [])).

-spec(stop_listener(listener()) -> ok | {error, term()}).
stop_listener(#{proto := Proto, listen_on := ListenOn, opts := Opts}) ->
    stop_listener(Proto, ListenOn, Opts).

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

identifier(Proto, Name) when is_atom(Proto) ->
    identifier(atom_to_list(Proto), Name);
identifier(Proto, Name) ->
    iolist_to_binary(["mqtt", ":", Proto, ":", Name]).

find_by_listen_on(_ListenOn, []) -> false;
find_by_listen_on(ListenOn, [#{listen_on := ListenOn} = L | _]) -> L;
find_by_listen_on(ListenOn, [_ | Rest]) -> find_by_listen_on(ListenOn, Rest).

find_by_id(_Id, []) -> false;
find_by_id(Id, [L | Rest]) ->
    case identifier(L) =:= Id of
        true -> L;
        false -> find_by_id(Id, Rest)
    end.

%% @doc Called by Enterprise edition to dynamically reload configs.
-spec maybe_register_crl_urls([esockd:option()]) -> ok.
maybe_register_crl_urls(Options) ->
    CRLOptions = proplists:get_value(crl_options, Options, []),
    case proplists:get_bool(crl_check_enabled, CRLOptions) of
        false ->
            ok;
        true ->
            URLs =
                lists:usort(
                  [URL
                   || URL <- proplists:get_value(crl_cache_urls, CRLOptions, [])]),
            lists:foreach(
              fun(URL) ->
                emqx_crl_cache:refresh(URL)
              end,
              URLs)
    end.
