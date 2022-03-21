%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-elvis([{elvis_style, dont_repeat_yourself, #{min_complexity => 10000}}]).

-include("emqx_mqtt.hrl").
-include("logger.hrl").

%% APIs
-export([
    list/0,
    start/0,
    restart/0,
    stop/0,
    is_running/1,
    current_conns/2,
    max_conns/2,
    id_example/0
]).

-export([
    start_listener/1,
    start_listener/3,
    stop_listener/1,
    stop_listener/3,
    restart_listener/1,
    restart_listener/3,
    has_enabled_listener_conf_by_type/1
]).

-export([
    listener_id/2,
    parse_listener_id/1
]).

-export([post_config_update/5]).

-export([format_addr/1]).

-define(CONF_KEY_PATH, [listeners]).
-define(TYPES_STRING, ["tcp", "ssl", "ws", "wss", "quic"]).

-spec id_example() -> atom().
id_example() ->
    id_example(list()).

id_example([]) ->
    {ID, _} = hd(list()),
    ID;
id_example([{'tcp:default', _} | _]) ->
    'tcp:default';
id_example([_ | Listeners]) ->
    id_example(Listeners).

%% @doc List configured listeners.
-spec list() -> [{ListenerId :: atom(), ListenerConf :: map()}].
list() ->
    [{listener_id(Type, LName), LConf} || {Type, LName, LConf} <- do_list()].

do_list() ->
    Listeners = maps:to_list(emqx:get_config([listeners], #{})),
    lists:append([list(Type, maps:to_list(Conf)) || {Type, Conf} <- Listeners]).

list(Type, Conf) ->
    [
        begin
            Running = is_running(Type, listener_id(Type, LName), LConf),
            {Type, LName, maps:put(running, Running, LConf)}
        end
     || {LName, LConf} <- Conf, is_map(LConf)
    ].

-spec is_running(ListenerId :: atom()) -> boolean() | {error, no_found}.
is_running(ListenerId) ->
    case
        lists:filtermap(
            fun({_Type, Id, #{running := IsRunning}}) ->
                Id =:= ListenerId andalso {true, IsRunning}
            end,
            do_list()
        )
    of
        [IsRunning] -> IsRunning;
        [] -> {error, not_found}
    end.

is_running(Type, ListenerId, #{bind := ListenOn}) when Type =:= tcp; Type =:= ssl ->
    try esockd:listener({ListenerId, ListenOn}) of
        Pid when is_pid(Pid) ->
            true
    catch
        _:_ ->
            false
    end;
is_running(Type, ListenerId, _Conf) when Type =:= ws; Type =:= wss ->
    try
        Info = ranch:info(ListenerId),
        proplists:get_value(status, Info) =:= running
    catch
        _:_ ->
            false
    end;
is_running(quic, _ListenerId, _Conf) ->
    %% TODO: quic support
    {error, no_found}.

current_conns(ID, ListenOn) ->
    {Type, Name} = parse_listener_id(ID),
    current_conns(Type, Name, ListenOn).

current_conns(Type, Name, ListenOn) when Type == tcp; Type == ssl ->
    esockd:get_current_connections({listener_id(Type, Name), ListenOn});
current_conns(Type, Name, _ListenOn) when Type =:= ws; Type =:= wss ->
    proplists:get_value(all_connections, ranch:info(listener_id(Type, Name)));
current_conns(_, _, _) ->
    {error, not_support}.

max_conns(ID, ListenOn) ->
    {Type, Name} = parse_listener_id(ID),
    max_conns(Type, Name, ListenOn).

max_conns(Type, Name, ListenOn) when Type == tcp; Type == ssl ->
    esockd:get_max_connections({listener_id(Type, Name), ListenOn});
max_conns(Type, Name, _ListenOn) when Type =:= ws; Type =:= wss ->
    proplists:get_value(max_connections, ranch:info(listener_id(Type, Name)));
max_conns(_, _, _) ->
    {error, not_support}.

%% @doc Start all listeners.
-spec start() -> ok.
start() ->
    %% The ?MODULE:start/0 will be called by emqx_app when emqx get started,
    %% so we install the config handler here.
    ok = emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    foreach_listeners(fun start_listener/3).

-spec start_listener(atom()) -> ok | {error, term()}.
start_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun start_listener/3).

-spec start_listener(atom(), atom(), map()) -> ok | {error, term()}.
start_listener(Type, ListenerName, #{bind := Bind} = Conf) ->
    case do_start_listener(Type, ListenerName, Conf) of
        {ok, {skipped, Reason}} when
            Reason =:= listener_disabled;
            Reason =:= quic_app_missing
        ->
            console_print(
                "Listener ~ts is NOT started due to: ~p~n.",
                [listener_id(Type, ListenerName), Reason]
            );
        {ok, _} ->
            console_print(
                "Listener ~ts on ~ts started.~n",
                [listener_id(Type, ListenerName), format_addr(Bind)]
            );
        {error, {already_started, Pid}} ->
            {error, {already_started, Pid}};
        {error, Reason} ->
            ?ELOG(
                "Failed to start listener ~ts on ~ts: ~0p~n",
                [listener_id(Type, ListenerName), format_addr(Bind), Reason]
            ),
            error(Reason)
    end.

%% @doc Restart all listeners
-spec restart() -> ok.
restart() ->
    foreach_listeners(fun restart_listener/3).

-spec restart_listener(atom()) -> ok | {error, term()}.
restart_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun restart_listener/3).

-spec restart_listener(atom(), atom(), map()) -> ok | {error, term()}.
restart_listener(Type, ListenerName, {OldConf, NewConf}) ->
    restart_listener(Type, ListenerName, OldConf, NewConf);
restart_listener(Type, ListenerName, Conf) ->
    restart_listener(Type, ListenerName, Conf, Conf).

restart_listener(Type, ListenerName, OldConf, NewConf) ->
    case do_stop_listener(Type, ListenerName, OldConf) of
        ok -> start_listener(Type, ListenerName, NewConf);
        {error, not_found} -> start_listener(Type, ListenerName, NewConf);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop all listeners.
-spec stop() -> ok.
stop() ->
    %% The ?MODULE:stop/0 will be called by emqx_app when emqx is going to shutdown,
    %% so we uninstall the config handler here.
    _ = emqx_config_handler:remove_handler(?CONF_KEY_PATH),
    foreach_listeners(fun stop_listener/3).

-spec stop_listener(atom()) -> ok | {error, term()}.
stop_listener(ListenerId) ->
    apply_on_listener(ListenerId, fun stop_listener/3).

stop_listener(Type, ListenerName, #{bind := Bind} = Conf) ->
    case do_stop_listener(Type, ListenerName, Conf) of
        ok ->
            console_print(
                "Listener ~ts on ~ts stopped.~n",
                [listener_id(Type, ListenerName), format_addr(Bind)]
            ),
            ok;
        {error, Reason} ->
            ?ELOG(
                "Failed to stop listener ~ts on ~ts: ~0p~n",
                [listener_id(Type, ListenerName), format_addr(Bind), Reason]
            ),
            {error, Reason}
    end.

-spec do_stop_listener(atom(), atom(), map()) -> ok | {error, term()}.
do_stop_listener(Type, ListenerName, #{bind := ListenOn}) when Type == tcp; Type == ssl ->
    esockd:close(listener_id(Type, ListenerName), ListenOn);
do_stop_listener(Type, ListenerName, _Conf) when Type == ws; Type == wss ->
    cowboy:stop_listener(listener_id(Type, ListenerName));
do_stop_listener(quic, ListenerName, _Conf) ->
    quicer:stop_listener(listener_id(quic, ListenerName)).

-ifndef(TEST).
console_print(Fmt, Args) -> ?ULOG(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.

%% Start MQTT/TCP listener
-spec do_start_listener(atom(), atom(), map()) ->
    {ok, pid() | {skipped, atom()}} | {error, term()}.
do_start_listener(_Type, _ListenerName, #{enabled := false}) ->
    {ok, {skipped, listener_disabled}};
do_start_listener(Type, ListenerName, #{bind := ListenOn} = Opts) when
    Type == tcp; Type == ssl
->
    esockd:open(
        listener_id(Type, ListenerName),
        ListenOn,
        merge_default(esockd_opts(Type, Opts)),
        {emqx_connection, start_link, [
            #{
                listener => {Type, ListenerName},
                zone => zone(Opts),
                limiter => limiter(Opts)
            }
        ]}
    );
%% Start MQTT/WS listener
do_start_listener(Type, ListenerName, #{bind := ListenOn} = Opts) when
    Type == ws; Type == wss
->
    Id = listener_id(Type, ListenerName),
    RanchOpts = ranch_opts(Type, ListenOn, Opts),
    WsOpts = ws_opts(Type, ListenerName, Opts),
    case Type of
        ws -> cowboy:start_clear(Id, RanchOpts, WsOpts);
        wss -> cowboy:start_tls(Id, RanchOpts, WsOpts)
    end;
%% Start MQTT/QUIC listener
do_start_listener(quic, ListenerName, #{bind := ListenOn} = Opts) ->
    case [A || {quicer, _, _} = A <- application:which_applications()] of
        [_] ->
            DefAcceptors = erlang:system_info(schedulers_online) * 8,
            ListenOpts = [
                {cert, maps:get(certfile, Opts)},
                {key, maps:get(keyfile, Opts)},
                {alpn, ["mqtt"]},
                {conn_acceptors, lists:max([DefAcceptors, maps:get(acceptors, Opts, 0)])},
                {idle_timeout_ms,
                    lists:max([
                        emqx_config:get_zone_conf(zone(Opts), [mqtt, idle_timeout]) * 3,
                        timer:seconds(maps:get(idle_timeout, Opts))
                    ])}
            ],
            ConnectionOpts = #{
                conn_callback => emqx_quic_connection,
                peer_unidi_stream_count => 1,
                peer_bidi_stream_count => 10,
                zone => zone(Opts),
                listener => {quic, ListenerName},
                limiter => limiter(Opts)
            },
            StreamOpts = [{stream_callback, emqx_quic_stream}],
            quicer:start_listener(
                listener_id(quic, ListenerName),
                port(ListenOn),
                {ListenOpts, ConnectionOpts, StreamOpts}
            );
        [] ->
            {ok, {skipped, quic_app_missing}}
    end.

delete_authentication(Type, ListenerName, _Conf) ->
    emqx_authentication:delete_chain(listener_id(Type, ListenerName)).

%% Update the listeners at runtime
post_config_update(_, _Req, NewListeners, OldListeners, _AppEnvs) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_listeners(NewListeners, OldListeners),
    perform_listener_changes(fun stop_listener/3, Removed),
    perform_listener_changes(fun delete_authentication/3, Removed),
    perform_listener_changes(fun start_listener/3, Added),
    perform_listener_changes(fun restart_listener/3, Updated).

perform_listener_changes(Action, MapConfs) ->
    lists:foreach(
        fun({Id, Conf}) ->
            {Type, Name} = parse_listener_id(Id),
            Action(Type, Name, Conf)
        end,
        maps:to_list(MapConfs)
    ).

diff_listeners(NewListeners, OldListeners) ->
    emqx_map_lib:diff_maps(flatten_listeners(NewListeners), flatten_listeners(OldListeners)).

flatten_listeners(Conf0) ->
    maps:from_list(
        lists:append([
            do_flatten_listeners(Type, Conf)
         || {Type, Conf} <- maps:to_list(Conf0)
        ])
    ).

do_flatten_listeners(Type, Conf0) ->
    [
        {listener_id(Type, Name), maps:remove(authentication, Conf)}
     || {Name, Conf} <- maps:to_list(Conf0)
    ].

esockd_opts(Type, Opts0) ->
    Opts1 = maps:with([acceptors, max_connections, proxy_protocol, proxy_protocol_timeout], Opts0),
    Limiter = limiter(Opts0),
    Opts2 =
        case maps:get(connection, Limiter, undefined) of
            undefined ->
                Opts1;
            BucketName ->
                Opts1#{
                    limiter => emqx_esockd_htb_limiter:new_create_options(connection, BucketName)
                }
        end,
    Opts3 = Opts2#{
        access_rules => esockd_access_rules(maps:get(access_rules, Opts0, [])),
        tune_fun => {emqx_olp, backoff_new_conn, [zone(Opts0)]}
    },
    maps:to_list(
        case Type of
            tcp -> Opts3#{tcp_options => tcp_opts(Opts0)};
            ssl -> Opts3#{ssl_options => ssl_opts(Opts0), tcp_options => tcp_opts(Opts0)}
        end
    ).

ws_opts(Type, ListenerName, Opts) ->
    WsPaths = [
        {maps:get(mqtt_path, Opts, "/mqtt"), emqx_ws_connection, #{
            zone => zone(Opts),
            listener => {Type, ListenerName},
            limiter => limiter(Opts)
        }}
    ],
    Dispatch = cowboy_router:compile([{'_', WsPaths}]),
    ProxyProto = maps:get(proxy_protocol, Opts, false),
    #{env => #{dispatch => Dispatch}, proxy_header => ProxyProto}.

ranch_opts(Type, ListenOn, Opts) ->
    NumAcceptors = maps:get(acceptors, Opts, 4),
    MaxConnections = maps:get(max_connections, Opts, 1024),
    SocketOpts =
        case Type of
            wss -> tcp_opts(Opts) ++ proplists:delete(handshake_timeout, ssl_opts(Opts));
            ws -> tcp_opts(Opts)
        end,
    #{
        num_acceptors => NumAcceptors,
        max_connections => MaxConnections,
        handshake_timeout => maps:get(handshake_timeout, Opts, 15000),
        socket_opts => ip_port(ListenOn) ++
            %% cowboy don't allow us to set 'reuseaddr'
            proplists:delete(reuseaddr, SocketOpts)
    }.

ip_port(Port) when is_integer(Port) ->
    [{port, Port}];
ip_port({Addr, Port}) ->
    [{ip, Addr}, {port, Port}].

port(Port) when is_integer(Port) -> Port;
port({_Addr, Port}) when is_integer(Port) -> Port.

esockd_access_rules(StrRules) ->
    Access = fun(S) ->
        [A, CIDR] = string:tokens(S, " "),
        {
            list_to_atom(A),
            case CIDR of
                "all" -> all;
                _ -> CIDR
            end
        }
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
    io_lib:format(":~w", [Port]);
%% Print only the port number when bound on all interfaces
format_addr({{0, 0, 0, 0}, Port}) ->
    format_addr(Port);
format_addr({{0, 0, 0, 0, 0, 0, 0, 0}, Port}) ->
    format_addr(Port);
format_addr({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~ts:~w", [Addr, Port]);
format_addr({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~ts:~w", [inet:ntoa(Addr), Port]).

listener_id(Type, ListenerName) ->
    list_to_atom(lists:append([str(Type), ":", str(ListenerName)])).

parse_listener_id(Id) ->
    case string:split(str(Id), ":", leading) of
        [Type, Name] ->
            case lists:member(Type, ?TYPES_STRING) of
                true -> {list_to_existing_atom(Type), list_to_atom(Name)};
                false -> {error, {invalid_listener_id, Id}}
            end;
        _ ->
            {error, {invalid_listener_id, Id}}
    end.

zone(Opts) ->
    maps:get(zone, Opts, undefined).

limiter(Opts) ->
    maps:get(limiter, Opts).

ssl_opts(Opts) ->
    maps:to_list(
        emqx_tls_lib:drop_tls13_for_old_otp(
            maps:without(
                [enable],
                maps:get(ssl, Opts, #{})
            )
        )
    ).

tcp_opts(Opts) ->
    maps:to_list(
        maps:without(
            [active_n],
            maps:get(tcp, Opts, #{})
        )
    ).

foreach_listeners(Do) ->
    lists:foreach(
        fun({Type, LName, LConf}) ->
            Do(Type, LName, LConf)
        end,
        do_list()
    ).

has_enabled_listener_conf_by_type(Type) ->
    lists:any(
        fun({Type0, _LName, LConf}) when is_map(LConf) ->
            Type =:= Type0 andalso maps:get(enabled, LConf, true)
        end,
        do_list()
    ).

apply_on_listener(ListenerId, Do) ->
    {Type, ListenerName} = parse_listener_id(ListenerId),
    case emqx_config:find_listener_conf(Type, ListenerName, []) of
        {not_found, _, _} -> error({listener_config_not_found, Type, ListenerName});
        {ok, Conf} -> Do(Type, ListenerName, Conf)
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
