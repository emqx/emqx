%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_utils_conf).

-moduledoc """
    Utility functions for EMQX gateway configuration management.

    Each gateway is configured from three sources.

    * User-defined configuration map. This comes from the EMQX config and hash the following
      general structure:
      ```erlang
      #{
          ... some general configuration for the gateway ...
          listeners => #{
              dtls => #{
                  default => #{
                    ... listener configuration for dtls listener named "default" ...
                  }
              },
              udp => #{
                  default => #{
                    ... listener configuration for udp listener named "default" ...
                  }
              },
              ...
          }
      }
      ```

    * Gateway-specific static configuration (called "ModConf" in the code).
      This configuration defines which modules/callbacks to use
      to serve the gateway (some keys are required, some are optional),
      ```erlang
        #{
            dtls => #{
                frame_mod => emqx_frame_mod,
                chann_mod => emqx_channel_mod,
                connection_mod => ...
                esockd_proxy_opts => #{
                    connection_mod => ...
                }
                ...
            },
            ....
        }
      ```
      This static configuration may be different for different listener types.

    * Context. This configuration is passed to the callback modules as the `ctx` argument.
      Context holds runtime data. Callback modules (channel implementations) use `ctx` for
      accessing authentication, channel_manager, etc.

    In the runtime, the data is organized differently.

    We have several supervised TCP/UDP/DTLS/etc listeners, each of which receives its own configuration, like:
    * name
    * transport address to listen on
    * transport configuration (socket options, etc.)
    * transport callbacks and their arguments (application-level gateway handling)

    One can see that runtime configuration is merged and inversed:
    Listener configurations become top-level, while gateway general configuration becomes duplicated
    and nested into listener callbacks arguments.

    This module provides functions make such transformations.
""".

-export([
    to_rt_listener_configs/4,
    to_rt_listener_ids/2,
    rt_listener_id/1,
    rt_listener_id/2,
    diff_rt_listener_configs/2,
    listener_configs/1,
    ip_port/1
]).

-type gw_name() :: atom().

-type ranch_opts() :: term().
-type ws_opts() :: term().

-type socket_opts() :: list(tuple()).
-type listen_on() :: term().

-type listener_id() :: atom().
-type listener_name() :: atom().

-type esockd_type() :: tcp | ssl | udp | dtls.
-type cowboy_type() :: ws | wss.

-type listener_type() :: esockd_type() | cowboy_type().

-type esockd_opts() :: #{
    type => esockd_type(),
    socket_opts => socket_opts(),
    mfa => {atom(), atom(), list()}
}.
-type cowboy_opts() :: #{
    type => cowboy_type(),
    ranch_opts => ranch_opts(),
    ws_opts => ws_opts()
}.

-type listener_opts() :: {esockd, esockd_opts()} | {cowboy, cowboy_opts()}.

-type listener_runtime_config() :: #{
    listener_id := listener_id(),
    listen_on := listen_on(),
    listener_opts := listener_opts(),
    %% Used for error reporting
    original_listener_config := map()
}.

%% Minimal runtime information about a listener.
%% Listeners may be stopped/queried by this id.
-type listener_runtime_id() :: #{
    listener_id := listener_id(),
    listen_on := listen_on(),
    listener_type := {esockd, esockd_type()} | {cowboy, cowboy_type()}
}.

-export_type([
    gw_name/0,
    listener_runtime_config/0,
    listener_runtime_id/0,
    listener_id/0,
    listen_on/0,
    listener_opts/0,
    %% esockd options
    esockd_opts/0,
    esockd_type/0,
    socket_opts/0,
    %% cowboy options
    cowboy_opts/0,
    cowboy_type/0,
    ranch_opts/0,
    ws_opts/0
]).

%% Helper macros

-define(IS_ESOCKD_LISTENER(T),
    (T == tcp orelse T == ssl orelse T == udp orelse T == dtls orelse
        T == <<"tcp">> orelse T == <<"ssl">> orelse T == <<"udp">> orelse T == <<"dtls">>)
).
-define(IS_COWBOY_LISTENER(T),
    (T == ws orelse T == wss orelse T == <<"ws">> orelse T == <<"wss">>)
).

-define(TRANSPORT_KEYS, [
    bind,
    acceptors,
    max_connections,
    max_conn_rate,
    tcp_options,
    ssl_options,
    udp_options,
    dtls_options
]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-doc """
    Convert
    * configuration from config
    * callback module configuration
    * runtime context
    into a list of runtime listener configurations (ready to pass to the transport module esockd/cowboy)
""".
-spec to_rt_listener_configs(gw_name(), map(), map(), term()) -> list(listener_runtime_config()).
to_rt_listener_configs(GwName, GwConfig0, ModConfig0, Ctx) ->
    GwConfig1 = maps:without([listeners], GwConfig0),

    %% Get plain list of all listener configurations of all types
    ListenerConfigList = listener_configs(GwConfig0),

    %% Convert each listener configuration to a runtime configuration
    lists:map(
        fun({Type, Name, ListenerConfig}) ->
            %% Eval effective ModConf depending on type.
            %% Gateways may want to use different callback modules for different types e.g. different
            %% modules for dtls and udp listeners.
            ModConfig = get_effective_mod_config(Type, ModConfig0),

            %% Find callback module for the listener.
            CallbackModule = maps:get(connection_mod, ModConfig, default_connection_mod(Type)),

            %% Filter out transport options from listener-specific configuration.
            ListenerConfigNoTransportOpts = filter_out_transport_opts(ListenerConfig),

            %% Merge all non-transport configurations into a single map. It will be passed to the callback module.
            CallbackConfig = emqx_utils_maps:merge(
                [
                    GwConfig1,
                    ListenerConfigNoTransportOpts,
                    ModConfig,
                    #{ctx => Ctx, listener => {GwName, Type, Name}}
                ]
            ),

            %% Some common configuration entries
            ListenerId = listener_id(GwName, Type, Name),
            ListenOn = maps:get(bind, ListenerConfig),

            ListenerOpts =
                case Type of
                    T when ?IS_ESOCKD_LISTENER(T) ->
                        {esockd, #{
                            type => Type,
                            socket_opts => esockd_opts(Type, ListenerConfig),
                            mfa => {CallbackModule, start_link, [CallbackConfig]}
                        }};
                    T when ?IS_COWBOY_LISTENER(T) ->
                        {cowboy, #{
                            type => Type,
                            ranch_opts => cowboy_ranch_opts(Type, ListenOn, ListenerConfig),
                            ws_opts => cowboy_ws_opts(
                                ListenerConfig, CallbackModule, CallbackConfig
                            )
                        }}
                end,
            #{
                listener_id => ListenerId,
                listen_on => ListenOn,
                listener_opts => ListenerOpts,
                original_listener_config => ListenerConfig
            }
        end,
        ListenerConfigList
    ).

-doc """
    Get plain list of all listener configurations from configuration map.
""".
-spec listener_configs(map()) -> list({listener_type(), listener_name(), map()}).
listener_configs(GwConfig) ->
    ListenerConfigMap = maps:get(listeners, GwConfig, #{}),
    [
        {Type, Name, ListenerConfig}
     || {Type, TypeListeners} <- maps:to_list(ListenerConfigMap),
        {Name, ListenerConfig} <- maps:to_list(TypeListeners)
    ].

-doc """
    Diff two lists of runtime listener configurations and return the listeners to stop, update, and start.
""".
-spec diff_rt_listener_configs(list(listener_runtime_config()), list(listener_runtime_config())) ->
    #{
        stop => list(listener_runtime_config()),
        update => list(listener_runtime_config()),
        start => list(listener_runtime_config())
    }.
diff_rt_listener_configs(OldListenerConfigs, NewListenerConfigs) ->
    {Stop, Update, Start} = lists:foldl(
        fun(NewConfig, {StopAcc0, UpdateAcc, StartAcc}) ->
            #{listener_id := ListenerId, listen_on := NewListenOn, listener_opts := NewOpts} =
                NewConfig,
            NewType = type(NewConfig),
            case take_listener(ListenerId, StopAcc0) of
                false ->
                    {StopAcc0, UpdateAcc, [NewConfig | StartAcc]};
                {value, OldConfig, StopAcc} ->
                    #{listen_on := OldListenOn, listener_opts := OldOpts} = OldConfig,
                    OldType = type(OldConfig),
                    case {NewType, NewListenOn, NewOpts} of
                        {dtls, NewListenOn, NewOpts} ->
                            %% Always stop and start dtls listeners to update the options
                            {StopAcc0, UpdateAcc, [NewConfig | StartAcc]};
                        {OldType, OldListenOn, OldOpts} ->
                            %% No change, keep the old listener
                            {StopAcc, UpdateAcc, StartAcc};
                        {OldType, OldListenOn, _OldOpts} ->
                            %% Type and listen on are the same, but the options are different
                            %% Update the listener
                            {StopAcc, [NewConfig | UpdateAcc], StartAcc};
                        {_OldType, _OldListenOn, _OldOpts} ->
                            %% ListenOn or type are different, stop and start the new listener
                            {StopAcc0, UpdateAcc, [NewConfig | StartAcc]}
                    end
            end
        end,
        {OldListenerConfigs, [], []},
        NewListenerConfigs
    ),
    #{stop => Stop, update => Update, start => Start}.

-doc """
    Convert a gateway configuration to a list of runtime listener ids,
    i.e. identifiers of running listeners that are sufficient to address them
    via transport modules (esockd/cowboy).
""".
to_rt_listener_ids(GwName, GwConfig0) ->
    %% Get plain list of all listener configurations of all types
    ListenerConfigList = listener_configs(GwConfig0),

    %% Convert listener configurations to runtime ids
    lists:map(
        fun({Type, Name, ListenerConfig}) ->
            ListenerId = listener_id(GwName, Type, Name),
            ListenOn = maps:get(bind, ListenerConfig),
            ListenerType =
                case Type of
                    T when ?IS_ESOCKD_LISTENER(T) ->
                        {esockd, Type};
                    T when ?IS_COWBOY_LISTENER(T) ->
                        {cowboy, Type}
                end,
            #{
                listener_id => ListenerId,
                listen_on => ListenOn,
                listener_type => ListenerType
            }
        end,
        ListenerConfigList
    ).

-doc """
    Convert a listener id and raw configuration to a runtime id.
""".
-spec rt_listener_id(listener_id(), map()) ->
    listener_runtime_id().
rt_listener_id(ListenerId, ListenerRawConfig) ->
    {_GwName, TypeBin, _NameBin} = emqx_gateway_utils:parse_listener_id(ListenerId),
    ListenOn = maps:get(<<"bind">>, ListenerRawConfig),
    ListenerType =
        case binary_to_existing_atom(TypeBin) of
            T when ?IS_ESOCKD_LISTENER(T) ->
                {esockd, T};
            T when ?IS_COWBOY_LISTENER(T) ->
                {cowboy, T}
        end,
    #{
        listener_id => ListenerId,
        listen_on => ListenOn,
        listener_type => ListenerType
    }.

-doc """
    Convert a listener runtime configuration to a runtime id.
""".
-spec rt_listener_id(listener_runtime_config()) -> listener_runtime_id().
rt_listener_id(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {esockd, #{type := Type}}
}) ->
    #{
        listener_id => ListenerId,
        listen_on => ListenOn,
        listener_type => {esockd, Type}
    };
rt_listener_id(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {cowboy, #{type := Type}}
}) ->
    #{
        listener_id => ListenerId,
        listen_on => ListenOn,
        listener_type => {cowboy, Type}
    }.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

default_connection_mod(Type) when ?IS_ESOCKD_LISTENER(Type) ->
    emqx_gateway_conn;
default_connection_mod(Type) when ?IS_COWBOY_LISTENER(Type) ->
    emqx_gateway_conn_ws.

take_listener(ListenerId, ListenerConfigs) ->
    take_listener(ListenerId, ListenerConfigs, []).

take_listener(ListenerId, [#{listener_id := ListenerId} = ListenerConfig | ListenerConfigs], Acc) ->
    {value, ListenerConfig, lists:reverse(Acc) ++ ListenerConfigs};
take_listener(ListenerId, [ListenerConfig | ListenerConfigs], Acc) ->
    take_listener(ListenerId, ListenerConfigs, [ListenerConfig | Acc]);
take_listener(_, [], _) ->
    false.

type(#{listener_opts := {esockd, #{type := Type}}}) ->
    Type;
type(#{listener_opts := {cowboy, #{type := Type}}}) ->
    Type.

filter_out_transport_opts(ListenerConfig) ->
    maps:without(?TRANSPORT_KEYS, ListenerConfig).

get_effective_mod_config(Type, ModConfig0) ->
    case ModConfig0 of
        #{Type := ModConfig} -> ModConfig;
        #{default := ModConfig} -> ModConfig;
        _ -> ModConfig0
    end.

listener_id(GwName, Type, LisName) ->
    emqx_gateway_utils:listener_id(GwName, Type, LisName).

esockd_opts(Type, Opts0) when ?IS_ESOCKD_LISTENER(Type) ->
    Opts1 = maps:with(
        [
            acceptors,
            max_connections,
            max_conn_rate,
            proxy_protocol,
            proxy_protocol_timeout,
            health_check
        ],
        Opts0
    ),
    Opts2 = Opts1#{
        access_rules => emqx_listeners:esockd_access_rules(maps:get(access_rules, Opts0, []))
    },
    maps:to_list(
        case Type of
            tcp ->
                Opts2#{tcp_options => tcp_opts_with_defaults(Opts0)};
            ssl ->
                Opts2#{
                    tcp_options => tcp_opts_with_defaults(Opts0),
                    ssl_options => ssl_opts(ssl_options, Opts0)
                };
            udp ->
                Opts2#{udp_options => udp_opts(Opts0)};
            dtls ->
                UDPOpts = udp_opts(Opts0),
                DTLSOpts = ssl_opts(dtls_options, Opts0),
                Opts2#{
                    udp_options => UDPOpts,
                    dtls_options => DTLSOpts
                }
        end
    ).

cowboy_ranch_opts(Type, ListenOn, Opts) ->
    NumAcceptors = maps:get(acceptors, Opts, 4),
    MaxConnections = maps:get(max_connections, Opts, 1024),
    SocketOpts1 =
        case Type of
            wss ->
                tcp_opts(Opts) ++
                    proplists:delete(handshake_timeout, ssl_opts(ssl_options, Opts));
            ws ->
                tcp_opts(Opts)
        end,
    SocketOpts = ip_port(ListenOn) ++ proplists:delete(reuseaddr, SocketOpts1),
    #{
        num_acceptors => NumAcceptors,
        max_connections => MaxConnections,
        handshake_timeout => maps:get(handshake_timeout, Opts, 15000),
        socket_opts => SocketOpts
    }.

cowboy_ws_opts(ListenerConfig, CallbackModule, CallbackConfig) ->
    WsPaths = [
        {
            emqx_utils_maps:deep_get([websocket, path], ListenerConfig, "") ++ "/[...]",
            CallbackModule,
            CallbackConfig
        }
    ],
    Dispatch = cowboy_router:compile([{'_', WsPaths}]),
    ProxyProto = maps:get(proxy_protocol, ListenerConfig, false),
    #{env => #{dispatch => Dispatch}, proxy_header => ProxyProto}.

ip_port(Port) when is_integer(Port) ->
    [{port, Port}];
ip_port({Addr, Port}) ->
    [{ip, Addr}, {port, Port}].

tcp_opts(Opts) ->
    emqx_listeners:tcp_opts(Opts).

tcp_opts_with_defaults(Opts) ->
    emqx_utils:merge_opts(default_tcp_options(), tcp_opts(Opts)).

udp_opts(Opts) ->
    maps:to_list(
        maps:without(
            [active_n],
            maps:get(udp_options, Opts, #{})
        )
    ).

ssl_opts(Name, Opts) ->
    SSLConf = maps:get(Name, Opts, #{}),
    SSLOpts = ssl_server_opts(Name, SSLConf),
    ensure_dtls_protocol(Name, SSLOpts).

ensure_dtls_protocol(dtls_options, SSLOpts) ->
    [{protocol, dtls} | SSLOpts];
ensure_dtls_protocol(_, SSLOpts) ->
    SSLOpts.

ssl_server_opts(ssl_options, SSLOpts) ->
    emqx_tls_lib:to_server_opts(tls, SSLOpts);
ssl_server_opts(dtls_options, SSLOpts) ->
    emqx_tls_lib:to_server_opts(dtls, SSLOpts).

default_tcp_options() ->
    [
        binary,
        {packet, raw},
        {reuseaddr, true},
        {nodelay, true},
        {backlog, 512}
    ].
