%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_conf).

-moduledoc """
Connection settings derived from listener and zone config, shared between
connections.

The entries live in `persistent_term`, under two kinds of key:

- `{emqx_connection_conf, Zone, frame}`: the frame parser and serializer
  options of the zone. The value holds two groups of terms:
  - `connect`: the initial parse state and the initial serializer options that
    a connection uses until it receives CONNECT.
  - `common`: for each MQTT protocol version, the initial parse state and the
    serializer options that a connection uses after CONNECT.
- `{emqx_connection_conf, Listener, Zone, conf}`: the `#conf{}` record of a
  connection on that listener in that zone. It holds the listener settings
  (`active_n`, `sendq_watermark`) and the zone settings (`hibernate_after`,
  `minor_gc_after`, `force_gc`, `force_shutdown`).

A term read out of `persistent_term` is not copied into the heap of the
reading process. So every connection that stores one of these terms in its
state refers to the same instance. `process_info(Pid, memory)` does not count
the shared terms. `sys:get_state/1`, `erlang:external_size/1` and
`erts_debug:flat_size/1` do count them, so they overstate the size of a
connection state.

The `gen_server` of this module, a child of `emqx_kernel_sup`, owns the
entries, and nothing else writes them. An entry is created on first use: a connection that finds no entry
builds the terms from config, keeps its own copy, and sends them here, so the
connections after it share one instance. On a zone or listener config change
the server rebuilds the entries it owns, and erases the entries of a deleted
zone or listener; the next connection registers them again.

Replacing or erasing an entry makes the runtime copy the old terms into the
heap of every process that still refers to them. An entry is rewritten only
when its value changes, so a config change that leaves the derived terms
equal costs nothing. The two kinds are separate entries, so a change to one
kind leaves the other shared.

The server erases its entries when it stops in an orderly way. After a crash
it takes its entries back from `persistent_term` and rebuilds each one from
the current config, since an application restart can load a different config.
""".

-behaviour(gen_server).

-include("emqx_mqtt.hrl").
-include("emqx_connection_conf.hrl").
-include("logger.hrl").

-export([start_link/0]).

-export([
    frame_opts/1,
    pre_connect_codec/1,
    post_connect_codec/4,
    conn_conf/2,
    post_zone_config_update/2,
    listeners_changed/1,
    sync/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([pre_connect_codec/0, conn_conf/0, listener/0]).

-define(FRAME_KEY(Zone), {?MODULE, Zone, frame}).
-define(CONN_KEY(Listener, Zone), {?MODULE, Listener, Zone, conf}).
-define(PROTO_VERS, [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]).
%% A QUIC stream has no socket to activate; the control stream keeps the
%% default of `emqx_connection'.
-define(QUIC_ACTIVE_N, 10).
-define(CALL_TIMEOUT, 2000).

-type listener() :: {Type :: atom(), Name :: atom()}.

-type pre_connect_codec() :: #{
    initial_parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type post_connect_codec() :: #{
    initial_parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type frame() :: #{
    connect := pre_connect_codec(),
    common := #{emqx_types:proto_ver() => post_connect_codec()}
}.

-type conn_conf() :: #conf{}.

-type key() :: {?MODULE, emqx_types:zone(), frame} | {?MODULE, listener(), emqx_types:zone(), conf}.

-record(register, {key :: key(), value :: frame() | conn_conf()}).
-record(zones_updated, {zones :: emqx_config:config()}).
%% The checked config of a listener, or `undefined' for a deleted one.
-record(listeners_changed, {listeners :: [{listener(), map() | undefined}]}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Return the frame options configured for the zone.".
-spec frame_opts(emqx_types:zone()) -> emqx_frame:options().
frame_opts(Zone) ->
    frame_opts(
        emqx_config:get_zone_conf(Zone, [mqtt, strict_mode]),
        emqx_config:get_zone_conf(Zone, [mqtt, max_packet_size]),
        emqx_config:get_zone_conf(Zone, [mqtt, max_connect_packet_size]),
        emqx_config:get_zone_conf(Zone, [mqtt, max_connect_user_properties])
    ).

-doc """
Return the parse state and the serializer options for a new connection in
the zone.
""".
-spec pre_connect_codec(emqx_types:zone()) -> pre_connect_codec().
pre_connect_codec(Zone) ->
    case persistent_term:get(?FRAME_KEY(Zone), undefined) of
        #{connect := PreConnect} ->
            PreConnect;
        undefined ->
            Frame = build_frame(emqx_config:get([zones, Zone])),
            ok = register_entry(?FRAME_KEY(Zone), Frame),
            maps:get(connect, Frame)
    end.

-doc """
Replace the post-CONNECT parse state and serializer options with the shared
terms of the zone and protocol version.

Each term is replaced only when the shared term is equal to it. Otherwise the
given term is returned unchanged. For example, the serializer options of a
client that sends `Maximum-Packet-Size` are never replaced.
""".
-spec post_connect_codec(
    emqx_types:zone(),
    emqx_types:proto_ver(),
    emqx_frame:parse_state(),
    emqx_frame:serialize_opts()
) ->
    {emqx_frame:parse_state(), emqx_frame:serialize_opts()}.
post_connect_codec(Zone, ProtoVer, ParseState, SerializeOpts) ->
    case persistent_term:get(?FRAME_KEY(Zone), undefined) of
        #{common := #{ProtoVer := Shared}} ->
            #{
                initial_parse_state := SharedParseState,
                serialize_opts := SharedSerializeOpts
            } = Shared,
            {
                same_or_given(SharedParseState, ParseState),
                same_or_given(SharedSerializeOpts, SerializeOpts)
            };
        _ ->
            {ParseState, SerializeOpts}
    end.

-doc "Return the settings of a connection on the listener in the zone.".
-spec conn_conf(listener(), emqx_types:zone()) -> conn_conf().
conn_conf(Listener, Zone) ->
    case persistent_term:get(?CONN_KEY(Listener, Zone), undefined) of
        #conf{} = Conf ->
            Conf;
        undefined ->
            {Type, Name} = Listener,
            ListenerConf = emqx_config:get_listener_conf(Type, Name, []),
            Conf = build_conn_conf(Listener, Zone, ListenerConf, emqx_config:get([zones, Zone])),
            ok = register_entry(?CONN_KEY(Listener, Zone), Conf),
            Conf
    end.

-doc """
Rebuild the entries of the zones in `NewZones` and delete the entries of the
other zones.

`emqx_config_zones:post_update/2` calls this each time the zones config is
written. The first write at boot happens before this server starts, and
nothing is registered by then.
""".
-spec post_zone_config_update(emqx_config:config(), emqx_config:config()) -> ok.
post_zone_config_update(_OldZones, NewZones) ->
    safe_call(#zones_updated{zones = NewZones}).

-doc """
Rebuild the entries of the given listeners from their new config, and delete
the entries of the listeners given as `undefined`.

`emqx_listeners:post_config_update/5` calls this with the listeners that are
updated or deleted, before the new config is stored.
""".
-spec listeners_changed([{listener(), map() | undefined}]) -> ok.
listeners_changed([]) ->
    ok;
listeners_changed(Listeners) ->
    safe_call(#listeners_changed{listeners = Listeners}).

-doc """
Return once the server has handled the registrations sent before the call.

For tests only: a connection never waits for the server.
""".
-spec sync() -> ok.
sync() ->
    gen_server:call(?MODULE, sync, ?CALL_TIMEOUT).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = process_flag(trap_exit, true),
    %% After a crash, take the entries written before back, rebuilt for the
    %% current config: an application restart can load another config.
    Zones = emqx_config:get([zones], #{}),
    Keys = [Key || {Key, _} <- persistent_term:get(), is_own_key(Key), rebuild(Key, Zones)],
    {ok, #{keys => maps:from_keys(Keys, true)}}.

handle_call(#zones_updated{zones = Zones}, _From, #{keys := Keys0} = State) ->
    Keys = maps:filter(fun(Key, true) -> rebuild(Key, Zones) end, Keys0),
    {reply, ok, State#{keys := Keys}};
handle_call(#listeners_changed{listeners = Listeners}, _From, #{keys := Keys0} = State) ->
    Changed = maps:from_list(Listeners),
    Zones = emqx_config:get([zones], #{}),
    Keys = maps:filter(
        fun
            (?CONN_KEY(Listener, _Zone) = Key, true) when is_map_key(Listener, Changed) ->
                rebuild(Key, Zones, maps:get(Listener, Changed));
            (_Key, true) ->
                true
        end,
        Keys0
    ),
    {reply, ok, State#{keys := Keys}};
handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call(Req, _From, State) ->
    {reply, {error, {unknown_call, Req}}, State}.

handle_cast(#register{key = Key, value = Value}, #{keys := Keys} = State) ->
    %% An entry written before, or rebuilt from a later config, wins.
    case persistent_term:get(Key, undefined) of
        undefined ->
            ok = persistent_term:put(Key, Value),
            {noreply, State#{keys := Keys#{Key => true}}};
        _ ->
            {noreply, State}
    end;
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #{keys := Keys}) ->
    case is_orderly(Reason) of
        true -> lists:foreach(fun erase_entry/1, maps:keys(Keys));
        false -> ok
    end.

is_orderly(normal) -> true;
is_orderly(shutdown) -> true;
is_orderly({shutdown, _}) -> true;
is_orderly(_) -> false.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

register_entry(Key, Value) ->
    gen_server:cast(?MODULE, #register{key = Key, value = Value}).

%% The config hooks run at boot before this server starts; nothing is
%% registered by then, so there is nothing to do. A config update must not
%% fail because this server is slow: the caller stops waiting, and the
%% request stays queued for the server to handle when it gets to it.
safe_call(Req) ->
    try
        gen_server:call(?MODULE, Req, ?CALL_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ok;
        exit:{timeout, _} ->
            ?SLOG(warning, #{
                msg => "connection_conf_update_not_awaited",
                request => element(1, Req),
                hint => "shared connection setting updates might be delayed"
            }),
            ok
    end.

is_own_key(?FRAME_KEY(_Zone)) -> true;
is_own_key(?CONN_KEY(_Listener, _Zone)) -> true;
is_own_key(_Key) -> false.

%% Rebuild the entry for the current config, or erase it when its zone or
%% listener is gone. Returns whether the entry is kept.
rebuild(?FRAME_KEY(Zone) = Key, Zones) ->
    case Zones of
        #{Zone := ZoneConf} -> put_if_changed(Key, build_frame(ZoneConf));
        #{} -> erase_entry(Key)
    end;
rebuild(?CONN_KEY({Type, Name}, _Zone) = Key, Zones) ->
    rebuild(Key, Zones, emqx_config:get_listener_conf(Type, Name, [], undefined)).

rebuild(?CONN_KEY(Listener, Zone) = Key, Zones, ListenerConf) ->
    case {Zones, ListenerConf} of
        {#{Zone := ZoneConf}, #{}} ->
            put_if_changed(Key, build_conn_conf(Listener, Zone, ListenerConf, ZoneConf));
        _ ->
            erase_entry(Key)
    end.

%% Write the entry only when its value changes.
put_if_changed(Key, Value) ->
    case persistent_term:get(Key, undefined) of
        Value -> ok;
        _ -> persistent_term:put(Key, Value)
    end,
    true.

erase_entry(Key) ->
    _ = persistent_term:erase(Key),
    false.

same_or_given(Shared, Given) when Shared =:= Given ->
    Shared;
same_or_given(_Shared, Given) ->
    Given.

-spec build_frame(map()) -> frame().
build_frame(#{
    mqtt := #{
        strict_mode := StrictMode,
        max_packet_size := MaxSize,
        max_connect_packet_size := MaxConnectSize,
        max_connect_user_properties := MaxConnectUserProps
    }
}) ->
    FrameOpts = frame_opts(StrictMode, MaxSize, MaxConnectSize, MaxConnectUserProps),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    #{
        connect => #{
            initial_parse_state => ParseState,
            serialize_opts => emqx_frame:initial_serialize_opts(FrameOpts)
        },
        common => maps:from_list([
            {ProtoVer, #{
                initial_parse_state => emqx_frame:post_connect_parse_state(ProtoVer, ParseState),
                serialize_opts => emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE)
            }}
         || ProtoVer <- ?PROTO_VERS
        ])
    }.

-spec build_conn_conf(listener(), emqx_types:zone(), map(), map()) -> conn_conf().
build_conn_conf(
    Listener,
    Zone,
    ListenerConf,
    #{
        mqtt := #{hibernate_after := HibernateAfter, minor_gc_after := MinorGcAfter},
        force_gc := ForceGc,
        force_shutdown := ForceShutdown
    }
) ->
    {ActiveN, Watermark} = listener_settings(Listener, ListenerConf),
    #conf{
        listener = Listener,
        zone = Zone,
        active_n = ActiveN,
        sendq_watermark = Watermark,
        hibernate_after = HibernateAfter,
        minor_gc_after = MinorGcAfter,
        force_gc = force_gc(ForceGc),
        force_shutdown = ForceShutdown
    }.

%% `{ActiveN, SendQueueWatermark}' of a listener.
listener_settings({quic, _Name}, _ListenerConf) ->
    {?QUIC_ACTIVE_N, 0};
listener_settings(_Listener, #{tcp_options := #{active_n := ActiveN, high_watermark := Watermark}}) ->
    {emqx_listeners:clamp_active_n(ActiveN), Watermark}.

force_gc(#{enable := false}) ->
    false;
force_gc(#{enable := true, count := Count, bytes := Bytes}) ->
    {Count, Bytes}.

frame_opts(StrictMode, MaxSize, MaxConnectSize, MaxConnectUserProps) ->
    #{
        strict_mode => StrictMode,
        max_size => MaxSize,
        max_connect_size => MaxConnectSize,
        max_connect_user_properties => MaxConnectUserProps,
        %% Any packet received before CONNECT is rejected by the parser.
        expect_connect => true
    }.
