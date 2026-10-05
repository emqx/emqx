%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_conf).

-moduledoc """
Connection settings derived from listener and zone config, shared between
connections.

The module keeps two kinds of `persistent_term` entries:

- `{emqx_connection_conf, Zone, frame}`: the frame parser and serializer
  options of the zone. The value holds two groups of terms:
  - `connect`: the initial parse state and the initial serializer options that
    a connection uses until it receives CONNECT.
  - `common`: for each MQTT protocol version, the initial parse state and the
    serializer options that a connection uses after CONNECT.
- `{emqx_connection_conf, Listener, Zone, conf}`: the `#conf{}` record of a
  connection on that listener in that zone. It holds the listener settings
  (`active_n`, `sendq_watermark`) and the zone settings (`hibernate_after`,
  `minor_gc_after`, `force_gc`, `force_shutdown`). There is one entry for every
  TCP, SSL and QUIC listener combined with every zone, so a connection whose
  zone was overridden at authentication still finds its entry.

The two kinds are separate entries because replacing an entry makes the
runtime copy the old terms into the heap of every process that still refers
to them. With separate entries, a change to one kind leaves the other shared.

A term read out of `persistent_term` is not copied into the heap of the
reading process. So every connection that stores one of these terms in its
state refers to the same instance. `process_info(Pid, memory)` does not count
the shared terms. `sys:get_state/1`, `erlang:external_size/1` and
`erts_debug:flat_size/1` do count them, so they overstate the size of a
connection state.

Only the config update hooks write the entries: `post_zone_config_update/2`
on every write of the zones config, and `post_listener_config_update/1` on
every write of the listeners config. Connection processes only read them.
When an entry is absent, the functions here build the terms locally from
config.
""".

-include("emqx_mqtt.hrl").
-include("emqx_connection_conf.hrl").

-export([
    frame_opts/1,
    pre_connect_codec/1,
    post_connect_codec/4,
    conn_conf/2,
    post_zone_config_update/2,
    post_listener_config_update/1
]).

-export_type([pre_connect_codec/0, conn_conf/0, listener/0]).

-define(FRAME_KEY(Zone), {?MODULE, Zone, frame}).
-define(CONN_KEY(Listener, Zone), {?MODULE, Listener, Zone, conf}).
-define(PROTO_VERS, [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]).
%% A QUIC stream has no socket to activate; the control stream keeps the
%% default of `emqx_connection'.
-define(QUIC_ACTIVE_N, 10).
-define(CONF_LISTENER_TYPES, [tcp, ssl, quic]).

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

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

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
            build_pre_connect_codec(frame_opts(Zone))
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
            build_conn_conf(Listener, Zone, ListenerConf, emqx_config:get([zones, Zone]))
    end.

-doc """
Rebuild the frame entry of every zone in `NewZones`, delete the frame entries
of the zones that are removed, and rebuild the connection settings of every
listener and zone pair.

`emqx_config_zones:post_update/2` calls this each time the zones config is
written, including the first write at boot.
""".
-spec post_zone_config_update(emqx_config:config(), emqx_config:config()) -> ok.
post_zone_config_update(OldZones, NewZones) ->
    ok = maps:foreach(fun update_zone_frame/2, NewZones),
    lists:foreach(
        fun(Zone) -> _ = persistent_term:erase(?FRAME_KEY(Zone)) end,
        maps:keys(maps:without(maps:keys(NewZones), OldZones))
    ),
    refresh_conn_confs(NewZones, emqx_config:get([listeners], #{})).

-doc """
Rebuild the connection settings of every listener and zone pair from the
given listeners config.

`emqx_listeners:post_config_update/5` calls this each time the listeners
config is written. It runs before the new config is stored, so the caller
passes the new listeners config in.
""".
-spec post_listener_config_update(emqx_config:config()) -> ok.
post_listener_config_update(Listeners) ->
    refresh_conn_confs(emqx_config:get([zones], #{}), Listeners).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

same_or_given(Shared, Given) when Shared =:= Given ->
    Shared;
same_or_given(_Shared, Given) ->
    Given.

update_zone_frame(Zone, ZoneConf) ->
    put_if_changed(?FRAME_KEY(Zone), build_frame(ZoneConf)).

%% One entry per listener and zone. Entries of pairs that no longer exist
%% are erased.
refresh_conn_confs(Zones, Listeners) ->
    Wanted = maps:from_list([
        {?CONN_KEY(Listener, Zone), build_conn_conf(Listener, Zone, ListenerConf, ZoneConf)}
     || {Listener, ListenerConf} <- conf_listeners(Listeners),
        {Zone, ZoneConf} <- maps:to_list(Zones)
    ]),
    ok = maps:foreach(fun put_if_changed/2, Wanted),
    lists:foreach(
        fun
            ({?CONN_KEY(_, _) = Key, _Value}) when not is_map_key(Key, Wanted) ->
                _ = persistent_term:erase(Key);
            (_Entry) ->
                ok
        end,
        persistent_term:get()
    ).

%% The listeners whose connections use `#conf{}': TCP, SSL and QUIC.
conf_listeners(Listeners) ->
    maps:fold(
        fun(Type, ByName, Acc) ->
            case lists:member(Type, ?CONF_LISTENER_TYPES) of
                true -> conf_listeners(Type, ByName, Acc);
                false -> Acc
            end
        end,
        [],
        Listeners
    ).

conf_listeners(Type, ByName, Acc) ->
    maps:fold(
        fun
            (Name, ListenerConf, Acc1) when is_map(ListenerConf) ->
                [{{Type, Name}, ListenerConf} | Acc1];
            (_Name, _Tombstone, Acc1) ->
                Acc1
        end,
        Acc,
        ByName
    ).

%% Write the entry only when its value changes.
put_if_changed(Key, Value) ->
    case persistent_term:get(Key, undefined) of
        Value -> ok;
        _ -> persistent_term:put(Key, Value)
    end.

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
    PreConnect = build_pre_connect_codec(FrameOpts),
    #{initial_parse_state := ParseState} = PreConnect,
    #{
        connect => PreConnect,
        common => maps:from_list([
            {ProtoVer, #{
                initial_parse_state => emqx_frame:post_connect_parse_state(ProtoVer, ParseState),
                serialize_opts => emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE)
            }}
         || ProtoVer <- ?PROTO_VERS
        ])
    }.

build_pre_connect_codec(FrameOpts) ->
    #{
        initial_parse_state => emqx_frame:initial_parse_state(FrameOpts),
        serialize_opts => emqx_frame:initial_serialize_opts(FrameOpts)
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
