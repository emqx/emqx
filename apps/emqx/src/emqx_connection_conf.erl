%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_conf).

-moduledoc """
Connection settings derived from zone config and shared between connections.

Each zone has two `persistent_term` entries:

- `{emqx_connection_conf, Zone, frame}`: the frame parser and serializer
  options. The value holds two groups of terms:
  - `connect`: the initial parse state and the initial serializer options that
    a connection uses until it receives CONNECT.
  - `common`: for each MQTT protocol version, the initial parse state and the
    serializer options that a connection uses after CONNECT.
- `{emqx_connection_conf, Zone, conf}`: the `#zone_conf{}` record, which holds
  the zone settings of the connection process: `hibernate_after`, `force_gc`
  and `force_shutdown`.

The two groups are separate entries because replacing an entry makes the
runtime copy the old terms into the heap of every process that still refers
to them. With separate entries, a change to one group leaves the other group
shared.

A term read out of `persistent_term` is not copied into the heap of the
reading process. So every connection that stores one of these terms in its
state refers to the same instance. `process_info(Pid, memory)` does not count
the shared terms. `sys:get_state/1`, `erlang:external_size/1` and
`erts_debug:flat_size/1` do count them, so they overstate the size of a
connection state.

Only `post_zone_config_update/2` writes the entries. Connection processes
only read them. When an entry is absent, the functions here build the terms
locally from zone config.
""".

-include("emqx_mqtt.hrl").
-include("emqx_connection_conf.hrl").

-export([
    frame_opts/1,
    pre_connect/1,
    connected/4,
    zone_conf/1,
    post_zone_config_update/2
]).

-export_type([pre_connect/0, zone_conf/0]).

-define(FRAME_KEY(Zone), {?MODULE, Zone, frame}).
-define(CONF_KEY(Zone), {?MODULE, Zone, conf}).
-define(PROTO_VERS, [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]).

-type pre_connect() :: #{
    initial_parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type connected() :: #{
    initial_parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type frame() :: #{
    connect := pre_connect(),
    common := #{emqx_types:proto_ver() => connected()}
}.

-type zone_conf() :: #zone_conf{}.

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
-spec pre_connect(emqx_types:zone()) -> pre_connect().
pre_connect(Zone) ->
    case persistent_term:get(?FRAME_KEY(Zone), undefined) of
        #{connect := PreConnect} ->
            PreConnect;
        undefined ->
            build_pre_connect(frame_opts(Zone))
    end.

-doc """
Replace the post-CONNECT parse state and serializer options with the shared
terms of the zone and protocol version.

Each term is replaced only when the shared term is equal to it. Otherwise the
given term is returned unchanged. For example, the serializer options of a
client that sends `Maximum-Packet-Size` are never replaced.
""".
-spec connected(
    emqx_types:zone(),
    emqx_types:proto_ver(),
    emqx_frame:parse_state(),
    emqx_frame:serialize_opts()
) ->
    {emqx_frame:parse_state(), emqx_frame:serialize_opts()}.
connected(Zone, ProtoVer, ParseState, SerializeOpts) ->
    case persistent_term:get(?FRAME_KEY(Zone), undefined) of
        #{common := #{ProtoVer := Shared}} ->
            #{initial_parse_state := SharedParseState, serialize_opts := SharedSerializeOpts} =
                Shared,
            {
                same_or_given(SharedParseState, ParseState),
                same_or_given(SharedSerializeOpts, SerializeOpts)
            };
        _ ->
            {ParseState, SerializeOpts}
    end.

-doc "Return the connection process settings of the zone.".
-spec zone_conf(emqx_types:zone()) -> zone_conf().
zone_conf(Zone) ->
    case persistent_term:get(?CONF_KEY(Zone), undefined) of
        #zone_conf{} = ZoneConf ->
            ZoneConf;
        undefined ->
            zone_conf(
                Zone,
                emqx_config:get_zone_conf(Zone, [mqtt, hibernate_after]),
                emqx_config:get_zone_conf(Zone, [force_gc]),
                emqx_config:get_zone_conf(Zone, [force_shutdown])
            )
    end.

-doc """
Rebuild the entries of every zone in `NewZones` and delete the entries of the
zones that are removed.

`emqx_config_zones:post_update/2` calls this each time the zones config is
written, including the first write at boot.
""".
-spec post_zone_config_update(emqx_config:config(), emqx_config:config()) -> ok.
post_zone_config_update(OldZones, NewZones) ->
    ok = maps:foreach(fun update_zone/2, NewZones),
    lists:foreach(
        fun(Zone) ->
            _ = persistent_term:erase(?FRAME_KEY(Zone)),
            _ = persistent_term:erase(?CONF_KEY(Zone))
        end,
        maps:keys(maps:without(maps:keys(NewZones), OldZones))
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

same_or_given(Shared, Given) when Shared =:= Given ->
    Shared;
same_or_given(_Shared, Given) ->
    Given.

update_zone(Zone, ZoneConf) ->
    ok = put_or_erase(?FRAME_KEY(Zone), build_frame(ZoneConf)),
    ok = put_or_erase(?CONF_KEY(Zone), build_zone_conf(Zone, ZoneConf)).

%% Write the entry only when its value changes. A value of `undefined' means
%% the zone config is not complete yet.
put_or_erase(Key, undefined) ->
    _ = persistent_term:erase(Key),
    ok;
put_or_erase(Key, Value) ->
    case persistent_term:get(Key, undefined) of
        Value -> ok;
        _ -> persistent_term:put(Key, Value)
    end.

-spec build_frame(map()) -> frame() | undefined.
build_frame(#{
    mqtt := #{
        strict_mode := StrictMode,
        max_packet_size := MaxSize,
        max_connect_packet_size := MaxConnectSize,
        max_connect_user_properties := MaxConnectUserProps
    }
}) ->
    FrameOpts = frame_opts(StrictMode, MaxSize, MaxConnectSize, MaxConnectUserProps),
    PreConnect = build_pre_connect(FrameOpts),
    #{initial_parse_state := ParseState} = PreConnect,
    #{
        connect => PreConnect,
        common => maps:from_list([
            {ProtoVer, #{
                initial_parse_state => emqx_frame:connect_parsed(ProtoVer, ParseState),
                serialize_opts => emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE)
            }}
         || ProtoVer <- ?PROTO_VERS
        ])
    };
build_frame(_ZoneConf) ->
    undefined.

build_pre_connect(FrameOpts) ->
    #{
        initial_parse_state => emqx_frame:initial_parse_state(FrameOpts),
        serialize_opts => emqx_frame:initial_serialize_opts(FrameOpts)
    }.

-spec build_zone_conf(emqx_types:zone(), map()) -> zone_conf() | undefined.
build_zone_conf(Zone, #{
    mqtt := #{hibernate_after := HibernateAfter},
    force_gc := ForceGc,
    force_shutdown := ForceShutdown
}) ->
    zone_conf(Zone, HibernateAfter, ForceGc, ForceShutdown);
build_zone_conf(_Zone, _ZoneConf) ->
    undefined.

zone_conf(Zone, HibernateAfter, ForceGc, ForceShutdown) ->
    #zone_conf{
        name = Zone,
        hibernate_after = HibernateAfter,
        force_gc = force_gc(ForceGc),
        force_shutdown = ForceShutdown
    }.

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
