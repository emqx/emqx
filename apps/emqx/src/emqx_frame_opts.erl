%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_frame_opts).

-moduledoc """
Frame parser and serializer options, derived from zone config and shared
between connections.

Each zone has one `persistent_term` entry under the key
`{frame_parser_opts, Zone}`. Its value holds two groups of terms:

- `pre_connect`: the initial parse state and the initial serializer options
  that a connection uses until it receives CONNECT.
- `common`: for each MQTT protocol version, the parse state and the
  serializer options that a connection uses after CONNECT.

A term read out of `persistent_term` is not copied into the heap of the
reading process. So every connection that stores one of these terms in its
state refers to the same instance.

Only `post_zone_config_update/2` writes the entries. Connection processes
only read them. When an entry is absent, the functions here build the terms
locally from zone config.
""".

-include("emqx_mqtt.hrl").

-export([
    frame_opts/1,
    pre_connect/1,
    connected/4,
    post_zone_config_update/2
]).

-export_type([pre_connect/0]).

-define(KEY(Zone), {frame_parser_opts, Zone}).
-define(PROTO_VERS, [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]).

-type pre_connect() :: #{
    parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type connected() :: #{
    parse_state := emqx_frame:parse_state_initial(),
    serialize_opts := emqx_frame:serialize_opts()
}.

-type value() :: #{
    pre_connect := pre_connect(),
    common := #{emqx_types:proto_ver() => connected()}
}.

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
    case persistent_term:get(?KEY(Zone), undefined) of
        #{pre_connect := PreConnect} ->
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
    case persistent_term:get(?KEY(Zone), undefined) of
        #{common := #{ProtoVer := Shared}} ->
            #{parse_state := SharedParseState, serialize_opts := SharedSerializeOpts} = Shared,
            {
                same_or_given(SharedParseState, ParseState),
                same_or_given(SharedSerializeOpts, SerializeOpts)
            };
        _ ->
            {ParseState, SerializeOpts}
    end.

-doc """
Rebuild the entry of every zone in `NewZones` and delete the entries of the
zones that are removed.

`emqx_config_zones:post_update/2` calls this each time the zones config is
written, including the first write at boot.
""".
-spec post_zone_config_update(emqx_config:config(), emqx_config:config()) -> ok.
post_zone_config_update(OldZones, NewZones) ->
    ok = maps:foreach(fun update_zone/2, NewZones),
    lists:foreach(
        fun(Zone) -> _ = persistent_term:erase(?KEY(Zone)) end,
        maps:keys(maps:without(maps:keys(NewZones), OldZones))
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

same_or_given(Shared, Given) when Shared =:= Given ->
    Shared;
same_or_given(_Shared, Given) ->
    Given.

update_zone(Zone, #{
    mqtt := #{
        strict_mode := StrictMode,
        max_packet_size := MaxSize,
        max_connect_packet_size := MaxConnectSize,
        max_connect_user_properties := MaxConnectUserProps
    }
}) ->
    Value = build(frame_opts(StrictMode, MaxSize, MaxConnectSize, MaxConnectUserProps)),
    case persistent_term:get(?KEY(Zone), undefined) of
        Value -> ok;
        _ -> persistent_term:put(?KEY(Zone), Value)
    end;
update_zone(Zone, _ZoneConf) ->
    %% The zone config is not complete yet.
    _ = persistent_term:erase(?KEY(Zone)),
    ok.

-spec build(emqx_frame:options()) -> value().
build(FrameOpts) ->
    PreConnect = build_pre_connect(FrameOpts),
    #{parse_state := ParseState} = PreConnect,
    #{
        pre_connect => PreConnect,
        common => maps:from_list([
            {ProtoVer, #{
                parse_state => emqx_frame:connect_parsed(ProtoVer, ParseState),
                serialize_opts => emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE)
            }}
         || ProtoVer <- ?PROTO_VERS
        ])
    }.

build_pre_connect(FrameOpts) ->
    #{
        parse_state => emqx_frame:initial_parse_state(FrameOpts),
        serialize_opts => emqx_frame:initial_serialize_opts(FrameOpts)
    }.

frame_opts(StrictMode, MaxSize, MaxConnectSize, MaxConnectUserProps) ->
    #{
        strict_mode => StrictMode,
        max_size => MaxSize,
        max_connect_size => MaxConnectSize,
        max_connect_user_properties => MaxConnectUserProps,
        %% Any packet received before CONNECT is rejected by the parser.
        expect_connect => true
    }.
