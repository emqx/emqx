%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_emqx_frame).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_serialize_parse_connect() ->
    ?FORALL(
        Opts = #{version := ProtoVer},
        parse_opts(),
        begin
            ProtoName = proplists:get_value(ProtoVer, ?PROTOCOL_NAMES),
            Packet = ?CONNECT_PACKET(#mqtt_packet_connect{
                proto_name = ProtoName,
                proto_ver = ProtoVer,
                clientid = <<"clientId">>,
                will_qos = ?QOS_1,
                will_flag = true,
                will_retain = true,
                will_topic = <<"will">>,
                will_props = #{},
                will_payload = <<"bye">>,
                clean_start = true,
                properties = #{}
            }),
            Packet =:= parse_serialize(Packet, Opts)
        end
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

parse_serialize(Packet, Opts) when is_map(Opts) ->
    Ver = maps:get(version, Opts, ?MQTT_PROTO_V4),
    Bin = iolist_to_binary(emqx_frame:serialize(Packet, Ver)),
    ParseState = emqx_frame:initial_parse_state(Opts),
    {NPacket, <<>>, _} = emqx_frame:parse(Bin, ParseState),
    NPacket.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

parse_opts() ->
    ?LET(PropList, [{strict_mode, boolean()}, {version, range(4, 5)}], maps:from_list(PropList)).
