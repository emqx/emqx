%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_frame).

-behaviour(emqx_gateway_frame).

-include("emqx_jt808.hrl").
-include_lib("emqx/include/logger.hrl").

%% emqx_gateway_frame callbacks
-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

-define(FLAG, 1 / binary).
-define(BYTE, 8 / big - integer).
-define(WORD, 16 / big - integer).
-define(DWORD, 32 / big - integer).

-define(NO_FRAGMENT, 0).
-define(HAS_FRAGMENT, 1).

-type frame() :: map().

-type phase() :: searching_head_hex7e | {escaping_hex7d, binary()}.

-type parser_state() :: #{
    data => binary(),
    phase => phase(),
    opts => map()
}.

-export_type([frame/0]).

%%--------------------------------------------------------------------
%% Callback APIs
%%--------------------------------------------------------------------

-spec initial_parse_state(map()) -> parser_state().
initial_parse_state(Opts) ->
    #{data => <<>>, phase => searching_head_hex7e, opts => Opts}.

-spec serialize_opts() -> emqx_gateway_frame:serialize_options().
serialize_opts() ->
    #{}.

-spec parse(binary(), parser_state()) ->
    emqx_gateway_frame:parse_result().

parse(Bin, State) ->
    do_parse(Bin, State).

serialize_pkt(Frame, _Opts) ->
    serialize(Frame).

format(Msg) ->
    io_lib:format("~p", [Msg]).

type(_) ->
    jt808.

is_message(#{}) ->
    true;
is_message(_) ->
    false.

%%--------------------------------------------------------------------
%% Parse Message
%%--------------------------------------------------------------------
do_parse(Packet, State) ->
    escape_head_hex7e(Packet, State).

escape_head_hex7e(<<16#7e, Rest/binary>>, State = #{phase := searching_head_hex7e}) ->
    %% 0x7e is start of a valid message
    escape_frame(Rest, State);
escape_head_hex7e(Packet, State = #{phase := searching_head_hex7e}) ->
    %% First byte is not 0x7e, find and discard bytes until 0x7e or end of data
    case find_head_hex7e(Packet, <<>>) of
        {found, Discarded, Rest} ->
            %% Log discarded bytes as warning
            ?SLOG(warning, #{
                msg => discarded_invalid_data,
                reason => missing_frame_header,
                discarded_bytes => byte_size(Discarded),
                discarded_data => Discarded
            }),
            escape_frame(Rest, State);
        {not_found, Discarded} ->
            %% No 0x7e found, log and wait for more data
            ?SLOG(warning, #{
                msg => discarded_invalid_data,
                reason => missing_frame_header,
                discarded_bytes => byte_size(Discarded),
                discarded_data => Discarded
            }),
            {more, State}
    end;
escape_head_hex7e(<<16#02, Rest/binary>>, State = #{data := Acc, phase := escaping_hex7d}) ->
    %% corner case: 0x7d has been received in the end of last frame segment
    escape_frame(Rest, State#{data => <<Acc/binary, 16#7e>>});
escape_head_hex7e(<<16#01, Rest/binary>>, State = #{data := Acc, phase := escaping_hex7d}) ->
    %% corner case: 0x7d has been received in the end of last frame segment
    escape_frame(Rest, State#{data => <<Acc/binary, 16#7d>>});
escape_head_hex7e(Rest, State = #{data := _Acc, phase := escaping_hex7d}) ->
    %% continue parsing to escape 0x7d
    escape_frame(Rest, State).

%% @doc Find the first 0x7E byte in the packet, collecting discarded bytes
find_head_hex7e(<<>>, Discarded) ->
    {not_found, Discarded};
find_head_hex7e(<<16#7e, Rest/binary>>, Discarded) ->
    {found, Discarded, Rest};
find_head_hex7e(<<Byte:8, Rest/binary>>, Discarded) ->
    find_head_hex7e(Rest, <<Discarded/binary, Byte>>).

escape_frame(Rest, State = #{data := Acc}) ->
    Opts = maps:get(opts, State, #{}),
    case do_escape_frame(Rest, Acc) of
        {ok, Msg, NRest} ->
            {ok, parse_message(Msg, Opts), NRest, State#{
                data => <<>>, phase => searching_head_hex7e
            }};
        {more_data_follow, NRest} ->
            {more, #{data => NRest, phase => escaping_hex7d}}
    end.

do_escape_frame(<<16#7d, 16#02, Rest/binary>>, Acc) ->
    do_escape_frame(Rest, <<Acc/binary, 16#7e>>);
do_escape_frame(<<16#7d, 16#01, Rest/binary>>, Acc) ->
    do_escape_frame(Rest, <<Acc/binary, 16#7d>>);
do_escape_frame(<<16#7d, _Other:8, _Rest/binary>>, _Acc) ->
    %% only 0x02 and 0x01 is allowed to follow 0x7d
    error(invalid_message);
do_escape_frame(<<16#7d>>, Acc) ->
    %% corner case: last byte of the frame segment is 0x7d,
    %% 0x01 or 0x02 is expected in next frame segment
    {more_data_follow, Acc};
do_escape_frame(<<16#7e, _Rest/binary>>, <<>>) ->
    %% empty message
    error(invalid_message);
do_escape_frame(<<16#7e, Rest/binary>>, Acc) ->
    %% end of a normal message
    Msg = check(Acc),
    {ok, Msg, Rest};
do_escape_frame(<<Byte:8, Rest/binary>>, Acc) ->
    do_escape_frame(Rest, <<Acc/binary, Byte:8>>);
do_escape_frame(<<>>, Acc) ->
    {more_data_follow, Acc}.

parse_message(Binary, Opts) ->
    case parse_message_header(Binary) of
        {ok, Header = #{<<"msg_id">> := MsgId}, RestBinary} ->
            %% 2013 & 2019 JT/T 808 message parsing
            %% 0 means unknown proto ver (should be 2013 format, no proto ver field)
            %% 1 means 2019 format
            ProtoVer = maps:get(<<"proto_ver">>, Header, ?PROTO_VER_2013),
            Body =
                try
                    parse_message_body(MsgId, RestBinary, ProtoVer)
                catch
                    error:unknown_message_id ->
                        case maps:get(parse_unknown_message, Opts, true) of
                            true ->
                                #{
                                    <<"unknown_id">> => true,
                                    <<"data">> => base64:encode(RestBinary)
                                };
                            false ->
                                ?SLOG(error, #{
                                    msg => "unknown_message_id", id => MsgId, msg_body => RestBinary
                                }),
                                error(invalid_message)
                        end
                end,
            #{<<"header">> => Header, <<"body">> => Body};
        invalid_message ->
            error(invalid_message)
    end.

parse_message_header(
    <<MsgId:?WORD, _:1, 1:1, ?NO_FRAGMENT:1, Encrypt:3, Length:10, ProtoVer:?BYTE,
        PhoneBCD:10/binary, MsgSn:?WORD, Rest/binary>>
) ->
    %% 2019 format: bit 14 (version identifier) = 1
    %% Header: 17 bytes without fragmentation
    {ok,
        #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => Encrypt,
            <<"len">> => Length,
            <<"proto_ver">> => ProtoVer,
            <<"phone">> => from_bcd(PhoneBCD, []),
            <<"msg_sn">> => MsgSn
        },
        Rest};
parse_message_header(
    <<MsgId:?WORD, _:1, 1:1, ?HAS_FRAGMENT:1, Encrypt:3, Length:10, ProtoVer:?BYTE,
        PhoneBCD:10/binary, MsgSn:?WORD, FragTotal:?WORD, FragSeq:?WORD, Rest/binary>>
) ->
    %% 2019 format with fragmentation: 21 bytes header
    {ok,
        #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => Encrypt,
            <<"len">> => Length,
            <<"proto_ver">> => ProtoVer,
            <<"phone">> => from_bcd(PhoneBCD, []),
            <<"msg_sn">> => MsgSn,
            <<"frag_total">> => FragTotal,
            <<"frag_sn">> => FragSeq
        },
        Rest};
parse_message_header(
    <<MsgId:?WORD, _:2, ?NO_FRAGMENT:1, Encrypt:3, Length:10, PhoneBCD:6/binary, MsgSn:?WORD,
        Rest/binary>>
) ->
    %% 2013 format: bit 14 = 0 (reserved)
    %% Header: 12 bytes without fragmentation
    {ok,
        #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => Encrypt,
            <<"len">> => Length,
            <<"phone">> => from_bcd(PhoneBCD, []),
            <<"msg_sn">> => MsgSn
        },
        Rest};
parse_message_header(
    <<MsgId:?WORD, _:2, ?HAS_FRAGMENT:1, Encrypt:3, Length:10, PhoneBCD:6/binary, MsgSn:?WORD,
        FragTotal:?WORD, FragSeq:?WORD, Rest/binary>>
) ->
    %% 2013 format with fragmentation: 16 bytes header
    {ok,
        #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => Encrypt,
            <<"len">> => Length,
            <<"phone">> => from_bcd(PhoneBCD, []),
            <<"msg_sn">> => MsgSn,
            <<"frag_total">> => FragTotal,
            <<"frag_sn">> => FragSeq
        },
        Rest};
parse_message_header(_) ->
    invalid_message.

parse_message_body(?MC_GENERAL_RESPONSE, <<Seq:?WORD, Id:?WORD, Result:?BYTE>>, _ProtoVer) ->
    #{<<"seq">> => Seq, <<"id">> => Id, <<"result">> => Result};
parse_message_body(?MC_HEARTBEAT, <<>>, _ProtoVer) ->
    #{};
parse_message_body(?MC_DEREGISTER, <<>>, _ProtoVer) ->
    #{};
parse_message_body(?MC_QUERY_SERVER_TIME, <<>>, _ProtoVer) ->
    %% 2019: Query server time request - empty body
    #{};
parse_message_body(?MC_REQUEST_FRAGMENT, <<Seq:?WORD, Count:?WORD, Rest/binary>>, _ProtoVer) ->
    %% 2019: Terminal packet retransmission request
    {Ids, _} = word_array(Count, Rest, []),
    #{<<"seq">> => Seq, <<"count">> => Count, <<"ids">> => Ids};
parse_message_body(
    ?MC_REGISTER,
    <<Province:?WORD, City:?WORD, Manufacturer:11/binary, Model:30/binary, DevId:30/binary,
        Color:?BYTE, LicNumber/binary>>,
    ProtoVer
) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019 format: 11-byte manufacturer, 30-byte model, 30-byte device ID
    #{
        <<"province">> => Province,
        <<"city">> => City,
        <<"manufacturer">> => remove_tail_zero(Manufacturer),
        <<"model">> => remove_tail_zero(Model),
        <<"dev_id">> => remove_tail_zero(DevId),
        <<"color">> => Color,
        <<"license_number">> => LicNumber
    };
parse_message_body(
    ?MC_REGISTER,
    <<Province:?WORD, City:?WORD, Manufacturer:5/binary, Model:20/binary, DevId:7/binary,
        Color:?BYTE, LicNumber/binary>>,
    _ProtoVer
) ->
    %% 2013 format: 5-byte manufacturer, 20-byte model, 7-byte device ID
    #{
        <<"province">> => Province,
        <<"city">> => City,
        <<"manufacturer">> => Manufacturer,
        <<"model">> => remove_tail_zero(Model),
        <<"dev_id">> => remove_tail_zero(DevId),
        <<"color">> => Color,
        <<"license_number">> => LicNumber
    };
parse_message_body(?MC_AUTH, <<CodeLen:?BYTE, Rest/binary>>, ProtoVer) when
    ProtoVer >= ?PROTO_VER_2019
->
    %% 2019 format: length prefix + code + IMEI + software version
    <<Code:CodeLen/binary, IMEI:15/binary, SoftwareVersion:20/binary>> = Rest,
    #{
        <<"code">> => Code,
        <<"imei">> => remove_tail_zero(IMEI),
        <<"software_version">> => remove_tail_zero(SoftwareVersion)
    };
parse_message_body(?MC_AUTH, Binary, _ProtoVer) ->
    %% 2013 format: raw auth code
    #{<<"code">> => Binary};
parse_message_body(?MC_QUERY_PARAM_ACK, <<Seq:?WORD, Rest/binary>>, _ProtoVer) ->
    {Length, Params} = parse_client_params(Rest),
    #{<<"seq">> => Seq, <<"length">> => Length, <<"params">> => Params};
parse_message_body(
    ?MC_QUERY_ATTRIB_ACK,
    <<Type:?WORD, Manufacturer:5/binary, Model:30/binary, Id:30/binary, ICCID:10/binary,
        HVLen:?BYTE, Rest/binary>>,
    ProtoVer
) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019 format: 30-byte model, 30-byte ID
    <<HV:HVLen/binary, FVLen:?BYTE, Rest2/binary>> = Rest,
    <<FV:FVLen/binary, GNSSProp:?BYTE, CommProp:?BYTE>> = Rest2,
    #{
        <<"type">> => Type,
        <<"manufacturer">> => Manufacturer,
        <<"model">> => remove_tail_zero(Model),
        <<"id">> => remove_tail_zero(Id),
        <<"iccid">> => from_bcd(ICCID, []),
        <<"hardware_version">> => HV,
        <<"firmware_version">> => FV,
        <<"gnss_prop">> => GNSSProp,
        <<"comm_prop">> => CommProp
    };
parse_message_body(
    ?MC_QUERY_ATTRIB_ACK,
    <<Type:?WORD, Manufacturer:5/binary, Model:20/binary, Id:7/binary, ICCID:10/binary, HVLen:?BYTE,
        Rest/binary>>,
    _ProtoVer
) ->
    %% 2013 format: 20-byte model, 7-byte ID
    <<HV:HVLen/binary, FVLen:?BYTE, Rest2/binary>> = Rest,
    <<FV:FVLen/binary, GNSSProp:?BYTE, CommProp:?BYTE>> = Rest2,
    #{
        <<"type">> => Type,
        <<"manufacturer">> => Manufacturer,
        <<"model">> => remove_tail_zero(Model),
        <<"id">> => remove_tail_zero(Id),
        <<"iccid">> => from_bcd(ICCID, []),
        <<"hardware_version">> => HV,
        <<"firmware_version">> => FV,
        <<"gnss_prop">> => GNSSProp,
        <<"comm_prop">> => CommProp
    };
parse_message_body(?MC_OTA_ACK, <<Type:?BYTE, Result:?BYTE>>, _ProtoVer) ->
    #{<<"type">> => Type, <<"result">> => Result};
parse_message_body(?MC_LOCATION_REPORT, Binary, ProtoVer) ->
    parse_location_report(Binary, ProtoVer);
parse_message_body(?MC_QUERY_LOCATION_ACK, <<Seq:?WORD, Rest/binary>>, ProtoVer) ->
    Params = parse_location_report(Rest, ProtoVer),
    #{<<"seq">> => Seq, <<"params">> => Params};
parse_message_body(?MC_EVENT_REPORT, <<Id:?BYTE>>, _ProtoVer) ->
    #{<<"id">> => Id};
parse_message_body(?MC_QUESTION_ACK, <<Seq:?WORD, Id:?BYTE>>, _ProtoVer) ->
    #{<<"seq">> => Seq, <<"id">> => Id};
parse_message_body(?MC_INFO_REQ_CANCEL, <<Id:?BYTE, Flag:?BYTE>>, _ProtoVer) ->
    #{<<"id">> => Id, <<"flag">> => Flag};
parse_message_body(?MC_VEHICLE_CTRL_ACK, <<Seq:?WORD, Location/binary>>, ProtoVer) ->
    #{<<"seq">> => Seq, <<"location">> => parse_location_report(Location, ProtoVer)};
parse_message_body(?MC_QUERY_AREA_ROUTE_ACK, <<Type:?BYTE, Count:?DWORD, Rest/binary>>, _ProtoVer) ->
    %% 2019: Query area/route data response
    #{<<"type">> => Type, <<"count">> => Count, <<"data">> => base64:encode(Rest)};
parse_message_body(?MC_DRIVE_RECORD_REPORT, <<Seq:?WORD, Command:?BYTE, Data/binary>>, _ProtoVer) ->
    #{<<"seq">> => Seq, <<"command">> => Command, <<"data">> => base64:encode(Data)};
parse_message_body(?MC_WAYBILL_REPORT, <<Length:?DWORD, Data/binary>>, _ProtoVer) ->
    #{<<"length">> => Length, <<"data">> => base64:encode(Data)};
parse_message_body(
    ?MC_DRIVER_ID_REPORT,
    <<Status:?BYTE, TimeBCD:6/binary, Rest/binary>>,
    ProtoVer
) ->
    Base = #{
        <<"status">> => Status,
        <<"time">> => from_bcd(TimeBCD, [])
    },
    case Status of
        1 ->
            <<IcResult:?BYTE, Rest1/binary>> = Rest,
            case IcResult of
                0 ->
                    <<NameLength:?BYTE, Rest2/binary>> = Rest1,
                    <<Name:NameLength/binary, Certificate:20/binary, OrgLength:?BYTE, Rest3/binary>> =
                        Rest2,
                    <<Orgnization:OrgLength/binary, CertExpiryBCD:4/binary, Rest4/binary>> = Rest3,
                    Base1 = Base#{
                        <<"ic_result">> => IcResult,
                        <<"driver_name">> => Name,
                        <<"certificate">> => Certificate,
                        <<"organization">> => Orgnization,
                        <<"cert_expiry">> => from_bcd(CertExpiryBCD, [])
                    },
                    case ProtoVer >= ?PROTO_VER_2019 of
                        true ->
                            <<IdCard:20/binary>> = Rest4,
                            Base1#{<<"id_card">> => remove_tail_zero(IdCard)};
                        false ->
                            Base1
                    end;
                _ ->
                    Base#{
                        <<"ic_result">> => IcResult
                    }
            end;
        _ ->
            Base
    end;
parse_message_body(?MC_BULK_LOCATION_REPORT, <<Count:?WORD, Type:?BYTE, Rest/binary>>, ProtoVer) ->
    #{
        <<"type">> => Type,
        <<"length">> => Count,
        <<"location">> => parse_bulk_location_report(Count, Rest, ProtoVer, [])
    };
parse_message_body(?MC_CAN_BUS_REPORT, <<Count:?WORD, TimeBCD:5/binary, Rest/binary>>, _ProtoVer) ->
    CanData = parse_can_data(Count, Rest, []),
    #{<<"length">> => Count, <<"time">> => from_bcd(TimeBCD, []), <<"can_data">> => CanData};
parse_message_body(
    ?MC_MULTIMEDIA_EVENT_REPORT,
    <<Id:?DWORD, Type:?BYTE, Format:?BYTE, Event:?BYTE, Channel:?BYTE>>,
    _ProtoVer
) ->
    #{
        <<"id">> => Id,
        <<"type">> => Type,
        <<"format">> => Format,
        <<"event">> => Event,
        <<"channel">> => Channel
    };
parse_message_body(
    ?MC_MULTIMEDIA_DATA_REPORT,
    <<Id:?DWORD, Type:?BYTE, Format:?BYTE, Event:?BYTE, Channel:?BYTE, Location:28/binary,
        Multimedia/binary>>,
    ProtoVer
) ->
    #{
        <<"id">> => Id,
        <<"type">> => Type,
        <<"format">> => Format,
        <<"event">> => Event,
        <<"channel">> => Channel,
        <<"location">> => parse_location_report(Location, ProtoVer),
        <<"multimedia">> => base64:encode(Multimedia)
    };
parse_message_body(
    ?MC_CAMERA_SHOT_ACK, <<Seq:?WORD, Result:?BYTE, Count:?WORD, Rest/binary>>, _ProtoVer
) when
    Result =:= 0
->
    %% if Result is 0, means suceeded, "length" & "ids" present
    {Array, _} = dword_array(Count, Rest, []),
    #{<<"seq">> => Seq, <<"result">> => Result, <<"length">> => Count, <<"ids">> => Array};
parse_message_body(?MC_CAMERA_SHOT_ACK, <<Seq:?WORD, Result:?BYTE>>, _ProtoVer) ->
    %% if Result is not 0, means failed, no "length" & "ids"
    #{<<"seq">> => Seq, <<"result">> => Result};
parse_message_body(?MC_MM_DATA_SEARCH_ACK, <<Seq:?WORD, Count:?WORD, Rest/binary>>, ProtoVer) ->
    #{
        <<"seq">> => Seq,
        <<"length">> => Count,
        <<"result">> => parse_multimedia_search_result(Count, Rest, ProtoVer, [])
    };
parse_message_body(?MC_SEND_TRANSPARENT_DATA, <<Type:?BYTE, Data/binary>>, _ProtoVer) ->
    #{<<"type">> => Type, <<"data">> => base64:encode(Data)};
parse_message_body(?MC_SEND_ZIP_DATA, <<Length:?DWORD, Data/binary>>, _ProtoVer) ->
    #{<<"length">> => Length, <<"data">> => base64:encode(Data)};
parse_message_body(?MC_RSA_KEY, <<E:?DWORD, N:128/binary>>, _ProtoVer) ->
    #{<<"e">> => E, <<"n">> => base64:encode(N)};
parse_message_body(_, _, _ProtoVer) ->
    error(unknown_message_id).

parse_client_params(<<Count:?BYTE, Rest/binary>>) ->
    {Count, parse_client_params2(Count, Rest, [])}.

parse_client_params2(0, _Rest, Acc) ->
    lists:reverse(Acc);
parse_client_params2(Count, <<Id:?DWORD, Length:?BYTE, Rest/binary>>, Acc) ->
    {Value, Rest3} =
        case client_param_data_type(Id) of
            dword -> decode_cp_dword(Rest);
            word -> decode_cp_word(Rest);
            byte -> decode_cp_byte(Rest);
            string -> decode_cp_string(Length, Rest);
            byte8 -> decode_cp_byte8(Rest);
            reserved -> decode_cp_reserved(Length, Rest)
        end,
    parse_client_params2(Count - 1, Rest3, [#{<<"id">> => Id, <<"value">> => Value} | Acc]).

decode_cp_dword(<<Value:?DWORD, Rest/binary>>) ->
    {Value, Rest}.

decode_cp_word(<<Value:?WORD, Rest/binary>>) ->
    {Value, Rest}.

decode_cp_byte(<<Value:?BYTE, Rest/binary>>) ->
    {Value, Rest}.

decode_cp_string(Length, Binary) ->
    <<Value:Length/binary, Rest/binary>> = Binary,
    {Value, Rest}.

decode_cp_byte8(<<Value:8/binary, Rest/binary>>) ->
    {base64:encode(Value), Rest}.

decode_cp_reserved(Length, Binary) ->
    <<Value:Length/binary, Rest/binary>> = Binary,
    {base64:encode(Value), Rest}.

parse_location_report(
    <<Alarm:?DWORD, Status:?DWORD, Latitude:?DWORD, Longitude:?DWORD, Altitude:?WORD, Speed:?WORD,
        Direction:?WORD, TimeBCD:6/binary, Rest/binary>>,
    ProtoVer
) ->
    Ret = #{
        <<"alarm">> => Alarm,
        <<"status">> => Status,
        <<"latitude">> => Latitude,
        <<"longitude">> => Longitude,
        <<"altitude">> => Altitude,
        <<"speed">> => Speed,
        <<"direction">> => Direction,
        <<"time">> => from_bcd(TimeBCD, [])
    },
    case Rest of
        <<>> -> Ret;
        _ -> Ret#{<<"extra">> => parse_location_report_extra(Rest, ProtoVer, #{})}
    end.

parse_location_report_extra(<<>>, _ProtoVer, Acc) ->
    Acc;
parse_location_report_extra(
    <<?CP_POS_EXTRA_MILEAGE:?BYTE, 4:?BYTE, MileAge:?DWORD, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"mileage">> => MileAge});
parse_location_report_extra(
    <<?CP_POS_EXTRA_FUEL_METER:?BYTE, 2:?BYTE, FuelMeter:?WORD, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"fuel_meter">> => FuelMeter});
parse_location_report_extra(
    <<?CP_POS_EXTRA_SPEED:?BYTE, 2:?BYTE, Speed:?WORD, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"speed">> => Speed});
parse_location_report_extra(
    <<?CP_POS_EXTRA_ALARM_ID:?BYTE, 2:?BYTE, AlarmID:?WORD, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"alarm_id">> => AlarmID});
parse_location_report_extra(
    <<?CP_POS_EXTRA_TIRE_PRESSURE:?BYTE, Length:?BYTE, TirePressure:Length/binary, Rest/binary>>,
    ProtoVer,
    Acc
) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019: Tire pressure, variable length, unit: Pa
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"tire_pressure">> => base64:encode(TirePressure)
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_CARRIAGE_TEMP:?BYTE, 2:?BYTE, Temp:16/signed-big, Rest/binary>>, ProtoVer, Acc
) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019: Carriage temperature, 2 bytes, unit: Celsius, range: -32767 ~ +32767
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"carriage_temp">> => Temp});
parse_location_report_extra(
    <<?CP_POS_EXTRA_OVERSPEED_ALARM:?BYTE, Length:?BYTE, Rest/binary>>, ProtoVer, Acc
) ->
    case Length of
        1 ->
            <<Type:?BYTE, Rest2/binary>> = Rest,
            parse_location_report_extra(Rest2, ProtoVer, Acc#{
                <<"overspeed_alarm">> => #{<<"type">> => Type}
            });
        5 ->
            <<Type:?BYTE, Id:?DWORD, Rest2/binary>> = Rest,
            parse_location_report_extra(Rest2, ProtoVer, Acc#{
                <<"overspeed_alarm">> => #{<<"type">> => Type, <<"id">> => Id}
            })
    end;
parse_location_report_extra(
    <<?CP_POS_EXTRA_IN_OUT_ALARM:?BYTE, 6:?BYTE, Type:?BYTE, Id:?DWORD, Direction:?BYTE,
        Rest/binary>>,
    ProtoVer,
    Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"in_out_alarm">> => #{<<"type">> => Type, <<"id">> => Id, <<"direction">> => Direction}
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_PATH_TIME_ALARM:?BYTE, 7:?BYTE, Id:?DWORD, Time:?WORD, Result:?BYTE,
        Rest/binary>>,
    ProtoVer,
    Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"path_time_alarm">> => #{<<"id">> => Id, <<"time">> => Time, <<"result">> => Result}
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_EXPANDED_SIGNAL:?BYTE, 4:?BYTE, Signal:4/binary, Rest/binary>>, ProtoVer, Acc
) ->
    <<LowBeam:1, HighBeam:1, RightTurnSignal:1, LeftTurnSignal:1, Brake:1, Reverse:1, Fog:1,
        SideMarker:1, Horn:1, AirConditioner:1, Neutral:1, Retarder:1, ABS:1, Heater:1, Cluth:1,
        _:17>> = Signal,
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"signal">> => #{
            <<"low_beam">> => LowBeam,
            <<"high_beam">> => HighBeam,
            <<"right_turn">> => RightTurnSignal,
            <<"left_turn">> => LeftTurnSignal,
            <<"brake">> => Brake,
            <<"reverse">> => Reverse,
            <<"fog">> => Fog,
            <<"side_marker">> => SideMarker,
            <<"horn">> => Horn,
            <<"air_conditioner">> => AirConditioner,
            <<"neutral">> => Neutral,
            <<"retarder">> => Retarder,
            <<"abs">> => ABS,
            <<"heater">> => Heater,
            <<"cluth">> => Cluth
        }
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_IO_STATUS:?BYTE, 2:?BYTE, DeepSleep:1, Sleep:1, _:14, Rest/binary>>,
    ProtoVer,
    Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"io_status">> => #{<<"deep_sleep">> => DeepSleep, <<"sleep">> => Sleep}
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_ANALOG:?BYTE, 4:?BYTE, AD1:?WORD, AD0:?WORD, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{
        <<"analog">> => #{<<"ad0">> => AD0, <<"ad1">> => AD1}
    });
parse_location_report_extra(
    <<?CP_POS_EXTRA_RSSI:?BYTE, 1:?BYTE, Rssi:?BYTE, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"rssi">> => Rssi});
parse_location_report_extra(
    <<?CP_POS_EXTRA_GNSS_SAT_NUM:?BYTE, 1:?BYTE, SatNum:?BYTE, Rest/binary>>, ProtoVer, Acc
) ->
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"gnss_sat_num">> => SatNum});
%% TODO: ensure custom data
parse_location_report_extra(
    <<?CP_POS_EXTRA_CUSTOME:?BYTE, Size:?BYTE, Rest/binary>>, ProtoVer, Acc
) ->
    <<Data:Size/binary, _Rest2/binary>> = Rest,
    parse_location_report_extra(Rest, ProtoVer, Acc#{<<"custome">> => base64:encode(Data)});
parse_location_report_extra(<<CustomeId:?BYTE, Size:?BYTE, Rest/binary>>, ProtoVer, Acc) when
    CustomeId > ?CP_POS_EXTRA_CUSTOME andalso CustomeId =< ?CP_POS_EXTRA_MAX
->
    <<Data:Size/binary, Rest2/binary>> = Rest,
    Custome = maps:get(<<"custome">>, Acc, #{}),
    NCustomeId = integer_to_binary(CustomeId),
    parse_location_report_extra(
        Rest2,
        ProtoVer,
        Acc#{<<"custome">> => Custome#{NCustomeId => base64:encode(Data)}}
    );
parse_location_report_extra(<<ReservedId0:?BYTE, Size:?BYTE, Rest/binary>>, ProtoVer, Acc) ->
    <<Data:Size/binary, Rest2/binary>> = Rest,
    ReservedId = integer_to_binary(ReservedId0),
    parse_location_report_extra(
        Rest2,
        ProtoVer,
        Acc#{ReservedId => base64:encode(Data)}
    ).

parse_bulk_location_report(0, _Binary, _ProtoVer, Acc) ->
    lists:reverse(Acc);
parse_bulk_location_report(Count, <<Length:?WORD, Rest/binary>>, ProtoVer, Acc) ->
    <<Data:Length/binary, Rest2/binary>> = Rest,
    parse_bulk_location_report(Count - 1, Rest2, ProtoVer, [
        parse_location_report(Data, ProtoVer) | Acc
    ]).

parse_can_data(0, _, Acc) ->
    lists:reverse(Acc);
parse_can_data(
    Count,
    <<CanCh:1, CanFrameType:1, CanDataMethod:1, CanId:29/integer-big, Data:8/binary, Rest/binary>>,
    Acc
) ->
    parse_can_data(Count - 1, Rest, [
        #{
            <<"channel">> => CanCh,
            <<"frame_type">> => CanFrameType,
            <<"data_method">> => CanDataMethod,
            <<"id">> => CanId,
            <<"data">> => base64:encode(Data)
        }
        | Acc
    ]).

word_array(0, Binary, Acc) ->
    {lists:reverse(Acc), Binary};
word_array(Count, <<Value:?WORD, Rest/binary>>, Acc) ->
    word_array(Count - 1, Rest, [Value | Acc]).

dword_array(0, Binary, Acc) ->
    {lists:reverse(Acc), Binary};
dword_array(Count, <<Value:?DWORD, Rest/binary>>, Acc) ->
    dword_array(Count - 1, Rest, [Value | Acc]).

parse_multimedia_search_result(0, _, _ProtoVer, Acc) ->
    lists:reverse(Acc);
parse_multimedia_search_result(
    Count,
    <<Id:?DWORD, Type:?BYTE, Channel:?BYTE, Event:?BYTE, Location:28/binary, Rest/binary>>,
    ProtoVer,
    Acc
) ->
    %% Location is 28 bytes (basic location info) for both 2013 and 2019
    %% ProtoVer is passed to parse_location_report for version-aware interpretation
    parse_multimedia_search_result(Count - 1, Rest, ProtoVer, [
        #{
            <<"id">> => Id,
            <<"type">> => Type,
            <<"channel">> => Channel,
            <<"event">> => Event,
            <<"location">> => parse_location_report(Location, ProtoVer)
        }
        | Acc
    ]).

%%--------------------------------------------------------------------
%% Serialize JT808 Message
%%--------------------------------------------------------------------
serialize(Json) ->
    Header = maps:get(<<"header">>, Json),
    Body =
        case maps:is_key(<<"body">>, Json) of
            true -> maps:get(<<"body">>, Json);
            false -> <<>>
        end,
    %% Extract proto_ver for version-aware body serialization (0 for 2013, >=1 for 2019)
    ProtoVer = maps:get(<<"proto_ver">>, Header, ?PROTO_VER_2013),
    BodyStream = serialize_body(maps:get(<<"msg_id">>, Header), Body, ProtoVer),
    %% TODO: encrypt body here
    HeaderStream = serialize_header(Header#{<<"len">> => size(BodyStream)}),
    packet(<<HeaderStream/binary, BodyStream/binary>>).

serialize_header(
    Header = #{
        <<"msg_id">> := MsgId,
        <<"encrypt">> := Encrypt,
        <<"len">> := Length,
        <<"phone">> := Phone,
        <<"msg_sn">> := MsgSn,
        <<"proto_ver">> := ProtoVer
    }
) ->
    %% 2019 format: proto_ver present, use BCD[10] phone, set bit 14 = 1
    PhoneBCD = to_bcd(Phone, 10),
    {Fragment, Total, Seq} = extract_fragment_info(Header),
    Binary =
        <<MsgId:?WORD, 0:1, 1:1, Fragment:1, Encrypt:3, Length:10/integer-big, ProtoVer:?BYTE,
            PhoneBCD:10/binary, MsgSn:?WORD>>,
    maybe_append_fragment(Binary, Fragment, Total, Seq);
serialize_header(
    Header = #{
        <<"msg_id">> := MsgId,
        <<"encrypt">> := Encrypt,
        <<"len">> := Length,
        <<"phone">> := Phone,
        <<"msg_sn">> := MsgSn
    }
) ->
    %% 2013 format: no proto_ver, use BCD[6] phone, bit 14 = 0
    PhoneBCD = to_bcd(Phone, 6),
    {Fragment, Total, Seq} = extract_fragment_info(Header),
    Binary =
        <<MsgId:?WORD, 0:2, Fragment:1, Encrypt:3, Length:10/integer-big, PhoneBCD:6/binary,
            MsgSn:?WORD>>,
    maybe_append_fragment(Binary, Fragment, Total, Seq).

serialize_body(?MS_GENERAL_RESPONSE, Body, _ProtoVer) ->
    Seq = maps:get(<<"seq">>, Body),
    Id = maps:get(<<"id">>, Body),
    Result = maps:get(<<"result">>, Body),
    <<Seq:?WORD, Id:?WORD, Result:?BYTE>>;
serialize_body(?MS_REQUEST_FRAGMENT, Body, ProtoVer) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019 format: Length is WORD (changed from BYTE in 2013)
    Seq = maps:get(<<"seq">>, Body),
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    LastStream = encode_word_array(Length, Ids, <<>>),
    <<Seq:?WORD, Length:?WORD, LastStream/binary>>;
serialize_body(?MS_REQUEST_FRAGMENT, Body, _ProtoVer) ->
    %% 2013 format: Length is BYTE
    Seq = maps:get(<<"seq">>, Body),
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    LastStream = encode_word_array(Length, Ids, <<>>),
    <<Seq:?WORD, Length:?BYTE, LastStream/binary>>;
serialize_body(?MS_SERVER_TIME_ACK, Body, _ProtoVer) ->
    %% 2019: Server time response - BCD[6] UTC time
    Time = maps:get(<<"time">>, Body),
    TimeBCD = to_bcd(Time, 6),
    TimeBCD;
serialize_body(?MS_REGISTER_ACK, Body, _ProtoVer) ->
    Seq = maps:get(<<"seq">>, Body),
    %% XXX: replaced by maroc?
    Result = maps:get(<<"result">>, Body),
    case maps:is_key(<<"auth_code">>, Body) of
        true ->
            Code = maps:get(<<"auth_code">>, Body),
            <<Seq:?WORD, Result:?BYTE, Code/binary>>;
        false ->
            %% If the terminal regiter failed, it don't contain auth code
            <<Seq:?WORD, Result:?BYTE>>
    end;
serialize_body(?MS_SET_CLIENT_PARAM, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    ParamList = maps:get(<<"params">>, Body),
    serialize_client_param(<<Length:?BYTE>>, ParamList);
serialize_body(?MS_QUERY_CLIENT_ALL_PARAM, _Body, _ProtoVer) ->
    <<>>;
serialize_body(?MS_QUERY_CLIENT_PARAM, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    List = maps:get(<<"ids">>, Body),
    encode_dword_array(Length, List, <<Length:?BYTE>>);
serialize_body(?MS_CLIENT_CONTROL, Body, _ProtoVer) ->
    Command = maps:get(<<"command">>, Body),
    Param = maps:get(<<"param">>, Body),
    <<Command:?BYTE, Param/binary>>;
serialize_body(?MS_QUERY_CLIENT_ATTRIB, _Body, _ProtoVer) ->
    <<>>;
serialize_body(?MS_OTA, Body, _ProtoVer) ->
    %% TODO: OTA in this way?
    Type = maps:get(<<"type">>, Body),
    Manuf = maps:get(<<"manufacturer">>, Body),
    VerLength = maps:get(<<"ver_len">>, Body),
    Version = maps:get(<<"version">>, Body),
    FwLen = maps:get(<<"fw_len">>, Body),
    Firmware = maps:get(<<"firmware">>, Body),
    <<Type:?BYTE, Manuf:5/binary, VerLength:?BYTE, Version/binary, FwLen:?DWORD, Firmware/binary>>;
serialize_body(?MS_QUERY_LOCATION, _Body, _ProtoVer) ->
    <<>>;
serialize_body(?MS_TRACE_LOCATION, Body, _ProtoVer) ->
    Period = maps:get(<<"period">>, Body),
    Expiry = maps:get(<<"expiry">>, Body),
    <<Period:?WORD, Expiry:?DWORD>>;
serialize_body(?MS_CONFIRM_ALARM, Body, _ProtoVer) ->
    Seq = maps:get(<<"seq">>, Body),
    Type = maps:get(<<"type">>, Body),
    <<Seq:?WORD, Type:?DWORD>>;
serialize_body(?MS_LINK_DETECT, _Body, _ProtoVer) ->
    %% 2019: Link detection - empty body
    <<>>;
serialize_body(?MS_SEND_TEXT, Body, ProtoVer) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019 format: Flag + Type + Text
    Flag = maps:get(<<"flag">>, Body),
    TextType = maps:get(<<"text_type">>, Body, 1),
    Text = maps:get(<<"text">>, Body),
    <<Flag:?BYTE, TextType:?BYTE, Text/binary>>;
serialize_body(?MS_SEND_TEXT, Body, _ProtoVer) ->
    %% 2013 format: Flag + Text (no type field)
    Flag = maps:get(<<"flag">>, Body),
    Text = maps:get(<<"text">>, Body),
    <<Flag:?BYTE, Text/binary>>;
serialize_body(?MS_SET_EVENT, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    %% FIXME: If the type is 0, the length and events is empty
    Length = maps:get(<<"length">>, Body),
    Events = maps:get(<<"events">>, Body),
    serialize_events(Events, <<Type:?BYTE, Length:?BYTE>>);
serialize_body(?MS_SEND_QUESTION, Body, _ProtoVer) ->
    Flag = maps:get(<<"flag">>, Body),
    Length = maps:get(<<"length">>, Body),
    Question = maps:get(<<"question">>, Body),
    Answers = maps:get(<<"answers">>, Body),
    serialize_candidate_answers(Answers, <<Flag:?BYTE, Length:?BYTE, Question/binary>>);
serialize_body(?MS_SET_MENU, Body, _ProtoVer) ->
    %% XXX: If the tpye is delete all menu, the remaining bytes should be drop?
    Type = maps:get(<<"type">>, Body),
    Length = maps:get(<<"length">>, Body),
    Menus = maps:get(<<"menus">>, Body),
    serialize_menus(Menus, <<Type:?BYTE, Length:?BYTE>>);
serialize_body(?MS_INFO_CONTENT, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Length = maps:get(<<"length">>, Body),
    Info = maps:get(<<"info">>, Body),
    <<Type:?BYTE, Length:?WORD, Info/binary>>;
serialize_body(?MS_PHONE_CALLBACK, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Phone = maps:get(<<"phone">>, Body),
    <<Type:?BYTE, Phone/binary>>;
serialize_body(?MS_SET_PHONE_NUMBER, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Length = maps:get(<<"length">>, Body),
    Contacts = maps:get(<<"contacts">>, Body),
    serialize_contacts(Contacts, <<Type:?BYTE, Length:?BYTE>>);
serialize_body(?MS_VEHICLE_CONTROL, Body, ProtoVer) when ProtoVer >= ?PROTO_VER_2019 ->
    %% 2019 format: Control type list
    Count = maps:get(<<"count">>, Body, 0),
    Controls = maps:get(<<"controls">>, Body, []),
    serialize_control_types(Controls, <<Count:?WORD>>);
serialize_body(?MS_VEHICLE_CONTROL, Body, _ProtoVer) ->
    %% 2013 format: Single flag byte
    Flag = maps:get(<<"flag">>, Body),
    <<Flag:?BYTE>>;
serialize_body(?MS_SET_CIRCLE_AREA, Body, ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Length = maps:get(<<"length">>, Body),
    Areas = maps:get(<<"areas">>, Body),
    serialize_circle_area(Areas, <<Type:?BYTE, Length:?BYTE>>, ProtoVer);
serialize_body(?MS_DEL_CIRCLE_AREA, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    encode_dword_array(Length, Ids, <<Length:?BYTE>>);
serialize_body(?MS_SET_RECT_AREA, Body, ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Length = maps:get(<<"length">>, Body),
    Areas = maps:get(<<"areas">>, Body),
    serialize_rect_area(Areas, <<Type:?BYTE, Length:?BYTE>>, ProtoVer);
serialize_body(?MS_DEL_RECT_AREA, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    encode_dword_array(Length, Ids, <<Length:?BYTE>>);
serialize_body(?MS_SET_POLY_AREA, Body, ProtoVer) ->
    Id = maps:get(<<"id">>, Body),
    Flag = maps:get(<<"flag">>, Body),
    StartTime = maps:get(<<"start_time">>, Body),
    EndTime = maps:get(<<"end_time">>, Body),
    MaxSpeed = maps:get(<<"max_speed">>, Body),
    Overspeed = maps:get(<<"overspeed_duration">>, Body),
    Length = maps:get(<<"length">>, Body),
    Points = maps:get(<<"points">>, Body),
    StartBCD = to_bcd(StartTime, 6),
    EndBCD = to_bcd(EndTime, 6),
    PointsStream = serialize_poly_point(
        Length,
        Points,
        <<Id:?DWORD, Flag:?WORD, StartBCD:6/binary, EndBCD:6/binary, MaxSpeed:?WORD,
            Overspeed:?BYTE, Length:?WORD>>
    ),
    %% 2019 adds night_max_speed and name after points
    case ProtoVer >= ?PROTO_VER_2019 of
        true ->
            NightMaxSpeed = maps:get(<<"night_max_speed">>, Body, 0),
            Name = maps:get(<<"name">>, Body, <<>>),
            NameLen = byte_size(Name),
            <<PointsStream/binary, NightMaxSpeed:?WORD, NameLen:?WORD, Name/binary>>;
        false ->
            PointsStream
    end;
serialize_body(?MS_DEL_POLY_AREA, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    encode_dword_array(Length, Ids, <<Length:?BYTE>>);
serialize_body(?MS_SET_PATH, Body, ProtoVer) ->
    Id = maps:get(<<"id">>, Body),
    Flag = maps:get(<<"flag">>, Body),
    StartTime = maps:get(<<"start_time">>, Body),
    EndTime = maps:get(<<"end_time">>, Body),
    Length = maps:get(<<"length">>, Body),
    Points = maps:get(<<"points">>, Body),
    StartBCD = to_bcd(StartTime, 6),
    EndBCD = to_bcd(EndTime, 6),
    PointsStream = serialize_corner_point(
        Length, Points, <<Id:?DWORD, Flag:?WORD, StartBCD:6/binary, EndBCD:6/binary, Length:?WORD>>
    ),
    %% 2019 adds name_length and name after corner points
    case ProtoVer >= ?PROTO_VER_2019 of
        true ->
            Name = maps:get(<<"name">>, Body, <<>>),
            NameLen = byte_size(Name),
            <<PointsStream/binary, NameLen:?WORD, Name/binary>>;
        false ->
            PointsStream
    end;
serialize_body(?MS_DEL_PATH, Body, _ProtoVer) ->
    Length = maps:get(<<"length">>, Body),
    Ids = maps:get(<<"ids">>, Body),
    encode_dword_array(Length, Ids, <<Length:?BYTE>>);
serialize_body(?MS_QUERY_AREA_ROUTE, Body, _ProtoVer) ->
    %% 2019: Query area/route data
    Type = maps:get(<<"type">>, Body),
    Count = maps:get(<<"count">>, Body),
    case Count of
        0 ->
            <<Type:?BYTE, Count:?DWORD>>;
        _ ->
            Ids = maps:get(<<"ids">>, Body),
            encode_dword_array(Count, Ids, <<Type:?BYTE, Count:?DWORD>>)
    end;
serialize_body(?MS_DRIVE_RECORD_CAPTURE, Body, _ProtoVer) ->
    Command = maps:get(<<"command">>, Body),
    Param = maps:get(<<"param">>, Body),
    RawParam = base64:decode(Param),
    <<Command:?BYTE, RawParam/binary>>;
serialize_body(?MS_DRIVE_REC_PARAM_SEND, Body, _ProtoVer) ->
    Command = maps:get(<<"command">>, Body),
    Param = maps:get(<<"param">>, Body),
    RawParam = base64:decode(Param),
    <<Command:?BYTE, RawParam/binary>>;
serialize_body(?MS_REQ_DRIVER_ID, _Body, _ProtoVer) ->
    <<>>;
serialize_body(?MS_MULTIMEDIA_DATA_ACK, Body, _ProtoVer) ->
    MmId = maps:get(<<"mm_id">>, Body),
    Length = maps:get(<<"length">>, Body),
    RetxIds = maps:get(<<"retx_ids">>, Body),
    encode_word_array(Length, RetxIds, <<MmId:?DWORD, Length:?BYTE>>);
serialize_body(?MS_CAMERA_SHOT, Body, _ProtoVer) ->
    ChId = maps:get(<<"channel_id">>, Body),
    Command = maps:get(<<"command">>, Body),
    Period = maps:get(<<"period">>, Body),
    Save = maps:get(<<"save">>, Body),
    Resolution = maps:get(<<"resolution">>, Body),
    Quality = maps:get(<<"quality">>, Body),
    Bright = maps:get(<<"bright">>, Body),
    Contrast = maps:get(<<"contrast">>, Body),
    Saturate = maps:get(<<"saturate">>, Body),
    Chromaticity = maps:get(<<"chromaticity">>, Body),
    <<ChId:?BYTE, Command:?WORD, Period:?WORD, Save:?BYTE, Resolution:?BYTE, Quality:?BYTE,
        Bright:?BYTE, Contrast:?BYTE, Saturate:?BYTE, Chromaticity:?BYTE>>;
serialize_body(?MS_MM_DATA_SEARCH, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    Channel = maps:get(<<"channel">>, Body),
    Event = maps:get(<<"event">>, Body),
    StartTime = maps:get(<<"start_time">>, Body),
    EndTime = maps:get(<<"end_time">>, Body),
    StartBCD = to_bcd(StartTime, 6),
    EndBCD = to_bcd(EndTime, 6),
    <<Type:?BYTE, Channel:?BYTE, Event:?BYTE, StartBCD:6/binary, EndBCD:6/binary>>;
serialize_body(?MS_MM_DATA_UPLOAD, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    ChId = maps:get(<<"channel">>, Body),
    Event = maps:get(<<"event">>, Body),
    Start = maps:get(<<"start_time">>, Body),
    End = maps:get(<<"end_time">>, Body),
    Delete = maps:get(<<"delete">>, Body),
    StartBCD = to_bcd(Start, 6),
    EndBCD = to_bcd(End, 6),
    <<Type:?BYTE, ChId:?BYTE, Event:?BYTE, StartBCD:6/binary, EndBCD:6/binary, Delete:?BYTE>>;
serialize_body(?MS_VOICE_RECORD, Body, _ProtoVer) ->
    Command = maps:get(<<"command">>, Body),
    Time = maps:get(<<"time">>, Body),
    Save = maps:get(<<"save">>, Body),
    Rate = maps:get(<<"rate">>, Body),
    <<Command:?BYTE, Time:?WORD, Save:?BYTE, Rate:?BYTE>>;
serialize_body(?MS_SINGLE_MM_DATA_CTRL, Body, _ProtoVer) ->
    Id = maps:get(<<"id">>, Body),
    Flag = maps:get(<<"flag">>, Body),
    <<Id:?DWORD, Flag:?BYTE>>;
serialize_body(?MS_SEND_TRANSPARENT_DATA, Body, _ProtoVer) ->
    Type = maps:get(<<"type">>, Body),
    DataBase64 = maps:get(<<"data">>, Body),
    Data = base64:decode(DataBase64),
    <<Type:?BYTE, Data/binary>>;
serialize_body(?MS_RSA_KEY, Body, _ProtoVer) ->
    E = maps:get(<<"e">>, Body),
    N = maps:get(<<"n">>, Body),
    <<E:?DWORD, N:128/binary>>;
serialize_body(_UnkonwnMsgId, _Body, _ProtoVer) ->
    error(invalid_input).

%% Helper for 2019 vehicle control type list
serialize_control_types([], Acc) ->
    Acc;
serialize_control_types([#{<<"id">> := Id, <<"param">> := Param} | T], Acc) ->
    serialize_control_types(T, <<Acc/binary, Id:?WORD, Param/binary>>).

serialize_corner_point(0, [], Acc) ->
    Acc;
serialize_corner_point(
    Count,
    [
        #{
            <<"point_id">> := PointId,
            <<"path_id">> := PathId,
            <<"point_lat">> := Lat,
            <<"point_lng">> := Lng,
            <<"width">> := Width,
            <<"attrib">> := Attrib,
            <<"passed">> := Passed,
            <<"uncovered">> := Uncovered,
            <<"max_speed">> := MaxSpeed,
            <<"overspeed_duration">> := Overspeed
        }
        | T
    ],
    Acc
) ->
    serialize_corner_point(
        Count - 1,
        T,
        <<Acc/binary, PointId:?DWORD, PathId:?DWORD, Lat:?DWORD, Lng:?DWORD, Width:?BYTE,
            Attrib:?BYTE, Passed:?WORD, Uncovered:?WORD, MaxSpeed:?WORD, Overspeed:?BYTE>>
    ).

serialize_poly_point(0, _, Acc) ->
    Acc;
serialize_poly_point(Count, [#{<<"lat">> := Lat, <<"lng">> := Lng} | T], Acc) ->
    serialize_poly_point(Count - 1, T, <<Acc/binary, Lat:?DWORD, Lng:?DWORD>>).

serialize_rect_area([], Acc, _ProtoVer) ->
    Acc;
serialize_rect_area(
    [
        H = #{
            <<"id">> := Id,
            <<"flag">> := Flag,
            <<"lt_lat">> := LtLatitude,
            <<"lt_lng">> := LtLongitude,
            <<"rb_lat">> := RbLatitude,
            <<"rb_lng">> := RbLongitude,
            <<"start_time">> := StartTime,
            <<"end_time">> := EndTime,
            <<"max_speed">> := MaxSpeed,
            <<"overspeed_duration">> := Overspeed
        }
        | T
    ],
    Acc,
    ProtoVer
) ->
    StartBCD = to_bcd(StartTime, 6),
    EndBCD = to_bcd(EndTime, 6),
    Base =
        <<Acc/binary, Id:?DWORD, Flag:?WORD, LtLatitude:?DWORD, LtLongitude:?DWORD,
            RbLatitude:?DWORD, RbLongitude:?DWORD, StartBCD:6/binary, EndBCD:6/binary,
            MaxSpeed:?WORD, Overspeed:?BYTE>>,
    %% 2019 adds night_max_speed and name fields
    NewAcc =
        case ProtoVer >= ?PROTO_VER_2019 of
            true ->
                NightMaxSpeed = maps:get(<<"night_max_speed">>, H, 0),
                Name = maps:get(<<"name">>, H, <<>>),
                NameLen = byte_size(Name),
                <<Base/binary, NightMaxSpeed:?WORD, NameLen:?WORD, Name/binary>>;
            false ->
                Base
        end,
    serialize_rect_area(T, NewAcc, ProtoVer).

serialize_circle_area([], Acc, _ProtoVer) ->
    Acc;
serialize_circle_area(
    [
        H = #{
            <<"id">> := Id,
            <<"flag">> := Flag,
            <<"center_latitude">> := Latitude,
            <<"center_longitude">> := Longitude,
            <<"radius">> := Radius
        }
        | T
    ],
    Acc,
    ProtoVer
) ->
    First = <<Acc/binary, Id:?DWORD, Flag:?WORD, Latitude:?DWORD, Longitude:?DWORD, Radius:?DWORD>>,
    Second =
        case maps:is_key(<<"start_time">>, H) of
            true ->
                #{<<"start_time">> := StartTime, <<"end_time">> := EndTime} = H,
                StartBCD = to_bcd(StartTime, 6),
                EndBCD = to_bcd(EndTime, 6),
                <<First/binary, StartBCD:6/binary, EndBCD:6/binary>>;
            false ->
                First
        end,
    Third =
        case maps:is_key(<<"max_speed">>, H) of
            true ->
                #{<<"max_speed">> := MaxSpeed, <<"overspeed_duration">> := Overspeed} = H,
                <<Second/binary, MaxSpeed:?WORD, Overspeed:?BYTE>>;
            false ->
                Second
        end,
    %% 2019 adds night_max_speed and name fields (only if max_speed is present)
    Fourth =
        case ProtoVer >= ?PROTO_VER_2019 andalso maps:is_key(<<"max_speed">>, H) of
            true ->
                NightMaxSpeed = maps:get(<<"night_max_speed">>, H, 0),
                Name = maps:get(<<"name">>, H, <<>>),
                NameLen = byte_size(Name),
                <<Third/binary, NightMaxSpeed:?WORD, NameLen:?WORD, Name/binary>>;
            false ->
                Third
        end,
    serialize_circle_area(T, Fourth, ProtoVer).

serialize_contacts([], Acc) ->
    Acc;
serialize_contacts(
    [
        #{
            <<"type">> := Type,
            <<"phone_len">> := PhoneLen,
            <<"phone">> := Phone,
            <<"name_len">> := NameLen,
            <<"name">> := Name
        }
        | T
    ],
    Acc
) ->
    serialize_contacts(
        T, <<Acc/binary, Type:?BYTE, PhoneLen:?BYTE, Phone/binary, NameLen:?BYTE, Name/binary>>
    ).

serialize_menus([], Acc) ->
    Acc;
serialize_menus([#{<<"type">> := Type, <<"length">> := Length, <<"info">> := Info} | T], Acc) ->
    serialize_menus(T, <<Acc/binary, Type:?BYTE, Length:?WORD, Info/binary>>).

serialize_candidate_answers([], Acc) ->
    Acc;
serialize_candidate_answers([#{<<"id">> := Id, <<"len">> := Len, <<"answer">> := Answer} | T], Acc) ->
    serialize_candidate_answers(T, <<Acc/binary, Id:?BYTE, Len:?WORD, Answer/binary>>).

serialize_events([], Acc) ->
    Acc;
serialize_events([#{<<"id">> := Id, <<"length">> := Len, <<"content">> := Content} | T], Acc) ->
    serialize_events(T, <<Acc/binary, Id:?BYTE, Len:?BYTE, Content/binary>>).

serialize_client_param(Acc, []) ->
    Acc;
serialize_client_param(Acc, [#{<<"id">> := Id, <<"value">> := Value} | T]) ->
    NewAcc = encode_client_param(Id, Value, Acc),
    serialize_client_param(NewAcc, T).

encode_client_param(Id, Value, Acc) ->
    case client_param_data_type(Id) of
        dword -> encode_cp_dword(Id, Value, Acc);
        word -> encode_cp_word(Id, Value, Acc);
        byte -> encode_cp_byte(Id, Value, Acc);
        string -> encode_cp_string(Id, Value, Acc);
        byte8 -> encode_cp_byte8(Id, Value, Acc);
        reserved -> encode_cp_reserved(Id, Value, Acc)
    end.

client_param_data_type(?CP_HEARTBEAT_DURATION) ->
    dword;
client_param_data_type(?CP_TCP_TIMEOUT) ->
    dword;
client_param_data_type(?CP_TCP_RETX) ->
    dword;
client_param_data_type(?CP_UDP_TIMEOUT) ->
    dword;
client_param_data_type(?CP_UDP_RETX) ->
    dword;
client_param_data_type(?CP_SMS_TIMEOUT) ->
    dword;
client_param_data_type(?CP_SMS_RETX) ->
    dword;
client_param_data_type(?CP_SERVER_APN) ->
    string;
client_param_data_type(?CP_DIAL_USERNAME) ->
    string;
client_param_data_type(?CP_DIAL_PASSWORD) ->
    string;
client_param_data_type(?CP_SERVER_ADDRESS) ->
    string;
client_param_data_type(?CP_BACKUP_SERVER_APN) ->
    string;
client_param_data_type(?CP_BACKUP_DIAL_USERNAME) ->
    string;
client_param_data_type(?CP_BACKUP_DIAL_PASSWORD) ->
    string;
client_param_data_type(?CP_BACKUP_SERVER_ADDRESS) ->
    string;
client_param_data_type(?CP_SERVER_TCP_PORT) ->
    dword;
client_param_data_type(?CP_SERVER_UDP_PORT) ->
    dword;
client_param_data_type(?CP_IC_CARD_SERVER_ADDRESS) ->
    string;
client_param_data_type(?CP_IC_CARD_SERVER_TCP_PORT) ->
    dword;
client_param_data_type(?CP_IC_CARD_SERVER_UDP_PORT) ->
    dword;
client_param_data_type(?CP_IC_CARD_BACKUP_SERVER_ADDRESS) ->
    string;
client_param_data_type(?CP_POS_REPORT_POLICY) ->
    dword;
client_param_data_type(?CP_POS_REPORT_CONTROL) ->
    dword;
client_param_data_type(?CP_DRIVER_NLOGIN_REPORT_INTERVAL) ->
    dword;
client_param_data_type(?CP_REPORT_INTERVAL_DURING_SLEEP) ->
    dword;
client_param_data_type(?CP_EMERGENCY_ALARM_REPORT_INTERVAL) ->
    dword;
client_param_data_type(?CP_DEFAULT_REPORT_INTERVAL) ->
    dword;
client_param_data_type(?CP_DEFAULT_DISTANCE_INTERVAL) ->
    dword;
client_param_data_type(?CP_DRIVER_NLOGIN_DISTANCE_INTERVAL) ->
    dword;
client_param_data_type(?CP_DISTANCE_INTERVAL_DURING_SLEEP) ->
    dword;
client_param_data_type(?CP_EMERGENCY_ALARM_DISTANCE_INTERVAL) ->
    dword;
client_param_data_type(?CP_SET_TURN_ANGLE) ->
    dword;
client_param_data_type(?CP_EFENCE_RADIUS) ->
    word;
client_param_data_type(?CP_MONITOR_PHONE) ->
    string;
client_param_data_type(?CP_RESETING_PHONE) ->
    string;
client_param_data_type(?CP_RECOVERY_PHONE) ->
    string;
client_param_data_type(?CP_SMS_MONITOR_PHONE) ->
    string;
client_param_data_type(?CP_EMERGENCY_SMS_PHONE) ->
    string;
client_param_data_type(?CP_ACCEPT_CALL_POLICY) ->
    dword;
client_param_data_type(?CP_MAX_CALL_DURATION) ->
    dword;
client_param_data_type(?CP_MAX_CALL_DURATION_OF_MONTH) ->
    dword;
client_param_data_type(?CP_SPY_PHONE) ->
    string;
client_param_data_type(?CP_PRIVILEGE_SMS_PHONE) ->
    string;
client_param_data_type(?CP_ALARM_MASK) ->
    dword;
client_param_data_type(?CP_ALARM_SEND_SMS_MASK) ->
    dword;
client_param_data_type(?CP_ALARM_CAMERA_SHOT_MASK) ->
    dword;
client_param_data_type(?CP_ALARM_PICTURE_SAVE_MASK) ->
    dword;
client_param_data_type(?CP_ALARM_KEY_MASK) ->
    dword;
client_param_data_type(?CP_MAX_SPEED) ->
    dword;
client_param_data_type(?CP_OVERSPEED_ELAPSED) ->
    dword;
client_param_data_type(?CP_CONT_DRIVE_THRESHOLD) ->
    dword;
client_param_data_type(?CP_ACC_DRIVE_TIME_ONE_DAY_THRESHOLD) ->
    dword;
client_param_data_type(?CP_MIN_BREAK_TIME) ->
    dword;
client_param_data_type(?CP_MAX_PARK_TIME) ->
    dword;
client_param_data_type(?CP_OVERSPEED_ALARM_DELTA) ->
    word;
client_param_data_type(?CP_DRIVER_FATIGUE_ALARM_DELTA) ->
    word;
client_param_data_type(?CP_SET_CRASH_ALARM_PARAM) ->
    word;
client_param_data_type(?CP_SET_ROLLOVER_PARAM) ->
    word;
client_param_data_type(?CP_TIME_CONTROLED_CAMERA) ->
    dword;
client_param_data_type(?CP_DISTANCE_CONTROLED_CAMERA) ->
    dword;
client_param_data_type(?CP_PICTURE_QUALITY) ->
    dword;
client_param_data_type(?CP_PICTURE_BRIGHTNESS) ->
    dword;
client_param_data_type(?CP_PICTURE_CONTRAST) ->
    dword;
client_param_data_type(?CP_PICTURE_SATURATE) ->
    dword;
client_param_data_type(?CP_PICTURE_CHROMATICITY) ->
    dword;
client_param_data_type(?CP_ODOMETER) ->
    dword;
client_param_data_type(?CP_REGISTERED_PROVINCE) ->
    word;
client_param_data_type(?CP_REGISTERED_CITY) ->
    word;
client_param_data_type(?CP_VEHICLE_LICENSE_NUMBER) ->
    string;
client_param_data_type(?CP_VEHICLE_LICENSE_PLATE_COLOR) ->
    byte;
client_param_data_type(?CP_GNSS_MODE) ->
    byte;
client_param_data_type(?CP_GNSS_BAUDRATE) ->
    byte;
client_param_data_type(?CP_GNSS_OUTPUT_RATE) ->
    byte;
client_param_data_type(?CP_GNSS_SAMPLING_RATE) ->
    dword;
client_param_data_type(?CP_GNSS_UPLOAD_MODE) ->
    byte;
client_param_data_type(?CP_GNSS_UPLOAD_UNIT) ->
    dword;
client_param_data_type(?CP_CAN_BUS_CH1_SAMPLING) ->
    dword;
client_param_data_type(?CP_CAN_BUS_CH1_UPLOAD) ->
    word;
client_param_data_type(?CP_CAN_BUS_CH2_SAMPLING) ->
    dword;
client_param_data_type(?CP_CAN_BUS_CH2_UPLOAD) ->
    word;
%% 0x0110~0x01FF: CAN bus ID individual collection settings, BYTE[8]
client_param_data_type(Id) when
    Id >= ?CP_CAN_BUS_ID_PARAM_START andalso Id =< ?CP_CAN_BUS_ID_PARAM_END
->
    byte8;
client_param_data_type(_) ->
    reserved.

-spec encode_cp_byte(integer(), integer(), binary()) -> binary().
encode_cp_byte(Id, Value, Acc) ->
    <<Acc/binary, Id:?DWORD, 1:?BYTE, Value:?BYTE>>.

-spec encode_cp_word(integer(), integer(), binary()) -> binary().
encode_cp_word(Id, Value, Acc) ->
    <<Acc/binary, Id:?DWORD, 2:?BYTE, Value:?WORD>>.

-spec encode_cp_dword(integer(), integer(), binary()) -> binary().
encode_cp_dword(Id, Value, Acc) ->
    <<Acc/binary, Id:?DWORD, 4:?BYTE, Value:?DWORD>>.

-spec encode_cp_string(integer(), binary(), binary()) -> binary().
encode_cp_string(Id, StringBinary, Acc) ->
    Length = size(StringBinary),
    <<Acc/binary, Id:?DWORD, Length:?BYTE, StringBinary/binary>>.

-spec encode_cp_byte8(integer(), binary(), binary()) -> binary().
encode_cp_byte8(Id, Base64Binary, Acc) ->
    Binary = base64:decode(Base64Binary),
    <<Acc/binary, Id:?DWORD, 8:?BYTE, Binary:8/binary>>.

-spec encode_cp_reserved(integer(), binary(), binary()) -> binary().
encode_cp_reserved(Id, Base64Binary0, Acc) ->
    Binary = base64:decode(Base64Binary0),
    Length = size(Binary),
    <<Acc/binary, Id:?DWORD, Length:?BYTE, Binary/binary>>.

packet(Binary) ->
    packet2(Binary, undefined, <<16#7e:?BYTE>>).

packet2(<<>>, Check, Acc) ->
    Stream = pack(Acc, Check),
    <<Stream/binary, 16#7e:?BYTE>>;
packet2(<<C:?BYTE, Rest/binary>>, Check, Acc) ->
    NewCheck = cal_xor(C, Check),
    packet2(Rest, NewCheck, pack(Acc, C)).

pack(Stream, 16#7e) ->
    <<Stream/binary, 16#7d:?BYTE, 16#02:?BYTE>>;
pack(Stream, 16#7d) ->
    <<Stream/binary, 16#7d:?BYTE, 16#01:?BYTE>>;
pack(Stream, C) ->
    <<Stream/binary, C:?BYTE>>.

encode_word_array(0, _, Acc) ->
    Acc;
encode_word_array(Count, [H | T], Acc) ->
    encode_word_array(Count - 1, T, <<Acc/binary, H:?WORD>>).

encode_dword_array(0, _, Acc) ->
    Acc;
encode_dword_array(Count, [H | T], Acc) ->
    encode_dword_array(Count - 1, T, <<Acc/binary, H:?DWORD>>).

from_bcd(<<>>, Acc) ->
    list_to_binary(Acc);
from_bcd(<<N1:4, N2:4, Rest/binary>>, Acc) ->
    from_bcd(Rest, Acc ++ [$0 + N1, $0 + N2]).

to_bcd(String, BCDMaxSize) ->
    StringSize = size(String),
    Prefix =
        case StringSize < BCDMaxSize of
            true -> padding_zero(BCDMaxSize * 2 - StringSize, <<>>);
            false -> <<>>
        end,
    encode_bcd(String, Prefix).

padding_zero(0, Acc) ->
    Acc;
padding_zero(Count, Acc) ->
    padding_zero(Count - 1, <<Acc/binary, 0:4>>).

encode_bcd(<<>>, Acc) ->
    Acc;
encode_bcd(<<H:8, Rest/binary>>, Acc) ->
    C = H - $0,
    encode_bcd(Rest, <<Acc/bitstring, C:4>>).

%% Extract fragment info from header
extract_fragment_info(Header) ->
    case maps:is_key(<<"frag_total">>, Header) of
        true -> {1, maps:get(<<"frag_total">>, Header), maps:get(<<"frag_sn">>, Header)};
        false -> {0, 0, 0}
    end.

%% Append fragment info to binary if fragmented
maybe_append_fragment(Binary, 0, _Total, _Seq) ->
    Binary;
maybe_append_fragment(Binary, 1, Total, Seq) ->
    <<Binary/binary, Total:?WORD, Seq:?WORD>>.

check(Bin) ->
    case check(Bin, undefined) of
        true ->
            Size = size(Bin) - 1,
            <<Msg:Size/binary, _:8>> = Bin,
            Msg;
        false ->
            error(invalid_check_sum)
    end.

check(<<>>, _) ->
    false;
check(<<_Byte:8>>, undefined) ->
    false;
check(<<Byte:8>>, XorValue) ->
    Byte == XorValue;
check(<<Byte:8, Rest/binary>>, undefined) ->
    check(Rest, Byte);
check(<<Byte:8, Rest/binary>>, XorValue) ->
    check(Rest, Byte bxor XorValue).

cal_xor(C, undefined) ->
    C;
cal_xor(C, XorValue) ->
    C bxor XorValue.

remove_tail_zero(<<>>) ->
    <<>>;
remove_tail_zero(Bin) ->
    LastIdx = search_tail_zero_pos(Bin, size(Bin) - 1),
    binary:part(Bin, 0, LastIdx + 1).

search_tail_zero_pos(_Bin, 0) ->
    0;
search_tail_zero_pos(Bin, Pos) ->
    case binary:at(Bin, Pos) of
        0 -> search_tail_zero_pos(Bin, Pos - 1);
        _ -> Pos
    end.
