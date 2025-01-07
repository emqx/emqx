%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_parser_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_jt808.hrl").

-define(LOGT(Format, Args), ct:print("TEST_SUITE: " ++ Format, Args)).

-define(FRM_FLAG, 16#7e:8).
-define(RESERVE, 0).
-define(NO_FRAGMENT, 0).
-define(WITH_FRAGMENT, 1).
-define(NO_ENCRYPT, 0).
-define(MSG_SIZE(X), X:10 / big - integer).

-define(word, 16 / big - integer).
-define(dword, 32 / big - integer).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

t_case01_register(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    Plate = <<"ujvl239">>,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color, Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size,
                <<"msg_sn">> => MsgSn
            },
            <<"body">> => #{
                <<"province">> => 58,
                <<"city">> => 59,
                <<"manufacturer">> => Manuf,
                <<"model">> => Model,
                <<"dev_id">> => DevId,
                <<"color">> => Color,
                <<"license_number">> => Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case02_register_ack(_Config) ->
    % register ack
    MsgId = 16#8100,
    MsgSn = 35,
    Seq = 22,
    Result = 1,
    Code = <<"abcdef">>,
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{<<"seq">> => Seq, <<"result">> => Result, <<"auth_code">> => Code}
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),

    RegisterAckPacket = <<Seq:?word, Result:8, Code/binary>>,

    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,

    Size = size(RegisterAckPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    StreamByHand = encode(Header, RegisterAckPacket),

    ?assertEqual(StreamByHand, Stream),
    ok.

t_case04_MC_LOCATION_REPORT(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Data =
        <<126, 2, 0, 0, 60, 1, 136, 118, 99, 137, 114, 0, 229, 0, 0, 0, 0, 0, 4, 0, 0, 1, 49, 122,
            103, 6, 147, 104, 81, 0, 14, 0, 0, 0, 39, 23, 16, 25, 25, 53, 56, 1, 4, 0, 0, 63, 178,
            3, 2, 0, 0, 37, 4, 0, 0, 0, 0, 42, 2, 0, 0, 43, 4, 0, 0, 0, 0, 48, 1, 31, 49, 1, 0, 171,
            126>>,
    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Data, Parser),
    ?assertEqual(
        #{
            <<"header">> =>
                #{
                    <<"encrypt">> => 0,
                    <<"len">> => 60,
                    <<"msg_id">> => 512,
                    <<"msg_sn">> => 229,
                    <<"phone">> => <<"018876638972">>
                },
            <<"body">> =>
                #{
                    <<"alarm">> => 0,
                    <<"altitude">> => 14,
                    <<"direction">> => 39,
                    <<"extra">> =>
                        #{
                            <<"analog">> => #{<<"ad0">> => 0, <<"ad1">> => 0},
                            <<"gnss_sat_num">> => 0,
                            <<"io_status">> => #{<<"deep_sleep">> => 0, <<"sleep">> => 0},
                            <<"mileage">> => 16306,
                            <<"rssi">> => 31,
                            <<"signal">> =>
                                #{
                                    <<"abs">> => 0,
                                    <<"air_conditioner">> => 0,
                                    <<"brake">> => 0,
                                    <<"cluth">> => 0,
                                    <<"fog">> => 0,
                                    <<"heater">> => 0,
                                    <<"high_beam">> => 0,
                                    <<"horn">> => 0,
                                    <<"left_turn">> => 0,
                                    <<"low_beam">> => 0,
                                    <<"neutral">> => 0,
                                    <<"retarder">> => 0,
                                    <<"reverse">> => 0,
                                    <<"right_turn">> => 0,
                                    <<"side_marker">> => 0
                                },
                            <<"speed">> => 0
                        },
                    <<"latitude">> => 20019815,
                    <<"longitude">> => 110323793,
                    <<"speed">> => 0,
                    <<"status">> => 262144,
                    <<"time">> => <<"171019193538">>
                }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case05_MC_BULK_LOCATION_REPORT(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Data =
        <<126, 7, 4, 1, 57, 1, 136, 118, 99, 137, 114, 0, 231, 0, 5, 1, 0, 60, 0, 0, 0, 0, 0, 4, 0,
            3, 1, 49, 115, 43, 6, 145, 211, 81, 0, 31, 0, 166, 0, 171, 23, 8, 49, 16, 73, 51, 1, 4,
            0, 0, 60, 85, 3, 2, 0, 0, 37, 4, 0, 0, 0, 0, 42, 2, 0, 0, 43, 4, 0, 0, 0, 0, 48, 1, 0,
            49, 1, 12, 0, 60, 0, 0, 0, 0, 0, 4, 0, 3, 1, 49, 114, 74, 6, 145, 212, 2, 0, 29, 0, 222,
            0, 125, 2, 23, 8, 49, 16, 73, 56, 1, 4, 0, 0, 60, 85, 3, 2, 0, 0, 37, 4, 0, 0, 0, 0, 42,
            2, 0, 0, 43, 4, 0, 0, 0, 0, 48, 1, 0, 49, 1, 12, 0, 60, 0, 0, 0, 0, 0, 4, 0, 3, 1, 49,
            109, 222, 6, 145, 225, 80, 0, 37, 1, 169, 0, 109, 23, 8, 49, 16, 80, 17, 1, 4, 0, 0, 60,
            89, 3, 2, 0, 0, 37, 4, 0, 0, 0, 0, 42, 2, 0, 0, 43, 4, 0, 0, 0, 0, 48, 1, 0, 49, 1, 12,
            0, 60, 0, 0, 0, 0, 0, 4, 0, 3, 1, 49, 90, 235, 6, 146, 0, 24, 0, 43, 2, 136, 0, 114, 23,
            8, 49, 16, 81, 17, 1, 4, 0, 0, 60, 99, 3, 2, 0, 0, 37, 4, 0, 0, 0, 0, 42, 2, 0, 0, 43,
            4, 0, 0, 0, 0, 48, 1, 0, 49, 1, 12, 0, 60, 0, 0, 0, 0, 0, 4, 0, 3, 1, 49, 91, 120, 6,
            146, 43, 21, 0, 43, 2, 247, 0, 83, 23, 8, 49, 16, 82, 20, 1, 4, 0, 0, 60, 111, 3, 2, 0,
            0, 37, 4, 0, 0, 0, 0, 42, 2, 0, 0, 43, 4, 0, 0, 0, 0, 48, 1, 0, 49, 1, 12, 132, 126>>,
    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Data, Parser),
    ?assertEqual(
        #{
            <<"body">> =>
                #{
                    <<"length">> => 5,
                    <<"location">> =>
                        [
                            #{
                                <<"alarm">> => 0,
                                <<"altitude">> => 31,
                                <<"direction">> => 171,
                                <<"extra">> =>
                                    #{
                                        <<"analog">> =>
                                            #{
                                                <<"ad0">> => 0,
                                                <<"ad1">> => 0
                                            },
                                        <<"gnss_sat_num">> => 12,
                                        <<"io_status">> =>
                                            #{
                                                <<"deep_sleep">> => 0,
                                                <<"sleep">> => 0
                                            },
                                        <<"mileage">> => 15445,
                                        <<"rssi">> => 0,
                                        <<"signal">> =>
                                            #{
                                                <<"abs">> => 0,
                                                <<"air_conditioner">> =>
                                                    0,
                                                <<"brake">> => 0,
                                                <<"cluth">> => 0,
                                                <<"fog">> => 0,
                                                <<"heater">> => 0,
                                                <<"high_beam">> => 0,
                                                <<"horn">> => 0,
                                                <<"left_turn">> => 0,
                                                <<"low_beam">> => 0,
                                                <<"neutral">> => 0,
                                                <<"retarder">> => 0,
                                                <<"reverse">> => 0,
                                                <<"right_turn">> => 0,
                                                <<"side_marker">> => 0
                                            },
                                        <<"speed">> => 0
                                    },
                                <<"latitude">> => 20017963,
                                <<"longitude">> => 110220113,
                                <<"speed">> => 166,
                                <<"status">> => 262147,
                                <<"time">> => <<"170831104933">>
                            },
                            #{
                                <<"alarm">> => 0,
                                <<"altitude">> => 29,
                                <<"direction">> => 126,
                                <<"extra">> =>
                                    #{
                                        <<"analog">> =>
                                            #{
                                                <<"ad0">> => 0,
                                                <<"ad1">> => 0
                                            },
                                        <<"gnss_sat_num">> => 12,
                                        <<"io_status">> =>
                                            #{
                                                <<"deep_sleep">> => 0,
                                                <<"sleep">> => 0
                                            },
                                        <<"mileage">> => 15445,
                                        <<"rssi">> => 0,
                                        <<"signal">> =>
                                            #{
                                                <<"abs">> => 0,
                                                <<"air_conditioner">> =>
                                                    0,
                                                <<"brake">> => 0,
                                                <<"cluth">> => 0,
                                                <<"fog">> => 0,
                                                <<"heater">> => 0,
                                                <<"high_beam">> => 0,
                                                <<"horn">> => 0,
                                                <<"left_turn">> => 0,
                                                <<"low_beam">> => 0,
                                                <<"neutral">> => 0,
                                                <<"retarder">> => 0,
                                                <<"reverse">> => 0,
                                                <<"right_turn">> => 0,
                                                <<"side_marker">> => 0
                                            },
                                        <<"speed">> => 0
                                    },
                                <<"latitude">> => 20017738,
                                <<"longitude">> => 110220290,
                                <<"speed">> => 222,
                                <<"status">> => 262147,
                                <<"time">> => <<"170831104938">>
                            },
                            #{
                                <<"alarm">> => 0,
                                <<"altitude">> => 37,
                                <<"direction">> => 109,
                                <<"extra">> =>
                                    #{
                                        <<"analog">> =>
                                            #{
                                                <<"ad0">> => 0,
                                                <<"ad1">> => 0
                                            },
                                        <<"gnss_sat_num">> => 12,
                                        <<"io_status">> =>
                                            #{
                                                <<"deep_sleep">> => 0,
                                                <<"sleep">> => 0
                                            },
                                        <<"mileage">> => 15449,
                                        <<"rssi">> => 0,
                                        <<"signal">> =>
                                            #{
                                                <<"abs">> => 0,
                                                <<"air_conditioner">> =>
                                                    0,
                                                <<"brake">> => 0,
                                                <<"cluth">> => 0,
                                                <<"fog">> => 0,
                                                <<"heater">> => 0,
                                                <<"high_beam">> => 0,
                                                <<"horn">> => 0,
                                                <<"left_turn">> => 0,
                                                <<"low_beam">> => 0,
                                                <<"neutral">> => 0,
                                                <<"retarder">> => 0,
                                                <<"reverse">> => 0,
                                                <<"right_turn">> => 0,
                                                <<"side_marker">> => 0
                                            },
                                        <<"speed">> => 0
                                    },
                                <<"latitude">> => 20016606,
                                <<"longitude">> => 110223696,
                                <<"speed">> => 425,
                                <<"status">> => 262147,
                                <<"time">> => <<"170831105011">>
                            },
                            #{
                                <<"alarm">> => 0,
                                <<"altitude">> => 43,
                                <<"direction">> => 114,
                                <<"extra">> =>
                                    #{
                                        <<"analog">> =>
                                            #{
                                                <<"ad0">> => 0,
                                                <<"ad1">> => 0
                                            },
                                        <<"gnss_sat_num">> => 12,
                                        <<"io_status">> =>
                                            #{
                                                <<"deep_sleep">> => 0,
                                                <<"sleep">> => 0
                                            },
                                        <<"mileage">> => 15459,
                                        <<"rssi">> => 0,
                                        <<"signal">> =>
                                            #{
                                                <<"abs">> => 0,
                                                <<"air_conditioner">> =>
                                                    0,
                                                <<"brake">> => 0,
                                                <<"cluth">> => 0,
                                                <<"fog">> => 0,
                                                <<"heater">> => 0,
                                                <<"high_beam">> => 0,
                                                <<"horn">> => 0,
                                                <<"left_turn">> => 0,
                                                <<"low_beam">> => 0,
                                                <<"neutral">> => 0,
                                                <<"retarder">> => 0,
                                                <<"reverse">> => 0,
                                                <<"right_turn">> => 0,
                                                <<"side_marker">> => 0
                                            },
                                        <<"speed">> => 0
                                    },
                                <<"latitude">> => 20011755,
                                <<"longitude">> => 110231576,
                                <<"speed">> => 648,
                                <<"status">> => 262147,
                                <<"time">> => <<"170831105111">>
                            },
                            #{
                                <<"alarm">> => 0,
                                <<"altitude">> => 43,
                                <<"direction">> => 83,
                                <<"extra">> =>
                                    #{
                                        <<"analog">> =>
                                            #{
                                                <<"ad0">> => 0,
                                                <<"ad1">> => 0
                                            },
                                        <<"gnss_sat_num">> => 12,
                                        <<"io_status">> =>
                                            #{
                                                <<"deep_sleep">> => 0,
                                                <<"sleep">> => 0
                                            },
                                        <<"mileage">> => 15471,
                                        <<"rssi">> => 0,
                                        <<"signal">> =>
                                            #{
                                                <<"abs">> => 0,
                                                <<"air_conditioner">> =>
                                                    0,
                                                <<"brake">> => 0,
                                                <<"cluth">> => 0,
                                                <<"fog">> => 0,
                                                <<"heater">> => 0,
                                                <<"high_beam">> => 0,
                                                <<"horn">> => 0,
                                                <<"left_turn">> => 0,
                                                <<"low_beam">> => 0,
                                                <<"neutral">> => 0,
                                                <<"retarder">> => 0,
                                                <<"reverse">> => 0,
                                                <<"right_turn">> => 0,
                                                <<"side_marker">> => 0
                                            },
                                        <<"speed">> => 0
                                    },
                                <<"latitude">> => 20011896,
                                <<"longitude">> => 110242581,
                                <<"speed">> => 759,
                                <<"status">> => 262147,
                                <<"time">> => <<"170831105214">>
                            }
                        ],
                    <<"type">> => 1
                },
            <<"header">> =>
                #{
                    <<"encrypt">> => 0,
                    <<"len">> => 313,
                    <<"msg_id">> => 1796,
                    <<"msg_sn">> => 231,
                    <<"phone">> => <<"018876638972">>
                }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case10_segmented_packet(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    Plate = <<"ujvl239">>,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color, Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    <<Part1:7/binary, Part2/binary>> = Stream,
    {more, Parser2} = emqx_jt808_frame:parse(Part1, Parser),
    ?assertMatch(#{phase := escaping_hex7d}, Parser2),
    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Part2, Parser2),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size,
                <<"msg_sn">> => MsgSn
            },
            <<"body">> => #{
                <<"province">> => 58,
                <<"city">> => 59,
                <<"manufacturer">> => Manuf,
                <<"model">> => Model,
                <<"dev_id">> => DevId,
                <<"color">> => Color,
                <<"license_number">> => Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case11_prefix_register(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    Plate = <<"ujvl239">>,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color, Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),
    MessBinary = <<0, 1, 2, 3, Stream/binary>>,
    ?LOGT("MessBinary=~p", [binary_to_hex_string(MessBinary)]),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(MessBinary, Parser),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size,
                <<"msg_sn">> => MsgSn
            },
            <<"body">> => #{
                <<"province">> => 58,
                <<"city">> => 59,
                <<"manufacturer">> => Manuf,
                <<"model">> => Model,
                <<"dev_id">> => DevId,
                <<"color">> => Color,
                <<"license_number">> => Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case12_0x7e_in_message(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    Plate = <<"ujvl239">>,
    % pay attention to this
    AlarmInt = 16#7e,
    AlarmDigit = 16#7d,
    RegisterPacket =
        <<AlarmInt:?word, AlarmDigit:?word, Manuf/binary, Model/binary, DevId/binary, Color,
            Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size,
                <<"msg_sn">> => MsgSn
            },
            <<"body">> => #{
                <<"province">> => AlarmInt,
                <<"city">> => AlarmDigit,
                <<"manufacturer">> => Manuf,
                <<"model">> => Model,
                <<"dev_id">> => DevId,
                <<"color">> => Color,
                <<"license_number">> => Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case13_partial_0x7d_in_message(_Config) ->
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    Plate = <<"ujvl239">>,
    % pay attention to this
    AlarmInt = 16#7e,
    AlarmDigit = 16#7d,
    RegisterPacket =
        <<AlarmInt:?word, AlarmDigit:?word, Manuf/binary, Model/binary, DevId/binary, Color,
            Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    <<Part1:15/binary, Part2/binary>> = Stream,
    <<_:14/binary, 16#7d:8>> = Part1,

    {more, Parser2} = emqx_jt808_frame:parse(Part1, Parser),
    ?assertMatch(#{phase := escaping_hex7d}, Parser2),
    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Part2, Parser2),
    ?assertEqual(
        #{
            <<"header">> => #{
                <<"msg_id">> => MsgId,
                <<"encrypt">> => ?NO_ENCRYPT,
                <<"phone">> => <<"000123456789">>,
                <<"len">> => Size,
                <<"msg_sn">> => MsgSn
            },
            <<"body">> => #{
                <<"province">> => AlarmInt,
                <<"city">> => AlarmDigit,
                <<"manufacturer">> => Manuf,
                <<"model">> => Model,
                <<"dev_id">> => DevId,
                <<"color">> => Color,
                <<"license_number">> => Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    ok.

t_case14_custome_location_data(_) ->
    Bin =
        <<126, 2, 0, 0, 64, 1, 65, 72, 7, 53, 80, 3, 106, 0, 0, 0, 0, 0, 0, 0, 1, 1, 195, 232, 22,
            6, 89, 10, 16, 0, 0, 0, 0, 0, 0, 33, 6, 34, 9, 21, 69, 1, 4, 0, 0, 0, 0, 48, 1, 23, 49,
            1, 10, 235, 22, 0, 12, 0, 178, 137, 134, 4, 66, 25, 25, 144, 147, 71, 153, 0, 6, 0, 137,
            255, 255, 255, 255, 36, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

t_case14_reserved_location_data(_) ->
    Bin =
        <<126, 2, 0, 0, 49, 1, 145, 114, 3, 130, 104, 2, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 211, 181,
            215, 6, 51, 228, 71, 0, 4, 0, 0, 0, 138, 34, 4, 25, 22, 5, 70, 1, 4, 0, 0, 0, 0, 5, 3,
            0, 0, 0, 48, 1, 31, 49, 1, 15, 130, 2, 0, 125, 2, 23, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

t_case15_custome_client_query_ack(_) ->
    Bin =
        <<126, 1, 4, 2, 17, 78, 244, 128, 18, 137, 25, 0, 43, 0, 42, 52, 0, 0, 0, 1, 4, 0, 0, 0,
            180, 0, 0, 0, 8, 4, 0, 0, 1, 44, 0, 0, 0, 16, 5, 99, 109, 110, 101, 116, 0, 0, 0, 17, 0,
            0, 0, 0, 18, 0, 0, 0, 0, 19, 12, 52, 55, 46, 57, 57, 46, 57, 56, 46, 50, 53, 52, 0, 0,
            0, 23, 7, 48, 46, 48, 46, 48, 46, 48, 0, 0, 0, 24, 4, 0, 0, 31, 249, 0, 0, 0, 32, 4, 0,
            0, 0, 0, 0, 0, 0, 39, 4, 0, 0, 0, 30, 0, 0, 0, 41, 4, 0, 0, 0, 30, 0, 0, 0, 48, 4, 0, 0,
            0, 20, 0, 0, 0, 64, 1, 0, 0, 0, 0, 67, 1, 0, 0, 0, 0, 68, 1, 0, 0, 0, 0, 69, 4, 0, 0, 0,
            0, 0, 0, 0, 72, 1, 0, 0, 0, 0, 73, 1, 0, 0, 0, 0, 85, 4, 0, 0, 0, 120, 0, 0, 0, 86, 4,
            0, 0, 0, 20, 0, 0, 0, 93, 2, 0, 30, 0, 0, 0, 128, 4, 0, 0, 136, 203, 0, 0, 0, 129, 2, 0,
            0, 0, 0, 0, 130, 2, 0, 0, 0, 0, 0, 131, 0, 0, 0, 0, 132, 1, 0, 0, 0, 240, 1, 2, 16, 0,
            0, 0, 240, 2, 2, 3, 57, 0, 0, 240, 3, 1, 41, 0, 0, 240, 4, 2, 0, 116, 0, 0, 240, 5, 4,
            0, 53, 111, 127, 0, 0, 240, 6, 4, 0, 3, 51, 18, 0, 0, 240, 7, 1, 1, 0, 0, 240, 8, 2, 0,
            30, 0, 0, 240, 10, 2, 0, 30, 0, 0, 240, 11, 6, 0, 0, 0, 0, 0, 0, 0, 0, 240, 12, 1, 1, 0,
            0, 240, 13, 18, 117, 112, 103, 114, 97, 100, 101, 46, 99, 112, 115, 100, 110, 97, 46,
            99, 111, 109, 0, 0, 240, 14, 4, 0, 0, 123, 40, 0, 0, 240, 15, 1, 1, 0, 0, 240, 16, 3, 0,
            16, 3, 0, 0, 240, 17, 2, 0, 32, 0, 0, 240, 18, 3, 0, 22, 40, 0, 0, 240, 19, 4, 0, 17,
            25, 32, 0, 0, 240, 20, 3, 55, 0, 10, 0, 0, 240, 21, 1, 50, 0, 0, 240, 22, 2, 3, 25, 0,
            0, 240, 23, 2, 30, 50, 0, 0, 240, 24, 21, 48, 44, 48, 44, 53, 56, 46, 50, 49, 53, 46,
            53, 48, 46, 53, 48, 44, 54, 48, 48, 56, 0, 0, 241, 1, 1, 160, 0, 0, 241, 2, 1, 130, 0,
            0, 255, 148, 93, 14, 22, 1, 56, 57, 56, 54, 48, 52, 57, 53, 49, 48, 50, 49, 55, 48, 51,
            48, 52, 53, 53, 56, 17, 2, 52, 54, 48, 48, 56, 49, 53, 56, 51, 52, 48, 52, 53, 53, 56,
            4, 3, 0, 1, 3, 5, 0, 3, 6, 0, 3, 7, 0, 5, 8, 3, 0, 1, 5, 8, 14, 0, 0, 5, 8, 15, 0, 0, 5,
            8, 41, 0, 0, 5, 8, 48, 0, 0, 5, 8, 42, 0, 0, 5, 8, 43, 0, 0, 5, 8, 4, 0, 0, 138, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(#{data => <<>>, phase => searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

t_throw_error_if_parse_failed(_) ->
    Bin =
        <<126, 2, 5, 0, 128, 1, 137, 96, 146, 0, 51, 3, 64, 72, 66, 77, 54, 48, 49, 67, 86, 77, 48,
            53, 54, 51, 52, 50, 48, 50, 52, 45, 48, 56, 45, 49, 54, 253, 255, 2, 0, 255, 127, 0,
            128, 80, 17, 1, 54, 69, 67, 56, 48, 48, 77, 58, 32, 49, 44, 34, 51, 50, 51, 65, 56, 54,
            56, 48, 49, 57, 48, 55, 51, 55, 48, 51, 50, 50, 54, 52, 54, 48, 48, 56, 56, 53, 53, 52,
            49, 48, 48, 52, 57, 56, 56, 57, 56, 54, 48, 56, 49, 53, 50, 54, 50, 51, 56, 48, 49, 49,
            48, 52, 57, 56, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0,
            0, 0, 0, 207, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    try emqx_jt808_frame:parse(Bin, Parser) of
        _ -> ?assert(false)
    catch
        error:invalid_message ->
            ok
    end.

encode(Header, Body) ->
    S1 = <<Header/binary, Body/binary>>,
    Crc = make_crc(S1, undefined),
    S2 = do_escape(<<S1/binary, Crc:8>>),
    Stream = <<16#7e:8, S2/binary, 16#7e:8>>,
    %?LOGT("encode a packet=~p", [binary_to_hex_string(Stream)]),
    Stream.

make_crc(<<>>, Xor) ->
    ?LOGT("crc is ~p", [Xor]),
    Xor;
make_crc(<<C:8, Rest/binary>>, undefined) ->
    make_crc(Rest, C);
make_crc(<<C:8, Rest/binary>>, Xor) ->
    make_crc(Rest, C bxor Xor).

do_escape(Binary) ->
    do_escape(Binary, <<>>).

do_escape(<<>>, Acc) ->
    Acc;
do_escape(<<16#7e, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, 16#7d, 16#02>>);
do_escape(<<16#7d, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, 16#7d, 16#01>>);
do_escape(<<C, Rest/binary>>, Acc) ->
    do_escape(Rest, <<Acc/binary, C:8>>).

binary_to_hex_string(Data) ->
    lists:flatten([io_lib:format("~2.16.0B ", [X]) || <<X:8>> <= Data]).
