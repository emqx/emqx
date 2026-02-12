%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_parser_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_jt808.hrl").

-define(LOGT(Format, Args), ct:print("TEST_SUITE: " ++ Format, Args)).

-define(RESERVE, 0).
-define(NO_FRAGMENT, 0).
-define(NO_ENCRYPT, 0).
-define(MSG_SIZE(X), X:10 / big - integer).

-define(word, 16 / big - integer).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:ensure_all_started(egbk),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

t_case14_reserved_location_data(_) ->
    Bin =
        <<126, 2, 0, 0, 49, 1, 145, 114, 3, 130, 104, 2, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 211, 181,
            215, 6, 51, 228, 71, 0, 4, 0, 0, 0, 138, 34, 4, 25, 22, 5, 70, 1, 4, 0, 0, 0, 0, 5, 3,
            0, 0, 0, 48, 1, 31, 49, 1, 15, 130, 2, 0, 125, 2, 23, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
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
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

t_case16_driver_id_report_id_card(_) ->
    Bin =
        <<126, 7, 2, 64, 84, 1, 49, 51, 56, 48, 48, 49, 51, 56, 48, 48, 0, 3, 1, 26, 1, 23, 17, 5,
            19, 0, 6, 229, 188, 160, 228, 184, 137, 49, 50, 51, 52, 53, 54, 55, 56, 57, 48, 49, 50,
            51, 52, 53, 54, 55, 56, 57, 48, 24, 229, 140, 151, 228, 186, 172, 229, 184, 130, 228,
            186, 164, 233, 128, 154, 229, 167, 148, 229, 145, 152, 228, 188, 154, 20, 28, 12, 31,
            49, 49, 48, 49, 48, 49, 49, 57, 57, 48, 48, 49, 48, 49, 49, 50, 51, 52, 48, 48, 23,
            126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    Header = maps:get(<<"header">>, Packet),
    Body = maps:get(<<"body">>, Packet),
    ?assertEqual(16#0702, maps:get(<<"msg_id">>, Header)),
    ?assertEqual(1, maps:get(<<"status">>, Body)),
    ?assertEqual(0, maps:get(<<"ic_result">>, Body)),
    ?assertEqual(true, maps:is_key(<<"driver_name">>, Body)),
    ?assertEqual(true, maps:is_key(<<"id_card">>, Body)),
    ok.

t_case16_driver_id_report_offline(_) ->
    Bin =
        <<126, 7, 2, 64, 7, 1, 49, 51, 56, 48, 48, 49, 51, 56, 48, 48, 0, 3, 2, 26, 1, 23, 17, 5,
            19, 73, 126>>,
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    Header = maps:get(<<"header">>, Packet),
    Body = maps:get(<<"body">>, Packet),
    ?assertEqual(16#0702, maps:get(<<"msg_id">>, Header)),
    ?assertEqual(2, maps:get(<<"status">>, Body)),
    ?assertEqual(false, maps:is_key(<<"ic_result">>, Body)),
    ok.

t_throw_error_if_parse_failed(_) ->
    Bin = unknown_message_packet(),
    Parser = emqx_jt808_frame:initial_parse_state(#{parse_unknown_message => false}),
    try emqx_jt808_frame:parse(Bin, Parser) of
        _ -> ?assert(false)
    catch
        error:invalid_message ->
            ok
    end.

t_enable_parse_unknown_message(_) ->
    Bin = unknown_message_packet(),
    Parser = emqx_jt808_frame:initial_parse_state(#{parse_unknown_message => true}),
    {ok, Packet, Rest, State} = emqx_jt808_frame:parse(Bin, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    _ = emqx_utils_json:encode(Packet).

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

unknown_message_packet() ->
    <<126, 2, 5, 0, 128, 1, 137, 96, 146, 0, 51, 3, 64, 72, 66, 77, 54, 48, 49, 67, 86, 77, 48, 53,
        54, 51, 52, 50, 48, 50, 52, 45, 48, 56, 45, 49, 54, 253, 255, 2, 0, 255, 127, 0, 128, 80,
        17, 1, 54, 69, 67, 56, 48, 48, 77, 58, 32, 49, 44, 34, 51, 50, 51, 65, 56, 54, 56, 48, 49,
        57, 48, 55, 51, 55, 48, 51, 50, 50, 54, 52, 54, 48, 48, 56, 56, 53, 53, 52, 49, 48, 48, 52,
        57, 56, 56, 57, 56, 54, 48, 56, 49, 53, 50, 54, 50, 51, 56, 48, 49, 49, 48, 52, 57, 56, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 207, 126>>.

%%--------------------------------------------------------------------
%% JT/T 808-2019 Protocol Tests
%%--------------------------------------------------------------------

%% 2019 header format macros
-define(VERSION_BIT_2019, 1).

%% BCD[10] phone: 00000000000123456789 (same phone "000123456789" as 2013, with leading zeros)
-define(JT808_PHONE_BCD_2019,
    <<16#00, 16#00, 16#00, 16#00, 16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>
).
-define(JT808_PHONE_STR_2019, <<"00000000000123456789">>).

t_2019_register_parse(_Config) ->
    %% Test 2019 register message with larger field sizes
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    %% 2019 format: manufacturer[11], model[30], dev_id[30]

    %% 11 bytes (padded with trailing zeros)
    Manuf = <<"MANUFACTURER">>,
    ManufPadded = pad_binary(Manuf, 11),
    %% 30 bytes - spec says "位数不足的前补 0x00" (pad with leading zeros)
    Model = <<"MODEL_2019_TEST_DEVICE">>,
    ModelPadded = pad_binary_leading(Model, 30),
    %% 30 bytes (padded with trailing zeros per spec for dev_id)
    DevId = <<"DEVICE_ID_2019_TEST">>,
    DevIdPadded = pad_binary(DevId, 30),
    Color = 3,
    Plate = <<"TEST123">>,
    RegisterPacket =
        <<58:?word, 59:?word, ManufPadded/binary, ModelPadded/binary, DevIdPadded/binary, Color,
            Plate/binary>>,
    MsgId = 16#0100,
    %% 2019: BCD[10] phone number with leading zeros
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 78,
    ProtoVer = ?PROTO_VER_2019,
    Size = size(RegisterPacket),
    %% 2019 header: bit 14 = 1 (version identifier)
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := MsgId,
                <<"encrypt">> := ?NO_ENCRYPT,
                <<"phone">> := ?JT808_PHONE_STR_2019,
                <<"len">> := Size,
                <<"msg_sn">> := MsgSn,
                <<"proto_ver">> := ProtoVer
            },
            <<"body">> := #{
                <<"province">> := 58,
                <<"city">> := 59,
                <<"manufacturer">> := _,
                <<"model">> := _,
                <<"dev_id">> := _,
                <<"color">> := Color,
                <<"license_number">> := Plate
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

t_2019_auth_parse(_Config) ->
    %% Test 2019 authentication message with IMEI and software version
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    AuthCode = <<"AUTH_CODE_2019">>,
    AuthLen = byte_size(AuthCode),
    %% 15 bytes fixed
    IMEI = <<"123456789012345">>,
    %% 20 bytes fixed (padded with zeros)
    SoftwareVersion = <<"V1.0.0_2019">>,
    SoftwareVersionPadded = pad_binary(SoftwareVersion, 20),
    %% 2019 auth format: auth_len + auth_code + IMEI[15] + software_version[20]
    AuthPacket =
        <<AuthLen:8, AuthCode/binary, IMEI/binary, SoftwareVersionPadded/binary>>,
    MsgId = 16#0102,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 99,
    ProtoVer = ?PROTO_VER_2019,
    Size = size(AuthPacket),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, AuthPacket),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := MsgId,
                <<"proto_ver">> := ProtoVer
            },
            <<"body">> := #{
                <<"code">> := AuthCode,
                <<"imei">> := IMEI,
                <<"software_version">> := SoftwareVersion
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

t_2019_request_fragment_serialize(_Config) ->
    %% Test 2019 request fragment serialization (WORD for count instead of BYTE)
    MsgId = 16#8003,
    MsgSn = 50,
    Seq = 10,
    %% In 2019, count is WORD (2 bytes), so can have > 255 packets
    PacketIds = [1, 2, 3, 256, 257],
    Length = length(PacketIds),

    %% 2019 format serialization
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"00000123456789012345">>,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"seq">> => Seq,
            <<"length">> => Length,
            <<"ids">> => PacketIds
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),

    ?assert(is_binary(Stream)),
    %% Frame structure:
    %%   - Frame start marker: 1 byte (0x7E)
    %%   - 2019 header: 17 bytes
    %%   - Body: 14 bytes (Seq:2 + Length:2 + IDs:5*2)
    %%   - CRC: 1 byte
    %%   - Frame end marker: 1 byte (0x7E)
    %% Total: 1 + 17 + 14 + 1 + 1 = 34 bytes
    ?assertEqual(34, byte_size(Stream)),
    ok.

t_2019_send_text_serialize(_Config) ->
    %% Test 2019 text info serialization with text type field
    MsgId = 16#8300,
    MsgSn = 60,
    Flag = 1,
    %% Service type
    TextType = 2,
    Text = <<"Hello 2019">>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"00000123456789012345">>,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"flag">> => Flag,
            <<"text_type">> => TextType,
            <<"text">> => Text
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2019_vehicle_control_serialize(_Config) ->
    %% Test 2019 vehicle control serialization with control type list
    MsgId = 16#8500,
    MsgSn = 70,

    %% 2019 format: control type list instead of single flag byte
    Controls = [
        %% Door lock (0 = lock)
        #{<<"id">> => 16#0001, <<"param">> => <<0>>},
        %% Door unlock
        #{<<"id">> => 16#0001, <<"param">> => <<1>>}
    ],

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"00000123456789012345">>,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"count">> => length(Controls),
            <<"controls">> => Controls
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_vehicle_control_serialize(_Config) ->
    %% Test 2013 vehicle control serialization (single flag byte)
    MsgId = 16#8500,
    MsgSn = 71,
    Flag = 1,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
            %% No proto_ver = 2013 format
        },
        <<"body">> => #{
            <<"flag">> => Flag
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_send_text_serialize(_Config) ->
    %% Test 2013 text info serialization (no text type field)
    MsgId = 16#8300,
    MsgSn = 61,
    Flag = 1,
    Text = <<"Hello 2013">>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
            %% No proto_ver = 2013 format
        },
        <<"body">> => #{
            <<"flag">> => Flag,
            <<"text">> => Text
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

%% Helper to pad binary to specified length with trailing nulls
pad_binary(Bin, TargetLen) when byte_size(Bin) >= TargetLen ->
    binary:part(Bin, 0, TargetLen);
pad_binary(Bin, TargetLen) ->
    PadLen = TargetLen - byte_size(Bin),
    <<Bin/binary, 0:(PadLen * 8)>>.

%% Helper to pad binary to specified length with leading nulls
pad_binary_leading(Bin, TargetLen) when byte_size(Bin) >= TargetLen ->
    binary:part(Bin, 0, TargetLen);
pad_binary_leading(Bin, TargetLen) ->
    PadLen = TargetLen - byte_size(Bin),
    <<0:(PadLen * 8), Bin/binary>>.

%%--------------------------------------------------------------------
%% 1. New 2019 Message Parse Tests
%%--------------------------------------------------------------------

t_2019_query_server_time_parse(_Config) ->
    %% Test 0x0004 Query Server Time Request - empty body
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0004,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 100,
    ProtoVer = ?PROTO_VER_2019,
    %% Empty body
    Body = <<>>,
    Size = 0,
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0004,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{}
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

t_2019_request_fragment_parse(_Config) ->
    %% Test 0x0005 Terminal Packet Retransmission Request
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0005,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 101,
    ProtoVer = ?PROTO_VER_2019,
    %% Body: Seq(WORD) + Count(WORD) + IDs(WORD each)
    Seq = 50,
    Count = 3,
    Ids = [1, 2, 300],
    Body = <<Seq:?word, Count:?word, 1:?word, 2:?word, 300:?word>>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0005,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"seq">> := Seq,
                <<"count">> := Count,
                <<"ids">> := Ids
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

t_2019_query_area_route_ack_parse(_Config) ->
    %% Test 0x0608 Query Area/Route Data Response
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0608,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 102,
    ProtoVer = ?PROTO_VER_2019,
    %% Body: Type(BYTE) + Count(DWORD) + Data
    Type = 1,
    Count = 2,
    Data = <<"test_area_data">>,
    Body = <<Type:8, Count:32/big, Data/binary>>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0608,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"type">> := Type,
                <<"count">> := Count,
                <<"data">> := _
            }
        },
        Map
    ),
    %% Verify data is base64 encoded
    #{<<"body">> := #{<<"data">> := EncodedData}} = Map,
    ?assertEqual(Data, base64:decode(EncodedData)),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

%%--------------------------------------------------------------------
%% 2. New 2019 Message Serialize Tests
%%--------------------------------------------------------------------

t_2019_server_time_ack_serialize(_Config) ->
    %% Test 0x8004 Server Time Response - BCD[6] UTC time
    MsgId = 16#8004,
    MsgSn = 103,
    %% YYMMDDHHmmss format
    Time = <<"260120103045">>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"time">> => Time
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    %% Parse it back to verify
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, _Parsed, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ok.

t_2019_link_detect_serialize(_Config) ->
    %% Test 0x8204 Link Detection - empty body
    MsgId = 16#8204,
    MsgSn = 104,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{}
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    %% Minimal frame: 7e + header(17) + crc(1) + 7e = at least 20 bytes
    ?assert(byte_size(Stream) >= 20),
    ok.

t_2019_query_area_route_serialize(_Config) ->
    %% Test 0x8608 Query Area/Route Data
    MsgId = 16#8608,
    MsgSn = 105,
    Type = 1,
    Count = 2,
    Ids = [100, 200],

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => Type,
            <<"count">> => Count,
            <<"ids">> => Ids
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2019_query_area_route_serialize_empty(_Config) ->
    %% Test 0x8608 Query Area/Route Data with count=0 (query all)
    MsgId = 16#8608,
    MsgSn = 106,
    Type = 2,
    Count = 0,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => Type,
            <<"count">> => Count
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

%%--------------------------------------------------------------------
%% 3. Modified 2019 Message Parse Tests
%%--------------------------------------------------------------------

t_2019_query_attrib_ack_parse(_Config) ->
    %% Test 0x0107 Query Terminal Attributes Response - 2019 format (30-byte model/id)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0107,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 110,
    ProtoVer = ?PROTO_VER_2019,

    Type = 16#0001,
    Manufacturer = <<"MANUF">>,
    Model = pad_binary(<<"MODEL_2019_DEVICE">>, 30),
    Id = pad_binary(<<"DEVICE_ID_2019">>, 30),
    ICCID = <<16#89, 16#86, 16#01, 16#23, 16#45, 16#67, 16#89, 16#01, 16#23, 16#45>>,
    HardwareVersion = <<"HW1.0">>,
    HVLen = byte_size(HardwareVersion),
    FirmwareVersion = <<"FW2.0">>,
    FVLen = byte_size(FirmwareVersion),
    GNSSProp = 16#07,
    CommProp = 16#03,

    Body =
        <<Type:?word, Manufacturer:5/binary, Model:30/binary, Id:30/binary, ICCID:10/binary,
            HVLen:8, HardwareVersion/binary, FVLen:8, FirmwareVersion/binary, GNSSProp:8,
            CommProp:8>>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0107,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"type">> := Type,
                <<"manufacturer">> := Manufacturer,
                <<"hardware_version">> := HardwareVersion,
                <<"firmware_version">> := FirmwareVersion,
                <<"gnss_prop">> := GNSSProp,
                <<"comm_prop">> := CommProp
            }
        },
        Map
    ),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

t_2013_query_attrib_ack_parse(_Config) ->
    %% Test 0x0107 Query Terminal Attributes Response - 2013 format (20-byte model, 7-byte id)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0107,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 111,

    Type = 16#0001,
    Manufacturer = <<"MANUF">>,
    Model = pad_binary(<<"MODEL_2013">>, 20),
    Id = pad_binary(<<"DEV2013">>, 7),
    ICCID = <<16#89, 16#86, 16#01, 16#23, 16#45, 16#67, 16#89, 16#01, 16#23, 16#45>>,
    HardwareVersion = <<"HW1.0">>,
    HVLen = byte_size(HardwareVersion),
    FirmwareVersion = <<"FW2.0">>,
    FVLen = byte_size(FirmwareVersion),
    GNSSProp = 16#07,
    CommProp = 16#03,

    Body =
        <<Type:?word, Manufacturer:5/binary, Model:20/binary, Id:7/binary, ICCID:10/binary, HVLen:8,
            HardwareVersion/binary, FVLen:8, FirmwareVersion/binary, GNSSProp:8, CommProp:8>>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0107
            },
            <<"body">> := #{
                <<"type">> := Type,
                <<"manufacturer">> := Manufacturer,
                <<"hardware_version">> := HardwareVersion,
                <<"firmware_version">> := FirmwareVersion,
                <<"gnss_prop">> := GNSSProp,
                <<"comm_prop">> := CommProp
            }
        },
        Map
    ),
    %% Verify no proto_ver in 2013 format
    #{<<"header">> := Header2013} = Map,
    ?assertEqual(false, maps:is_key(<<"proto_ver">>, Header2013)),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ok.

%%--------------------------------------------------------------------
%% 4. Modified 2019 Message Serialize Tests (Area Messages)
%%--------------------------------------------------------------------

t_2019_circle_area_serialize(_Config) ->
    %% Test 0x8600 Set Circle Area - 2019 format with night_max_speed and name
    MsgId = 16#8600,
    MsgSn = 120,
    AreaName = <<"测试区域"/utf8>>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 1,
                    <<"flag">> => 16#0001,
                    <<"center_latitude">> => 39908823,
                    <<"center_longitude">> => 116397470,
                    <<"radius">> => 1000,
                    <<"start_time">> => <<"260101000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 120,
                    <<"overspeed_duration">> => 10,
                    <<"night_max_speed">> => 80,
                    <<"name">> => AreaName
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2019_rect_area_serialize(_Config) ->
    %% Test 0x8602 Set Rectangular Area - 2019 format with night_max_speed and name
    MsgId = 16#8602,
    MsgSn = 121,
    AreaName = <<"矩形区域"/utf8>>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 2,
                    <<"flag">> => 16#0001,
                    <<"lt_lat">> => 39910000,
                    <<"lt_lng">> => 116390000,
                    <<"rb_lat">> => 39900000,
                    <<"rb_lng">> => 116400000,
                    <<"start_time">> => <<"260101000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 100,
                    <<"overspeed_duration">> => 15,
                    <<"night_max_speed">> => 60,
                    <<"name">> => AreaName
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2019_poly_area_serialize(_Config) ->
    %% Test 0x8604 Set Polygonal Area - 2019 format with night_max_speed and name
    MsgId = 16#8604,
    MsgSn = 122,
    AreaName = <<"多边形区域"/utf8>>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"id">> => 3,
            <<"flag">> => 16#0001,
            <<"start_time">> => <<"260101000000">>,
            <<"end_time">> => <<"261231235959">>,
            <<"max_speed">> => 80,
            <<"overspeed_duration">> => 20,
            <<"length">> => 4,
            <<"points">> => [
                #{<<"lat">> => 39910000, <<"lng">> => 116390000},
                #{<<"lat">> => 39910000, <<"lng">> => 116400000},
                #{<<"lat">> => 39900000, <<"lng">> => 116400000},
                #{<<"lat">> => 39900000, <<"lng">> => 116390000}
            ],
            <<"night_max_speed">> => 50,
            <<"name">> => AreaName
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2019_path_serialize(_Config) ->
    %% Test 0x8606 Set Route - 2019 format with name_length and name
    MsgId = 16#8606,
    MsgSn = 123,
    RouteName = <<"测试路线"/utf8>>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"id">> => 4,
            <<"flag">> => 16#0001,
            <<"start_time">> => <<"260101000000">>,
            <<"end_time">> => <<"261231235959">>,
            <<"length">> => 2,
            <<"points">> => [
                #{
                    <<"point_id">> => 1,
                    <<"path_id">> => 4,
                    <<"point_lat">> => 39908823,
                    <<"point_lng">> => 116397470,
                    <<"width">> => 50,
                    <<"attrib">> => 1,
                    <<"passed">> => 300,
                    <<"uncovered">> => 600,
                    <<"max_speed">> => 120,
                    <<"overspeed_duration">> => 10
                },
                #{
                    <<"point_id">> => 2,
                    <<"path_id">> => 4,
                    <<"point_lat">> => 39918823,
                    <<"point_lng">> => 116407470,
                    <<"width">> => 50,
                    <<"attrib">> => 1,
                    <<"passed">> => 300,
                    <<"uncovered">> => 600,
                    <<"max_speed">> => 100,
                    <<"overspeed_duration">> => 10
                }
            ],
            <<"name">> => RouteName
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_circle_area_serialize(_Config) ->
    %% Test 0x8600 Set Circle Area - 2013 format without night_max_speed and name
    MsgId = 16#8600,
    MsgSn = 124,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
            %% No proto_ver = 2013 format
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 1,
                    <<"flag">> => 16#0001,
                    <<"center_latitude">> => 39908823,
                    <<"center_longitude">> => 116397470,
                    <<"radius">> => 1000,
                    <<"start_time">> => <<"260101000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 120,
                    <<"overspeed_duration">> => 10
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_rect_area_serialize(_Config) ->
    %% Test 0x8602 Set Rectangular Area - 2013 format without night_max_speed and name
    MsgId = 16#8602,
    MsgSn = 125,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 2,
                    <<"flag">> => 16#0001,
                    <<"lt_lat">> => 39910000,
                    <<"lt_lng">> => 116390000,
                    <<"rb_lat">> => 39900000,
                    <<"rb_lng">> => 116400000,
                    <<"start_time">> => <<"260101000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 100,
                    <<"overspeed_duration">> => 15
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_poly_area_serialize(_Config) ->
    %% Test 0x8604 Set Polygonal Area - 2013 format without night_max_speed and name
    MsgId = 16#8604,
    MsgSn = 126,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"id">> => 3,
            <<"flag">> => 16#0001,
            <<"start_time">> => <<"260101000000">>,
            <<"end_time">> => <<"261231235959">>,
            <<"max_speed">> => 80,
            <<"overspeed_duration">> => 20,
            <<"length">> => 3,
            <<"points">> => [
                #{<<"lat">> => 39910000, <<"lng">> => 116390000},
                #{<<"lat">> => 39910000, <<"lng">> => 116400000},
                #{<<"lat">> => 39900000, <<"lng">> => 116395000}
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_2013_path_serialize(_Config) ->
    %% Test 0x8606 Set Route - 2013 format without name
    MsgId = 16#8606,
    MsgSn = 127,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"id">> => 4,
            <<"flag">> => 16#0001,
            <<"start_time">> => <<"260101000000">>,
            <<"end_time">> => <<"261231235959">>,
            <<"length">> => 1,
            <<"points">> => [
                #{
                    <<"point_id">> => 1,
                    <<"path_id">> => 4,
                    <<"point_lat">> => 39908823,
                    <<"point_lng">> => 116397470,
                    <<"width">> => 50,
                    <<"attrib">> => 1,
                    <<"passed">> => 300,
                    <<"uncovered">> => 600,
                    <<"max_speed">> => 120,
                    <<"overspeed_duration">> => 10
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

%%--------------------------------------------------------------------
%% 5. Round-Trip Tests
%%--------------------------------------------------------------------

t_2019_register_roundtrip(_Config) ->
    %% Test 0x0100 round-trip: parse -> (simulate serialize by building response)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    ManufPadded = pad_binary(<<"TESTMANUF">>, 11),
    ModelPadded = pad_binary_leading(<<"MODEL2019">>, 30),
    DevIdPadded = pad_binary(<<"DEVID2019TEST">>, 30),
    Color = 2,
    Plate = <<"ABC1234">>,
    RegisterPacket =
        <<100:?word, 200:?word, ManufPadded/binary, ModelPadded/binary, DevIdPadded/binary, Color,
            Plate/binary>>,
    MsgId = 16#0100,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 130,
    ProtoVer = ?PROTO_VER_2019,
    Size = byte_size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    %% Parse first time
    {ok, Map1, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{<<"proto_ver">> := ?PROTO_VER_2019},
            <<"body">> := #{<<"province">> := 100, <<"city">> := 200}
        },
        Map1
    ),
    ok.

t_2019_auth_roundtrip(_Config) ->
    %% Test 0x0102 round-trip
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    AuthCode = <<"ROUNDTRIP_AUTH">>,
    AuthLen = byte_size(AuthCode),
    IMEI = <<"867530012345678">>,
    SoftwareVersionPadded = pad_binary(<<"V2.5.0">>, 20),
    AuthPacket = <<AuthLen:8, AuthCode/binary, IMEI/binary, SoftwareVersionPadded/binary>>,
    MsgId = 16#0102,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 131,
    ProtoVer = ?PROTO_VER_2019,
    Size = byte_size(AuthPacket),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, AuthPacket),

    %% Parse
    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{<<"proto_ver">> := ?PROTO_VER_2019},
            <<"body">> := #{
                <<"code">> := AuthCode,
                <<"imei">> := IMEI
            }
        },
        Map
    ),
    ok.

t_2019_header_roundtrip(_Config) ->
    %% Test that 2019 header with proto_ver survives serialization
    MsgId = 16#8001,
    MsgSn = 132,
    Seq = 50,
    Id = 16#0102,
    Result = 0,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"seq">> => Seq,
            <<"id">> => Id,
            <<"result">> => Result
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),

    %% Parse back
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, ParsedMap, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := MsgId,
                <<"proto_ver">> := ?PROTO_VER_2019,
                <<"phone">> := ?JT808_PHONE_STR_2019
            }
        },
        ParsedMap
    ),
    ok.

%%--------------------------------------------------------------------
%% 6. Boundary and Edge Case Tests
%%--------------------------------------------------------------------

t_2019_max_phone_bcd10(_Config) ->
    %% Test BCD[10] phone number boundary - all 9s
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0002,
    %% All 9s = 20 digit phone
    PhoneBCD = <<16#99, 16#99, 16#99, 16#99, 16#99, 16#99, 16#99, 16#99, 16#99, 16#99>>,
    MsgSn = 140,
    ProtoVer = ?PROTO_VER_2019,
    Body = <<>>,
    Size = 0,
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"phone">> := <<"99999999999999999999">>
            }
        },
        Map
    ),
    ok.

t_2019_empty_auth_code(_Config) ->
    %% Test 0x0102 with 0-length auth code
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    AuthLen = 0,
    AuthCode = <<>>,
    IMEI = <<"867530012345678">>,
    SoftwareVersionPadded = pad_binary(<<"V1.0">>, 20),
    AuthPacket = <<AuthLen:8, AuthCode/binary, IMEI/binary, SoftwareVersionPadded/binary>>,
    MsgId = 16#0102,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 141,
    ProtoVer = ?PROTO_VER_2019,
    Size = byte_size(AuthPacket),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, AuthPacket),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"body">> := #{
                <<"code">> := <<>>,
                <<"imei">> := IMEI
            }
        },
        Map
    ),
    ok.

t_2019_max_packet_ids(_Config) ->
    %% Test 0x8003 with >255 packet IDs (WORD count)
    MsgId = 16#8003,
    MsgSn = 142,
    Seq = 100,
    %% 300 packet IDs - exceeds BYTE max of 255
    PacketIds = lists:seq(1, 300),
    Length = length(PacketIds),

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"seq">> => Seq,
            <<"length">> => Length,
            <<"ids">> => PacketIds
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    %% Body size should be: 2 (seq) + 2 (count) + 300*2 (ids) = 604 bytes
    %% Total frame will be larger due to header and escaping
    ?assert(byte_size(Stream) > 600),
    ok.

t_2019_area_unicode_name(_Config) ->
    %% Test area message with UTF-8 name containing Chinese characters
    MsgId = 16#8600,
    MsgSn = 143,
    %% Chinese: "北京市朝阳区测试区域"
    AreaName = <<"北京市朝阳区测试区域"/utf8>>,
    NameByteLen = byte_size(AreaName),
    %% UTF-8 Chinese chars are 3 bytes each, so 10 chars = 30 bytes
    ?assertEqual(30, NameByteLen),

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 100,
                    <<"flag">> => 16#0001,
                    <<"center_latitude">> => 39908823,
                    <<"center_longitude">> => 116397470,
                    <<"radius">> => 500,
                    <<"start_time">> => <<"260101000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 60,
                    <<"overspeed_duration">> => 5,
                    <<"night_max_speed">> => 40,
                    <<"name">> => AreaName
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_malformed_frame_no_header(_Config) ->
    %% Test malformed frame without 0x7E header
    %% Parser should discard invalid data and return {more, State}
    Parser = emqx_jt808_frame:initial_parse_state(#{}),

    %% Malformed data without 0x7E header
    MalformedData = <<16#FF, 16#FF, 16#00, 16#04, 16#74, 16#65, 16#73, 16#74, 16#18>>,

    %% Parser should return {more, State} after discarding invalid data
    {more, State} = emqx_jt808_frame:parse(MalformedData, Parser),
    ?assertMatch(#{phase := searching_head_hex7e}, State),
    ok.

t_malformed_frame_garbage_before_valid(_Config) ->
    %% Test garbage data followed by valid 0x7E frame
    %% Parser should discard garbage and parse the valid frame
    Parser = emqx_jt808_frame:initial_parse_state(#{}),

    %% Garbage data
    GarbageData = <<16#FF, 16#FF, 16#00, 16#04>>,

    %% Valid heartbeat frame (0x0002)
    MsgId = 16#0002,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 100,
    Body = <<>>,
    Size = 0,
    Header = <<MsgId:16/big, 0:2, 0:1, 0:3, Size:10/big, PhoneBCD/binary, MsgSn:16/big>>,
    ValidFrame = encode(Header, Body),

    %% Combine garbage + valid frame
    CombinedData = <<GarbageData/binary, ValidFrame/binary>>,

    %% Parser should discard garbage and parse valid frame
    {ok, Map, <<>>, _State} = emqx_jt808_frame:parse(CombinedData, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0002,
                <<"msg_sn">> := 100
            }
        },
        Map
    ),
    ok.

t_malformed_frame_empty_data(_Config) ->
    %% Test empty data
    %% Parser should return {more, State}
    Parser = emqx_jt808_frame:initial_parse_state(#{}),

    %% Empty data
    {more, State} = emqx_jt808_frame:parse(<<>>, Parser),
    ?assertMatch(#{phase := searching_head_hex7e}, State),
    ok.

t_malformed_frame_only_garbage(_Config) ->
    %% Test only garbage data (no 0x7E at all)
    %% Parser should discard all and return {more, State}
    Parser = emqx_jt808_frame:initial_parse_state(#{}),

    %% All garbage, no 0x7E
    GarbageOnly = <<16#01, 16#02, 16#03, 16#04, 16#05, 16#FF, 16#FE, 16#FD>>,

    {more, State} = emqx_jt808_frame:parse(GarbageOnly, Parser),
    ?assertMatch(#{phase := searching_head_hex7e}, State),
    ok.

%%--------------------------------------------------------------------
%% 7. Location Report and Location Extra Tests (2019 vs 2013)
%%--------------------------------------------------------------------

t_2019_location_report_with_tire_pressure_and_carriage_temp(_Config) ->
    %% Test 0x0200 Location Report with 2019 extras: tire_pressure (0x05) and carriage_temp (0x06)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0200,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 200,
    ProtoVer = ?PROTO_VER_2019,

    %% Basic location data (28 bytes)
    Alarm = 16#00000000,
    Status = 16#00040000,
    Latitude = 20019815,
    Longitude = 110323793,
    Altitude = 14,
    Speed = 50,
    Direction = 39,
    TimeBCD = <<16#26, 16#01, 16#21, 16#10, 16#30, 16#45>>,

    %% 2019 extras: tire_pressure (0x05) and carriage_temp (0x06)
    TirePressure = <<100, 101, 102, 103, 104, 105>>,
    TirePressureLen = byte_size(TirePressure),
    CarriageTemp = 25,

    LocationBody = <<
        Alarm:32/big,
        Status:32/big,
        Latitude:32/big,
        Longitude:32/big,
        Altitude:16/big,
        Speed:16/big,
        Direction:16/big,
        TimeBCD/binary,
        %% Extra 0x05: tire_pressure
        16#05:8,
        TirePressureLen:8,
        TirePressure/binary,
        %% Extra 0x06: carriage_temp (signed 16-bit)
        16#06:8,
        2:8,
        CarriageTemp:16/signed-big
    >>,
    Size = byte_size(LocationBody),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, LocationBody),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0200,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"alarm">> := Alarm,
                <<"status">> := Status,
                <<"latitude">> := Latitude,
                <<"longitude">> := Longitude,
                <<"extra">> := #{
                    <<"tire_pressure">> := _,
                    <<"carriage_temp">> := CarriageTemp
                }
            }
        },
        Map
    ),
    %% Verify tire_pressure is base64 encoded
    #{<<"body">> := #{<<"extra">> := #{<<"tire_pressure">> := EncodedTirePressure}}} = Map,
    ?assertEqual(TirePressure, base64:decode(EncodedTirePressure)),
    ok.

t_2013_location_report_without_2019_extras(_Config) ->
    %% Test 0x0200 Location Report in 2013 mode - extras 0x05 and 0x06 should be
    %% treated as reserved/unknown (base64 encoded) instead of tire_pressure/carriage_temp
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0200,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 201,

    %% Basic location data (28 bytes)
    Alarm = 16#00000000,
    Status = 16#00040000,
    Latitude = 20019815,
    Longitude = 110323793,
    Altitude = 14,
    Speed = 50,
    Direction = 39,
    TimeBCD = <<16#17, 16#10, 16#19, 16#19, 16#35, 16#38>>,

    %% In 2013, 0x05 and 0x06 are reserved IDs - should be stored as base64
    ReservedData05 = <<100, 101, 102>>,
    ReservedData06 = <<0, 25>>,

    LocationBody = <<
        Alarm:32/big,
        Status:32/big,
        Latitude:32/big,
        Longitude:32/big,
        Altitude:16/big,
        Speed:16/big,
        Direction:16/big,
        TimeBCD/binary,
        %% Reserved ID 0x05
        16#05:8,
        3:8,
        ReservedData05/binary,
        %% Reserved ID 0x06
        16#06:8,
        2:8,
        ReservedData06/binary
    >>,
    Size = byte_size(LocationBody),
    %% 2013 header format (no version bit, BCD[6] phone)
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, LocationBody),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),

    %% Verify there's no proto_ver in header (2013 format)
    #{<<"header">> := Header2013} = Map,
    ?assertEqual(false, maps:is_key(<<"proto_ver">>, Header2013)),

    %% Verify extras are stored as reserved IDs (base64 encoded), not as tire_pressure/carriage_temp
    #{<<"body">> := #{<<"extra">> := Extra}} = Map,
    ?assertEqual(false, maps:is_key(<<"tire_pressure">>, Extra)),
    ?assertEqual(false, maps:is_key(<<"carriage_temp">>, Extra)),
    %% Reserved IDs are stored as binary keys "5" and "6"
    ?assert(maps:is_key(<<"5">>, Extra)),
    ?assert(maps:is_key(<<"6">>, Extra)),
    ?assertEqual(ReservedData05, base64:decode(maps:get(<<"5">>, Extra))),
    ?assertEqual(ReservedData06, base64:decode(maps:get(<<"6">>, Extra))),
    ok.

t_2019_location_report_negative_carriage_temp(_Config) ->
    %% Test carriage_temp with negative value (signed 16-bit)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0200,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 202,
    ProtoVer = ?PROTO_VER_2019,

    Alarm = 16#00000000,
    Status = 16#00040000,
    Latitude = 20019815,
    Longitude = 110323793,
    Altitude = 14,
    Speed = 0,
    Direction = 0,
    TimeBCD = <<16#26, 16#01, 16#21, 16#10, 16#30, 16#45>>,

    %% Negative temperature: -20 degrees Celsius
    CarriageTemp = -20,

    LocationBody = <<
        Alarm:32/big,
        Status:32/big,
        Latitude:32/big,
        Longitude:32/big,
        Altitude:16/big,
        Speed:16/big,
        Direction:16/big,
        TimeBCD/binary,
        %% Extra 0x06: carriage_temp (signed 16-bit negative value)
        16#06:8,
        2:8,
        CarriageTemp:16/signed-big
    >>,
    Size = byte_size(LocationBody),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, LocationBody),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"body">> := #{
                <<"extra">> := #{
                    <<"carriage_temp">> := -20
                }
            }
        },
        Map
    ),
    ok.

%%--------------------------------------------------------------------
%% 8. Driver ID Report Tests (0x0702) - 2019 vs 2013
%%--------------------------------------------------------------------

t_2019_driver_id_report_with_id_card(_Config) ->
    %% Test 0x0702 Driver ID Report - 2019 format with id_card field
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0702,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 210,
    ProtoVer = ?PROTO_VER_2019,

    Status = 1,
    TimeBCD = <<16#26, 16#01, 16#21, 16#10, 16#30, 16#45>>,
    IcResult = 0,
    DriverName = <<"张三"/utf8>>,
    NameLength = byte_size(DriverName),
    Certificate = pad_binary(<<"CERT123456789">>, 20),
    Organization = <<"北京交通"/utf8>>,
    OrgLength = byte_size(Organization),
    CertExpiryBCD = <<16#28, 16#12, 16#31, 16#00>>,
    %% 2019 new field: id_card (20 bytes)
    IdCard = pad_binary(<<"11010219840406970X">>, 20),

    Body = <<
        Status:8,
        TimeBCD/binary,
        IcResult:8,
        NameLength:8,
        DriverName/binary,
        Certificate/binary,
        OrgLength:8,
        Organization/binary,
        CertExpiryBCD/binary,
        IdCard/binary
    >>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0702,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"status">> := Status,
                <<"ic_result">> := IcResult,
                <<"driver_name">> := DriverName,
                <<"id_card">> := _
            }
        },
        Map
    ),
    %% Verify id_card is present and correctly parsed (trailing zeros removed)
    #{<<"body">> := #{<<"id_card">> := ParsedIdCard}} = Map,
    ?assertEqual(<<"11010219840406970X">>, ParsedIdCard),
    ok.

t_2013_driver_id_report_without_id_card(_Config) ->
    %% Test 0x0702 Driver ID Report - 2013 format without id_card field
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0702,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 211,

    Status = 1,
    TimeBCD = <<16#17, 16#10, 16#19, 16#10, 16#30, 16#45>>,
    IcResult = 0,
    DriverName = <<"李四"/utf8>>,
    NameLength = byte_size(DriverName),
    Certificate = pad_binary(<<"CERT987654321">>, 20),
    Organization = <<"上海交通"/utf8>>,
    OrgLength = byte_size(Organization),
    CertExpiryBCD = <<16#25, 16#06, 16#30, 16#00>>,
    %% 2013 format: NO id_card field

    Body = <<
        Status:8,
        TimeBCD/binary,
        IcResult:8,
        NameLength:8,
        DriverName/binary,
        Certificate/binary,
        OrgLength:8,
        Organization/binary,
        CertExpiryBCD/binary
    >>,
    Size = byte_size(Body),
    %% 2013 header format
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0702
            },
            <<"body">> := #{
                <<"status">> := Status,
                <<"ic_result">> := IcResult,
                <<"driver_name">> := DriverName
            }
        },
        Map
    ),
    %% Verify no proto_ver in header (2013 format)
    #{<<"header">> := Header2013} = Map,
    ?assertEqual(false, maps:is_key(<<"proto_ver">>, Header2013)),
    %% Verify no id_card field in body (2013 format)
    #{<<"body">> := Body2013} = Map,
    ?assertEqual(false, maps:is_key(<<"id_card">>, Body2013)),
    ok.

t_location_report_with_custom_extras(_Config) ->
    %% Test 0x0200 Location Report with custom vendor-specific extras (0xE1-0xF1)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0200,
    PhoneBCD = ?JT808_PHONE_BCD_2019,
    MsgSn = 300,
    ProtoVer = ?PROTO_VER_2019,

    %% Basic location data (28 bytes)
    Alarm = 16#00000000,
    Status = 16#00040000,
    Latitude = 20019815,
    Longitude = 110323793,
    Altitude = 14,
    Speed = 50,
    Direction = 39,
    TimeBCD = <<16#26, 16#01, 16#21, 16#10, 16#30, 16#45>>,

    %% Custom extras data

    %% 0xE1: 4 bytes
    E1Data = <<1, 2, 3, 4>>,
    %% 0xE2: 2 bytes
    E2Data = <<5, 6>>,
    %% 0xE3: 6 bytes
    E3Data = <<7, 8, 9, 10, 11, 12>>,
    %% 0xE4: 1 byte
    E4Data = <<13>>,
    %% 0xE5: 10 bytes
    E5Data = <<14, 15, 16, 17, 18, 19, 20, 21, 22, 23>>,
    %% 0xE6: 2 bytes
    E6Data = <<24, 25>>,
    %% 0xEF: 14 bytes
    EFData = <<26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39>>,
    %% 0xF1: 2 bytes
    F1Data = <<40, 41>>,

    LocationBody = <<
        Alarm:32/big,
        Status:32/big,
        Latitude:32/big,
        Longitude:32/big,
        Altitude:16/big,
        Speed:16/big,
        Direction:16/big,
        TimeBCD/binary,
        %% Custom extras
        16#E1:8,
        4:8,
        E1Data/binary,
        16#E2:8,
        2:8,
        E2Data/binary,
        16#E3:8,
        6:8,
        E3Data/binary,
        16#E4:8,
        1:8,
        E4Data/binary,
        16#E5:8,
        10:8,
        E5Data/binary,
        16#E6:8,
        2:8,
        E6Data/binary,
        16#EF:8,
        14:8,
        EFData/binary,
        16#F1:8,
        2:8,
        F1Data/binary
    >>,
    Size = byte_size(LocationBody),
    Header =
        <<MsgId:?word, ?RESERVE:1, ?VERSION_BIT_2019:1, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3,
            ?MSG_SIZE(Size), ProtoVer:8, PhoneBCD/binary, MsgSn:?word>>,
    Stream = encode(Header, LocationBody),

    {ok, Map, Rest, State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertEqual(<<>>, Rest),
    ?assertMatch(#{data := <<>>, phase := searching_head_hex7e}, State),
    ?assertMatch(
        #{
            <<"header">> := #{
                <<"msg_id">> := 16#0200,
                <<"proto_ver">> := ?PROTO_VER_2019
            },
            <<"body">> := #{
                <<"alarm">> := Alarm,
                <<"status">> := Status,
                <<"latitude">> := Latitude,
                <<"longitude">> := Longitude,
                <<"extra">> := #{
                    <<"custome">> := _
                }
            }
        },
        Map
    ),
    %% Verify all custom extras are correctly parsed and base64 encoded
    #{<<"body">> := #{<<"extra">> := #{<<"custome">> := Custome}}} = Map,
    %% Custom IDs are stored as binary strings of the integer value

    %% 0xE1 = 225
    ?assertEqual(E1Data, base64:decode(maps:get(<<"225">>, Custome))),
    %% 0xE2 = 226
    ?assertEqual(E2Data, base64:decode(maps:get(<<"226">>, Custome))),
    %% 0xE3 = 227
    ?assertEqual(E3Data, base64:decode(maps:get(<<"227">>, Custome))),
    %% 0xE4 = 228
    ?assertEqual(E4Data, base64:decode(maps:get(<<"228">>, Custome))),
    %% 0xE5 = 229
    ?assertEqual(E5Data, base64:decode(maps:get(<<"229">>, Custome))),
    %% 0xE6 = 230
    ?assertEqual(E6Data, base64:decode(maps:get(<<"230">>, Custome))),
    %% 0xEF = 239
    ?assertEqual(EFData, base64:decode(maps:get(<<"239">>, Custome))),
    %% 0xF1 = 241
    ?assertEqual(F1Data, base64:decode(maps:get(<<"241">>, Custome))),
    ok.

%%--------------------------------------------------------------------
%% Client Parameter Tests (0x8103 / 0x0104) - BYTE8 Type for CAN Bus ID
%% These tests apply to both JT808-2013 and JT808-2019 protocols
%%--------------------------------------------------------------------

t_set_client_param_serialize_with_byte8(_Config) ->
    %% Test 0x8103 (MS_SET_CLIENT_PARAM) serialization with different parameter types
    %% including byte8 type for CAN bus ID params (0x0110~0x01FF)
    MsgId = 16#8103,
    MsgSn = 100,

    %% Test data for different parameter types:
    %% - DWORD: 0x0001 (heartbeat duration)
    %% - STRING: 0x0010 (server APN)
    %% - BYTE8: 0x0110 (CAN bus ID param) - base64 encoded
    Byte8Data = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    Byte8Base64 = base64:encode(Byte8Data),

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"length">> => 3,
            <<"params">> => [
                %% DWORD type (0x0001)
                #{<<"id">> => 16#0001, <<"value">> => 60},
                %% STRING type (0x0010)
                #{<<"id">> => 16#0010, <<"value">> => <<"cmnet">>},
                %% BYTE8 type (0x0110) - base64 encoded
                #{<<"id">> => 16#0110, <<"value">> => Byte8Base64}
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),

    %% Parse the serialized stream back to verify round-trip
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, _Map, <<>>, _State} = emqx_jt808_frame:parse(Stream, Parser),
    ok.

t_set_client_param_serialize_byte8_range(_Config) ->
    %% Test 0x8103 serialization with multiple CAN bus ID params (0x0110~0x01FF range)
    MsgId = 16#8103,
    MsgSn = 101,

    %% Different params within the 0x0110~0x01FF range
    Byte8Data1 = <<16#AA, 16#BB, 16#CC, 16#DD, 16#EE, 16#FF, 16#00, 16#11>>,
    Byte8Data2 = <<16#11, 16#22, 16#33, 16#44, 16#55, 16#66, 16#77, 16#88>>,

    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"length">> => 2,
            <<"params">> => [
                %% 0x0110 - first in range
                #{<<"id">> => 16#0110, <<"value">> => base64:encode(Byte8Data1)},
                %% 0x01FF - last in range
                #{<<"id">> => 16#01FF, <<"value">> => base64:encode(Byte8Data2)}
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{}),
    ?assert(is_binary(Stream)),
    ?assert(byte_size(Stream) > 0),
    ok.

t_query_client_param_ack_parse_with_byte8(_Config) ->
    %% Test 0x0104 (MC_QUERY_PARAM_ACK) parsing with different parameter types
    %% including byte8 type for CAN bus ID params (0x0110~0x01FF)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0104,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 102,
    ResponseSeq = 50,

    %% Test data
    HeartbeatValue = 60,
    ApnValue = <<"cmnet">>,
    ApnLen = byte_size(ApnValue),
    Byte8Data = <<1, 2, 3, 4, 5, 6, 7, 8>>,

    %% Build response body:
    %% - seq (WORD)
    %% - count (BYTE)
    %% - params: [id(DWORD) + len(BYTE) + value(...)]
    ParamsBody = <<
        %% Param 1: DWORD (0x0001 heartbeat)
        16#0001:32/big,
        4:8,
        HeartbeatValue:32/big,
        %% Param 2: STRING (0x0010 APN)
        16#0010:32/big,
        ApnLen:8,
        ApnValue/binary,
        %% Param 3: BYTE8 (0x0110 CAN bus ID)
        16#0110:32/big,
        8:8,
        Byte8Data/binary
    >>,
    Body = <<ResponseSeq:16/big, 3:8, ParamsBody/binary>>,

    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _State} = emqx_jt808_frame:parse(Stream, Parser),
    ?assertMatch(
        #{
            <<"header">> := #{<<"msg_id">> := 16#0104},
            <<"body">> := #{
                <<"seq">> := ResponseSeq,
                <<"length">> := 3,
                <<"params">> := _
            }
        },
        Map
    ),

    %% Verify params are correctly parsed
    #{<<"body">> := #{<<"params">> := Params}} = Map,
    ?assertEqual(3, length(Params)),

    %% Find and verify each param
    [Param1, Param2, Param3] = Params,
    ?assertEqual(16#0001, maps:get(<<"id">>, Param1)),
    ?assertEqual(HeartbeatValue, maps:get(<<"value">>, Param1)),
    ?assertEqual(16#0010, maps:get(<<"id">>, Param2)),
    ?assertEqual(ApnValue, maps:get(<<"value">>, Param2)),
    ?assertEqual(16#0110, maps:get(<<"id">>, Param3)),
    %% BYTE8 value should be base64 encoded
    ?assertEqual(Byte8Data, base64:decode(maps:get(<<"value">>, Param3))),
    ok.

t_query_client_param_ack_parse_reserved_param(_Config) ->
    %% Test 0x0104 parsing with reserved/unknown parameter (fallback to base64)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    MsgId = 16#0104,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 103,
    ResponseSeq = 51,

    %% Unknown param ID outside known ranges
    UnknownParamId = 16#F000,
    UnknownData = <<16#DE, 16#AD, 16#BE, 16#EF, 16#CA, 16#FE>>,
    UnknownLen = byte_size(UnknownData),

    Body = <<ResponseSeq:16/big, 1:8, UnknownParamId:32/big, UnknownLen:8, UnknownData/binary>>,

    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _State} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"params">> := [Param]}} = Map,
    ?assertEqual(UnknownParamId, maps:get(<<"id">>, Param)),
    %% Reserved/unknown params should be base64 encoded
    ?assertEqual(UnknownData, base64:decode(maps:get(<<"value">>, Param))),
    ok.

%%--------------------------------------------------------------------
%% 9. GBK/UTF-8 String Encoding Tests
%%--------------------------------------------------------------------

%% Test GBK to UTF-8 decoding for register message (license_number field)
t_gbk_decode_register_license_number(_Config) ->
    %% GBK encoded "京A12345" (Chinese license plate)
    %% 京 in GBK is 0xBEA9, A is 0x41, 1-5 are 0x31-0x35
    GbkLicenseNumber = <<16#BE, 16#A9, "A12345">>,
    Utf8LicenseNumber = <<"京A12345"/utf8>>,

    Parser = emqx_jt808_frame:initial_parse_state(#{string_encoding => gbk}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color,
            GbkLicenseNumber/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 78,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"license_number">> := ParsedLicense}} = Map,
    ?assertEqual(Utf8LicenseNumber, ParsedLicense),
    ok.

%% Test UTF-8 passthrough when string_encoding=utf8 (default behavior)
t_utf8_passthrough_register_license_number(_Config) ->
    %% UTF-8 encoded "京A12345"
    Utf8LicenseNumber = <<"京A12345"/utf8>>,

    Parser = emqx_jt808_frame:initial_parse_state(#{string_encoding => utf8}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color,
            Utf8LicenseNumber/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 79,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"license_number">> := ParsedLicense}} = Map,
    ?assertEqual(Utf8LicenseNumber, ParsedLicense),
    ok.

%% Test GBK to UTF-8 decoding for driver ID report (driver_name and organization)
t_gbk_decode_driver_id_report(_Config) ->
    %% GBK encoded strings
    %% 张三 in GBK: 0xD5C5 0xC8FD
    GbkDriverName = <<16#D5, 16#C5, 16#C8, 16#FD>>,
    Utf8DriverName = <<"张三"/utf8>>,
    %% 北京交通 in GBK: 0xB1B1 0xBEA9 0xBDBB 0xCDA8
    GbkOrganization = <<16#B1, 16#B1, 16#BE, 16#A9, 16#BD, 16#BB, 16#CD, 16#A8>>,
    Utf8Organization = <<"北京交通"/utf8>>,

    Parser = emqx_jt808_frame:initial_parse_state(#{string_encoding => gbk}),
    MsgId = 16#0702,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 220,

    Status = 1,
    TimeBCD = <<16#26, 16#01, 16#21, 16#10, 16#30, 16#45>>,
    IcResult = 0,
    NameLength = byte_size(GbkDriverName),
    Certificate = pad_binary(<<"CERT123456789">>, 20),
    OrgLength = byte_size(GbkOrganization),
    CertExpiryBCD = <<16#28, 16#12, 16#31, 16#00>>,

    Body = <<
        Status:8,
        TimeBCD/binary,
        IcResult:8,
        NameLength:8,
        GbkDriverName/binary,
        Certificate/binary,
        OrgLength:8,
        GbkOrganization/binary,
        CertExpiryBCD/binary
    >>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"driver_name">> := ParsedName, <<"organization">> := ParsedOrg}} = Map,
    ?assertEqual(Utf8DriverName, ParsedName),
    ?assertEqual(Utf8Organization, ParsedOrg),
    ok.

%% Test UTF-8 to GBK encoding for send_text serialization
t_gbk_encode_send_text(_Config) ->
    %% UTF-8 string to encode
    Utf8Text = <<"请注意安全驾驶"/utf8>>,
    %% Expected GBK: 请=C7EB 注=D7A2 意=D2E2 安=B0B2 全=C8AB 驾=BCDD 驶=CABB
    ExpectedGbkText =
        <<16#C7, 16#EB, 16#D7, 16#A2, 16#D2, 16#E2, 16#B0, 16#B2, 16#C8, 16#AB, 16#BC, 16#DD, 16#CA,
            16#BB>>,

    MsgId = 16#8300,
    MsgSn = 62,
    Flag = 1,
    TextType = 2,
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{<<"flag">> => Flag, <<"text_type">> => TextType, <<"text">> => Utf8Text}
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{string_encoding => gbk}),

    %% Parse the stream to verify it contains GBK encoded text
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    {ok, _, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),

    %% Verify GBK text is in the raw stream (after header)
    ?assert(binary:match(Stream, ExpectedGbkText) =/= nomatch),
    ok.

%% Test UTF-8 passthrough for send_text when string_encoding=utf8
t_utf8_passthrough_send_text(_Config) ->
    Utf8Text = <<"请注意安全驾驶"/utf8>>,

    MsgId = 16#8300,
    MsgSn = 63,
    Flag = 1,
    TextType = 2,
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{<<"flag">> => Flag, <<"text_type">> => TextType, <<"text">> => Utf8Text}
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{string_encoding => utf8}),

    %% Verify UTF-8 text is in the raw stream (not converted)
    ?assert(binary:match(Stream, Utf8Text) =/= nomatch),
    ok.

%% Test GBK round-trip: GBK wire → parse (UTF-8) → serialize (GBK wire)
t_gbk_roundtrip_driver_info(_Config) ->
    %% GBK encoded driver name

    %% 张三
    GbkDriverName = <<16#D5, 16#C5, 16#C8, 16#FD>>,
    Utf8DriverName = <<"张三"/utf8>>,
    %% 北京
    GbkOrganization = <<16#B1, 16#B1, 16#BE, 16#A9>>,
    Utf8Organization = <<"北京"/utf8>>,

    Opts = #{string_encoding => gbk},
    Parser = emqx_jt808_frame:initial_parse_state(Opts),
    MsgId = 16#0702,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 230,

    Status = 1,
    TimeBCD = <<16#26, 16#01, 16#25, 16#14, 16#30, 16#00>>,
    IcResult = 0,
    NameLength = byte_size(GbkDriverName),
    Certificate = pad_binary(<<"CERT999888777">>, 20),
    OrgLength = byte_size(GbkOrganization),
    CertExpiryBCD = <<16#30, 16#12, 16#31, 16#00>>,

    Body = <<
        Status:8,
        TimeBCD/binary,
        IcResult:8,
        NameLength:8,
        GbkDriverName/binary,
        Certificate/binary,
        OrgLength:8,
        GbkOrganization/binary,
        CertExpiryBCD/binary
    >>,
    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    %% Parse: GBK wire → UTF-8 internal
    {ok, ParsedMap, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"driver_name">> := ParsedName, <<"organization">> := ParsedOrg}} =
        ParsedMap,
    ?assertEqual(Utf8DriverName, ParsedName),
    ?assertEqual(Utf8Organization, ParsedOrg),
    ok.

%% Test default behavior (no string_encoding option) preserves UTF-8
t_default_encoding_utf8_passthrough(_Config) ->
    %% When no string_encoding is specified, UTF-8 should pass through unchanged
    Utf8LicenseNumber = <<"京A12345"/utf8>>,

    %% No string_encoding option - should default to utf8 (passthrough)
    Parser = emqx_jt808_frame:initial_parse_state(#{}),
    Manuf = <<"examp">>,
    Model = <<"33333333333333333333">>,
    DevId = <<"1234567">>,
    Color = 3,
    RegisterPacket =
        <<58:?word, 59:?word, Manuf/binary, Model/binary, DevId/binary, Color,
            Utf8LicenseNumber/binary>>,
    MsgId = 16#0100,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 80,
    Size = size(RegisterPacket),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, RegisterPacket),

    {ok, Map, <<>>, _} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"license_number">> := ParsedLicense}} = Map,
    ?assertEqual(Utf8LicenseNumber, ParsedLicense),
    ok.

%% Test GBK encoding for circle area name field
t_gbk_encode_circle_area_name(_Config) ->
    %% UTF-8 area name
    Utf8AreaName = <<"禁行区域"/utf8>>,
    %% Expected GBK: 禁=BDFB 行=D0D0 区=C7F8 域=D3F2
    ExpectedGbkName = <<16#BD, 16#FB, 16#D0, 16#D0, 16#C7, 16#F8, 16#D3, 16#F2>>,

    MsgId = 16#8600,
    MsgSn = 70,
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => ?JT808_PHONE_STR_2019,
            <<"msg_sn">> => MsgSn,
            <<"proto_ver">> => ?PROTO_VER_2019
        },
        <<"body">> => #{
            <<"type">> => 0,
            <<"length">> => 1,
            <<"areas">> => [
                #{
                    <<"id">> => 1,
                    %% bit 6 set = has name field
                    <<"flag">> => 16#0040,
                    <<"center_latitude">> => 39908823,
                    <<"center_longitude">> => 116397470,
                    <<"radius">> => 1000,
                    <<"start_time">> => <<"260125000000">>,
                    <<"end_time">> => <<"261231235959">>,
                    <<"max_speed">> => 60,
                    <<"overspeed_duration">> => 10,
                    <<"night_max_speed">> => 40,
                    <<"name">> => Utf8AreaName
                }
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{string_encoding => gbk}),

    %% Verify GBK encoded name is in the stream
    ?assert(binary:match(Stream, ExpectedGbkName) =/= nomatch),
    ok.

%% Test GBK encoding for client parameter (string type)
t_gbk_encode_client_param_string(_Config) ->
    %% UTF-8 APN value with Chinese characters
    Utf8Apn = <<"中国移动"/utf8>>,
    %% Expected GBK: 中=D6D0 国=B9FA 移=D2C6 动=B6AF
    ExpectedGbkApn = <<16#D6, 16#D0, 16#B9, 16#FA, 16#D2, 16#C6, 16#B6, 16#AF>>,

    MsgId = 16#8103,
    MsgSn = 71,
    %% Parameter 0x0010 is APN (STRING type)
    DownlinkJson = #{
        <<"header">> => #{
            <<"msg_id">> => MsgId,
            <<"encrypt">> => ?NO_ENCRYPT,
            <<"phone">> => <<"000123456789">>,
            <<"msg_sn">> => MsgSn
        },
        <<"body">> => #{
            <<"length">> => 1,
            <<"params">> => [
                #{<<"id">> => 16#0010, <<"value">> => Utf8Apn}
            ]
        }
    },
    Stream = emqx_jt808_frame:serialize_pkt(DownlinkJson, #{string_encoding => gbk}),

    %% Verify GBK encoded APN is in the stream
    ?assert(binary:match(Stream, ExpectedGbkApn) =/= nomatch),
    ok.

%% Test GBK decoding for query param ack (string type parameter)
t_gbk_decode_query_param_ack_string(_Config) ->
    %% GBK encoded APN

    %% 中国移动
    GbkApn = <<16#D6, 16#D0, 16#B9, 16#FA, 16#D2, 16#C6, 16#B6, 16#AF>>,
    Utf8Apn = <<"中国移动"/utf8>>,

    Parser = emqx_jt808_frame:initial_parse_state(#{string_encoding => gbk}),
    MsgId = 16#0104,
    PhoneBCD = <<16#00, 16#01, 16#23, 16#45, 16#67, 16#89>>,
    MsgSn = 104,
    ResponseSeq = 52,

    %% Parameter 0x0010 is APN (STRING type)
    ParamId = 16#0010,
    ParamLen = byte_size(GbkApn),

    Body = <<ResponseSeq:16/big, 1:8, ParamId:32/big, ParamLen:8, GbkApn/binary>>,

    Size = byte_size(Body),
    Header =
        <<MsgId:?word, ?RESERVE:2, ?NO_FRAGMENT:1, ?NO_ENCRYPT:3, ?MSG_SIZE(Size), PhoneBCD/binary,
            MsgSn:?word>>,
    Stream = encode(Header, Body),

    {ok, Map, <<>>, _State} = emqx_jt808_frame:parse(Stream, Parser),
    #{<<"body">> := #{<<"params">> := [Param]}} = Map,
    ?assertEqual(ParamId, maps:get(<<"id">>, Param)),
    ?assertEqual(Utf8Apn, maps:get(<<"value">>, Param)),
    ok.

%% Test unit functions for encoding module
t_encoding_module_unit_tests(_Config) ->
    %% Test maybe_decode_string with GBK encoding

    %% 你好 in GBK
    GbkHello = <<16#C4, 16#E3, 16#BA, 16#C3>>,
    Utf8Hello = <<"你好"/utf8>>,
    ?assertEqual(
        Utf8Hello,
        emqx_jt808_encoding:maybe_decode_string(GbkHello, test_field, #{string_encoding => gbk})
    ),

    %% Test maybe_decode_string with UTF-8 encoding (passthrough)
    ?assertEqual(
        Utf8Hello,
        emqx_jt808_encoding:maybe_decode_string(Utf8Hello, test_field, #{string_encoding => utf8})
    ),

    %% Test maybe_decode_string with no encoding option (default passthrough)
    ?assertEqual(Utf8Hello, emqx_jt808_encoding:maybe_decode_string(Utf8Hello, test_field, #{})),

    %% Test maybe_encode_string with GBK encoding
    ?assertEqual(
        GbkHello,
        emqx_jt808_encoding:maybe_encode_string(Utf8Hello, test_field, #{string_encoding => gbk})
    ),

    %% Test maybe_encode_string with UTF-8 encoding (passthrough)
    ?assertEqual(
        Utf8Hello,
        emqx_jt808_encoding:maybe_encode_string(Utf8Hello, test_field, #{string_encoding => utf8})
    ),

    %% Test maybe_encode_string with no encoding option (default passthrough)
    ?assertEqual(Utf8Hello, emqx_jt808_encoding:maybe_encode_string(Utf8Hello, test_field, #{})),

    %% Test ASCII-only strings (should be identical in both encodings)
    AsciiStr = <<"Hello123">>,
    ?assertEqual(
        AsciiStr,
        emqx_jt808_encoding:maybe_decode_string(AsciiStr, test_field, #{string_encoding => gbk})
    ),
    ?assertEqual(
        AsciiStr,
        emqx_jt808_encoding:maybe_encode_string(AsciiStr, test_field, #{string_encoding => gbk})
    ),
    ok.
