%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tlv_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(LOGT(Format, Args), logger:debug("TEST_SUITE: " ++ Format, Args)).

-include("emqx_lwm2m.hrl").
-include_lib("lwm2m_coap/include/coap.hrl").
-include_lib("eunit/include/eunit.hrl").


all() -> [case01, case02, case03, case03_0, case04, case05, case06, case07, case08, case09].



init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.


case01(_Config) ->
    Data = <<16#C8, 16#00, 16#14, 16#4F, 16#70, 16#65, 16#6E, 16#20, 16#4D, 16#6F, 16#62, 16#69, 16#6C, 16#65, 16#20, 16#41, 16#6C, 16#6C, 16#69, 16#61, 16#6E, 16#63, 16#65>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_resource_with_value => 16#00, value => <<"Open Mobile Alliance">>}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case02(_Config) ->
    Data = <<16#86, 16#06, 16#41, 16#00, 16#01, 16#41, 16#01, 16#05>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_multiple_resource => 16#06, value => [
                                                                    #{tlv_resource_instance => 16#00, value => <<1>>},
                                                                    #{tlv_resource_instance => 16#01, value => <<5>>}
            ]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case03(_Config) ->
    Data = <<16#C8, 16#00, 16#14, 16#4F, 16#70, 16#65, 16#6E, 16#20, 16#4D, 16#6F, 16#62, 16#69, 16#6C, 16#65, 16#20, 16#41, 16#6C, 16#6C, 16#69, 16#61, 16#6E, 16#63, 16#65, 16#C8, 16#01, 16#16, 16#4C, 16#69, 16#67, 16#68, 16#74, 16#77, 16#65, 16#69, 16#67, 16#68, 16#74, 16#20, 16#4D, 16#32, 16#4D, 16#20, 16#43, 16#6C, 16#69, 16#65, 16#6E, 16#74, 16#C8, 16#02, 16#09, 16#33, 16#34, 16#35, 16#30, 16#30, 16#30, 16#31, 16#32, 16#33>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
            #{tlv_resource_with_value => 16#00, value => <<"Open Mobile Alliance">>},
            #{tlv_resource_with_value => 16#01, value => <<"Lightweight M2M Client">>},
            #{tlv_resource_with_value => 16#02, value => <<"345000123">>}
            ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case03_0(_Config) ->
    Data = <<16#87, 16#02, 16#41, 16#7F, 16#07, 16#61, 16#01, 16#36, 16#01>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_multiple_resource => 16#02, value => [
            #{tlv_resource_instance => 16#7F, value => <<16#07>>},
            #{tlv_resource_instance => 16#0136, value => <<16#01>>}
        ]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case04(_Config) ->
    % 6.4.3.1 Single Object Instance Request Example
    Data = <<16#C8, 16#00, 16#14, 16#4F, 16#70, 16#65, 16#6E, 16#20, 16#4D, 16#6F, 16#62, 16#69, 16#6C, 16#65, 16#20, 16#41, 16#6C, 16#6C, 16#69, 16#61, 16#6E, 16#63, 16#65, 16#C8, 16#01, 16#16, 16#4C, 16#69, 16#67, 16#68, 16#74, 16#77, 16#65, 16#69, 16#67, 16#68, 16#74, 16#20, 16#4D, 16#32, 16#4D, 16#20, 16#43, 16#6C, 16#69, 16#65, 16#6E, 16#74, 16#C8, 16#02, 16#09, 16#33, 16#34, 16#35, 16#30, 16#30, 16#30, 16#31, 16#32, 16#33, 16#C3, 16#03, 16#31, 16#2E, 16#30, 16#86, 16#06, 16#41, 16#00, 16#01, 16#41, 16#01, 16#05, 16#88, 16#07, 16#08, 16#42, 16#00, 16#0E, 16#D8, 16#42, 16#01, 16#13, 16#88, 16#87, 16#08, 16#41, 16#00, 16#7D, 16#42, 16#01, 16#03, 16#84, 16#C1, 16#09, 16#64, 16#C1, 16#0A, 16#0F, 16#83, 16#0B, 16#41, 16#00, 16#00, 16#C4, 16#0D, 16#51, 16#82, 16#42, 16#8F, 16#C6, 16#0E, 16#2B, 16#30, 16#32, 16#3A, 16#30, 16#30, 16#C1, 16#10, 16#55>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
            #{tlv_resource_with_value => 16#00, value => <<"Open Mobile Alliance">>},
            #{tlv_resource_with_value => 16#01, value => <<"Lightweight M2M Client">>},
            #{tlv_resource_with_value => 16#02, value => <<"345000123">>},
            #{tlv_resource_with_value => 16#03, value => <<"1.0">>},
            #{tlv_multiple_resource => 16#06, value => [
                                                                        #{tlv_resource_instance => 16#00, value => <<1>>},
                                                                        #{tlv_resource_instance => 16#01, value => <<5>>}
                ]},
            #{tlv_multiple_resource => 16#07, value => [
                                                                        #{tlv_resource_instance => 16#00, value => <<16#0ED8:16>>},
                                                                        #{tlv_resource_instance => 16#01, value => <<16#1388:16>>}
                ]},
            #{tlv_multiple_resource => 16#08, value => [
                                                                        #{tlv_resource_instance => 16#00, value => <<16#7d>>},
                                                                        #{tlv_resource_instance => 16#01, value => <<16#0384:16>>}
            ]},
            #{tlv_resource_with_value => 16#09, value => <<16#64>>},
            #{tlv_resource_with_value => 16#0A, value => <<16#0F>>},
            #{tlv_multiple_resource => 16#0B, value => [
                #{tlv_resource_instance => 16#00, value => <<16#00>>}
            ]},
            #{tlv_resource_with_value => 16#0D, value => <<16#5182428F:32>>},
            #{tlv_resource_with_value => 16#0E, value => <<"+02:00">>},
            #{tlv_resource_with_value => 16#10, value => <<"U">>}
          ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case05(_Config) ->
    % 6.4.3.2 Multiple Object Instance Request Examples
    %   A) Request on Single-Instance Object
    Data = <<16#08, 16#00, 16#79, 16#C8, 16#00, 16#14, 16#4F, 16#70, 16#65, 16#6E, 16#20, 16#4D, 16#6F, 16#62, 16#69, 16#6C, 16#65, 16#20, 16#41, 16#6C, 16#6C, 16#69, 16#61, 16#6E, 16#63, 16#65, 16#C8, 16#01, 16#16, 16#4C, 16#69, 16#67, 16#68, 16#74, 16#77, 16#65, 16#69, 16#67, 16#68, 16#74, 16#20, 16#4D, 16#32, 16#4D, 16#20, 16#43, 16#6C, 16#69, 16#65, 16#6E, 16#74, 16#C8, 16#02, 16#09, 16#33, 16#34, 16#35, 16#30, 16#30, 16#30, 16#31, 16#32, 16#33, 16#C3, 16#03, 16#31, 16#2E, 16#30, 16#86, 16#06, 16#41, 16#00, 16#01, 16#41, 16#01, 16#05, 16#88, 16#07, 16#08, 16#42, 16#00, 16#0E, 16#D8, 16#42, 16#01, 16#13, 16#88, 16#87, 16#08, 16#41, 16#00, 16#7D, 16#42, 16#01, 16#03, 16#84, 16#C1, 16#09, 16#64, 16#C1, 16#0A, 16#0F, 16#83, 16#0B, 16#41, 16#00, 16#00, 16#C4, 16#0D, 16#51, 16#82, 16#42, 16#8F, 16#C6, 16#0E, 16#2B, 16#30, 16#32, 16#3A, 16#30, 16#30, 16#C1, 16#10, 16#55>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_object_instance => 16#00, value => [
            #{tlv_resource_with_value => 16#00, value => <<"Open Mobile Alliance">>},
            #{tlv_resource_with_value => 16#01, value => <<"Lightweight M2M Client">>},
            #{tlv_resource_with_value => 16#02, value => <<"345000123">>},
            #{tlv_resource_with_value => 16#03, value => <<"1.0">>},
            #{tlv_multiple_resource => 16#06, value => [
                #{tlv_resource_instance => 16#00, value => <<1>>},
                #{tlv_resource_instance => 16#01, value => <<5>>}
            ]},
            #{tlv_multiple_resource => 16#07, value => [
                #{tlv_resource_instance => 16#00, value => <<16#0ED8:16>>},
                #{tlv_resource_instance => 16#01, value => <<16#1388:16>>}
            ]},
            #{tlv_multiple_resource => 16#08, value => [
                #{tlv_resource_instance => 16#00, value => <<16#7d>>},
                #{tlv_resource_instance => 16#01, value => <<16#0384:16>>}
            ]},
            #{tlv_resource_with_value => 16#09, value => <<16#64>>},
            #{tlv_resource_with_value => 16#0A, value => <<16#0F>>},
            #{tlv_multiple_resource => 16#0B, value => [
                #{tlv_resource_instance => 16#00, value => <<16#00>>}
            ]},
            #{tlv_resource_with_value => 16#0D, value => <<16#5182428F:32>>},
            #{tlv_resource_with_value => 16#0E, value => <<"+02:00">>},
            #{tlv_resource_with_value => 16#10, value => <<"U">>}
        ]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case06(_Config) ->
    % 6.4.3.2 Multiple Object Instance Request Examples
    %   B) Request on Multiple-Instances Object having 2 instances
    Data = <<16#08, 16#00, 16#0E, 16#C1, 16#00, 16#01, 16#C1, 16#01, 16#00, 16#83, 16#02, 16#41, 16#7F, 16#07, 16#C1, 16#03, 16#7F, 16#08, 16#02, 16#12, 16#C1, 16#00, 16#03, 16#C1, 16#01, 16#00, 16#87, 16#02, 16#41, 16#7F, 16#07, 16#61, 16#01, 16#36, 16#01, 16#C1, 16#03, 16#7F>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_object_instance => 16#00, value => [
            #{tlv_resource_with_value => 16#00, value => <<16#01>>},
            #{tlv_resource_with_value => 16#01, value => <<16#00>>},
            #{tlv_multiple_resource => 16#02, value => [
                #{tlv_resource_instance => 16#7F, value => <<16#07>>}
            ]},
            #{tlv_resource_with_value => 16#03, value => <<16#7F>>}
        ]},
        #{tlv_object_instance => 16#02, value => [
            #{tlv_resource_with_value => 16#00, value => <<16#03>>},
            #{tlv_resource_with_value => 16#01, value => <<16#00>>},
            #{tlv_multiple_resource => 16#02, value => [
                #{tlv_resource_instance => 16#7F, value => <<16#07>>},
                #{tlv_resource_instance => 16#0136, value => <<16#01>>}
            ]},
            #{tlv_resource_with_value => 16#03, value => <<16#7F>>}
        ]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case07(_Config) ->
    % 6.4.3.2 Multiple Object Instance Request Examples
    %   C) Request on Multiple-Instances Object having 1 instance only
    Data = <<16#08, 16#00, 16#0F, 16#C1, 16#00, 16#01, 16#C4, 16#01, 16#00, 16#01, 16#51, 16#80, 16#C1, 16#06, 16#01, 16#C1, 16#07, 16#55>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_object_instance => 16#00, value => [
            #{tlv_resource_with_value => 16#00, value => <<16#01>>},
            #{tlv_resource_with_value => 16#01, value => <<86400:32>>},
            #{tlv_resource_with_value => 16#06, value => <<16#01>>},
            #{tlv_resource_with_value => 16#07, value => <<$U>>}]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case08(_Config) ->
    % 6.4.3.3 Example of Request on an Object Instance containing an Object Link Resource
    %   Example 1) request to Object 65 Instance 0: Read /65/0
    Data = <<16#88, 16#00, 16#0C, 16#44, 16#00, 16#00, 16#42, 16#00, 16#00, 16#44, 16#01, 16#00, 16#42, 16#00, 16#01, 16#C8, 16#01, 16#0D, 16#38, 16#36, 16#31, 16#33, 16#38, 16#30, 16#30, 16#37, 16#35, 16#35, 16#35, 16#30, 16#30, 16#C4, 16#02, 16#12, 16#34, 16#56, 16#78>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_multiple_resource => 16#00, value => [
            #{tlv_resource_instance => 16#00, value => <<16#00, 16#42, 16#00, 16#00>>},
            #{tlv_resource_instance => 16#01, value => <<16#00, 16#42, 16#00, 16#01>>}
            ]},
        #{tlv_resource_with_value => 16#01, value => <<"8613800755500">>},
        #{tlv_resource_with_value => 16#02, value => <<16#12345678:32>>}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

case09(_Config) ->
    % 6.4.3.3 Example of Request on an Object Instance containing an Object Link Resource
    %   Example 2) request to Object 66: Read /66: TLV payload will contain 2 Object Instances
    Data = <<16#08, 16#00, 16#26, 16#C8, 16#00, 16#0B, 16#6D, 16#79, 16#53, 16#65, 16#72, 16#76, 16#69, 16#63, 16#65, 16#20, 16#31, 16#C8, 16#01, 16#0F, 16#49, 16#6E, 16#74, 16#65, 16#72, 16#6E, 16#65, 16#74, 16#2E, 16#31, 16#35, 16#2E, 16#32, 16#33, 16#34, 16#C4, 16#02, 16#00, 16#43, 16#00, 16#00, 16#08, 16#01, 16#26, 16#C8, 16#00, 16#0B, 16#6D, 16#79, 16#53, 16#65, 16#72, 16#76, 16#69, 16#63, 16#65, 16#20, 16#32, 16#C8, 16#01, 16#0F, 16#49, 16#6E, 16#74, 16#65, 16#72, 16#6E, 16#65, 16#74, 16#2E, 16#31, 16#35, 16#2E, 16#32, 16#33, 16#35, 16#C4, 16#02, 16#FF, 16#FF, 16#FF, 16#FF>>,
    R = emqx_lwm2m_tlv:parse(Data),
    Exp = [
        #{tlv_object_instance => 16#00, value => [
            #{tlv_resource_with_value => 16#00, value => <<"myService 1">>},
            #{tlv_resource_with_value => 16#01, value => <<"Internet.15.234">>},
            #{tlv_resource_with_value => 16#02, value => <<16#00, 16#43, 16#00, 16#00>>}
        ]},
        #{tlv_object_instance => 16#01, value => [
            #{tlv_resource_with_value => 16#00, value => <<"myService 2">>},
            #{tlv_resource_with_value => 16#01, value => <<"Internet.15.235">>},
            #{tlv_resource_with_value => 16#02, value => <<16#FF, 16#FF, 16#FF, 16#FF>>}
        ]}
    ],
    ?assertEqual(Exp, R),
    EncodedBinary = emqx_lwm2m_tlv:encode(Exp),
    ?assertEqual(EncodedBinary, Data).

