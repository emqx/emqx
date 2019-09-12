%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    [{proto, init_protocol()}|Config].

init_protocol() ->
    emqx_protocol:init(#mqtt_packet_connect{
                          proto_name  = <<"MQTT">>,
                          proto_ver   = ?MQTT_PROTO_V5,
                          is_bridge   = false,
                          clean_start = true,
                          keepalive   = 30,
                          properties  = #{},
                          client_id   = <<"clientid">>,
                          username    = <<"username">>,
                          password    = <<"passwd">>
                         }, testing).

end_per_suite(_Config) -> ok.

t_init_info_1(Config) ->
    Proto = proplists:get_value(proto, Config),
    ?assertEqual(#{proto_name    => <<"MQTT">>,
                   proto_ver     => ?MQTT_PROTO_V5,
                   clean_start   => true,
                   keepalive     => 30,
                   will_msg      => undefined,
                   client_id     => <<"clientid">>,
                   username      => <<"username">>,
                   topic_aliases => undefined,
                   alias_maximum => #{outbound => 0, inbound => 0}
                  }, emqx_protocol:info(Proto)).

t_init_info_2(Config) ->
    Proto = proplists:get_value(proto, Config),
    ?assertEqual(<<"MQTT">>, emqx_protocol:info(proto_name, Proto)),
    ?assertEqual(?MQTT_PROTO_V5, emqx_protocol:info(proto_ver, Proto)),
    ?assertEqual(true, emqx_protocol:info(clean_start, Proto)),
    ?assertEqual(30, emqx_protocol:info(keepalive, Proto)),
    ?assertEqual(<<"clientid">>, emqx_protocol:info(client_id, Proto)),
    ?assertEqual(<<"username">>, emqx_protocol:info(username, Proto)),
    ?assertEqual(undefined, emqx_protocol:info(will_msg, Proto)),
    ?assertEqual(0, emqx_protocol:info(will_delay_interval, Proto)),
    ?assertEqual(undefined, emqx_protocol:info(topic_aliases, Proto)),
    ?assertEqual(#{outbound => 0, inbound => 0}, emqx_protocol:info(alias_maximum, Proto)).

t_find_save_alias(Config) ->
    Proto = proplists:get_value(proto, Config),
    ?assertEqual(undefined, emqx_protocol:info(topic_aliases, Proto)),
    ?assertEqual(false, emqx_protocol:find_alias(1, Proto)),
    Proto1 = emqx_protocol:save_alias(1, <<"t1">>, Proto),
    Proto2 = emqx_protocol:save_alias(2, <<"t2">>, Proto1),
    ?assertEqual(#{1 => <<"t1">>, 2 => <<"t2">>},
                 emqx_protocol:info(topic_aliases, Proto2)),
    ?assertEqual({ok, <<"t1">>}, emqx_protocol:find_alias(1, Proto2)),
    ?assertEqual({ok, <<"t2">>}, emqx_protocol:find_alias(2, Proto2)).

