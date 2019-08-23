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

t_init_and_info(_) ->
    ConnPkt = #mqtt_packet_connect{
                 proto_name  = <<"MQTT">>,
                 proto_ver   = ?MQTT_PROTO_V4,
                 is_bridge   = false,
                 clean_start = true,
                 keepalive   = 30,
                 properties  = #{},
                 client_id   = <<"clientid">>,
                 username    = <<"username">>,
                 password    = <<"passwd">>
                },
    Proto = emqx_protocol:init(ConnPkt),
    ?assertEqual(<<"MQTT">>, emqx_protocol:info(proto_name, Proto)),
    ?assertEqual(?MQTT_PROTO_V4, emqx_protocol:info(proto_ver, Proto)),
    ?assertEqual(true, emqx_protocol:info(clean_start, Proto)),
    ?assertEqual(<<"clientid">>, emqx_protocol:info(client_id, Proto)),
    ?assertEqual(<<"username">>, emqx_protocol:info(username, Proto)),
    ?assertEqual(undefined, emqx_protocol:info(will_msg, Proto)),
    ?assertEqual(#{}, emqx_protocol:info(conn_props, Proto)).



