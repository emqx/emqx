-module(emqx_reason_codes_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() ->
    [t_get_v].

t_get_v(_) ->
 ?assertEqual(success, emqx_reason_codes:name(16#00,?MQTT_PROTO_V5)),
 ?assertEqual(<<"Success">>, emqx_reason_codes:text(16#00)),
 ?assertEqual(?CONNACK_PROTO_VER, emqx_reason_codes:compat(connack, 16#80)).






