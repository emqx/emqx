%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Direct, docker-free tests for `emqx_external_trace:connect_attrs/2'.
%%
%% This is a plain function of a CONNECT packet and a channel, so it is
%% tested here without a registered OTel provider and without Jaeger. The
%% end-to-end flow (real span export, will delivery) is covered by
%% `apps/emqx_opentelemetry/test/emqx_otel_SUITE.erl'.
-module(emqx_external_trace_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_limiter:create_listener_limiters('tcp:default', #{}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
A CONNECT with will user-properties must not crash `connect_attrs/2', and
`client.will_props' must decode back to the submitted pairs. Regresses
emqx/emqx#18673.
""".
t_connect_attrs_will_user_properties(_) ->
    WillProps = #{
        'User-Property' => [
            {<<"Will-Property1">>, <<"Will-Value1">>},
            {<<"Will-Property2">>, <<"Will-Value2">>}
        ]
    },
    Attrs = connect_attrs(#{
        will_flag => true,
        will_qos => 0,
        will_retain => false,
        will_topic => <<"will/topic">>,
        will_payload => <<"bye">>,
        will_props => WillProps
    }),
    ?assertNotMatch(#{'fallback_attr.msg' := _}, Attrs),
    #{'client.will_props' := WillPropsJson} = Attrs,
    Decoded = emqx_utils_json:decode_proplist(WillPropsJson),
    ?assertEqual(
        [
            {<<"Will-Property1">>, <<"Will-Value1">>},
            {<<"Will-Property2">>, <<"Will-Value2">>}
        ],
        proplists:get_value(<<"User-Property">>, Decoded)
    ).

-doc "`client.conn_props' with user properties keeps working, per the sibling fix in 1d5e783664.".
t_connect_attrs_conn_props_user_properties(_) ->
    ConnProps = #{
        'User-Property' => [{<<"Conn-Property1">>, <<"Conn-Value1">>}]
    },
    Attrs = connect_attrs(#{properties => ConnProps}),
    ?assertNotMatch(#{'fallback_attr.msg' := _}, Attrs),
    #{'client.conn_props' := ConnPropsJson} = Attrs,
    Decoded = emqx_utils_json:decode_proplist(ConnPropsJson),
    ?assertEqual(
        [{<<"Conn-Property1">>, <<"Conn-Value1">>}],
        proplists:get_value(<<"User-Property">>, Decoded)
    ).

-doc """
Will properties carrying a non-UTF-8 `Correlation-Data' still encode,
alongside user properties.
""".
t_connect_attrs_will_correlation_data_non_utf8(_) ->
    WillProps = #{
        'User-Property' => [{<<"Will-Property1">>, <<"Will-Value1">>}],
        'Correlation-Data' => <<0, 159, 146, 150>>
    },
    Attrs = connect_attrs(#{
        will_flag => true,
        will_props => WillProps
    }),
    ?assertNotMatch(#{'fallback_attr.msg' := _}, Attrs),
    #{'client.will_props' := WillPropsJson} = Attrs,
    ?assert(emqx_utils_json:is_json(WillPropsJson)).

-doc "A CONNECT with no will (will_flag = false, default will_props = #{}) is unchanged.".
t_connect_attrs_no_will(_) ->
    Attrs = connect_attrs(#{}),
    ?assertNotMatch(#{'fallback_attr.msg' := _}, Attrs),
    ?assertMatch(#{'client.will_flag' := false}, Attrs),
    #{'client.will_props' := WillPropsJson} = Attrs,
    ?assertEqual([], emqx_utils_json:decode_proplist(WillPropsJson)).

-doc """
An unencodable will-property value degrades only `client.will_props',
without killing the connect attrs.
""".
t_connect_attrs_unencodable_will_prop_degrades(_) ->
    WillProps = #{
        'User-Property' => [{<<"Will-Property1">>, <<"Will-Value1">>}],
        %% A bare 3-tuple is not valid ejson and is not converted by
        %% `emqx_utils_json:to_ejson/1', so jiffy rejects it.
        'Unencodable' => {1, 2, 3}
    },
    Attrs = connect_attrs(#{
        clientid => <<"c1">>,
        will_flag => true,
        will_props => WillProps
    }),
    ?assertMatch(
        #{
            'client.clientid' := <<"c1">>,
            'client.will_flag' := true,
            'client.will_props' := <<"encode_error">>
        },
        Attrs
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

connect_attrs(ConnectFields) ->
    ConnPkt = #mqtt_packet_connect{
        proto_name = <<"MQTT">>,
        proto_ver = ?MQTT_PROTO_V5,
        clientid = <<"clientid">>,
        username = <<"username">>
    },
    Packet = ?PACKET(?CONNECT, set_connect_fields(ConnPkt, ConnectFields)),
    emqx_external_trace:connect_attrs(Packet, channel()).

set_connect_fields(ConnPkt, Fields) ->
    maps:fold(fun set_connect_field/3, ConnPkt, Fields).

set_connect_field(will_flag, V, R) -> R#mqtt_packet_connect{will_flag = V};
set_connect_field(will_qos, V, R) -> R#mqtt_packet_connect{will_qos = V};
set_connect_field(will_retain, V, R) -> R#mqtt_packet_connect{will_retain = V};
set_connect_field(will_topic, V, R) -> R#mqtt_packet_connect{will_topic = V};
set_connect_field(will_payload, V, R) -> R#mqtt_packet_connect{will_payload = V};
set_connect_field(will_props, V, R) -> R#mqtt_packet_connect{will_props = V};
set_connect_field(properties, V, R) -> R#mqtt_packet_connect{properties = V};
set_connect_field(clientid, V, R) -> R#mqtt_packet_connect{clientid = V}.

channel() ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 3456},
        sockname => {{127, 0, 0, 1}, 1883},
        conn_mod => emqx_connection,
        proto_name => <<"MQTT">>,
        proto_ver => ?MQTT_PROTO_V5,
        clean_start => true,
        keepalive => 30,
        clientid => <<"clientid">>,
        username => <<"username">>,
        conn_props => #{},
        receive_maximum => 100,
        expiry_interval => 0
    },
    emqx_channel:init(ConnInfo, #{
        zone => default,
        limiter => undefined,
        listener => {tcp, default}
    }).
