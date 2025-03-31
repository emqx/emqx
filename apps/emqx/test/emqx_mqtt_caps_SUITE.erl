%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_caps_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

t_check_pub(_) ->
    OldConf = emqx:get_config([zones], #{}),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, retain_available], false),
    timer:sleep(50),
    ok = emqx_mqtt_caps:check_pub(default, #{qos => ?QOS_1, retain => false}),
    PubFlags1 = #{qos => ?QOS_2, retain => false},
    ?assertEqual(
        {error, ?RC_QOS_NOT_SUPPORTED},
        emqx_mqtt_caps:check_pub(default, PubFlags1)
    ),
    PubFlags2 = #{qos => ?QOS_1, retain => true},
    ?assertEqual(
        {error, ?RC_RETAIN_NOT_SUPPORTED},
        emqx_mqtt_caps:check_pub(default, PubFlags2)
    ),
    emqx_config:put([zones], OldConf).

t_check_sub(_) ->
    OldConf = emqx:get_config([zones], #{}),
    SubOpts = #{
        rh => 0,
        rap => 0,
        nl => 0,
        qos => ?QOS_2
    },
    emqx_config:put_zone_conf(default, [mqtt, max_topic_levels], 2),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, shared_subscription], false),
    emqx_config:put_zone_conf(default, [mqtt, wildcard_subscription], false),
    timer:sleep(50),
    ClientInfo = #{zone => default},

    ?assertEqual(
        {error, ?RC_TOPIC_FILTER_INVALID},
        emqx_mqtt_caps:check_sub(ClientInfo, <<"a/b/c/d">>, SubOpts)
    ),
    ?assertEqual(
        {error, ?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED},
        emqx_mqtt_caps:check_sub(ClientInfo, <<"+/#">>, SubOpts)
    ),
    ?assertEqual(
        {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED},
        emqx_mqtt_caps:check_sub(
            ClientInfo, #share{group = <<"group">>, topic = <<"topic">>}, SubOpts
        )
    ),

    %% return `ok` when allowed origin sub-qos (max_qos_allowed >= sub-qos)
    %% and `{ok, QoS}` when granted qos lower than origin sub-qos
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_0),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_0})
    ),
    ?assertEqual(
        {ok, ?QOS_0},
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_1})
    ),
    ?assertEqual(
        {ok, ?QOS_0},
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_2})
    ),

    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_0})
    ),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_1})
    ),
    ?assertEqual(
        {ok, ?QOS_1},
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_2})
    ),

    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_2),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_0})
    ),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_1})
    ),
    ?assertEqual(
        ok,
        emqx_mqtt_caps:check_sub(ClientInfo, <<"topic">>, SubOpts#{qos => ?QOS_2})
    ),

    emqx_config:put([zones], OldConf).
