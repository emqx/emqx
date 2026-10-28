%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_caps_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TC, Config) ->
    [{pre_zone_conf, emqx:get_config([zones], #{})} | Config].

end_per_testcase(_TC, Config) ->
    emqx_config:put([zones], proplists:get_value(pre_zone_conf, Config)).

-doc "Publish capability checks read QoS and retain flags from the message.".
t_check_pub(_) ->
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, retain_available], false),
    timer:sleep(50),
    Msg = #message{topic = <<"topic">>, qos = ?QOS_1, flags = #{retain => false}},
    ok = emqx_mqtt_caps:check_pub(default, Msg),
    PubMsg1 = Msg#message{qos = ?QOS_2},
    ?assertEqual(
        {error, ?RC_QOS_NOT_SUPPORTED},
        emqx_mqtt_caps:check_pub(default, PubMsg1)
    ),
    PubMsg2 = Msg#message{flags = #{retain => true}},
    ?assertEqual(
        {error, ?RC_RETAIN_NOT_SUPPORTED},
        emqx_mqtt_caps:check_pub(default, PubMsg2)
    ).

-doc "Topic limits take precedence over QoS and retain limits; zero disables the topic limit.".
t_check_pub_topic_levels(_) ->
    emqx_config:put_zone_conf(default, [mqtt, max_topic_levels], 2),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, retain_available], false),
    Msg = #message{topic = <<"a/b">>, qos = ?QOS_1, flags = #{retain => false}},
    ?assertEqual(ok, emqx_mqtt_caps:check_pub(default, Msg)),
    TooDeep = Msg#message{topic = <<"a/b/c">>, qos = ?QOS_2, flags = #{retain => true}},
    ?assertEqual({error, ?RC_TOPIC_NAME_INVALID}, emqx_mqtt_caps:check_pub(default, TooDeep)),
    emqx_config:put_zone_conf(default, [mqtt, max_topic_levels], 0),
    ?assertEqual({error, ?RC_QOS_NOT_SUPPORTED}, emqx_mqtt_caps:check_pub(default, TooDeep)),
    ?assertEqual(ok, emqx_mqtt_caps:check_pub(default, Msg#message{topic = <<"a/b/c">>})).

t_check_sub(_) ->
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
    ).

t_check_sub_max_qos_rules(_) ->
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_0),
    emqx_config:put_zone_conf(default, [mqtt, subscription_max_qos_rules], [
        mk_topic_qos_rule(equals, "t/1/2/3", ?QOS_2),
        mk_topic_qos_rule(equals, "t/4/5/6", ?QOS_2),
        mk_topic_qos_rule(matches, "dev/+/conf/#", ?QOS_1),
        mk_topic_qos_rule(matches, "root/+", ?QOS_1)
    ]),

    CI = #{zone => default},
    SubOpts = #{
        rh => 0,
        rap => 0,
        nl => 0,
        qos => ?QOS_2
    },
    %% No match, fallback:
    ?assertMatch({ok, ?QOS_0}, emqx_mqtt_caps:check_sub(CI, <<"topic">>, SubOpts)),
    %% Verify equality works as expected:
    ?assertMatch(ok = _QOS_2, emqx_mqtt_caps:check_sub(CI, <<"t/4/5/6">>, SubOpts)),
    ?assertMatch({ok, ?QOS_0}, emqx_mqtt_caps:check_sub(CI, <<"t/1/2/+">>, SubOpts)),
    %% Verify match works as expected:
    ?assertMatch({ok, ?QOS_1}, emqx_mqtt_caps:check_sub(CI, <<"dev/foo/conf/+">>, SubOpts)),
    ?assertMatch({ok, ?QOS_1}, emqx_mqtt_caps:check_sub(CI, <<"root/#">>, SubOpts)),
    ?assertMatch({ok, ?QOS_0}, emqx_mqtt_caps:check_sub(CI, <<"root/+/+">>, SubOpts)).

mk_topic_qos_rule(Pred, Topic, QoS) ->
    #{
        topic => #{Pred => iolist_to_binary(Topic)},
        qos => QoS
    }.
