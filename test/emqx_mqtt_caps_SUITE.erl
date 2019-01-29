%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mqtt_caps_SUITE).

-include_lib("eunit/include/eunit.hrl").

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

%% CT
-compile(export_all).
-compile(nowarn_export_all).

all() -> [t_get_set_caps, t_check_pub, t_check_sub].

t_get_set_caps(_) ->
    {ok, _} = emqx_zone:start_link(),
    Caps = #{
        max_packet_size => ?MAX_PACKET_SIZE,
        max_clientid_len => ?MAX_CLIENTID_LEN,
        max_topic_alias => 0,
        max_topic_levels => 0,
        max_qos_allowed => ?QOS_2,
        mqtt_retain_available => true,
        mqtt_shared_subscription => true,
        mqtt_wildcard_subscription => true
    },
    Caps2 = Caps#{max_packet_size => 1048576},
    case emqx_mqtt_caps:get_caps(zone) of
        Caps -> ok;
        Caps2 -> ok
    end,
    PubCaps = #{
        max_qos_allowed => ?QOS_2,
        mqtt_retain_available => true,
        max_topic_alias => 0
    },
    PubCaps = emqx_mqtt_caps:get_caps(zone, publish),
    NewPubCaps = PubCaps#{max_qos_allowed => ?QOS_1},
    emqx_zone:set_env(zone, '$mqtt_pub_caps', NewPubCaps),
    timer:sleep(100),
    NewPubCaps = emqx_mqtt_caps:get_caps(zone, publish),
    SubCaps = #{
        max_topic_levels => 0,
        max_qos_allowed => ?QOS_2,
        mqtt_shared_subscription => true,
        mqtt_wildcard_subscription => true
    },
    SubCaps = emqx_mqtt_caps:get_caps(zone, subscribe),
    emqx_zone:stop().

t_check_pub(_) ->
    {ok, _} = emqx_zone:start_link(),
    PubCaps = #{
        max_qos_allowed => ?QOS_1,
        mqtt_retain_available => false,
        max_topic_alias => 4
    },
    emqx_zone:set_env(zone, '$mqtt_pub_caps', PubCaps),
    timer:sleep(100),
    ct:log("~p", [emqx_mqtt_caps:get_caps(zone, publish)]),
    BadPubProps1 = #{
        qos => ?QOS_2,
        retain => false
    },
    {error, ?RC_QOS_NOT_SUPPORTED} = emqx_mqtt_caps:check_pub(zone, BadPubProps1),
    BadPubProps2 = #{
        qos => ?QOS_1,
        retain => true
    },
    {error, ?RC_RETAIN_NOT_SUPPORTED} = emqx_mqtt_caps:check_pub(zone, BadPubProps2),
    BadPubProps3 = #{
        qos => ?QOS_1,
        retain => false,
        topic_alias => 5
    },
    {error, ?RC_TOPIC_ALIAS_INVALID} = emqx_mqtt_caps:check_pub(zone, BadPubProps3),
    PubProps = #{
        qos => ?QOS_1,
        retain => false
    },
    ok = emqx_mqtt_caps:check_pub(zone, PubProps),
    emqx_zone:stop().

t_check_sub(_) ->
    {ok, _} = emqx_zone:start_link(),

    Opts = #{qos => ?QOS_2, share => true, rc => 0},
    Caps = #{
        max_topic_levels => 0,
        max_qos_allowed => ?QOS_2,
        mqtt_shared_subscription => true,
        mqtt_wildcard_subscription => true
    },

    ok = do_check_sub([{<<"client/stat">>, Opts}], [{<<"client/stat">>, Opts}]),
    ok = do_check_sub(Caps#{max_qos_allowed => ?QOS_1}, [{<<"client/stat">>, Opts}], [{<<"client/stat">>, Opts#{qos => ?QOS_1}}]),
    ok = do_check_sub(Caps#{max_topic_levels => 1},
                        [{<<"client/stat">>, Opts}],
                        [{<<"client/stat">>, Opts#{rc => ?RC_TOPIC_FILTER_INVALID}}]),
    ok = do_check_sub(Caps#{mqtt_shared_subscription => false},
                        [{<<"client/stat">>, Opts}],
                        [{<<"client/stat">>, Opts#{rc => ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}}]),

    ok = do_check_sub(Caps#{mqtt_wildcard_subscription => false},
                        [{<<"vlient/+/dsofi">>, Opts}],
                        [{<<"vlient/+/dsofi">>, Opts#{rc => ?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED}}]),
    emqx_zone:stop().



do_check_sub(TopicFilters, Topics) ->
    {ok, Topics} = emqx_mqtt_caps:check_sub(zone, TopicFilters),
    ok.
do_check_sub(Caps, TopicFilters, Topics) ->
    emqx_zone:set_env(zone, '$mqtt_sub_caps', Caps),
    timer:sleep(100),
    {_, Topics} = emqx_mqtt_caps:check_sub(zone, TopicFilters),
    ok.
