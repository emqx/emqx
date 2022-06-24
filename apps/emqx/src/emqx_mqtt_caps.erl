%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc MQTTv5 Capabilities
-module(emqx_mqtt_caps).

-include("emqx_mqtt.hrl").
-include("types.hrl").

-export([
    check_pub/2,
    check_sub/3
]).

-export([get_caps/1]).

-export_type([caps/0]).

-type caps() :: #{
    max_packet_size => integer(),
    max_clientid_len => integer(),
    max_topic_alias => integer(),
    max_topic_levels => integer(),
    max_qos_allowed => emqx_types:qos(),
    retain_available => boolean(),
    wildcard_subscription => boolean(),
    subscription_identifiers => boolean(),
    shared_subscription => boolean(),
    exclusive_subscription => boolean()
}.

-define(MAX_TOPIC_LEVELS, 65535).

-define(PUBCAP_KEYS, [
    max_topic_levels,
    max_qos_allowed,
    retain_available
]).

-define(SUBCAP_KEYS, [
    max_topic_levels,
    max_qos_allowed,
    wildcard_subscription,
    shared_subscription,
    exclusive_subscription
]).

-define(DEFAULT_CAPS, #{
    max_packet_size => ?MAX_PACKET_SIZE,
    max_clientid_len => ?MAX_CLIENTID_LEN,
    max_topic_alias => ?MAX_TOPIC_AlIAS,
    max_topic_levels => ?MAX_TOPIC_LEVELS,
    max_qos_allowed => ?QOS_2,
    retain_available => true,
    wildcard_subscription => true,
    subscription_identifiers => true,
    shared_subscription => true,
    exclusive_subscription => false
}).

-spec check_pub(
    emqx_types:zone(),
    #{
        qos := emqx_types:qos(),
        retain := boolean(),
        topic := emqx_types:topic()
    }
) ->
    ok_or_error(emqx_types:reason_code()).
check_pub(Zone, Flags) when is_map(Flags) ->
    do_check_pub(
        case maps:take(topic, Flags) of
            {Topic, Flags1} ->
                Flags1#{topic_levels => emqx_topic:levels(Topic)};
            error ->
                Flags
        end,
        maps:with(?PUBCAP_KEYS, get_caps(Zone))
    ).

do_check_pub(#{topic_levels := Levels}, #{max_topic_levels := Limit}) when
    Limit > 0, Levels > Limit
->
    {error, ?RC_TOPIC_NAME_INVALID};
do_check_pub(#{qos := QoS}, #{max_qos_allowed := MaxQoS}) when
    QoS > MaxQoS
->
    {error, ?RC_QOS_NOT_SUPPORTED};
do_check_pub(#{retain := true}, #{retain_available := false}) ->
    {error, ?RC_RETAIN_NOT_SUPPORTED};
do_check_pub(_Flags, _Caps) ->
    ok.

-spec check_sub(
    emqx_types:clientinfo(),
    emqx_types:topic(),
    emqx_types:subopts()
) ->
    ok_or_error(emqx_types:reason_code()).
check_sub(ClientInfo = #{zone := Zone}, Topic, SubOpts) ->
    Caps = maps:with(?SUBCAP_KEYS, get_caps(Zone)),
    Flags = lists:foldl(
        fun
            (max_topic_levels, Map) ->
                Map#{topic_levels => emqx_topic:levels(Topic)};
            (wildcard_subscription, Map) ->
                Map#{is_wildcard => emqx_topic:wildcard(Topic)};
            (shared_subscription, Map) ->
                Map#{is_shared => maps:is_key(share, SubOpts)};
            (exclusive_subscription, Map) ->
                Map#{is_exclusive => maps:get(is_exclusive, SubOpts, false)};
            %% Ignore
            (_Key, Map) ->
                Map
        end,
        #{},
        maps:keys(Caps)
    ),
    do_check_sub(Flags, Caps, ClientInfo, Topic).

do_check_sub(#{topic_levels := Levels}, #{max_topic_levels := Limit}, _, _) when
    Limit > 0, Levels > Limit
->
    {error, ?RC_TOPIC_FILTER_INVALID};
do_check_sub(#{is_wildcard := true}, #{wildcard_subscription := false}, _, _) ->
    {error, ?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED};
do_check_sub(#{is_shared := true}, #{shared_subscription := false}, _, _) ->
    {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED};
do_check_sub(#{is_exclusive := true}, #{exclusive_subscription := false}, _, _) ->
    {error, ?RC_TOPIC_FILTER_INVALID};
do_check_sub(#{is_exclusive := true}, #{exclusive_subscription := true}, ClientInfo, Topic) ->
    case emqx_exclusive_subscription:check_subscribe(ClientInfo, Topic) of
        deny ->
            {error, ?RC_QUOTA_EXCEEDED};
        _ ->
            ok
    end;
do_check_sub(_Flags, _Caps, _, _) ->
    ok.

get_caps(Zone) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            Acc#{K => emqx_config:get_zone_conf(Zone, [mqtt, K], V)}
        end,
        #{},
        maps:to_list(?DEFAULT_CAPS)
    ).
