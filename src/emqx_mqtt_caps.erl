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

%% @doc MQTTv5 capabilities
-module(emqx_mqtt_caps).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([ check_pub/2
        , check_sub/2
        , get_caps/1
        , get_caps/2
        ]).

-type(caps() :: #{max_packet_size  => integer(),
                  max_clientid_len => integer(),
                  max_topic_alias  => integer(),
                  max_topic_levels => integer(),
                  max_qos_allowed  => emqx_mqtt_types:qos(),
                  mqtt_retain_available      => boolean(),
                  mqtt_shared_subscription   => boolean(),
                  mqtt_wildcard_subscription => boolean()}).

-export_type([caps/0]).

-define(UNLIMITED, 0).
-define(DEFAULT_CAPS, [{max_packet_size,  ?MAX_PACKET_SIZE},
                       {max_clientid_len, ?MAX_CLIENTID_LEN},
                       {max_topic_alias,  ?UNLIMITED},
                       {max_topic_levels, ?UNLIMITED},
                       {max_qos_allowed,  ?QOS_2},
                       {mqtt_retain_available,      true},
                       {mqtt_shared_subscription,   true},
                       {mqtt_wildcard_subscription, true}]).

-define(PUBCAP_KEYS, [max_qos_allowed,
                      mqtt_retain_available,
                      max_topic_alias
                     ]).
-define(SUBCAP_KEYS, [max_qos_allowed,
                      max_topic_levels,
                      mqtt_shared_subscription,
                      mqtt_wildcard_subscription]).

-spec(check_pub(emqx_types:zone(), map()) -> ok | {error, emqx_mqtt_types:reason_code()}).
check_pub(Zone, Props) when is_map(Props) ->
    do_check_pub(Props, maps:to_list(get_caps(Zone, publish))).

do_check_pub(_Props, []) ->
    ok;
do_check_pub(Props = #{qos := QoS}, [{max_qos_allowed, MaxQoS}|Caps]) ->
    case QoS > MaxQoS of
        true  -> {error, ?RC_QOS_NOT_SUPPORTED};
        false -> do_check_pub(Props, Caps)
    end;
do_check_pub(Props = #{ topic_alias := TopicAlias}, [{max_topic_alias, MaxTopicAlias}| Caps]) ->
    case TopicAlias =< MaxTopicAlias andalso TopicAlias > 0 of
        false -> {error, ?RC_TOPIC_ALIAS_INVALID};
        true -> do_check_pub(Props, Caps)
    end;
do_check_pub(#{retain := true}, [{mqtt_retain_available, false}|_Caps]) ->
    {error, ?RC_RETAIN_NOT_SUPPORTED};
do_check_pub(Props, [{max_topic_alias, _} | Caps]) ->
    do_check_pub(Props, Caps);
do_check_pub(Props, [{mqtt_retain_available, _}|Caps]) ->
    do_check_pub(Props, Caps).

-spec(check_sub(emqx_types:zone(), emqx_mqtt_types:topic_filters())
      -> {ok | error, emqx_mqtt_types:topic_filters()}).
check_sub(Zone, TopicFilters) ->
    Caps = maps:to_list(get_caps(Zone, subscribe)),
    lists:foldr(fun({Topic, Opts}, {Ok, Result}) ->
                    case check_sub(Topic, Opts, Caps) of
                        {ok, Opts1} ->
                            {Ok, [{Topic, Opts1}|Result]};
                        {error, Opts1} ->
                            {error, [{Topic, Opts1}|Result]}
                    end
                end, {ok, []}, TopicFilters).

check_sub(_Topic, Opts, []) ->
    {ok, Opts};
check_sub(Topic, Opts = #{qos := QoS}, [{max_qos_allowed, MaxQoS}|Caps]) ->
    check_sub(Topic, Opts#{qos := min(QoS, MaxQoS)}, Caps);
check_sub(Topic, Opts, [{mqtt_shared_subscription, true}|Caps]) ->
    check_sub(Topic, Opts, Caps);
check_sub(Topic, Opts, [{mqtt_shared_subscription, false}|Caps]) ->
    case maps:is_key(share, Opts) of
        true  ->
            {error, Opts#{rc := ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}};
        false -> check_sub(Topic, Opts, Caps)
    end;
check_sub(Topic, Opts, [{mqtt_wildcard_subscription, true}|Caps]) ->
    check_sub(Topic, Opts, Caps);
check_sub(Topic, Opts, [{mqtt_wildcard_subscription, false}|Caps]) ->
    case emqx_topic:wildcard(Topic) of
        true  ->
            {error, Opts#{rc := ?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED}};
        false -> check_sub(Topic, Opts, Caps)
    end;
check_sub(Topic, Opts, [{max_topic_levels, ?UNLIMITED}|Caps]) ->
    check_sub(Topic, Opts, Caps);
check_sub(Topic, Opts, [{max_topic_levels, Limit}|Caps]) ->
    case emqx_topic:levels(Topic) of
        Levels when Levels > Limit ->
            {error, Opts#{rc := ?RC_TOPIC_FILTER_INVALID}};
        _ -> check_sub(Topic, Opts, Caps)
    end.

get_caps(Zone, publish) ->
    with_env(Zone, '$mqtt_pub_caps',
             fun() ->
                 filter_caps(?PUBCAP_KEYS, get_caps(Zone))
             end);

get_caps(Zone, subscribe) ->
    with_env(Zone, '$mqtt_sub_caps',
             fun() ->
                 filter_caps(?SUBCAP_KEYS, get_caps(Zone))
             end).

get_caps(Zone) ->
    with_env(Zone, '$mqtt_caps',
             fun() ->
                 maps:from_list([{Cap, emqx_zone:get_env(Zone, Cap, Def)}
                                 || {Cap, Def} <- ?DEFAULT_CAPS])
             end).

filter_caps(Keys, Caps) ->
    maps:filter(fun(Key, _Val) -> lists:member(Key, Keys) end, Caps).

with_env(Zone, Key, InitFun) ->
    case emqx_zone:get_env(Zone, Key) of
        undefined -> Caps = InitFun(),
                     ok = emqx_zone:set_env(Zone, Key, Caps),
                     Caps;
        ZoneCaps  -> ZoneCaps
    end.
