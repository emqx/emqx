%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    subscription_max_qos_rules => [topic_qos_rule()],
    wildcard_subscription => boolean(),
    shared_subscription => boolean(),
    exclusive_subscription => boolean()
}.

%% See "topic_qos_rule" struct in `emqx_schema`:
-type topic_qos_rule() ::
    #{is_match := topic_match_compiled(), qos := emqx_types:qos()}.

%% Reflects `emqx_variform:compiled/0` structure:
-type topic_match_compiled() :: #{form := topic_match_spec(), _ => _}.
-type topic_match_spec() ::
    {topic_equal, emqx_types:words()}
    | {topic_intersects, emqx_types:words()}
    | {topic_subset_of, emqx_types:words()}.

-define(DEFAULT_CAPS_KEYS, [
    max_packet_size,
    max_clientid_len,
    max_topic_alias,
    max_topic_levels,
    max_qos_allowed,
    retain_available,
    wildcard_subscription,
    shared_subscription,
    exclusive_subscription
]).

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
        emqx_config:get_zone_conf(Zone, [mqtt])
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
    emqx_types:topic() | emqx_types:share(),
    emqx_types:subopts()
) ->
    ok
    | {ok, emqx_types:reason_code()}
    | {error, emqx_types:reason_code()}.
check_sub(ClientInfo = #{zone := Zone}, Topic, SubOpts) ->
    Caps = emqx_config:get_zone_conf(Zone, [mqtt]),
    Flags = #{
        topic_levels => emqx_topic:levels(Topic),
        is_wildcard => emqx_topic:wildcard(Topic),
        is_shared => erlang:is_record(Topic, share),
        is_exclusive => maps:get(is_exclusive, SubOpts, false),
        qos => maps:get(qos, SubOpts, 0)
    },
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
do_check_sub(#{is_exclusive := true}, #{exclusive_subscription := true}, ClientInfo, Topic) when
    is_binary(Topic)
->
    case emqx_exclusive_subscription:check_subscribe(ClientInfo, Topic) of
        deny ->
            {error, ?RC_QUOTA_EXCEEDED};
        _ ->
            ok
    end;
do_check_sub(
    #{qos := QoS},
    #{subscription_max_qos_rules := Rules = [_ | _], max_qos_allowed := FallbackMaxQoS},
    _,
    TopicIn
) ->
    Topic = emqx_topic:words(emqx_topic:get_shared_real_topic(TopicIn)),
    MaxQoS = emqx_maybe:define(eval_max_qos_allowed(Rules, Topic), FallbackMaxQoS),
    case QoS > MaxQoS of
        %% Accepted, but with a lower QoS
        true -> {ok, MaxQoS};
        false -> ok
    end;
do_check_sub(#{qos := QoS}, #{max_qos_allowed := MaxQoS}, _, _) when
    QoS > MaxQoS
->
    %% Accepted, but with a lower QoS
    %% see: ?RC_GRANTED_QOS_0, ?RC_GRANTED_QOS_1, ?RC_GRANTED_QOS_2
    {ok, MaxQoS};
do_check_sub(_Flags, _Caps, _, _) ->
    ok.

%% @doc Evaluates Topic->QoS rules on a "real" topic, stripped from `$share/...`
%% components (if any).
eval_max_qos_allowed([Rule | Rest], Topic) ->
    case eval_topic_qos_rule(Rule, Topic) of
        QoS when is_integer(QoS) ->
            QoS;
        false ->
            eval_max_qos_allowed(Rest, Topic)
    end;
eval_max_qos_allowed([], _) ->
    undefined.

eval_topic_qos_rule(#{is_match := #{form := Match}, qos := QoS}, Topic) ->
    eval_topic_match(Match, Topic) andalso QoS.

eval_topic_match({topic_equal, To}, Topic) ->
    emqx_topic:is_equal(Topic, To);
eval_topic_match({topic_intersects, With}, Topic) ->
    emqx_topic:intersection(Topic, With) =/= false;
eval_topic_match({topic_subset_of, Of}, Topic) ->
    emqx_topic:is_subset(Topic, Of).

get_caps(Zone) ->
    get_caps(?DEFAULT_CAPS_KEYS, Zone).
get_caps(Keys, Zone) ->
    maps:with(
        Keys,
        emqx_config:get_zone_conf(Zone, [mqtt])
    ).
