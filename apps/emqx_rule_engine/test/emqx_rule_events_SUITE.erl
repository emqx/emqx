%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_events_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_mod_hook_fun(_) ->
    Events = emqx_rule_events:event_names(),
    lists:foreach(
        fun(E) ->
            ?assert(is_function(emqx_rule_events:hook_fun(E)))
        end,
        Events
    ),
    ?assertEqual(
        fun emqx_rule_events:on_bridge_message_received/3,
        emqx_rule_events:hook_fun(<<"$bridges/foo">>)
    ),
    ?assertEqual(
        fun emqx_rule_events:on_bridge_message_received/3,
        emqx_rule_events:hook_fun(<<"$sources/foo">>)
    ),
    ?assertError({invalid_event, foo}, emqx_rule_events:hook_fun(foo)).

t_special_events_name_topic_conversion(_) ->
    Bridge = <<"$bridges/foo:bar">>,
    AdHoc = <<"foo/bar">>,
    NonExisting = <<"$events/message_publish">>,
    ?assertEqual(Bridge, emqx_rule_events:event_name(Bridge)),
    ?assertEqual('message.publish', emqx_rule_events:event_name(AdHoc)),
    ?assertEqual('message.publish', emqx_rule_events:event_name(NonExisting)),
    ?assertEqual(NonExisting, emqx_rule_events:event_topic('message.publish')).

%% Checks that `emqx_rule_events:event_topics_enum`, `_:event_names` and `_:event_info`
%% are consistent amongst themselves.
t_event_topics_enum_and_names_consistency(_Config) ->
    EventInfos = emqx_rule_events:event_info(),
    EventNames = emqx_rule_events:event_names(),
    %% Note: these contain only new (namespaced) topics in them.
    EventTopics = lists:map(fun atom_to_binary/1, emqx_rule_events:event_topics_enum()),
    EITopics0 = lists:map(fun(#{event := Topic}) -> Topic end, EventInfos),
    %% Explicitly absent; not handled when matching events for common hooks.
    EITopics = EITopics0 -- [<<"$bridges/mqtt:*">>, <<"$events/message_publish">>],
    EINames = lists:map(fun emqx_rule_events:event_name/1, EITopics),
    MissingFromEventTopics = EITopics -- EventTopics,
    MissingFromEventNames = EINames -- EventNames,
    MissingNamesFromEventInfos = EventNames -- (EINames ++ ['message.publish']),
    LegacyEventTopics = [
        <<"$events/client_connected">>,
        <<"$events/client_disconnected">>,
        <<"$events/client_connack">>,
        <<"$events/client_check_authn_complete">>,
        <<"$events/client_check_authz_complete">>,
        <<"$events/session_subscribed">>,
        <<"$events/session_unsubscribed">>,
        <<"$events/message_delivered">>,
        <<"$events/message_acked">>,
        <<"$events/message_dropped">>,
        <<"$events/delivery_dropped">>,
        <<"$events/message_transformation_failed">>,
        <<"$events/schema_validation_failed">>
    ],
    MissingTopicsFromEventInfos = EventTopics -- (EITopics ++ LegacyEventTopics),
    Missing = #{
        missing_from_event_topics => MissingFromEventTopics,
        missing_from_event_names => MissingFromEventNames,
        missing_from_event_info_names => MissingNamesFromEventInfos,
        missing_from_event_info_topics => MissingTopicsFromEventInfos
    },
    ?assertEqual(#{}, maps:filter(fun(_, M) -> length(M) > 0 end, Missing)),
    ok.

-doc """
Checks the namespace restriction applied by `restrict_rules_to_namespace/2`:

- a global rule keeps system-wide visibility and matches messages from any namespace;
- a namespaced rule only matches traffic from its own namespace.
""".
t_restrict_rules_to_namespace(_) ->
    GlobalMsg = #message{topic = <<"hhag/v1/a/b/geolocation">>, headers = #{}},
    Ns1Msg = #message{
        topic = <<"hhag/v1/a/b/geolocation">>,
        headers = #{client_attrs => #{?CLIENT_ATTR_NAME_TNS => <<"ns1">>}}
    },
    GlobalRule = #{rule => #{namespace => ?global_ns}},
    Ns1Rule = #{rule => #{namespace => <<"ns1">>}},
    Ns2Rule = #{rule => #{namespace => <<"ns2">>}},
    All = [GlobalRule, Ns1Rule, Ns2Rule],
    %% Global (non-namespaced) message: only the global rule matches.
    ?assertEqual(
        [GlobalRule],
        emqx_rule_events:restrict_rules_to_namespace(All, GlobalMsg)
    ),
    %% Message from namespace `ns1`: the global rule still matches (system-wide
    %% visibility) and the `ns1` rule matches; the `ns2` rule does not.
    ?assertEqual(
        [GlobalRule, Ns1Rule],
        emqx_rule_events:restrict_rules_to_namespace(All, Ns1Msg)
    ),
    ok.
