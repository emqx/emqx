%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_events_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

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
        fun emqx_rule_events:on_bridge_message_received/2,
        emqx_rule_events:hook_fun(<<"$bridges/foo">>)
    ),
    ?assertError({invalid_event, foo}, emqx_rule_events:hook_fun(foo)).

t_event_name_topic_conversion(_) ->
    Events = emqx_rule_events:event_names() -- ['message.publish'],
    Topics = [atom_to_binary(A) || A <- emqx_rule_events:event_topics_enum()],
    Zip = lists:zip(Events, Topics),
    lists:foreach(
        fun({Event, Topic}) ->
            ?assertEqual(Event, emqx_rule_events:event_name(Topic), #{topic => Topic}),
            ?assertEqual(Topic, emqx_rule_events:event_topic(Event), #{event => Event})
        end,
        Zip
    ).

t_special_events_name_topic_conversion(_) ->
    Bridge = <<"$bridges/foo:bar">>,
    AdHoc = <<"foo/bar">>,
    NonExisting = <<"$events/message_publish">>,
    ?assertEqual(Bridge, emqx_rule_events:event_name(Bridge)),
    ?assertEqual('message.publish', emqx_rule_events:event_name(AdHoc)),
    ?assertEqual('message.publish', emqx_rule_events:event_name(NonExisting)),
    ?assertEqual(NonExisting, emqx_rule_events:event_topic('message.publish')).
