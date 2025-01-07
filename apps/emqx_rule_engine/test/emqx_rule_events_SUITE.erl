%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_events_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

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

t_printable_maps(_) ->
    Headers = #{
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 9980},
        sockname => {{127, 0, 0, 1}, 1883},
        redispatch_to => ?REDISPATCH_TO(<<"group">>, <<"sub/topic/+">>),
        shared_dispatch_ack => {self(), ref}
    },
    Converted = emqx_rule_events:printable_maps(Headers),
    ?assertMatch(
        #{
            peerhost := <<"127.0.0.1">>,
            peername := <<"127.0.0.1:9980">>,
            sockname := <<"127.0.0.1:1883">>
        },
        Converted
    ),
    ?assertNot(maps:is_key(redispatch_to, Converted)),
    ?assertNot(maps:is_key(shared_dispatch_ack, Converted)),
    ok.

t_event_name_topic_conversion(_) ->
    Events = emqx_rule_events:event_names() -- ['message.publish'],
    Topics = [atom_to_binary(A) || A <- emqx_rule_events:event_topics_enum()],
    Zip = lists:zip(Events, Topics),
    lists:foreach(
        fun({Event, Topic}) ->
            ?assertEqual(Event, emqx_rule_events:event_name(Topic)),
            ?assertEqual(Topic, emqx_rule_events:event_topic(Event))
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
