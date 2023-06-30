%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_sqltester).

-include("rule_engine.hrl").

-export([ test/1
        ]).

-spec(test(#{}) -> {ok, map() | list()} | {error, term()}).
test(#{<<"rawsql">> := Sql, <<"ctx">> := Context}) ->
    {ok, Select} = emqx_rule_sqlparser:parse_select(Sql),
    InTopic = maps:get(<<"topic">>, Context, <<>>),
    EventTopics = emqx_rule_sqlparser:select_from(Select),
    case lists:all(fun is_publish_topic/1, EventTopics) of
        true ->
            %% test if the topic matches the topic filters in the rule
            case emqx_rule_utils:can_topic_match_oneof(InTopic, EventTopics) of
                true -> test_rule(Sql, Select, Context, EventTopics);
                false -> {error, nomatch}
            end;
        false ->
            %% the rule is for both publish and events, test it directly
            test_rule(Sql, Select, Context, EventTopics)
    end.

test_rule(Sql, Select, Context, EventTopics) ->
    RuleId = iolist_to_binary(["test_rule", emqx_rule_id:gen()]),
    ActInstId = iolist_to_binary(["test_action", emqx_rule_id:gen()]),
    ok = emqx_rule_metrics:create_rule_metrics(RuleId),
    ok = emqx_rule_metrics:create_metrics(ActInstId),
    Rule = #rule{
        id = RuleId,
        rawsql = Sql,
        for = EventTopics,
        is_foreach = emqx_rule_sqlparser:select_is_foreach(Select),
        fields = emqx_rule_sqlparser:select_fields(Select),
        doeach = emqx_rule_sqlparser:select_doeach(Select),
        incase = emqx_rule_sqlparser:select_incase(Select),
        conditions = emqx_rule_sqlparser:select_where(Select),
        actions = [#action_instance{
                    id = ActInstId,
                    name = test_rule_sql}]
    },
    FullContext = fill_default_values(hd(EventTopics), emqx_rule_maps:atom_key_map(Context)),
    try
        ok = emqx_rule_registry:add_action_instance_params(
                #action_instance_params{id = ActInstId,
                                        params = #{},
                                        apply = sql_test_action()}),
        R = emqx_rule_runtime:apply_rule(Rule, FullContext),
        emqx_rule_metrics:clear_rule_metrics(RuleId),
        emqx_rule_metrics:clear_metrics(ActInstId),
        R
    of
        {ok, Data} -> {ok, flatten(Data)};
        {error, Reason} -> {error, Reason}
    after
        ok = emqx_rule_registry:remove_action_instance_params(ActInstId)
    end.

is_publish_topic(<<"$events/", _/binary>>) -> false;
is_publish_topic(_Topic) -> true.

flatten([]) -> [];
flatten([D1]) -> D1;
flatten([D1 | L]) when is_list(D1) ->
    D1 ++ flatten(L).

sql_test_action() ->
    fun(Data, _Envs) ->
        ?LOG(info, "Testing Rule SQL OK"), Data
    end.

fill_default_values(Event, Context) ->
    maps:merge(envs_examp(Event), Context).

envs_examp(EVENT_TOPIC) ->
    EventName = emqx_rule_events:event_name(EVENT_TOPIC),
    emqx_rule_maps:atom_key_map(
        maps:from_list(
            emqx_rule_events:columns_with_exam(EventName))).
