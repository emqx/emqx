%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-include("rule_engine.hrl").

-export([
    test/1,
    get_selected_data/3,
    %% Some SQL functions return different results in the test environment
    is_test_runtime_env/0,
    apply_rule/2
]).

apply_rule(
    RuleId,
    Parameters
) ->
    case emqx_rule_engine:get_rule(RuleId) of
        {ok, Rule} ->
            do_apply_rule(Rule, Parameters);
        not_found ->
            {error, rule_not_found}
    end.

do_apply_rule(
    Rule,
    #{
        context := Context,
        stop_action_after_template_rendering := StopAfterRender
    }
) ->
    InTopic = get_in_topic(Context),
    EventTopics = maps:get(from, Rule, []),
    case lists:all(fun is_publish_topic/1, EventTopics) of
        true ->
            %% test if the topic matches the topic filters in the rule
            case emqx_topic:match_any(InTopic, EventTopics) of
                true ->
                    do_apply_matched_rule(
                        Rule,
                        Context,
                        StopAfterRender,
                        EventTopics
                    );
                false ->
                    {error, nomatch}
            end;
        false ->
            case lists:member(InTopic, EventTopics) of
                true ->
                    %% the rule is for both publish and events, test it directly
                    do_apply_matched_rule(Rule, Context, StopAfterRender, EventTopics);
                false ->
                    {error, nomatch}
            end
    end.

do_apply_matched_rule(Rule, Context, StopAfterRender, EventTopics) ->
    PrevLoggerProcessMetadata = logger:get_process_metadata(),
    try
        update_process_trace_metadata(StopAfterRender),
        FullContext = fill_default_values(
            hd(EventTopics),
            emqx_rule_maps:atom_key_map(Context)
        ),
        ApplyRuleRes = emqx_rule_runtime:apply_rule(
            Rule,
            FullContext,
            apply_rule_environment()
        ),
        ApplyRuleRes
    after
        reset_logger_process_metadata(PrevLoggerProcessMetadata)
    end.

update_process_trace_metadata(true = _StopAfterRender) ->
    logger:update_process_metadata(#{
        stop_action_after_render => true
    });
update_process_trace_metadata(false = _StopAfterRender) ->
    ok.

reset_logger_process_metadata(undefined = _PrevProcessMetadata) ->
    logger:unset_process_metadata();
reset_logger_process_metadata(PrevProcessMetadata) ->
    logger:set_process_metadata(PrevProcessMetadata).

%% At the time of writing the environment passed to the apply rule function is
%% not used at all for normal actions. When it is used for custom functions it
%% is first merged with the context so there does not seem to be any need to
%% set this to anything else then the empty map.
apply_rule_environment() -> #{}.

-spec test(#{sql := binary(), context := map()}) -> {ok, map() | list()} | {error, term()}.
test(#{sql := Sql, context := Context}) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            InTopic = get_in_topic(Context),
            EventTopics = emqx_rule_sqlparser:select_from(Select),
            case lists:all(fun is_publish_topic/1, EventTopics) of
                true ->
                    %% test if the topic matches the topic filters in the rule
                    case emqx_topic:match_any(InTopic, EventTopics) of
                        true ->
                            test_rule(Sql, Select, Context, EventTopics);
                        false ->
                            {error, nomatch}
                    end;
                false ->
                    case emqx_topic:match_any(InTopic, EventTopics) of
                        true ->
                            %% the rule is for both publish and events, test it directly
                            test_rule(Sql, Select, Context, EventTopics);
                        false ->
                            {error, nomatch}
                    end
            end;
        {error, Reason} ->
            ?SLOG(
                debug,
                #{
                    msg => "rulesql_parse_error",
                    sql => Sql,
                    reason => Reason
                },
                #{tag => ?TAG}
            ),
            {error, Reason}
    end.

test_rule(Sql, Select, Context, EventTopics) ->
    RuleId = iolist_to_binary(["sql_tester:", emqx_utils:gen_id(16)]),
    ok = emqx_rule_engine:maybe_add_metrics_for_rule(RuleId),
    Rule = #{
        id => RuleId,
        sql => Sql,
        from => EventTopics,
        actions => [#{mod => ?MODULE, func => get_selected_data, args => #{}}],
        enable => true,
        is_foreach => emqx_rule_sqlparser:select_is_foreach(Select),
        fields => emqx_rule_sqlparser:select_fields(Select),
        doeach => emqx_rule_sqlparser:select_doeach(Select),
        incase => emqx_rule_sqlparser:select_incase(Select),
        conditions => emqx_rule_sqlparser:select_where(Select),
        created_at => erlang:system_time(millisecond)
    },
    FullContext = fill_default_values(hd(EventTopics), emqx_rule_maps:atom_key_map(Context)),
    set_is_test_runtime_env(),
    try emqx_rule_runtime:apply_rule(Rule, FullContext, #{}) of
        {ok, Data} ->
            {ok, flatten(Data)};
        {error, Reason} ->
            {error, Reason}
    after
        unset_is_test_runtime_env(),
        ok = emqx_rule_engine:clear_metrics_for_rule(RuleId)
    end.

get_selected_data(Selected, Envs, Args) ->
    ?TRACE("RULE", "testing_rule_sql_ok", #{selected => Selected, envs => Envs, args => Args}),
    {ok, Selected}.

is_publish_topic(<<"$events/", _/binary>>) -> false;
is_publish_topic(<<"$bridges/", _/binary>>) -> false;
is_publish_topic(_Topic) -> true.

flatten([]) ->
    [];
flatten([{ok, D}]) ->
    D;
flatten([D | L]) when is_list(D) ->
    [D0 || {ok, D0} <- D] ++ flatten(L).

fill_default_values(Event, Context) ->
    maps:merge(envs_examp(Event, Context), Context).

envs_examp(EventTopic, Context) ->
    EventName = maps:get(event, Context, emqx_rule_events:event_name(EventTopic)),
    Env = maps:from_list(emqx_rule_events:columns_with_exam(EventName)),
    emqx_rule_maps:atom_key_map(Env).

is_test_runtime_env_atom() ->
    'emqx_rule_sqltester:is_test_runtime_env'.

set_is_test_runtime_env() ->
    erlang:put(is_test_runtime_env_atom(), true),
    ok.

unset_is_test_runtime_env() ->
    erlang:erase(is_test_runtime_env_atom()),
    ok.

is_test_runtime_env() ->
    case erlang:get(is_test_runtime_env_atom()) of
        true -> true;
        _ -> false
    end.

%% Most events have the original `topic' input, but their own topic (i.e.: `$events/...')
%% is different from `topic'.
get_in_topic(#{event_type := schema_validation_failed}) ->
    <<"$events/schema_validation_failed">>;
get_in_topic(#{event_type := message_transformation_failed}) ->
    <<"$events/message_transformation_failed">>;
get_in_topic(Context) ->
    case maps:find(event_topic, Context) of
        {ok, EventTopic} ->
            EventTopic;
        error ->
            case maps:find(event, Context) of
                {ok, Event} ->
                    maybe_infer_in_topic(Context, Event);
                error ->
                    maps:get(topic, Context, <<>>)
            end
    end.

maybe_infer_in_topic(Context, 'message.publish') ->
    %% This is special because the common use in the frontend is to select this event, but
    %% test the input `topic' field against MQTT topic filters in the `FROM' clause rather
    %% than the corresponding `$events/message_publish'.
    maps:get(topic, Context, <<>>);
maybe_infer_in_topic(_Context, Event) ->
    emqx_rule_events:event_topic(Event).
