%%------------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%------------------------------------------------------------------------

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
    Topics0 = maps:get(from, Rule, []),
    Topics = lists:flatmap(
        fun emqx_rule_events:expand_legacy_event_topics/1,
        Topics0
    ),
    case match_any(InTopic, Topics) of
        {ok, Filter} ->
            do_apply_matched_rule(
                riched_rule(Rule, InTopic, Filter),
                Context,
                StopAfterRender,
                Topics
            );
        nomatch ->
            {error, nomatch}
    end.

do_apply_matched_rule(RichedRule, Context, StopAfterRender, EventTopics) ->
    PrevLoggerProcessMetadata = logger:get_process_metadata(),
    try
        update_process_trace_metadata(StopAfterRender),
        FullContext0 = fill_default_values(hd(EventTopics), Context),
        FullContext = remove_internal_fields(FullContext0),
        emqx_rule_runtime:apply_rule(RichedRule, FullContext, apply_rule_environment())
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

%% Some fields are injected internally to help resolve the event type, such as
%% `event_type', and should be removed before applying the rule to avoid confusing the
%% user into thinking they'll be present on real events.
remove_internal_fields(Context) ->
    maps:without([event_type], Context).

-spec test(#{sql := binary(), context := map()}) -> {ok, map() | list()} | {error, term()}.
test(#{sql := Sql, context := Context}) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Topic = get_in_topic(Context),
            EventTopics0 = emqx_rule_sqlparser:select_from(Select),
            EventTopics = lists:flatmap(
                fun emqx_rule_events:expand_legacy_event_topics/1,
                EventTopics0
            ),
            case match_any(Topic, EventTopics) of
                {ok, Filter} ->
                    test_rule(riched_rule(rule(Sql, Select, EventTopics), Topic, Filter), Context);
                nomatch ->
                    {error, nomatch}
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

test_rule(#{rule := #{id := RuleId, from := EventTopics}} = RichedRule, Context) ->
    FullContext0 = fill_default_values(hd(EventTopics), Context),
    FullContext = remove_internal_fields(FullContext0),
    set_is_test_runtime_env(),
    try emqx_rule_runtime:apply_rule(RichedRule, FullContext, #{}) of
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
    Env = emqx_rule_events:columns_with_exam(EventName),
    %% Only the top level keys need to be converted to atoms.
    lists:foldl(
        fun({K, V}, Acc) ->
            Acc#{to_known_atom(K) => V}
        end,
        #{},
        Env
    ).

to_known_atom(B) when is_binary(B) -> binary_to_existing_atom(B, utf8);
to_known_atom(S) when is_list(S) -> list_to_existing_atom(S);
to_known_atom(A) when is_atom(A) -> A.

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
get_in_topic(Context) ->
    case Context of
        #{event_topic := EventTopic} ->
            EventTopic;
        #{} ->
            case Context of
                #{event := Event} ->
                    maybe_infer_in_topic(Context, Event);
                #{} ->
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

rule(Sql, Select, EventTopics) ->
    RuleId = iolist_to_binary(["sql_tester:", emqx_utils:gen_id(16)]),
    ok = emqx_rule_engine:maybe_add_metrics_for_rule(RuleId),
    _Rule = #{
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
    }.

riched_rule(Rule, InTopic, Matched) ->
    #{rule => Rule, trigger => InTopic, matched => Matched}.

-spec match_any(binary(), list(binary())) ->
    {ok, binary()}
    | nomatch.
match_any(_Topic, []) ->
    nomatch;
match_any(Topic, [Filter | Rest]) ->
    case emqx_topic:match(Topic, Filter) of
        true -> {ok, Filter};
        false -> match_any(Topic, Rest)
    end.
