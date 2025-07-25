%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine).

-behaviour(gen_server).
-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include("rule_engine.hrl").
-include("emqx_rule_engine_internal.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([start_link/0]).

-export([
    post_config_update/6
]).

%% Rule Management

-export([load_rules/0]).

-export([
    create_rule/1,
    update_rule/1,
    delete_rule/2,
    get_rule/2
]).

-export([
    rule_resource_id/2,
    get_rules/1,
    get_rules_from_all_namespaces/0,
    get_rules_for_topic/1,
    get_enriched_rules_with_matching_event/2,
    get_enriched_rules_with_matching_event_all_namespaces/1,
    get_rule_ids_by_action/1,
    get_rule_ids_by_bridge_action/2,
    get_rule_ids_by_bridge_source/2,
    ensure_action_removed/3,
    get_rules_ordered_by_ts/1
]).

-export([
    load_hooks_for_rule/1,
    unload_hooks_for_rule/1,
    maybe_add_metrics_for_rule/1,
    clear_metrics_for_rule/1,
    reset_metrics_for_rule/1,
    reset_metrics_for_rule/2
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%% Internal exports for `emqx_rule_engine` application
-export([rule_resource_id/1]).

-export([now_ms/0]).

%% gen_server Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Data backup
-export([
    import_config/1
]).

%% For setting and getting extra rule engine SQL functions module
-export([
    get_external_function/1,
    register_external_functions/1,
    register_external_functions/2,
    unregister_external_functions/1,
    unregister_external_functions/2
]).

%% Only used by tests and debugging
-export([get_rules_with_same_event/2]).

-export_type([rule/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(RULE_ENGINE, ?MODULE).

-define(T_CALL, infinity).

-define(namespace, namespace).

%% NOTE: This order cannot be changed! This is to make the metric working during relup.
%% Append elements to this list to add new metrics.
-define(METRICS, [
    'matched',
    'passed',
    'failed',
    'failed.exception',
    'failed.no_result',
    'actions.total',
    'actions.success',
    'actions.failed',
    'actions.failed.out_of_service',
    'actions.failed.unknown',
    'actions.discarded'
]).

-define(RATE_METRICS, ['matched']).

-define(DEFAULT_EXTERNAL_FUNCTION_PREFIX, 'rsf_').
-define(EXTERNAL_FUNCTION_PT_KEY(NAME), {?MODULE, external_function, NAME}).

%% Calls/casts/infos
-record(insert_rule, {rule}).
-record(update_rule, {new_rule, old_rule}).
-record(delete_rule, {rule}).

-type action_name() :: binary() | #{function := binary()}.
-type bridge_action_id() :: binary().
-type bridge_source_id() :: binary().
-type namespace() :: binary().
-type maybe_namespace() :: ?global_ns | namespace().

-type rule() ::
    #{
        id := rule_id(),
        namespace := maybe_namespace(),
        name := binary(),
        sql := binary(),
        actions := [action()],
        enable := boolean(),
        description => binary(),
        %% epoch in millisecond precision
        created_at := integer(),
        %% epoch in millisecond precision
        updated_at := integer(),
        from := list(topic()),
        is_foreach := boolean(),
        fields := list(),
        doeach := term(),
        incase := term(),
        conditions := tuple()
    }.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?RULE_ENGINE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

post_config_update(?RULE_PATH(RuleId), '$remove', undefined, _OldRule, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    delete_rule(Namespace, bin(RuleId));
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, undefined, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    create_rule(NewRule#{id => bin(RuleId), ?namespace => Namespace});
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, _OldRule, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    update_rule(NewRule#{id => bin(RuleId), ?namespace => Namespace});
post_config_update(
    [rule_engine], _Req, #{rules := NewRules}, #{rules := OldRules}, _AppEnvs, ExtraContext
) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    #{added := Added, removed := Removed, changed := Updated} =
        emqx_utils_maps:diff_maps(NewRules, OldRules),
    try
        maps:foreach(
            fun(Id, {_Old, New}) ->
                {ok, _} = update_rule(New#{id => bin(Id), ?namespace => Namespace})
            end,
            Updated
        ),
        maps:foreach(
            fun(Id, _Rule) ->
                ok = delete_rule(Namespace, bin(Id))
            end,
            Removed
        ),
        maps:foreach(
            fun(Id, Rule) ->
                {ok, _} = create_rule(Rule#{id => bin(Id), ?namespace => Namespace})
            end,
            Added
        ),
        ok
    catch
        throw:#{kind := _} = Error ->
            {error, Error}
    end.

%%----------------------------------------------------------------------------------------
%% APIs for rules
%%----------------------------------------------------------------------------------------

-spec load_rules() -> ok.
load_rules() ->
    maps:foreach(
        fun(Namespace, RootCfg) ->
            Rules = maps:get(rules, RootCfg, #{}),
            maps:foreach(
                fun
                    (Id, #{metadata := #{created_at := CreatedAt}} = Rule) ->
                        create_rule(Rule#{id => bin(Id), ?namespace => Namespace}, CreatedAt);
                    (Id, Rule) ->
                        create_rule(Rule#{id => bin(Id), ?namespace => Namespace})
                end,
                Rules
            )
        end,
        get_root_config_from_all_namespaces()
    ).

-spec create_rule(map()) -> {ok, rule()} | {error, term()}.
create_rule(Params) ->
    create_rule(Params, maps:get(created_at, Params, now_ms())).

create_rule(Params = #{id := RuleId, ?namespace := Namespace}, CreatedAt) when is_binary(RuleId) ->
    case get_rule(Namespace, RuleId) of
        not_found -> with_parsed_rule(Params, CreatedAt, fun insert_rule/1);
        {ok, _} -> {error, already_exists}
    end.

-spec update_rule(map()) -> {ok, rule()} | {error, term()}.
update_rule(Params = #{id := RuleId, ?namespace := Namespace}) when is_binary(RuleId) ->
    case get_rule(Namespace, RuleId) of
        not_found ->
            {error, not_found};
        {ok, OldRule = #{created_at := CreatedAt}} ->
            with_parsed_rule(Params, CreatedAt, fun(NewRule) -> update_rule(NewRule, OldRule) end)
    end.

-spec delete_rule(maybe_namespace(), RuleId :: rule_id()) -> ok.
delete_rule(Namespace, RuleId) when is_binary(RuleId) ->
    case get_rule(Namespace, RuleId) of
        not_found ->
            ok;
        {ok, Rule} ->
            gen_server:call(?RULE_ENGINE, #delete_rule{rule = Rule}, ?T_CALL)
    end.

-spec insert_rule(Rule :: rule()) -> ok.
insert_rule(Rule) ->
    gen_server:call(?RULE_ENGINE, #insert_rule{rule = Rule}, ?T_CALL).

-spec update_rule(NewRule :: rule(), OldRule :: rule()) -> ok.
update_rule(#{id := Id, ?namespace := NS} = NewRule, #{id := Id, ?namespace := NS} = OldRule) ->
    gen_server:call(?RULE_ENGINE, #update_rule{new_rule = NewRule, old_rule = OldRule}, ?T_CALL).

%%----------------------------------------------------------------------------------------
%% Rule Management
%%----------------------------------------------------------------------------------------

-spec get_rules_from_all_namespaces() -> #{maybe_namespace() => [rule()]}.
get_rules_from_all_namespaces() ->
    lists:foldl(
        fun({?KEY(Namespace, Id), Rule}, Acc) ->
            maps:update_with(
                Namespace,
                fun(NsRules) -> [Rule#{id => Id} | NsRules] end,
                [Rule#{id => Id}],
                Acc
            )
        end,
        #{},
        ets:tab2list(?RULE_TAB)
    ).

-spec get_rules(maybe_namespace()) -> [rule()].
get_rules(Namespace) ->
    get_all_records(Namespace, ?RULE_TAB).

get_rules_ordered_by_ts(Namespace) ->
    lists:sort(
        fun(#{created_at := CreatedA}, #{created_at := CreatedB}) ->
            CreatedA =< CreatedB
        end,
        get_rules(Namespace)
    ).

-spec get_rules_for_topic(Topic) ->
    [
        #{
            rule => Rule,
            trigger := Topic,
            matched := TopicFilter
        }
    ]
when
    Topic :: binary(),
    TopicFilter :: binary(),
    Rule :: rule().
get_rules_for_topic(Topic) ->
    [
        #{
            rule => Rule,
            trigger => Topic,
            matched => join(MBinaryOrWords)
        }
     || {MBinaryOrWords, _} = M <- emqx_topic_index:matches(Topic, ?RULE_TOPIC_INDEX, [unique]),
        Rule <- lookup_rule(emqx_topic_index:get_id(M))
    ].

-spec get_enriched_rules_with_matching_event_all_namespaces(EventName :: atom()) ->
    [
        #{
            rule := rule(),
            trigger := _Topic :: binary(),
            matched := _TopicFilter :: binary()
        }
    ].
get_enriched_rules_with_matching_event_all_namespaces(EventName) ->
    NamespaceToRules = get_rules_from_all_namespaces(),
    lists:usort([
        #{
            rule => Rule,
            trigger => emqx_rule_events:event_topic(EventName),
            matched => Topic
        }
     || Rules <- maps:values(NamespaceToRules),
        Rule = #{from := Topics} <- Rules,
        Topic <- Topics,
        EventNameOther <- emqx_rule_events:match_event_names(Topic),
        EventNameOther == EventName
    ]).

get_enriched_rules_with_matching_event(Namespace, EventName) ->
    lists:usort([
        #{
            rule => Rule,
            trigger => emqx_rule_events:event_topic(EventName),
            matched => Topic
        }
     || Rule = #{from := Topics} <- get_rules(Namespace),
        Topic <- Topics,
        EventNameOther <- emqx_rule_events:match_event_names(Topic),
        EventNameOther == EventName
    ]).

-spec get_rules_with_matching_event(EventName :: atom()) -> [rule()].
get_rules_with_matching_event(EventName) ->
    NamespaceToRules = get_rules_from_all_namespaces(),
    lists:usort([
        Rule
     || Rules <- maps:values(NamespaceToRules),
        Rule = #{from := Topics} <- Rules,
        Topic <- Topics,
        EventNameOther <- emqx_rule_events:match_event_names(Topic),
        EventNameOther == EventName
    ]).

%% Helper used only by deprecated bridge v1 APIs
-spec get_rule_ids_by_action(action_name()) -> [rule_id()].
get_rule_ids_by_action(BridgeId) when is_binary(BridgeId) ->
    [
        Id
     || #{actions := Acts, id := Id, from := Froms} <- get_rules(?global_ns),
        forwards_to_bridge(Acts, BridgeId) orelse
            references_ingress_bridge(Froms, BridgeId)
    ];
get_rule_ids_by_action(#{function := FuncName}) when is_binary(FuncName) ->
    {Mod, Fun} =
        case string:split(FuncName, ":", leading) of
            [M, F] -> {binary_to_module(M), F};
            [F] -> {emqx_rule_actions, F}
        end,
    [
        Id
     || #{actions := Acts, id := Id} <- get_rules(?global_ns),
        contains_actions(Acts, Mod, Fun)
    ].

-spec get_rule_ids_by_bridge_action(maybe_namespace(), bridge_action_id()) -> [binary()].
get_rule_ids_by_bridge_action(Namespace, ActionId) ->
    %% ActionId = <<"type:name">>
    [
        Id
     || #{actions := Acts, id := Id} <- get_rules(Namespace),
        forwards_to_bridge(Acts, ActionId)
    ].

-spec get_rule_ids_by_bridge_source(maybe_namespace(), bridge_source_id()) -> [binary()].
get_rule_ids_by_bridge_source(Namespace, SourceId) ->
    %% SourceId = <<"type:name">>
    [
        Id
     || #{from := Froms, id := Id} <- get_rules(Namespace),
        references_ingress_bridge(Froms, SourceId)
    ].

-spec ensure_action_removed(maybe_namespace(), rule_id(), action_name()) -> ok.
ensure_action_removed(Namespace, RuleId, ActionName) ->
    FilterFunc =
        fun
            (Func, Func) -> false;
            (#{<<"function">> := Func}, #{function := Func}) -> false;
            (_, _) -> true
        end,
    case get_raw_config(Namespace, [rule_engine, rules, RuleId], not_found) of
        not_found ->
            ok;
        #{<<"actions">> := Acts} = Conf ->
            NewActs = [AName || AName <- Acts, FilterFunc(AName, ActionName)],
            {ok, _} = emqx_conf:update(
                ?RULE_PATH(RuleId),
                Conf#{<<"actions">> => NewActs},
                with_namespace(#{override_to => cluster}, Namespace)
            ),
            ok
    end.

is_of_event_name(EventName, Topic) ->
    EventName =:= emqx_rule_events:event_name(Topic).

-spec get_rule(maybe_namespace(), Id :: rule_id()) -> {ok, rule()} | not_found.
get_rule(Namespace, Id) ->
    case lookup_rule(Namespace, Id) of
        [Rule] -> {ok, Rule};
        [] -> not_found
    end.

lookup_rule({Namespace, Id}) ->
    lookup_rule(Namespace, Id).

lookup_rule(Namespace, Id) ->
    [Rule || {?KEY(_, _), Rule} <- ets:lookup(?RULE_TAB, ?KEY(Namespace, Id))].

load_hooks_for_rule(#{from := Topics}) ->
    lists:foreach(fun emqx_rule_events:load/1, Topics).

maybe_add_metrics_for_rule(Id) ->
    case emqx_metrics_worker:has_metrics(rule_metrics, Id) of
        true ->
            ok = reset_metrics_for_rule(Id);
        false ->
            ok = emqx_metrics_worker:create_metrics(rule_metrics, Id, ?METRICS, ?RATE_METRICS)
    end.

clear_metrics_for_rule(Id) ->
    ok = emqx_metrics_worker:clear_metrics(rule_metrics, Id).

%% Tip: Don't delete reset_metrics_for_rule/1, use before v572 rpc
-spec reset_metrics_for_rule(rule_id()) -> ok.
reset_metrics_for_rule(Id) ->
    reset_metrics_for_rule(Id, #{}).

-spec reset_metrics_for_rule(rule_id(), map()) -> ok.
reset_metrics_for_rule(Id, _Opts) ->
    emqx_metrics_worker:reset_metrics(rule_metrics, Id).

unload_hooks_for_rule(#{id := Id, from := Topics}) ->
    lists:foreach(
        fun(EventName) ->
            case get_rules_with_matching_event(EventName) of
                %% we are now deleting the last rule
                [#{id := Id0}] when Id0 == Id ->
                    emqx_rule_events:unload(EventName);
                _ ->
                    ok
            end
        end,
        lists:usort([
            EventName
         || Topic <- Topics,
            EventName <- emqx_rule_events:match_event_names(Topic)
        ])
    ).

%%----------------------------------------------------------------------------------------
%% Telemetry helper functions
%%----------------------------------------------------------------------------------------

-spec get_basic_usage_info() ->
    #{
        num_rules => non_neg_integer(),
        referenced_bridges =>
            #{BridgeType => non_neg_integer()}
    }
when
    BridgeType :: atom().
get_basic_usage_info() ->
    try
        NamespaceToRules = get_rules_from_all_namespaces(),
        Rules = lists:append(maps:values(NamespaceToRules)),
        EnabledRules =
            lists:filter(
                fun(#{enable := Enabled}) -> Enabled end,
                Rules
            ),
        NumRules = length(EnabledRules),
        ReferencedBridges =
            lists:foldl(
                fun(#{actions := Actions, from := Froms}, Acc) ->
                    BridgeIds0 = get_referenced_hookpoints(Froms),
                    BridgeIds1 = get_egress_bridges(Actions),
                    tally_referenced_bridges(BridgeIds0 ++ BridgeIds1, Acc)
                end,
                #{},
                EnabledRules
            ),
        #{
            num_rules => NumRules,
            referenced_bridges => ReferencedBridges
        }
    catch
        _:_ ->
            #{
                num_rules => 0,
                referenced_bridges => #{}
            }
    end.

tally_referenced_bridges(BridgeIds, Acc0) ->
    lists:foldl(
        fun(BridgeId, Acc) ->
            #{type := BridgeType} = emqx_bridge_resource:parse_bridge_id(
                BridgeId,
                #{atom_name => false}
            ),
            maps:update_with(
                BridgeType,
                fun(X) -> X + 1 end,
                1,
                Acc
            )
        end,
        Acc0,
        BridgeIds
    ).

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

%% TODO: namespace
import_config(#{<<"rule_engine">> := #{<<"rules">> := NewRules} = RuleEngineConf}) ->
    OldRules = emqx:get_raw_config(?KEY_PATH, #{}),
    RuleEngineConf1 = RuleEngineConf#{<<"rules">> => maps:merge(OldRules, NewRules)},
    case emqx_conf:update([rule_engine], RuleEngineConf1, #{override_to => cluster}) of
        {ok, #{raw_config := #{<<"rules">> := NewRawRules}}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawRules, OldRules)),
            ChangedPaths = [?RULE_PATH(Id) || Id <- maps:keys(Changed)],
            {ok, #{root_key => rule_engine, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => rule_engine, reason => Error}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => rule_engine, changed => []}}.

%%----------------------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(#insert_rule{rule = Rule}, _From, State) ->
    ok = do_insert_rule(Rule),
    ok = do_update_rule_index(Rule),
    {reply, ok, State};
handle_call(#update_rule{new_rule = NewRule, old_rule = OldRule}, _From, State) ->
    ok = do_delete_rule_index(OldRule),
    ok = do_insert_rule(NewRule),
    ok = do_update_rule_index(NewRule),
    {reply, ok, State};
handle_call(#delete_rule{rule = Rule}, _From, State) ->
    ok = do_delete_rule_index(Rule),
    ok = do_delete_rule(Rule),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", request => Req}, #{tag => ?TAG}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", request => Msg}, #{tag => ?TAG}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", request => Info}, #{tag => ?TAG}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------------------

with_parsed_rule(
    Params = #{id := RuleId, namespace := Namespace, sql := Sql, actions := Actions},
    CreatedAt0,
    Fun
) ->
    CreatedAt = emqx_utils_maps:deep_get([metadata, created_at], Params, CreatedAt0),
    LastModifiedAt = emqx_utils_maps:deep_get([metadata, last_modified_at], Params, CreatedAt),
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Rule0 = #{
                id => RuleId,
                namespace => Namespace,
                name => maps:get(name, Params, <<"">>),
                created_at => CreatedAt,
                updated_at => LastModifiedAt,
                sql => Sql,
                actions => parse_actions(Actions),
                description => maps:get(description, Params, ""),
                %% -- calculated fields:
                from => emqx_rule_sqlparser:select_from(Select),
                is_foreach => emqx_rule_sqlparser:select_is_foreach(Select),
                fields => emqx_rule_sqlparser:select_fields(Select),
                doeach => emqx_rule_sqlparser:select_doeach(Select),
                incase => emqx_rule_sqlparser:select_incase(Select),
                conditions => emqx_rule_sqlparser:select_where(Select)
                %% -- calculated fields end
            },
            InputEnable = maps:get(enable, Params, true),
            case validate_bridge_existence_in_actions(Rule0) of
                ok ->
                    ok;
                {error, NonExistentBridgeIds} ->
                    ?tp(error, "action_references_nonexistent_bridges", #{
                        rule_id => RuleId,
                        namespace => Namespace,
                        nonexistent_bridge_ids => NonExistentBridgeIds,
                        hint => <<"this rule will not work properly">>
                    })
            end,
            Rule = Rule0#{enable => InputEnable},
            ok = Fun(Rule),
            {ok, Rule};
        {error, Reason} ->
            {error, Reason}
    end.

rule_resource_id(#{id := Id, namespace := Namespace}) ->
    rule_resource_id(Namespace, Id).

rule_resource_id(?global_ns, Id) ->
    Id;
rule_resource_id(Namespace, Id) when is_binary(Namespace) ->
    iolist_to_binary([?NS_SEG, ?RES_SEP, Namespace, ?RES_SEP, Id]).

do_insert_rule(#{id := Id, namespace := Namespace} = Rule) ->
    RuleResId = rule_resource_id(Rule),
    ok = load_hooks_for_rule(Rule),
    ok = maybe_add_metrics_for_rule(RuleResId),
    true = ets:insert(?RULE_TAB, {?KEY(Namespace, Id), Rule}),
    ok.

do_delete_rule(#{id := Id, namespace := Namespace} = Rule) ->
    RuleResId = rule_resource_id(Rule),
    ok = unload_hooks_for_rule(Rule),
    ok = clear_metrics_for_rule(RuleResId),
    true = ets:delete(?RULE_TAB, ?KEY(Namespace, Id)),
    ok.

do_update_rule_index(#{id := Id, namespace := Namespace, from := From}) ->
    ok = lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:insert(Topic, ?KEY(Namespace, Id), [], ?RULE_TOPIC_INDEX)
        end,
        From
    ).

do_delete_rule_index(#{id := Id, namespace := Namespace, from := From}) ->
    ok = lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:delete(Topic, ?KEY(Namespace, Id), ?RULE_TOPIC_INDEX)
        end,
        From
    ).

parse_actions(Actions) ->
    [do_parse_action(Act) || Act <- Actions].

do_parse_action(Action) ->
    emqx_rule_actions:parse_action(Action).

get_all_records(Namespace, Tab) ->
    MS = {?KEY(Namespace, '$1'), '$2'},
    [Rule#{id => Id} || [Id, Rule] <- ets:match(Tab, MS)].

now_ms() ->
    erlang:system_time(millisecond).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.

binary_to_module(ModName) ->
    try
        binary_to_existing_atom(ModName, utf8)
    catch
        error:badarg ->
            not_exist_mod
    end.

contains_actions(Actions, Mod0, Func0) ->
    lists:any(
        fun
            (#{mod := Mod, func := Func}) when Mod =:= Mod0; Func =:= Func0 -> true;
            (_) -> false
        end,
        Actions
    ).

forwards_to_bridge(Actions, BridgeId) ->
    Action = do_parse_action(BridgeId),
    lists:any(fun(A) -> A =:= Action end, Actions).

references_ingress_bridge(Froms, BridgeId) ->
    lists:member(
        BridgeId,
        [
            RefBridgeId
         || From <- Froms,
            {ok, RefBridgeId} <-
                [emqx_bridge_resource:bridge_hookpoint_to_bridge_id(From)]
        ]
    ).

get_referenced_hookpoints(Froms) ->
    [
        BridgeId
     || From <- Froms,
        {ok, BridgeId} <-
            [emqx_bridge_resource:bridge_hookpoint_to_bridge_id(From)]
    ].

get_egress_bridges(Actions) ->
    lists:foldr(
        fun
            ({bridge, BridgeType, BridgeName, _ResId}, Acc) ->
                [emqx_bridge_resource:bridge_id(BridgeType, BridgeName) | Acc];
            ({bridge_v2, BridgeType, BridgeName}, Acc) ->
                [emqx_bridge_resource:bridge_id(BridgeType, BridgeName) | Acc];
            (_, Acc) ->
                Acc
        end,
        [],
        Actions
    ).

-doc """
`register_external_function` allows an external applications to add extra \"built-in\" functions.

`register_external_functions(mymodule)` will register all functions in `mymodule`
of the form `rsf_FUNCTION_NAME/1` under the name `FUNCTION_NAME` in the rule engine.

The `rsf_` prefix (stands for \"rule SQL function\") may be specified directly:
```
register_external_functions(mymodule, 'my_prefix_').
```

Also, the functions may be registered individually:
```
register_external_functions([{myfunction, {mymodule, my_actual_function}}]).
```
""".
-spec register_external_functions(module() | list({atom(), {module(), atom()}})) -> ok.
register_external_functions(Module) when is_atom(Module) ->
    Specs = external_functions_specs(Module, ?DEFAULT_EXTERNAL_FUNCTION_PREFIX),
    register_external_functions(Specs);
register_external_functions(Specs) when is_list(Specs) ->
    lists:foreach(fun register_external_function/1, Specs).

register_external_functions(Module, Prefix) ->
    Specs = external_functions_specs(Module, Prefix),
    register_external_functions(Specs).

register_external_function({FunctionRSName, {Module, FunctionName}}) ->
    persistent_term:put(?EXTERNAL_FUNCTION_PT_KEY(FunctionRSName), {ok, Module, FunctionName}).

external_functions_specs(Module, Prefix) ->
    PrefixBin = atom_to_binary(Prefix, utf8),
    PrefixLen = byte_size(PrefixBin),
    lists:filtermap(
        fun
            ({FunctionName, 1}) ->
                case atom_to_binary(FunctionName, utf8) of
                    <<PrefixBin:PrefixLen/binary, FunctionRSNameBin/binary>> ->
                        FunctionRSName = binary_to_atom(FunctionRSNameBin, utf8),
                        {true, {FunctionRSName, {Module, FunctionName}}};
                    _ ->
                        false
                end;
            (_) ->
                false
        end,
        apply(Module, module_info, [exports])
    ).

-spec unregister_external_functions(module() | list(atom())) -> ok.
unregister_external_functions(Module) when is_atom(Module) ->
    Specs = external_functions_specs(Module, ?DEFAULT_EXTERNAL_FUNCTION_PREFIX),
    unregister_external_functions(Specs);
unregister_external_functions(Specs) when is_list(Specs) ->
    lists:foreach(fun unregister_external_function/1, Specs).

unregister_external_functions(Module, Prefix) ->
    Specs = external_functions_specs(Module, Prefix),
    unregister_external_functions(Specs).

unregister_external_function({FunctionRSName, {_Module, _FunctionName}}) ->
    persistent_term:erase(?EXTERNAL_FUNCTION_PT_KEY(FunctionRSName));
unregister_external_function(FunctionRSName) when is_atom(FunctionRSName) ->
    persistent_term:erase(?EXTERNAL_FUNCTION_PT_KEY(FunctionRSName)).

-spec get_external_function(atom()) -> {ok, module(), atom()} | {error, not_found}.
get_external_function(FunctionRSName) ->
    persistent_term:get(?EXTERNAL_FUNCTION_PT_KEY(FunctionRSName), {error, not_found}).

%% Checks whether the referenced bridges in actions all exist.  If there are non-existent
%% ones, the rule shouldn't be allowed to be enabled.
%% The actions here are already parsed.
validate_bridge_existence_in_actions(#{} = Rule) ->
    #{
        namespace := Namespace,
        actions := Actions,
        from := Froms
    } = Rule,
    BridgeIds0 =
        lists:map(
            fun(BridgeId) ->
                %% FIXME: this supposedly returns an upgraded type, but it's fuzzy: it
                %% returns v1 types when attempting to "upgrade".....
                #{type := Type, name := Name} =
                    emqx_bridge_resource:parse_bridge_id(BridgeId, #{atom_name => false}),
                case emqx_action_info:is_action_type(Type) of
                    true -> {source, Type, Name};
                    false -> {bridge_v1, Type, Name}
                end
            end,
            get_referenced_hookpoints(Froms)
        ),
    BridgeIds1 =
        lists:filtermap(
            fun
                ({bridge_v2, Type, Name}) -> {true, {action, Type, Name}};
                ({bridge, Type, Name, _ResId}) -> {true, {bridge_v1, Type, Name}};
                (_) -> false
            end,
            Actions
        ),
    NonExistentBridgeIds =
        lists:filter(
            fun({Kind, Type, Name}) ->
                IsExist =
                    case Kind of
                        action ->
                            fun(Type1, Name1) ->
                                emqx_bridge_v2:is_action_exist(Namespace, Type1, Name1)
                            end;
                        source ->
                            fun(Type1, Name1) ->
                                emqx_bridge_v2:is_source_exist(Namespace, Type1, Name1)
                            end;
                        bridge_v1 ->
                            fun emqx_bridge:is_exist_v1/2
                    end,
                try
                    not IsExist(Type, Name)
                catch
                    _:_ -> true
                end
            end,
            BridgeIds0 ++ BridgeIds1
        ),
    case NonExistentBridgeIds of
        [] -> ok;
        _ -> {error, #{nonexistent_bridge_ids => NonExistentBridgeIds}}
    end.

join(BinaryTF) when is_binary(BinaryTF) ->
    BinaryTF;
join(Words) when is_list(Words) ->
    emqx_topic:join(Words).

get_root_config_from_all_namespaces() ->
    Default = #{},
    RootKey = rule_engine,
    NamespacedConfigs = emqx_config:get_root_from_all_namespaces(RootKey, Default),
    NamespacedConfigs#{?global_ns => emqx:get_config([RootKey], Default)}.

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.

get_raw_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_raw_namespaced_config(Namespace, KeyPath, Default);
get_raw_config(?global_ns, KeyPath, Default) ->
    emqx:get_raw_config(KeyPath, Default).

%%------------------------------------------------------------------------------
%% Only used by tests and debugging
%%------------------------------------------------------------------------------

-spec get_rules_with_same_event(maybe_namespace(), Topic :: binary()) -> [rule()].
get_rules_with_same_event(Namespace, Topic) ->
    [
        Rule
     || Rule = #{from := From} <- get_rules(Namespace),
        EventName <- emqx_rule_events:match_event_names(Topic),
        lists:any(fun(T) -> is_of_event_name(EventName, T) end, From)
    ].
