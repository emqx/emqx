%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-module(emqx_rule_engine).

-behaviour(gen_server).
-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).

-export([
    post_config_update/5
]).

%% Rule Management

-export([load_rules/0]).

-export([
    create_rule/1,
    update_rule/1,
    delete_rule/1,
    get_rule/1
]).

-export([
    get_rules/0,
    get_rules_for_topic/1,
    get_rules_with_same_event/1,
    get_rule_ids_by_action/1,
    get_rule_ids_by_bridge_action/1,
    get_rule_ids_by_bridge_source/1,
    ensure_action_removed/2,
    get_rules_ordered_by_ts/0
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
    extra_functions_module/0,
    set_extra_functions_module/1
]).

-define(RULE_ENGINE, ?MODULE).

-define(T_CALL, infinity).

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

-type action_name() :: binary() | #{function := binary()}.
-type bridge_action_id() :: binary().
-type bridge_source_id() :: binary().

-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?RULE_ENGINE}, ?MODULE, [], []).

%%----------------------------------------------------------------------------------------
%% The config handler for emqx_rule_engine
%%------------------------------------------------------------------------------
post_config_update(?RULE_PATH(RuleId), '$remove', undefined, _OldRule, _AppEnvs) ->
    delete_rule(bin(RuleId));
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, undefined, _AppEnvs) ->
    create_rule(NewRule#{id => bin(RuleId)});
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, _OldRule, _AppEnvs) ->
    update_rule(NewRule#{id => bin(RuleId)});
post_config_update([rule_engine], _Req, #{rules := NewRules}, #{rules := OldRules}, _AppEnvs) ->
    #{added := Added, removed := Removed, changed := Updated} =
        emqx_utils_maps:diff_maps(NewRules, OldRules),
    try
        maps_foreach(
            fun({Id, {_Old, New}}) ->
                {ok, _} = update_rule(New#{id => bin(Id)})
            end,
            Updated
        ),
        maps_foreach(
            fun({Id, _Rule}) ->
                ok = delete_rule(bin(Id))
            end,
            Removed
        ),
        maps_foreach(
            fun({Id, Rule}) ->
                {ok, _} = create_rule(Rule#{id => bin(Id)})
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
    maps_foreach(
        fun
            ({Id, #{metadata := #{created_at := CreatedAt}} = Rule}) ->
                create_rule(Rule#{id => bin(Id)}, CreatedAt);
            ({Id, Rule}) ->
                create_rule(Rule#{id => bin(Id)})
        end,
        emqx:get_config([rule_engine, rules], #{})
    ).

-spec create_rule(map()) -> {ok, rule()} | {error, term()}.
create_rule(Params) ->
    create_rule(Params, maps:get(created_at, Params, now_ms())).

create_rule(Params = #{id := RuleId}, CreatedAt) when is_binary(RuleId) ->
    case get_rule(RuleId) of
        not_found -> with_parsed_rule(Params, CreatedAt, fun insert_rule/1);
        {ok, _} -> {error, already_exists}
    end.

-spec update_rule(map()) -> {ok, rule()} | {error, term()}.
update_rule(Params = #{id := RuleId}) when is_binary(RuleId) ->
    case get_rule(RuleId) of
        not_found ->
            {error, not_found};
        {ok, RulePrev = #{created_at := CreatedAt}} ->
            with_parsed_rule(Params, CreatedAt, fun(Rule) -> update_rule(Rule, RulePrev) end)
    end.

-spec delete_rule(RuleId :: rule_id()) -> ok.
delete_rule(RuleId) when is_binary(RuleId) ->
    case get_rule(RuleId) of
        not_found ->
            ok;
        {ok, Rule} ->
            gen_server:call(?RULE_ENGINE, {delete_rule, Rule}, ?T_CALL)
    end.

-spec insert_rule(Rule :: rule()) -> ok.
insert_rule(Rule) ->
    gen_server:call(?RULE_ENGINE, {insert_rule, Rule}, ?T_CALL).

-spec update_rule(Rule :: rule(), RulePrev :: rule()) -> ok.
update_rule(Rule, RulePrev) ->
    gen_server:call(?RULE_ENGINE, {update_rule, Rule, RulePrev}, ?T_CALL).

%%----------------------------------------------------------------------------------------
%% Rule Management
%%----------------------------------------------------------------------------------------

-spec get_rules() -> [rule()].
get_rules() ->
    get_all_records(?RULE_TAB).

get_rules_ordered_by_ts() ->
    lists:sort(
        fun(#{created_at := CreatedA}, #{created_at := CreatedB}) ->
            CreatedA =< CreatedB
        end,
        get_rules()
    ).

-spec get_rules_for_topic(Topic :: binary()) -> [rule()].
get_rules_for_topic(Topic) ->
    [
        Rule
     || M <- emqx_topic_index:matches(Topic, ?RULE_TOPIC_INDEX, [unique]),
        Rule <- lookup_rule(emqx_topic_index:get_id(M))
    ].

-spec get_rules_with_same_event(Topic :: binary()) -> [rule()].
get_rules_with_same_event(Topic) ->
    EventName = emqx_rule_events:event_name(Topic),
    [
        Rule
     || Rule = #{from := From} <- get_rules(),
        lists:any(fun(T) -> is_of_event_name(EventName, T) end, From)
    ].

-spec get_rule_ids_by_action(action_name()) -> [rule_id()].
get_rule_ids_by_action(BridgeId) when is_binary(BridgeId) ->
    [
        Id
     || #{actions := Acts, id := Id, from := Froms} <- get_rules(),
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
     || #{actions := Acts, id := Id} <- get_rules(),
        contains_actions(Acts, Mod, Fun)
    ].

-spec get_rule_ids_by_bridge_action(bridge_action_id()) -> [binary()].
get_rule_ids_by_bridge_action(ActionId) ->
    %% ActionId = <<"type:name">>
    [
        Id
     || #{actions := Acts, id := Id} <- get_rules(),
        forwards_to_bridge(Acts, ActionId)
    ].

-spec get_rule_ids_by_bridge_source(bridge_source_id()) -> [binary()].
get_rule_ids_by_bridge_source(SourceId) ->
    %% SourceId = <<"type:name">>
    [
        Id
     || #{from := Froms, id := Id} <- get_rules(),
        references_ingress_bridge(Froms, SourceId)
    ].

-spec ensure_action_removed(rule_id(), action_name()) -> ok.
ensure_action_removed(RuleId, ActionName) ->
    FilterFunc =
        fun
            (Func, Func) -> false;
            (#{<<"function">> := Func}, #{function := Func}) -> false;
            (_, _) -> true
        end,
    case emqx:get_raw_config([rule_engine, rules, RuleId], not_found) of
        not_found ->
            ok;
        #{<<"actions">> := Acts} = Conf ->
            NewActs = [AName || AName <- Acts, FilterFunc(AName, ActionName)],
            {ok, _} = emqx_conf:update(
                ?RULE_PATH(RuleId),
                Conf#{<<"actions">> => NewActs},
                #{override_to => cluster}
            ),
            ok
    end.

is_of_event_name(EventName, Topic) ->
    EventName =:= emqx_rule_events:event_name(Topic).

-spec get_rule(Id :: rule_id()) -> {ok, rule()} | not_found.
get_rule(Id) ->
    case lookup_rule(Id) of
        [Rule] -> {ok, Rule};
        [] -> not_found
    end.

lookup_rule(Id) ->
    [Rule || {_Id, Rule} <- ets:lookup(?RULE_TAB, Id)].

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
        fun(Topic) ->
            case get_rules_with_same_event(Topic) of
                %% we are now deleting the last rule
                [#{id := Id0}] when Id0 == Id ->
                    emqx_rule_events:unload(Topic);
                _ ->
                    ok
            end
        end,
        Topics
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
        Rules = get_rules(),
        EnabledRules =
            lists:filter(
                fun(#{enable := Enabled}) -> Enabled end,
                Rules
            ),
        NumRules = length(EnabledRules),
        ReferencedBridges =
            lists:foldl(
                fun(#{actions := Actions, from := Froms}, Acc) ->
                    BridgeIDs0 = get_referenced_hookpoints(Froms),
                    BridgeIDs1 = get_egress_bridges(Actions),
                    tally_referenced_bridges(BridgeIDs0 ++ BridgeIDs1, Acc)
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

tally_referenced_bridges(BridgeIDs, Acc0) ->
    lists:foldl(
        fun(BridgeID, Acc) ->
            {BridgeType, _BridgeName} = emqx_bridge_resource:parse_bridge_id(
                BridgeID,
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
        BridgeIDs
    ).

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

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
    _TableId = ets:new(?KV_TAB, [
        named_table,
        set,
        public,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    ok = emqx_conf:add_handler(
        [rule_engine, jq_implementation_module],
        emqx_rule_engine_schema
    ),
    {ok, #{}}.

handle_call({insert_rule, Rule}, _From, State) ->
    ok = do_insert_rule(Rule),
    ok = do_update_rule_index(Rule),
    {reply, ok, State};
handle_call({update_rule, Rule, RulePrev}, _From, State) ->
    ok = do_delete_rule_index(RulePrev),
    ok = do_insert_rule(Rule),
    ok = do_update_rule_index(Rule),
    {reply, ok, State};
handle_call({delete_rule, Rule}, _From, State) ->
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

with_parsed_rule(Params = #{id := RuleId, sql := Sql, actions := Actions}, CreatedAt, Fun) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Rule0 = #{
                id => RuleId,
                name => maps:get(name, Params, <<"">>),
                created_at => CreatedAt,
                updated_at => now_ms(),
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
                {error, NonExistentBridgeIDs} ->
                    ?tp(error, "action_references_nonexistent_bridges", #{
                        rule_id => RuleId,
                        nonexistent_bridge_ids => NonExistentBridgeIDs,
                        hint => "this rule will be disabled"
                    })
            end,
            Rule = Rule0#{enable => InputEnable},
            ok = Fun(Rule),
            {ok, Rule};
        {error, Reason} ->
            {error, Reason}
    end.

do_insert_rule(#{id := Id} = Rule) ->
    ok = load_hooks_for_rule(Rule),
    ok = maybe_add_metrics_for_rule(Id),
    true = ets:insert(?RULE_TAB, {Id, Rule}),
    ok.

do_delete_rule(#{id := Id} = Rule) ->
    ok = unload_hooks_for_rule(Rule),
    ok = clear_metrics_for_rule(Id),
    true = ets:delete(?RULE_TAB, Id),
    ok.

do_update_rule_index(#{id := Id, from := From}) ->
    ok = lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:insert(Topic, Id, [], ?RULE_TOPIC_INDEX)
        end,
        From
    ).

do_delete_rule_index(#{id := Id, from := From}) ->
    ok = lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:delete(Topic, Id, ?RULE_TOPIC_INDEX)
        end,
        From
    ).

parse_actions(Actions) ->
    [do_parse_action(Act) || Act <- Actions].

do_parse_action(Action) ->
    emqx_rule_actions:parse_action(Action).

get_all_records(Tab) ->
    [Rule#{id => Id} || {Id, Rule} <- ets:tab2list(Tab)].

maps_foreach(Fun, Map) ->
    lists:foreach(Fun, maps:to_list(Map)).

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
        BridgeID
     || From <- Froms,
        {ok, BridgeID} <-
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

%% For allowing an external application to add extra "built-in" functions to the
%% rule engine SQL like language. The module set by
%% set_extra_functions_module/1 should export a function called
%% handle_rule_function with two parameters (the first being an atom for the
%% the function name and the second a list of arguments). The function should
%% should return the result or {error, no_match_for_function} if it cannot
%% handle the function. See '$handle_undefined_function' in the emqx_rule_funcs
%% module. See also callback function declaration in emqx_rule_funcs.erl.

-spec extra_functions_module() -> module() | undefined.
extra_functions_module() ->
    persistent_term:get({?MODULE, extra_functions}, undefined).

-spec set_extra_functions_module(module()) -> ok.
set_extra_functions_module(Mod) ->
    persistent_term:put({?MODULE, extra_functions}, Mod),
    ok.

%% Checks whether the referenced bridges in actions all exist.  If there are non-existent
%% ones, the rule shouldn't be allowed to be enabled.
%% The actions here are already parsed.
validate_bridge_existence_in_actions(#{actions := Actions, from := Froms} = _Rule) ->
    BridgeIDs0 =
        lists:map(
            fun(BridgeID) ->
                %% FIXME: this supposedly returns an upgraded type, but it's fuzzy: it
                %% returns v1 types when attempting to "upgrade".....
                {Type, Name} =
                    emqx_bridge_resource:parse_bridge_id(BridgeID, #{atom_name => false}),
                case emqx_action_info:is_action_type(Type) of
                    true -> {source, Type, Name};
                    false -> {bridge_v1, Type, Name}
                end
            end,
            get_referenced_hookpoints(Froms)
        ),
    BridgeIDs1 =
        lists:filtermap(
            fun
                ({bridge_v2, Type, Name}) -> {true, {action, Type, Name}};
                ({bridge, Type, Name, _ResId}) -> {true, {bridge_v1, Type, Name}};
                (_) -> false
            end,
            Actions
        ),
    NonExistentBridgeIDs =
        lists:filter(
            fun({Kind, Type, Name}) ->
                IsExist =
                    case Kind of
                        action -> fun emqx_bridge_v2:is_action_exist/2;
                        source -> fun emqx_bridge_v2:is_source_exist/2;
                        bridge_v1 -> fun emqx_bridge:is_exist_v1/2
                    end,
                try
                    not IsExist(Type, Name)
                catch
                    _:_ -> true
                end
            end,
            BridgeIDs0 ++ BridgeIDs1
        ),
    case NonExistentBridgeIDs of
        [] -> ok;
        _ -> {error, #{nonexistent_bridge_ids => NonExistentBridgeIDs}}
    end.
