%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-module(emqx_rule_registry).

-behaviour(gen_server).

-logger_header("[RuleRegistry]").
-include("rule_engine.hrl").

-export([start_link/0]).

%% Rule Management
-export([ get_rules/0
        , get_rules_for/1
        , get_active_rules_for/1
        , get_rules_with_same_event/1
        , get_rules_ordered_by_ts/0
        , get_rule/1
        , add_rule/1
        , add_rules/1
        , remove_rule/1
        , remove_rules/1
        ]).

%% Action Management
-export([ add_action/1
        , add_actions/1
        , get_actions/0
        , find_action/1
        , remove_action/1
        , remove_actions/1
        , remove_actions_of/1
        , add_action_instance_params/1
        , get_action_instance_params/1
        , remove_action_instance_params/1
        ]).

%% Resource Management
-export([ get_resources/0
        , add_resource/1
        , add_resource_params/1
        , find_resource/1
        , find_resource_params/1
        , get_resources_by_type/1
        , remove_resource/1
        , remove_resource_params/1
        ]).

%% Resource Types
-export([ get_resource_types/0
        , find_resource_type/1
        , find_rules_depends_on_resource/1
        , find_enabled_rules_depends_on_resource/1
        , register_resource_types/1
        , unregister_resource_types_of/1
        ]).

-export([ load_hooks_for_rule/1
        , unload_hooks_for_rule/1
        ]).

-export([ update_rules_cache/0
        , clear_rules_cache/0
        , get_rules_from_cache/0
        , update_rules_cache_locally/0
        ]).

%% for debug purposes
-export([dump/0]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(REGISTRY, ?MODULE).

-define(T_CALL, 10000).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true}]}],
    %% Rule table
    ok = ekka_mnesia:create_table(?RULE_TAB, [
                {disc_copies, [node()]},
                {record_name, rule},
                {index, [#rule.for]},
                {attributes, record_info(fields, rule)},
                {storage_properties, StoreProps}]),
    %% Rule action table
    ok = ekka_mnesia:create_table(?ACTION_TAB, [
                {ram_copies, [node()]},
                {record_name, action},
                {index, [#action.for, #action.app]},
                {attributes, record_info(fields, action)},
                {storage_properties, StoreProps}]),
    %% Resource table
    ok = ekka_mnesia:create_table(?RES_TAB, [
                {disc_copies, [node()]},
                {record_name, resource},
                {index, [#resource.type]},
                {attributes, record_info(fields, resource)},
                {storage_properties, StoreProps}]),
    %% Resource type table
    ok = ekka_mnesia:create_table(?RES_TYPE_TAB, [
                {ram_copies, [node()]},
                {record_name, resource_type},
                {index, [#resource_type.provider]},
                {attributes, record_info(fields, resource_type)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    %% Copy rule table
    ok = ekka_mnesia:copy_table(?RULE_TAB, disc_copies),
    %% Copy rule action table
    ok = ekka_mnesia:copy_table(?ACTION_TAB, ram_copies),
    %% Copy resource table
    ok = ekka_mnesia:copy_table(?RES_TAB, disc_copies),
    %% Copy resource type table
    ok = ekka_mnesia:copy_table(?RES_TYPE_TAB, ram_copies).

dump() ->
    io:format("Rules: ~p~n"
              "ActionInstParams: ~p~n"
              "Resources: ~p~n"
              "ResourceParams: ~p~n",
            [ets:tab2list(?RULE_TAB),
             ets:tab2list(?ACTION_INST_PARAMS_TAB),
             ets:tab2list(?RES_TAB),
             ets:tab2list(?RES_PARAMS_TAB)]).

%%------------------------------------------------------------------------------
%% Start the registry
%%------------------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

%% Use a single process to protect the cache updating to avoid race conditions
update_rules_cache_locally() ->
    gen_server:cast(?REGISTRY, update_rules_cache).

%%------------------------------------------------------------------------------
%% Rule Management
%%------------------------------------------------------------------------------
-define(PK_RULE_TAB, {?MODULE, ?RULE_TAB}).
-spec(get_rules() -> list(emqx_rule_engine:rule())).
get_rules() ->
    case get_rules_from_cache() of
        not_found ->
            update_rules_cache_locally(),
            get_all_records(?RULE_TAB);
        CachedRules -> CachedRules
    end.

get_rules_from_cache() ->
    persistent_term:get(?PK_RULE_TAB, not_found).

put_rules_to_cache(Rules) ->
    persistent_term:put(?PK_RULE_TAB, Rules).

update_rules_cache() ->
    put_rules_to_cache(get_all_records(?RULE_TAB)).

clear_rules_cache() ->
    _ = persistent_term:erase(?PK_RULE_TAB),
    ok.

get_rules_ordered_by_ts() ->
    lists:keysort(#rule.created_at, get_rules()).

-spec(get_rules_for(Topic :: binary()) -> list(emqx_rule_engine:rule())).
get_rules_for(Topic) ->
    [Rule || Rule = #rule{for = For} <- get_rules(),
             emqx_rule_utils:can_topic_match_oneof(Topic, For)].

-spec(get_active_rules_for(Topic :: binary()) -> list(emqx_rule_engine:rule())).
get_active_rules_for(Topic) ->
    [Rule || Rule = #rule{enabled = true, for = For} <- get_rules(),
             emqx_rule_utils:can_topic_match_oneof(Topic, For)].

-spec(get_rules_with_same_event(Topic :: binary()) -> list(emqx_rule_engine:rule())).
get_rules_with_same_event(Topic) ->
    EventName = emqx_rule_events:event_name(Topic),
    [Rule || Rule = #rule{for = For} <- get_rules(),
             lists:any(fun(T) -> is_of_event_name(EventName, T) end, For)].

is_of_event_name(EventName, Topic) ->
    EventName =:= emqx_rule_events:event_name(Topic).

-spec(get_rule(Id :: rule_id()) -> {ok, emqx_rule_engine:rule()} | not_found).
get_rule(Id) ->
    case mnesia:dirty_read(?RULE_TAB, Id) of
        [Rule] -> {ok, Rule};
        [] -> not_found
    end.

-spec(add_rule(emqx_rule_engine:rule()) -> ok).
add_rule(Rule) when is_record(Rule, rule) ->
    add_rules([Rule]).

-spec(add_rules(list(emqx_rule_engine:rule())) -> ok).
add_rules(Rules) ->
    gen_server:call(?REGISTRY, {add_rules, Rules}, ?T_CALL).

-spec(remove_rule(emqx_rule_engine:rule() | rule_id()) -> ok).
remove_rule(RuleOrId) ->
    remove_rules([RuleOrId]).

-spec(remove_rules(list(emqx_rule_engine:rule()) | list(rule_id())) -> ok).
remove_rules(Rules) ->
    gen_server:call(?REGISTRY, {remove_rules, Rules}, ?T_CALL).

%% @private
insert_rule(Rule) ->
    _ = ?CLUSTER_CALL(load_hooks_for_rule, [Rule]),
    mnesia:write(?RULE_TAB, Rule, write).

%% @private
delete_rule(RuleId) when is_binary(RuleId) ->
    case get_rule(RuleId) of
        {ok, Rule} -> delete_rule(Rule);
        not_found -> ok
    end;
delete_rule(Rule) ->
    _ = ?CLUSTER_CALL(unload_hooks_for_rule, [Rule]),
    mnesia:delete_object(?RULE_TAB, Rule, write).

load_hooks_for_rule(#rule{for = Topics}) ->
    lists:foreach(fun emqx_rule_events:load/1, Topics).

unload_hooks_for_rule(#rule{id = Id, for = Topics}) ->
    lists:foreach(fun(Topic) ->
            case get_rules_with_same_event(Topic) of
                [] -> %% no rules left, we can safely unload the hook
                    emqx_rule_events:unload(Topic);
                [#rule{id = Id0}] when Id0 =:= Id -> %% we are now deleting the last rule
                    emqx_rule_events:unload(Topic);
                _ -> ok
            end
        end, Topics).

%%------------------------------------------------------------------------------
%% Action Management
%%------------------------------------------------------------------------------

%% @doc Get all actions.
-spec(get_actions() -> list(emqx_rule_engine:action())).
get_actions() ->
    get_all_records(?ACTION_TAB).

%% @doc Find an action by name.
-spec(find_action(Name :: action_name()) -> {ok, emqx_rule_engine:action()} | not_found).
find_action(Name) ->
    case mnesia:dirty_read(?ACTION_TAB, Name) of
        [Action] -> {ok, Action};
        [] -> not_found
    end.

%% @doc Add an action.
-spec(add_action(emqx_rule_engine:action()) -> ok).
add_action(Action) when is_record(Action, action) ->
    trans(fun insert_action/1, [Action]).

%% @doc Add actions.
-spec(add_actions(list(emqx_rule_engine:action())) -> ok).
add_actions(Actions) when is_list(Actions) ->
    trans(fun lists:foreach/2, [fun insert_action/1, Actions]).

%% @doc Remove an action.
-spec(remove_action(emqx_rule_engine:action() | atom()) -> ok).
remove_action(Action) when is_record(Action, action) ->
    trans(fun delete_action/1, [Action]);

remove_action(Name) ->
    trans(fun mnesia:delete/1, [{?ACTION_TAB, Name}]).

%% @doc Remove actions.
-spec(remove_actions(list(emqx_rule_engine:action())) -> ok).
remove_actions(Actions) ->
    trans(fun lists:foreach/2, [fun delete_action/1, Actions]).

%% @doc Remove actions of the App.
-spec(remove_actions_of(App :: atom()) -> ok).
remove_actions_of(App) ->
    trans(fun() ->
            lists:foreach(fun delete_action/1, mnesia:index_read(?ACTION_TAB, App, #action.app))
          end).

%% @private
insert_action(Action) ->
    mnesia:write(?ACTION_TAB, Action, write).

%% @private
delete_action(Action) when is_record(Action, action) ->
    mnesia:delete_object(?ACTION_TAB, Action, write);
delete_action(Name) when is_atom(Name) ->
    mnesia:delete(?ACTION_TAB, Name, write).

%% @doc Add an action instance params.
-spec(add_action_instance_params(emqx_rule_engine:action_instance_params()) -> ok).
add_action_instance_params(ActionInstParams) when is_record(ActionInstParams, action_instance_params) ->
    ets:insert(?ACTION_INST_PARAMS_TAB, ActionInstParams),
    ok.

-spec(get_action_instance_params(action_instance_id()) -> {ok, emqx_rule_engine:action_instance_params()} | not_found).
get_action_instance_params(ActionInstId) ->
    case ets:lookup(?ACTION_INST_PARAMS_TAB, ActionInstId) of
        [ActionInstParams] -> {ok, ActionInstParams};
        [] -> not_found
    end.

%% @doc Delete an action instance params.
-spec(remove_action_instance_params(action_instance_id()) -> ok).
remove_action_instance_params(ActionInstId) ->
    ets:delete(?ACTION_INST_PARAMS_TAB, ActionInstId),
    ok.

%%------------------------------------------------------------------------------
%% Resource Management
%%------------------------------------------------------------------------------

-spec(get_resources() -> list(emqx_rule_engine:resource())).
get_resources() ->
    get_all_records(?RES_TAB).

-spec(add_resource(emqx_rule_engine:resource()) -> ok).
add_resource(Resource) when is_record(Resource, resource) ->
    trans(fun insert_resource/1, [Resource]).

-spec(add_resource_params(emqx_rule_engine:resource_params()) -> ok).
add_resource_params(ResParams) when is_record(ResParams, resource_params) ->
    ets:insert(?RES_PARAMS_TAB, ResParams),
    ok.

-spec(find_resource(Id :: resource_id()) -> {ok, emqx_rule_engine:resource()} | not_found).
find_resource(Id) ->
    case mnesia:dirty_read(?RES_TAB, Id) of
        [Res] -> {ok, Res};
        [] -> not_found
    end.

-spec(find_resource_params(Id :: resource_id())
        -> {ok, emqx_rule_engine:resource_params()} | not_found).
find_resource_params(Id) ->
    case ets:lookup(?RES_PARAMS_TAB, Id) of
        [ResParams] -> {ok, ResParams};
        [] -> not_found
    end.

-spec(remove_resource(emqx_rule_engine:resource() | emqx_rule_engine:resource_id()) -> ok | {error, term()}).
remove_resource(Resource) when is_record(Resource, resource) ->
    trans(fun delete_resource/1, [Resource#resource.id]);

remove_resource(ResId) when is_binary(ResId) ->
    trans(fun delete_resource/1, [ResId]).

-spec(remove_resource_params(emqx_rule_engine:resource_id()) -> ok).
remove_resource_params(ResId) ->
    ets:delete(?RES_PARAMS_TAB, ResId),
    ok.

%% @private
delete_resource(ResId) ->
    case find_enabled_rules_depends_on_resource(ResId) of
        [] -> mnesia:delete(?RES_TAB, ResId, write);
        Rules ->
            {error, {dependent_rules_exists, [Id || #rule{id = Id} <- Rules]}}
    end.

%% @private
insert_resource(Resource) ->
    mnesia:write(?RES_TAB, Resource, write).

find_enabled_rules_depends_on_resource(ResId) ->
    [R || #rule{enabled = true} = R <- find_rules_depends_on_resource(ResId)].

find_rules_depends_on_resource(ResId) ->
    lists:foldl(fun(#rule{actions = Actions} = R, Rules) ->
        case search_action_despends_on_resource(ResId, Actions) of
            false -> Rules;
            {value, _} -> [R | Rules]
        end
    end, [], get_rules()).

search_action_despends_on_resource(ResId, Actions) ->
    lists:search(fun
        (#action_instance{args = #{<<"$resource">> := ResId0}}) ->
            ResId0 =:= ResId;
        (_) ->
            false
    end, Actions).

%%------------------------------------------------------------------------------
%% Resource Type Management
%%------------------------------------------------------------------------------

-spec(get_resource_types() -> list(emqx_rule_engine:resource_type())).
get_resource_types() ->
    get_all_records(?RES_TYPE_TAB).

-spec(find_resource_type(Name :: resource_type_name()) -> {ok, emqx_rule_engine:resource_type()} | not_found).
find_resource_type(Name) ->
    case mnesia:dirty_read(?RES_TYPE_TAB, Name) of
        [ResType] -> {ok, ResType};
        [] -> not_found
    end.

-spec(get_resources_by_type(Type :: resource_type_name()) -> list(emqx_rule_engine:resource())).
get_resources_by_type(Type) ->
    mnesia:dirty_index_read(?RES_TAB, Type, #resource.type).

-spec(register_resource_types(list(emqx_rule_engine:resource_type())) -> ok).
register_resource_types(Types) ->
    trans(fun lists:foreach/2, [fun insert_resource_type/1, Types]).

%% @doc Unregister resource types of the App.
-spec(unregister_resource_types_of(App :: atom()) -> ok).
unregister_resource_types_of(App) ->
    trans(fun() ->
            lists:foreach(fun delete_resource_type/1, mnesia:index_read(?RES_TYPE_TAB, App, #resource_type.provider))
          end).

%% @private
insert_resource_type(Type) ->
    mnesia:write(?RES_TYPE_TAB, Type, write).

%% @private
delete_resource_type(Type) ->
    mnesia:delete_object(?RES_TYPE_TAB, Type, write).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    _TableId = ets:new(?KV_TAB, [named_table, set, public, {write_concurrency, true},
                                 {read_concurrency, true}]),
    ok = ensure_table_subscribed(),
    {ok, #{}}.

handle_call({add_rules, Rules}, _From, State) ->
    trans(fun lists:foreach/2, [fun insert_rule/1, Rules]),
    %% the multicall is necessary, because the other nodes maybe running an older emqx version
    %% so the table has not been subscribed
    update_rules_cache_on_all_nodes(),
    {reply, ok, State};

handle_call({remove_rules, Rules}, _From, State) ->
    trans(fun lists:foreach/2, [fun delete_rule/1, Rules]),
    update_rules_cache_on_all_nodes(),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call - ~p", [Req]),
    {reply, ignored, State}.

handle_cast(update_rules_cache, State) ->
    ok = ensure_table_subscribed(),
    ok = update_rules_cache(),
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected cast ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, _Tab, _NewRule, _OldRules, _Tid} = Event}, State) ->
    ?LOG(debug, "mnesia_table_event: ~p~n", [Event]),
    ok = update_rules_cache_locally(),
    {noreply, State};

handle_info({mnesia_table_event, {Delete, _Tab, _What, _OldRules, _Tid} = Event}, State)
        when Delete =:= delete; Delete =:= delete_object ->
    ?LOG(debug, "mnesia_table_event: ~p~n", [Event]),
    ok = update_rules_cache_locally(),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "unexpected info ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------
update_rules_cache_on_all_nodes() ->
    ok = update_rules_cache(),
    case ekka_mnesia:running_nodes() -- [node()] of
        [] -> ok;
        OtherNodes ->
            _ = rpc:multicall(OtherNodes, ?MODULE, update_rules_cache_locally, [], 5000),
            ok
    end.

ensure_table_subscribed() ->
    case mnesia:subscribe({table, ?RULE_TAB, detailed}) of
        {error, {already_exists, _}} -> ok;
        {ok, _} -> ok
    end.

get_all_records(Tab) ->
    %mnesia:dirty_match_object(Tab, mnesia:table_info(Tab, wild_pattern)).
    ets:tab2list(Tab).

trans(Fun) -> trans(Fun, []).
trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> error(Reason)
    end.
