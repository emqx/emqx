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

-module(emqx_rule_engine).

-include("rule_engine.hrl").

-export([ load_providers/0
        , unload_providers/0
        , refresh_resources/0
        , refresh_resource/1
        , refresh_rule/1
        , refresh_rules_when_boot/0
        , refresh_actions/1
        , refresh_actions/2
        , refresh_resource_status/0
        ]).

-export([ create_rule/1
        , update_rule/1
        , delete_rule/1
        , create_resource/1
        , test_resource/1
        , start_resource/1
        , start_all_resources_of_type/1
        , get_resource_status/1
        , is_resource_alive/1
        , is_resource_alive/2
        , is_resource_alive/3
        , get_resource_params/1
        , ensure_resource_deleted/1
        , delete_resource/1
        , update_resource/2
        ]).

-export([ init_resource/4
        , init_resource_with_retrier/4
        , init_action/4
        , clear_resource/4
        , clear_rule/1
        , clear_actions/1
        , clear_action/3
        ]).

-export([ restore_action_metrics/2
        ]).

-export([ fetch_resource_status/3
        ]).

-ifdef(TEST).
-export([alarm_name_of_resource_down/2]).
-endif.

-type(action() :: #action{}).
-type(resource() :: #resource{}).
-type(resource_type() :: #resource_type{}).
-type(resource_params() :: #resource_params{}).
-type(action_instance_params() :: #action_instance_params{}).

-export_type([ rule/0
             , action/0
             , resource/0
             , resource_type/0
             , resource_params/0
             , action_instance_params/0
             ]).

%% redefine this macro to confine the appup scope
-undef(RAISE).
-define(RAISE(_EXP_, _ERROR_CONTEXT_),
        ?RAISE(_EXP_, do_nothing, _ERROR_CONTEXT_)).
-define(RAISE(_EXP_, _EXP_ON_FAIL_, _ERROR_CONTEXT_),
        fun() ->
            try (_EXP_)
            catch
                throw : Reason ->
                    _EXP_ON_FAIL_,
                    throw({_ERROR_CONTEXT_, Reason});
                _EXCLASS_:_EXCPTION_:_ST_ ->
                    _EXP_ON_FAIL_,
                    throw({_ERROR_CONTEXT_, {_EXCLASS_, _EXCPTION_, _ST_}})
            end
        end()).

-define(GET_RES_ALIVE_TIMEOUT, 60000).
-define(PROBE_RES_PREFIX, "__probe__:").

%%------------------------------------------------------------------------------
%% Load resource/action providers from all available applications
%%------------------------------------------------------------------------------

%% Load all providers .
-spec(load_providers() -> ok).
load_providers() ->
    lists:foreach(fun(App) ->
        load_provider(App)
    end, ignore_lib_apps(application:loaded_applications())).

-spec(load_provider(App :: atom()) -> ok).
load_provider(App) when is_atom(App) ->
    ok = load_actions(App),
    ok = load_resource_types(App).

%%------------------------------------------------------------------------------
%% Unload providers
%%------------------------------------------------------------------------------
%% Load all providers .
-spec(unload_providers() -> ok).
unload_providers() ->
    lists:foreach(fun(App) ->
        unload_provider(App)
    end, ignore_lib_apps(application:loaded_applications())).

%% @doc Unload a provider.
-spec(unload_provider(App :: atom()) -> ok).
unload_provider(App) ->
    ok = emqx_rule_registry:remove_actions_of(App),
    ok = emqx_rule_registry:unregister_resource_types_of(App).

load_actions(App) ->
    Actions = find_actions(App),
    emqx_rule_registry:add_actions(Actions).

load_resource_types(App) ->
    ResourceTypes = find_resource_types(App),
    emqx_rule_registry:register_resource_types(ResourceTypes).

-spec(find_actions(App :: atom()) -> list(action())).
find_actions(App) ->
    lists:map(fun new_action/1, find_attrs(App, rule_action)).

-spec(find_resource_types(App :: atom()) -> list(resource_type())).
find_resource_types(App) ->
    lists:map(fun new_resource_type/1, find_attrs(App, resource_type)).

new_action({App, Mod, #{name := Name,
                        for := Hook,
                        types := Types,
                        create := Create,
                        params := ParamsSpec} = Params}) ->
    ok = emqx_rule_validator:validate_spec(ParamsSpec),
    #action{name = Name, for = Hook, app = App, types = Types,
            category = maps:get(category, Params, other),
            module = Mod, on_create = Create,
            hidden = maps:get(hidden, Params, false),
            on_destroy = maps:get(destroy, Params, undefined),
            params_spec = ParamsSpec,
            title = maps:get(title, Params, ?descr),
            description = maps:get(description, Params, ?descr)}.

new_resource_type({App, Mod, #{name := Name,
                               params := ParamsSpec,
                               create := Create} = Params}) ->
    ok = emqx_rule_validator:validate_spec(ParamsSpec),
    #resource_type{name = Name, provider = App,
                   params_spec = ParamsSpec,
                   on_create = {Mod, Create},
                   on_status = {Mod, maps:get(status, Params, undefined)},
                   on_destroy = {Mod, maps:get(destroy, Params, undefined)},
                   title = maps:get(title, Params, ?descr),
                   description = maps:get(description, Params, ?descr)}.

find_attrs(App, Def) ->
    [{App, Mod, Attr} || {ok, Modules} <- [application:get_key(App, modules)],
                         Mod <- Modules,
                         {Name, Attrs} <- module_attributes(Mod), Name =:= Def,
                         Attr <- Attrs].

module_attributes(Module) ->
    try Module:module_info(attributes)
    catch
        error:undef -> []
    end.

%%------------------------------------------------------------------------------
%% APIs for rules and resources
%%------------------------------------------------------------------------------

-spec create_rule(map()) -> {ok, rule()} | {error, term()}.
create_rule(Params = #{rawsql := Sql, actions := ActArgs}) ->
    case emqx_rule_sqlparser:parse_select(Sql) of
        {ok, Select} ->
            RuleId = maps:get(id, Params, rule_id()),
            Enabled = maps:get(enabled, Params, true),
            Rule = #rule{
                id = RuleId,
                rawsql = Sql,
                for = emqx_rule_sqlparser:select_from(Select),
                is_foreach = emqx_rule_sqlparser:select_is_foreach(Select),
                fields = emqx_rule_sqlparser:select_fields(Select),
                doeach = emqx_rule_sqlparser:select_doeach(Select),
                incase = emqx_rule_sqlparser:select_incase(Select),
                conditions = emqx_rule_sqlparser:select_where(Select),
                on_action_failed = maps:get(on_action_failed, Params, continue),
                actions = [],
                enabled = Enabled,
                created_at = erlang:system_time(millisecond),
                description = maps:get(description, Params, ""),
                state = normal
            },
            do_create_rule(Rule, ActArgs);
        Reason -> {error, Reason}
    end.

do_create_rule(Rule0 = #rule{id = RuleId, enabled = Enabled}, ActArgs) ->
    try prepare_actions(ActArgs, Enabled) of
        Actions ->
            Rule = Rule0#rule{actions = Actions},
            ok = emqx_rule_registry:add_rule(Rule),
            ok = emqx_rule_metrics:create_rule_metrics(RuleId),
            {ok, Rule}
    catch
        throw:{resource_not_initialized, _} when Enabled =:= true ->
            %% try again with rule disabled
            do_create_rule(Rule0#rule{enabled = false}, ActArgs);
        throw:{action_not_found, ActionName} ->
            {error, {action_not_found, ActionName}};
        throw:Reason ->
            {error, Reason}
    end.

-spec(update_rule(#{id := binary(), _=>_}) -> {ok, rule()} | {error, {not_found, rule_id()} | term()}).
update_rule(Params = #{id := RuleId}) ->
    case emqx_rule_registry:get_rule(RuleId) of
        {ok, Rule0} ->
            try may_update_rule_params(Rule0, Params) of
                Rule ->
                    ok = emqx_rule_registry:add_rule(Rule),
                    {ok, Rule}
            catch
                throw:Reason ->
                    {error, Reason}
            end;
        not_found ->
            {error, {not_found, RuleId}}
    end.

-spec(delete_rule(RuleId :: rule_id()) -> ok).
delete_rule(RuleId) ->
    case emqx_rule_registry:get_rule(RuleId) of
        {ok, Rule = #rule{actions = Actions}} ->
            try
                _ = ?CLUSTER_CALL(clear_rule, [Rule]),
                ok = emqx_rule_registry:remove_rule(Rule)
            catch
                Error:Reason:ST ->
                    ?LOG(error, "clear_rule ~p failed: ~p", [RuleId, {Error, Reason, ST}]),
                    refresh_actions(Actions)
            end;
        not_found ->
            ok
    end.

-spec(create_resource(#{type := _, config := _, _ => _}) -> {ok, resource()} | {error, Reason :: term()}).
create_resource(Params) ->
    create_resource(Params, with_retry).

create_resource(#{type := Type, config := Config0} = Params, Retry) ->
    case emqx_rule_registry:find_resource_type(Type) of
        {ok, #resource_type{on_create = {M, F}, params_spec = ParamSpec}} ->
            Config = emqx_rule_validator:validate_params(Config0, ParamSpec),
            ResId = maps:get(id, Params, resource_id()),
            Resource = #resource{id = ResId,
                                 type = Type,
                                 config = Config,
                                 description = iolist_to_binary(maps:get(description, Params, "")),
                                 created_at = erlang:system_time(millisecond)
                                },
            ok = emqx_rule_registry:add_resource(Resource),
            InitArgs = [M, F, ResId, Config],
            case Retry of
                with_retry ->
                    %% Note that we will return OK in case of resource creation failure,
                    %% A timer is started to re-start the resource later.
                    _ = try ?CLUSTER_CALL(init_resource_with_retrier, InitArgs, ok,
                                          init_resource, InitArgs)
                    catch throw : Reason ->
                        ?LOG_SENSITIVE(warning, "create_resource failed: ~0p", [Reason])
                    end,
                    {ok, Resource};
                no_retry ->
                    try
                        _ = ?CLUSTER_CALL(init_resource, InitArgs),
                        {ok, Resource}
                    catch throw : Reason ->
                        ?LOG_SENSITIVE(error, "create_resource failed: ~0p", [Reason]),
                        {error, Reason}
                    end
            end;
        not_found ->
            {error, {resource_type_not_found, Type}}
    end.

-spec(update_resource(resource_id(), map()) -> ok | {error, Reason :: term()}).
update_resource(ResId, NewParams) ->
    case emqx_rule_registry:find_enabled_rules_depends_on_resource(ResId) of
        [] -> check_and_update_resource(ResId, NewParams);
        Rules ->
            {error, {dependent_rules_exists, [Id || #rule{id = Id} <- Rules]}}
    end.

check_and_update_resource(Id, NewParams) ->
    case emqx_rule_registry:find_resource(Id) of
        {ok, #resource{id = Id, type = Type, config = OldConfig, description = OldDescr}} ->
            try
                Conifg = maps:get(<<"config">>, NewParams, OldConfig),
                Descr = maps:get(<<"description">>, NewParams, OldDescr),
                do_check_and_update_resource(#{id => Id, config => Conifg, type => Type,
                    description => Descr})
            catch Error:Reason:ST ->
                ?LOG_SENSITIVE(error, "check_and_update_resource failed: ~0p", [{Error, Reason, ST}]),
                {error, Reason}
            end;
        _Other ->
            {error, not_found}
    end.

do_check_and_update_resource(#{id := Id, type := Type, description := NewDescription,
                               config := NewConfig}) ->
    case emqx_rule_registry:find_resource_type(Type) of
        {ok, #resource_type{on_create = {Module, Create},
                            params_spec = ParamSpec}} ->
            Config = emqx_rule_validator:validate_params(NewConfig, ParamSpec),
            case test_resource(#{type => Type, config => NewConfig}) of
                ok ->
                    _ = delete_resource(Id),
                    _ = ?CLUSTER_CALL(init_resource, [Module, Create, Id, Config]),
                    emqx_rule_registry:add_resource(#resource{
                        id = Id,
                        type = Type,
                        config = Config,
                        description = NewDescription,
                        created_at = erlang:system_time(millisecond)
                    }),
                    ok;
               {error, Reason} ->
                    error({error, Reason})
            end
    end.

-spec(start_resource(resource_id()) -> ok | {error, Reason :: term()}).
start_resource(ResId) ->
    case emqx_rule_registry:find_resource(ResId) of
        {ok, Res} ->
            do_start_resource(Res);
        not_found ->
            {error, {resource_not_found, ResId}}
    end.

do_start_resource(#resource{id = ResId, type = ResType, config = Config}) ->
    {ok, #resource_type{on_create = {Mod, Create}}}
        = emqx_rule_registry:find_resource_type(ResType),
    try
        init_resource_with_retrier(Mod, Create, ResId, Config),
        refresh_actions_of_a_resource(ResId)
    catch
        throw:Reason -> {error, Reason}
    end.

-spec(start_all_resources_of_type(resource_type_name()) -> [{resource_id(), ok | {error, term()}}]).
start_all_resources_of_type(Type) ->
    [{ResId, do_start_resource(Res)}
        || #resource{id = ResId} = Res <- emqx_rule_registry:get_resources_by_type(Type)].

-spec(test_resource(#{type := _, config := _, _ => _}) -> ok | {error, Reason :: term()}).
test_resource(#{type := Type} = Params) ->
    case emqx_rule_registry:find_resource_type(Type) of
        {ok, #resource_type{}} ->
            %% Resource will be deleted after test.
            %% Use random resource id, ensure test func will not delete the resource in used.
            ResId = probe_resource_id(),
            try
                case create_resource(maps:put(id, ResId, Params), no_retry) of
                    {ok, _} ->
                        case is_resource_alive(ResId, #{fetch => true}) of
                            true ->
                                ok;
                            false ->
                                %% in is_resource_alive, the cluster-call RPC logs errors
                                %% so we do not log anything here
                                {error, {resource_down, ResId}}
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end
            catch E:R:S ->
                ?LOG_SENSITIVE(warning, "test resource failed, ~0p:~0p ~0p", [E, R, S]),
                {error, R}
            after
                _ = ?CLUSTER_CALL(ensure_resource_deleted, [ResId]),
                ok
            end;
        not_found ->
            {error, {resource_type_not_found, Type}}
    end.

is_resource_alive(ResId) ->
    is_resource_alive(ResId, #{fetch => false}).

is_resource_alive(ResId, Opts) ->
    is_resource_alive(ekka_mnesia:running_nodes(), ResId, Opts).

-spec(is_resource_alive(list(node()) | node(), resource_id(), #{fetch := boolean()}) -> boolean()).
is_resource_alive(Node, ResId, Opts) when is_atom(Node) ->
    is_resource_alive([Node], ResId, Opts);
is_resource_alive(Nodes, ResId, _Opts = #{fetch := true}) ->
    try
        case emqx_rule_registry:find_resource(ResId) of
            {ok, #resource{type = ResType}} ->
                {ok, #resource_type{on_status = {Mod, OnStatus}}}
                    = emqx_rule_registry:find_resource_type(ResType),
                case rpc:multicall(Nodes,
                         ?MODULE, fetch_resource_status, [Mod, OnStatus, ResId], ?GET_RES_ALIVE_TIMEOUT) of
                    {ResL, []} ->
                        is_resource_alive_(ResL);
                    {_, _Error} ->
                        false
                end;
            not_found ->
                false
        end
    catch E:R:S ->
        ?LOG(warning, "is_resource_alive failed, ~0p:~0p ~0p", [E, R, S]),
        false
    end;
is_resource_alive(Nodes, ResId, _Opts = #{fetch := false}) ->
    try
        case rpc:multicall(Nodes, ?MODULE, get_resource_status, [ResId], ?GET_RES_ALIVE_TIMEOUT) of
            {ResL, []} ->
                is_resource_alive_(ResL);
            {_, _Errors} ->
                false
        end
    catch E:R:S ->
        ?LOG(warning, "is_resource_alive failed, ~0p:~0p ~0p", [E, R, S]),
        false
    end.

%% fetch_resource_status -> #{is_alive => boolean()}
%% get_resource_status -> {ok, #{is_alive => boolean()}}
is_resource_alive_([]) -> true;
is_resource_alive_([#{is_alive := true} | ResL]) -> is_resource_alive_(ResL);
is_resource_alive_([#{is_alive := false} | _ResL]) -> false;
is_resource_alive_([{ok, #{is_alive := true}} | ResL]) -> is_resource_alive_(ResL);
is_resource_alive_([{ok, #{is_alive := false}} | _ResL]) -> false;
is_resource_alive_([_Error | _ResL]) -> false.

-spec(get_resource_status(resource_id()) -> {ok, resource_status()} | {error, Reason :: term()}).
get_resource_status(ResId) ->
    case emqx_rule_registry:find_resource_params(ResId) of
        {ok, #resource_params{status = Status}} ->
            {ok, Status};
        not_found ->
            {error, resource_not_initialized}
    end.

-spec(get_resource_params(resource_id()) -> {ok, map()} | {error, Reason :: term()}).
get_resource_params(ResId) ->
     case emqx_rule_registry:find_resource_params(ResId) of
        {ok, #resource_params{params = Params}} ->
            {ok, Params};
        not_found ->
            {error, resource_not_initialized}
    end.

-spec(ensure_resource_deleted(resource_id()) -> ok).
ensure_resource_deleted(ResId) ->
    _ = delete_resource(ResId),
    ok.

-spec(delete_resource(resource_id()) -> ok | {error, Reason :: term()}).
delete_resource(ResId) ->
    case emqx_rule_registry:find_resource(ResId) of
        {ok, #resource{type = ResType}} ->
            {ok, #resource_type{on_destroy = {ModD, Destroy}}}
                = emqx_rule_registry:find_resource_type(ResType),
            try
                case emqx_rule_registry:remove_resource(ResId) of
                    ok ->
                        _ = ?CLUSTER_CALL(clear_resource, [ModD, Destroy, ResId, ResType]),
                        emqx_plugin_libs_ssl:maybe_delete_dir("rules", ResId);
                    {error, _} = R -> R
                end
            catch
                throw:Reason -> {error, Reason}
            end;
        not_found ->
            %% always try to remove the dir as the resource might be created but have
            %% not been initialized yet.
            emqx_plugin_libs_ssl:maybe_delete_dir("rules", ResId),
            {error, not_found}
    end.

%%------------------------------------------------------------------------------
%% Re-establish resources
%%------------------------------------------------------------------------------

-spec(refresh_resources() -> ok).
refresh_resources() ->
    lists:foreach(fun refresh_resource/1,
                  emqx_rule_registry:get_resources()).

refresh_resource(Type) when is_atom(Type) ->
    lists:foreach(fun refresh_resource/1,
                  emqx_rule_registry:get_resources_by_type(Type));

refresh_resource(#resource{id = ResId, type = Type, config = Config}) ->
    {ok, #resource_type{on_create = {M, F}}} =
        emqx_rule_registry:find_resource_type(Type),
    try
        init_resource_with_retrier(M, F, ResId, Config)
    catch
        throw:Reason ->
            ?LOG_SENSITIVE(warning, "refresh_resource failed: ~0p", [Reason])
    end.

-spec(refresh_rules_when_boot() -> ok).
refresh_rules_when_boot() ->
    lists:foreach(fun
        (#rule{enabled = true} = Rule) ->
            ensure_rule_retrier(Rule);
        (#rule{enabled = false, state = refresh_failed_at_bootup} = Rule) ->
            %% the rule was previously disabled by emqx so we need to retry it
            ensure_rule_retrier(Rule);
        (#rule{enabled = false, id = RuleId}) ->
            ?LOG(warning, "rule ~s was disabled by the user, won't re-enable it", [RuleId])
    end, emqx_rule_registry:get_rules()).

ensure_rule_retrier(#rule{id = RuleId} = Rule) ->
    try refresh_rule(Rule)
    catch _:_ ->
        %% We set the enable = false when rule init failed to avoid bad rules running
        %% without actions created properly.
        %% The init failure might be caused by a disconnected resource, in this case the
        %% actions can not be created, so the rules won't work.
        %% After the user fixed the problem he can enable it manually,
        %% doing so will also recreate the actions.
        emqx_rule_registry:add_rule(Rule#rule{enabled = false, state = refresh_failed_at_bootup}),
        emqx_rule_monitor:ensure_rule_retrier(RuleId)
    end.

refresh_rule(#rule{id = RuleId, for = Topics, actions = Actions}) ->
    ok = emqx_rule_metrics:create_rule_metrics(RuleId),
    lists:foreach(fun emqx_rule_events:load/1, Topics),
    refresh_actions(Actions).

-spec(refresh_resource_status() -> ok).
refresh_resource_status() ->
    lists:foreach(
        fun(#resource{id = ResId, type = ResType}) ->
            case is_prober(ResId) of
                false ->
                    case emqx_rule_registry:find_resource_type(ResType) of
                        {ok, #resource_type{on_status = {Mod, OnStatus}}} ->
                            fetch_resource_status(Mod, OnStatus, ResId);
                        _ -> ok
                    end;
                true ->
                    ok
            end
        end, emqx_rule_registry:get_resources()).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
prepare_actions(Actions, NeedInit) ->
    [prepare_action(Action, NeedInit) || Action <- Actions].

prepare_action(#{name := Name, args := Args0} = Action, NeedInit) ->
    case emqx_rule_registry:find_action(Name) of
        {ok, #action{module = Mod, on_create = Create, params_spec = ParamSpec}} ->
            Args = emqx_rule_validator:validate_params(Args0, ParamSpec),
            ActionInstId = maps:get(id, Action, action_instance_id(Name)),
            case NeedInit of
                true ->
                    _ = ?CLUSTER_CALL(init_action, [Mod, Create, ActionInstId,
                            with_resource_params(Args)]),
                    ok;
                false -> ok
            end,
            #action_instance{
                id = ActionInstId, name = Name, args = Args,
                fallbacks = prepare_actions(maps:get(fallbacks, Action, []), NeedInit)
            };
        not_found ->
            throw({action_not_found, Name})
    end.

with_resource_params(Args = #{<<"$resource">> := ResId}) ->
    case emqx_rule_registry:find_resource_params(ResId) of
        {ok, #resource_params{params = Params}} ->
            maps:merge(Args, Params);
        not_found ->
            throw({resource_not_initialized, ResId})
    end;
with_resource_params(Args) -> Args.

may_update_rule_params(Rule, Params) ->
    %% NOTE: order matters, e.g. update_actions must be after update_enabled
    FL = [fun update_raw_sql/2,
          fun update_enabled/2,
          fun update_description/2,
          fun update_on_action_failed/2,
          fun update_actions/2
          ],
    lists:foldl(fun(F, RuleIn) ->
                        F(RuleIn, Params)
                end, Rule, FL).

update_raw_sql(Rule, #{rawsql := SQL}) ->
    case emqx_rule_sqlparser:parse_select(SQL) of
        {ok, Select} ->
                Rule#rule{
                    rawsql = SQL,
                    for = emqx_rule_sqlparser:select_from(Select),
                    is_foreach = emqx_rule_sqlparser:select_is_foreach(Select),
                    fields = emqx_rule_sqlparser:select_fields(Select),
                    doeach = emqx_rule_sqlparser:select_doeach(Select),
                    incase = emqx_rule_sqlparser:select_incase(Select),
                    conditions = emqx_rule_sqlparser:select_where(Select)
                };
        Reason ->
            throw(Reason)
    end;
update_raw_sql(Rule, _) ->
    Rule.

update_enabled(Rule = #rule{enabled = OldEnb, actions = Actions, state = OldState}, #{enabled := NewEnb}) ->
    State = case {OldEnb, NewEnb} of
        {false, true} ->
            _ = ?CLUSTER_CALL(refresh_rule, [Rule]),
            force_changed;
        {true, false} ->
            _ = ?CLUSTER_CALL(clear_actions, [Actions]),
            force_changed;
        _NoChange -> OldState
    end,
    Rule#rule{enabled = NewEnb, state = State};
update_enabled(Rule, _) ->
    Rule.

update_description(Rule, #{description := Descr}) ->
    Rule#rule{description = Descr};
update_description(Rule, _) ->
    Rule.

update_on_action_failed(Rule, #{on_action_failed := OnFailed}) ->
    Rule#rule{on_action_failed = OnFailed};
update_on_action_failed(Rule, _) ->
    Rule.

update_actions(Rule = #rule{actions = OldActions, enabled = Enabled}, #{actions := Actions}) ->
    %% prepare new actions before removing old ones
    NewActions = prepare_actions(Actions, Enabled),
    _ = ?CLUSTER_CALL(restore_action_metrics, [OldActions, NewActions]),
    _ = ?CLUSTER_CALL(clear_actions, [OldActions]),
    Rule#rule{actions = NewActions};
update_actions(Rule, _) ->
    Rule.

%% NOTE: if the user removed an action, but the action is not the last one in the list,
%% the `restore_action_metrics/2` will not work as expected!
restore_action_metrics([#action_instance{id = OldId} | OldActions],
                       [#action_instance{id = NewId} | NewActions]) ->
    emqx_rule_metrics:inc_actions_taken(NewId, emqx_rule_metrics:get_actions_taken(OldId)),
    emqx_rule_metrics:inc_actions_success(NewId, emqx_rule_metrics:get_actions_success(OldId)),
    emqx_rule_metrics:inc_actions_error(NewId, emqx_rule_metrics:get_actions_error(OldId)),
    emqx_rule_metrics:inc_actions_exception(NewId, emqx_rule_metrics:get_actions_exception(OldId)),
    restore_action_metrics(OldActions, NewActions);
restore_action_metrics(_, _) ->
    ok.

ignore_lib_apps(Apps) ->
    LibApps = [kernel, stdlib, sasl, appmon, eldap, erts,
               syntax_tools, ssl, crypto, mnesia, os_mon,
               inets, goldrush, gproc, runtime_tools,
               snmp, otp_mibs, public_key, asn1, ssh, hipe,
               common_test, observer, webtool, xmerl, tools,
               test_server, compiler, debugger, eunit, et,
               wx],
    [AppName || {AppName, _, _} <- Apps, not lists:member(AppName, LibApps)].

resource_id() ->
    gen_id("resource:", fun emqx_rule_registry:find_resource/1).

probe_resource_id() ->
    gen_id(?PROBE_RES_PREFIX, fun emqx_rule_registry:find_resource/1).

rule_id() ->
    gen_id("rule:", fun emqx_rule_registry:get_rule/1).

gen_id(Prefix, TestFun) ->
    Id = iolist_to_binary([Prefix, emqx_rule_id:gen()]),
    case TestFun(Id) of
        not_found -> Id;
        _Res -> gen_id(Prefix, TestFun)
    end.

action_instance_id(ActionName) ->
    iolist_to_binary([atom_to_list(ActionName), "_", integer_to_list(erlang:system_time())]).

init_resource(Module, OnCreate, ResId, Config) ->
    Params = ?RAISE(Module:OnCreate(ResId, Config),
                    emqx_plugin_libs_ssl:maybe_delete_dir("rules", ResId),
                    {Module, OnCreate}),
    ResParams = #resource_params{id = ResId,
                                 params = Params,
                                 status = #{is_alive => true}},
    maybe_resource_down(ResId, clear),
    emqx_rule_registry:add_resource_params(ResParams).

init_resource_with_retrier(Module, OnCreate, ResId, Config) ->
    Params = ?RAISE(Module:OnCreate(ResId, Config),
                emqx_rule_monitor:ensure_resource_retrier(ResId), {Module, OnCreate}),
    ResParams = #resource_params{id = ResId,
                                 params = Params,
                                 status = #{is_alive => true}},
    maybe_resource_down(ResId, clear),
    emqx_rule_registry:add_resource_params(ResParams).

init_action(Module, OnCreate, ActionInstId, Params) ->
    ok = emqx_rule_metrics:create_metrics(ActionInstId),
    case ?RAISE(Module:OnCreate(ActionInstId, Params),
                {init_action_failure, node(), Module, OnCreate}) of
        {Apply, NewParams} when is_function(Apply) -> %% BACKW: =< e4.2.2
            ok = emqx_rule_registry:add_action_instance_params(
                #action_instance_params{id = ActionInstId, params = NewParams, apply = Apply});
        {Bindings, NewParams} when is_list(Bindings) ->
            ok = emqx_rule_registry:add_action_instance_params(
            #action_instance_params{
                id = ActionInstId, params = NewParams,
                apply = #{mod => Module, bindings => maps:from_list(Bindings)}});
        Apply when is_function(Apply) -> %% BACKW: =< e4.2.2
            ok = emqx_rule_registry:add_action_instance_params(
                #action_instance_params{id = ActionInstId, params = Params, apply = Apply})
    end.

clear_resource(_Module, undefined, ResId, Type) ->
    clear_resource_down(ResId, Type),
    ok = emqx_rule_registry:remove_resource_params(ResId);
clear_resource(Module, Destroy, ResId, Type) ->
    clear_resource_down(ResId, Type),
    case emqx_rule_registry:find_resource_params(ResId) of
        {ok, #resource_params{params = Params}} ->
            ?RAISE(Module:Destroy(ResId, Params),
                   {destroy_resource_failure, node(), Module, Destroy}),
            ok = emqx_rule_registry:remove_resource_params(ResId);
        not_found ->
            ok
    end.

clear_rule(#rule{id = RuleId, actions = Actions}) ->
    clear_actions(Actions),
    emqx_rule_metrics:clear_rule_metrics(RuleId),
    ok.

clear_actions(Actions) ->
    lists:foreach(
        fun(#action_instance{id = Id, name = ActName, fallbacks = Fallbacks}) ->
            {ok, #action{module = Mod, on_destroy = Destory}} = emqx_rule_registry:find_action(ActName),
            clear_action(Mod, Destory, Id),
            clear_actions(Fallbacks)
        end, Actions).

clear_action(_Module, undefined, ActionInstId) ->
    emqx_rule_metrics:clear_metrics(ActionInstId),
    ok = emqx_rule_registry:remove_action_instance_params(ActionInstId);
clear_action(Module, Destroy, ActionInstId) ->
    case erlang:function_exported(Module, Destroy, 2) of
        true ->
            emqx_rule_metrics:clear_metrics(ActionInstId),
            case emqx_rule_registry:get_action_instance_params(ActionInstId) of
                {ok, #action_instance_params{params = Params}} ->
                    ?RAISE(Module:Destroy(ActionInstId, Params),
                           {destroy_action_failure, node(), Module, Destroy}),
                    ok = emqx_rule_registry:remove_action_instance_params(ActionInstId);
                not_found ->
                    ok
            end;
        false -> ok
    end.

fetch_resource_status(Module, OnStatus, ResId) ->
    case emqx_rule_registry:find_resource_params(ResId) of
        {ok, ResParams = #resource_params{params = Params, status = #{is_alive := LastIsAlive}}} ->
            NewStatus = try
                case Module:OnStatus(ResId, Params) of
                    #{is_alive := LastIsAlive} = Status -> Status;
                    #{is_alive := true} = Status ->
                        maybe_resource_down(ResId, clear),
                        Status;
                    #{is_alive := false} = Status ->
                        maybe_resource_down(ResId, alarm),
                        Status
                end
            catch _Error:Reason:STrace ->
                ?LOG(error, "get resource status for ~p failed: ~0p", [ResId, {Reason, STrace}]),
                #{is_alive => false}
            end,
            emqx_rule_registry:add_resource_params(ResParams#resource_params{status = NewStatus}),
            NewStatus;
        not_found ->
            #{is_alive => false}
    end.

refresh_actions_of_a_resource(ResId) ->
    R = fun (#action_instance{args = #{<<"$resource">> := ResId0}})
                when ResId0 =:= ResId -> true;
            (_) -> false
        end,
    F = fun(#rule{actions = Actions}) -> refresh_actions(Actions, R) end,
    lists:foreach(F, emqx_rule_registry:get_rules()).

refresh_actions(Actions) ->
    refresh_actions(Actions, fun(_) -> true end).
refresh_actions(Actions, Pred) ->
    lists:foreach(
        fun(#action_instance{args = Args,
                             id = Id, name = ActName,
                             fallbacks = Fallbacks} = ActionInst) ->
            case Pred(ActionInst) of
                true ->
                    {ok, #action{module = Mod, on_create = Create}}
                        = emqx_rule_registry:find_action(ActName),
                    _ = init_action(Mod, Create, Id, with_resource_params(Args)),
                    refresh_actions(Fallbacks, Pred);
                false -> ok
            end
        end, Actions).

maybe_resource_down(ResId, AlarmOrClear) ->
    case emqx_rule_registry:find_resource(ResId) of
        {ok, #resource{type = Type}} ->
            _ = case AlarmOrClear of
                alarm -> alarm_resource_down(ResId, Type);
                clear -> clear_resource_down(ResId, Type)
            end,
            ok;
        not_found ->
            ok
    end.

alarm_resource_down(ResId, Type) ->
    emqx_alarm:activate(alarm_name_of_resource_down(Type, ResId),
        #{id => ResId, type => Type}).
clear_resource_down(ResId, Type) ->
    emqx_alarm:deactivate(alarm_name_of_resource_down(Type, ResId)).

alarm_name_of_resource_down(Type, ResId) ->
    unicode:characters_to_binary(io_lib:format("resource/~ts/~ts/down", [Type, ResId])).

is_prober(<<?PROBE_RES_PREFIX, _/binary>>) ->
    true;
is_prober(_ResId) ->
    false.
