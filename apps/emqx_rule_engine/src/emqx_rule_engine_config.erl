%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_config).

-behaviour(emqx_config_backup).
-behaviour(emqx_config_handler).

%% API
-export([
    create_or_update_rule/3,
    delete_rule/2,
    list_raw_rules/1,
    get_raw_rule/2
]).

%% `emqx_config_backup' API
-export([import_config/2]).

%% `emqx_config_handler' API
-export([
    pre_config_update/4,
    post_config_update/6
]).

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(namespace, namespace).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

create_or_update_rule(Namespace, Id, Params) ->
    emqx_conf:update(
        ?RULE_PATH(Id),
        Params,
        with_namespace(#{override_to => cluster}, Namespace)
    ).

delete_rule(Namespace, Id) ->
    emqx_conf:remove(
        ?RULE_PATH(Id),
        with_namespace(#{override_to => cluster}, Namespace)
    ).

-spec list_raw_rules(emqx_config:maybe_namespace()) -> #{binary() => _RuleConfig}.
list_raw_rules(Namespace) ->
    get_raw_config(Namespace, [?ROOT_KEY_BIN, <<"rules">>], #{}).

get_raw_rule(Namespace, Id) ->
    case get_raw_config(Namespace, ?RULE_PATH(Id), undefined) of
        undefined ->
            {error, not_found};
        RawRule ->
            {ok, RawRule}
    end.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(Namespace, #{?ROOT_KEY_BIN := RuleEngineConf}) ->
    NewRules = maps:get(<<"rules">>, RuleEngineConf, #{}),
    OldRules = get_raw_config(Namespace, ?KEY_PATH, #{}),
    RuleEngineConf1 = RuleEngineConf#{<<"rules">> => maps:merge(OldRules, NewRules)},
    UpdateRes = emqx_conf:update(
        [?ROOT_KEY],
        RuleEngineConf1,
        with_namespace(#{override_to => cluster}, Namespace)
    ),
    case UpdateRes of
        {ok, #{raw_config := #{<<"rules">> := NewRawRules}}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawRules, OldRules)),
            ChangedPaths = [?RULE_PATH(Id) || Id <- maps:keys(Changed)],
            {ok, #{root_key => ?ROOT_KEY, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => ?ROOT_KEY, reason => Error}}
    end;
import_config(_Namespace, _RawConf) ->
    {ok, #{root_key => ?ROOT_KEY, changed => []}}.

%%------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

pre_config_update(_KeyPath, UpdateReq, _Old, _ExtraContext) ->
    {ok, UpdateReq}.

post_config_update(?RULE_PATH(RuleId), '$remove', undefined, _OldRule, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    emqx_rule_engine:delete_rule(Namespace, bin(RuleId));
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, undefined, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    emqx_rule_engine:create_rule(NewRule#{id => bin(RuleId), ?namespace => Namespace});
post_config_update(?RULE_PATH(RuleId), _Req, NewRule, _OldRule, _AppEnvs, ExtraContext) ->
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    emqx_rule_engine:update_rule(NewRule#{id => bin(RuleId), ?namespace => Namespace});
post_config_update(
    [?ROOT_KEY], _Req, #{rules := NewRules}, OldConfig, _AppEnvs, ExtraContext
) ->
    OldRules = maps:get(rules, OldConfig, #{}),
    Namespace = emqx_config_handler:get_namespace(ExtraContext),
    #{added := Added, removed := Removed, changed := Updated} =
        emqx_utils_maps:diff_maps(NewRules, OldRules),
    try
        maps:foreach(
            fun(Id, {_Old, New}) ->
                {ok, _} = emqx_rule_engine:update_rule(New#{id => bin(Id), ?namespace => Namespace})
            end,
            Updated
        ),
        maps:foreach(
            fun(Id, _Rule) ->
                ok = emqx_rule_engine:delete_rule(Namespace, bin(Id))
            end,
            Removed
        ),
        maps:foreach(
            fun(Id, Rule) ->
                {ok, _} = emqx_rule_engine:create_rule(Rule#{id => bin(Id), ?namespace => Namespace})
            end,
            Added
        ),
        ok
    catch
        throw:#{kind := _} = Error ->
            {error, Error}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

get_raw_config(Namespace, KeyPath, Default) when is_binary(Namespace) ->
    emqx:get_raw_namespaced_config(Namespace, KeyPath, Default);
get_raw_config(?global_ns, KeyPath, Default) ->
    emqx:get_raw_config(KeyPath, Default).

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.
