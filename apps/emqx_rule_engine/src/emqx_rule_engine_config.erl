%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_config).

-behaviour(emqx_config_backup).

%% API
-export([
    create_or_update_rule/3,
    delete_rule/2
]).

%% `emqx_config_backup' API
-export([import_config/2]).

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

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

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(Namespace, #{?ROOT_KEY_BIN := #{<<"rules">> := NewRules} = RuleEngineConf}) ->
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
