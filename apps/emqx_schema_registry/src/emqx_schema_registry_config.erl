%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_config).

-include("emqx_schema_registry.hrl").

%% API
-export([
    add_handlers/0,
    remove_handlers/0
]).

%% `emqx_config_handler' API
-export([post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SCHEMA_CONF_PATH(NAME), [?CONF_KEY_ROOT, schemas, NAME]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handlers() -> ok.
add_handlers() ->
    %% HTTP API handler
    ok = emqx_conf:add_handler([?CONF_KEY_ROOT, schemas, '?'], ?MODULE),
    %% Conf load / data import handler
    ok = emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    ok.

-spec remove_handlers() -> ok.
remove_handlers() ->
    ok = emqx_conf:remove_handler([?CONF_KEY_ROOT, schemas, '?']),
    ok = emqx_conf:remove_handler(?CONF_KEY_PATH),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

%% remove
post_config_update(
    ?SCHEMA_CONF_PATH(Name),
    '$remove',
    _NewSchemas,
    _OldSchemas,
    _AppEnvs
) ->
    emqx_schema_registry:async_delete_serdes([Name]),
    ok;
%% add or update
post_config_update(
    ?SCHEMA_CONF_PATH(NewName),
    _Cmd,
    NewSchema,
    OldSchema,
    _AppEnvs
) ->
    case OldSchema of
        undefined ->
            ok;
        _ ->
            emqx_schema_registry:ensure_serde_absent(NewName)
    end,
    case emqx_schema_registry:build_serdes([{NewName, NewSchema}]) of
        ok ->
            {ok, #{NewName => NewSchema}};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end;
post_config_update(?CONF_KEY_PATH, _Cmd, NewConf = #{schemas := NewSchemas}, OldConf, _AppEnvs) ->
    OldSchemas = maps:get(schemas, OldConf, #{}),
    #{
        added := Added,
        changed := Changed0,
        removed := Removed
    } = emqx_utils_maps:diff_maps(NewSchemas, OldSchemas),
    Changed = maps:map(fun(_N, {_Old, New}) -> New end, Changed0),
    RemovedNames = maps:keys(Removed),
    case RemovedNames of
        [] ->
            ok;
        _ ->
            emqx_schema_registry:async_delete_serdes(RemovedNames)
    end,
    SchemasToBuild = maps:to_list(maps:merge(Changed, Added)),
    ok = lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, [
        N
     || {N, _} <- SchemasToBuild
    ]),
    case emqx_schema_registry:build_serdes(SchemasToBuild) of
        ok ->
            {ok, NewConf};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun emqx_schema_registry:ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end;
post_config_update(_Path, _Cmd, NewConf, _OldConf, _AppEnvs) ->
    {ok, NewConf}.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{<<"schema_registry">> := #{<<"schemas">> := Schemas} = SchemaRegConf}) ->
    OldSchemas = emqx:get_raw_config([?CONF_KEY_ROOT, schemas], #{}),
    SchemaRegConf1 = SchemaRegConf#{<<"schemas">> => maps:merge(OldSchemas, Schemas)},
    case emqx_conf:update(?CONF_KEY_PATH, SchemaRegConf1, #{override_to => cluster}) of
        {ok, #{raw_config := #{<<"schemas">> := NewRawSchemas}}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawSchemas, OldSchemas)),
            ChangedPaths = [[?CONF_KEY_ROOT, schemas, Name] || Name <- maps:keys(Changed)],
            {ok, #{root_key => ?CONF_KEY_ROOT, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => ?CONF_KEY_ROOT, reason => Error}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_KEY_ROOT, changed => []}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
