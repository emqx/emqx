%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_conf_schema).

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, desc/1]).

-define(EE_SCHEMA_MODULES, [
    emqx_license_schema,
    emqx_s3_schema,
    emqx_ft_schema
]).

namespace() ->
    emqx_conf_schema:namespace().

roots() ->
    emqx_conf_schema:roots() ++ ee_roots().

fields(Name) ->
    ee_delegate(fields, ?EE_SCHEMA_MODULES, Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

desc(Name) ->
    ee_delegate(desc, ?EE_SCHEMA_MODULES, Name).

%%------------------------------------------------------------------------------
%% helpers
%%------------------------------------------------------------------------------

ee_roots() ->
    lists:flatmap(
        fun(Module) ->
            apply(Module, roots, [])
        end,
        ?EE_SCHEMA_MODULES
    ).

ee_delegate(Method, [EEMod | EEMods], Name) ->
    case lists:member(Name, apply(EEMod, roots, [])) of
        true ->
            apply(EEMod, Method, [Name]);
        false ->
            ee_delegate(Method, EEMods, Name)
    end;
ee_delegate(Method, [], Name) ->
    apply(emqx_conf_schema, Method, [Name]).
