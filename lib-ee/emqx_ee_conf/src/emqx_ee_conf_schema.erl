%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_conf_schema).

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, translations/0, translation/1]).

-define(EE_SCHEMA_MODULES, [
    emqx_license_schema,
    emqx_ft_schema
]).

namespace() ->
    emqx_conf_schema:namespace().

roots() ->
    lists:foldl(
        fun(Module, Roots) ->
            Roots ++ apply(Module, roots, [])
        end,
        emqx_conf_schema:roots(),
        ?EE_SCHEMA_MODULES
    ).

fields(Name) ->
    ee_fields(?EE_SCHEMA_MODULES, Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

%%------------------------------------------------------------------------------
%% helpers
%%------------------------------------------------------------------------------

ee_fields([EEMod | EEMods], Name) ->
    case lists:member(Name, apply(EEMod, roots, [])) of
        true ->
            apply(EEMod, fields, [Name]);
        false ->
            ee_fields(EEMods, Name)
    end;
ee_fields([], Name) ->
    emqx_conf_schema:fields(Name).
