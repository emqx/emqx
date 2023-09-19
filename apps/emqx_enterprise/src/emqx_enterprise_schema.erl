%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_schema).

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, desc/1, validations/0]).

-define(EE_SCHEMA_MODULES, [
    emqx_license_schema,
    emqx_schema_registry_schema,
    emqx_ft_schema,
    emqx_dashboard_sso_schema
]).

namespace() ->
    emqx_conf_schema:namespace().

roots() ->
    redefine_roots(emqx_conf_schema:roots()) ++ ee_roots().

fields("node") ->
    redefine_node(emqx_conf_schema:fields("node"));
fields(Name) ->
    ee_delegate(fields, ?EE_SCHEMA_MODULES, Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

desc(Name) ->
    ee_delegate(desc, ?EE_SCHEMA_MODULES, Name).

validations() ->
    emqx_conf_schema:validations() ++ emqx_license_schema:validations().

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

redefine_roots(Roots) ->
    Overrides = [{"node", #{type => hoconsc:ref(?MODULE, "node")}}],
    override(Roots, Overrides).

redefine_node(Fields) ->
    Overrides = [],
    override(Fields, Overrides).

override(Fields, []) ->
    Fields;
override(Fields, [{Name, Override} | More]) ->
    Schema = find_schema(Name, Fields),
    NewSchema = hocon_schema:override(Schema, Override),
    NewFields = replace_schema(Name, NewSchema, Fields),
    override(NewFields, More).

find_schema(Name, Fields) ->
    {Name, Schema} = lists:keyfind(Name, 1, Fields),
    Schema.

replace_schema(Name, Schema, Fields) ->
    lists:keyreplace(Name, 1, Fields, {Name, Schema}).
