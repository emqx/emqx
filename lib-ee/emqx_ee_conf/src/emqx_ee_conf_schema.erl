%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_conf_schema).

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, validations/0]).

-define(EE_SCHEMA_MODULES, [emqx_license_schema, emqx_ee_schema_registry_schema]).

namespace() ->
    emqx_conf_schema:namespace().

roots() ->
    redefine_roots(
        lists:foldl(
            fun(Module, Roots) ->
                Roots ++ apply(Module, roots, [])
            end,
            emqx_conf_schema:roots(),
            ?EE_SCHEMA_MODULES
        )
    ).

fields("node") ->
    redefine_node(emqx_conf_schema:fields("node"));
fields(Name) ->
    emqx_conf_schema:fields(Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

validations() ->
    emqx_conf_schema:validations().

redefine_node(Fields) ->
    Overrides = [{"applications", #{default => <<"emqx_license">>}}],
    override(Fields, Overrides).

redefine_roots(Roots) ->
    Overrides = [{"node", #{type => hoconsc:ref(?MODULE, "node")}}],
    override(Roots, Overrides).

override(Fields, []) ->
    Fields;
override(Fields, [{Name, Override}]) ->
    Schema = find_schema(Name, Fields),
    NewSchema = hocon_schema:override(Schema, Override),
    replace_schema(Name, NewSchema, Fields).

find_schema(Name, Fields) ->
    {Name, Schema} = lists:keyfind(Name, 1, Fields),
    Schema.

replace_schema(Name, Schema, Fields) ->
    lists:keyreplace(Name, 1, Fields, {Name, Schema}).
