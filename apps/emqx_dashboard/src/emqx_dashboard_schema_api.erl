%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module is for dashboard to retrieve the schema of
%% 1. hot-config
%% 2. bridge
%% 3. bridge_v2
%% 4. connector
-module(emqx_dashboard_schema_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

%% minirest API
-export([api_spec/0, paths/0, schema/1]).

-export([get_schema/2]).

%% for test
-export([bridge_schema_json/0]).

-define(TAGS, [<<"dashboard">>]).
-define(BAD_REQUEST, 'BAD_REQUEST').

-define(TO_REF(_N_, _F_), iolist_to_binary([to_bin(_N_), ".", to_bin(_F_)])).
-define(TO_COMPONENTS_SCHEMA(_M_, _F_),
    iolist_to_binary([
        <<"#/components/schemas/">>,
        ?TO_REF(emqx_dashboard_swagger:namespace(_M_), _F_)
    ])
).

-define(SCHEMA_VERSION, <<"0.2.0">>).

%%--------------------------------------------------------------------
%% minirest API and schema
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/schemas/:name"].

%% This is a rather hidden API, so we don't need to add translations for the description.
%% TODO(5.7): delete 'bridges'
schema("/schemas/:name") ->
    Schemas = [hotconf, bridges, actions, connectors],
    #{
        'operationId' => get_schema,
        get => #{
            parameters => [
                {name, hoconsc:mk(hoconsc:enum(Schemas), #{in => path})}
            ],
            desc => <<
                "Get the schema JSON of the specified name. "
                "NOTE: only intended for EMQX Dashboard."
            >>,
            tags => ?TAGS,
            security => [],
            responses => #{
                200 => hoconsc:mk(binary(), #{desc => <<"The JSON schema of the specified name.">>})
            }
        }
    }.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

get_schema(get, #{
    bindings := #{name := Name}
}) ->
    {200, gen_schema(Name)};
get_schema(get, _) ->
    {400, ?BAD_REQUEST, <<"unknown">>}.

gen_schema(hotconf) ->
    hotconf_schema_json();
gen_schema(bridges) ->
    bridge_schema_json();
gen_schema(actions) ->
    actions_schema_json();
gen_schema(connectors) ->
    connectors_schema_json().

hotconf_schema_json() ->
    SchemaInfo = #{
        title => <<"Hot Conf Schema">>,
        version => ?SCHEMA_VERSION
    },
    gen_api_schema_json_iodata(emqx_mgmt_api_configs, SchemaInfo).

bridge_schema_json() ->
    SchemaInfo = #{
        title => <<"Data Bridge Schema">>,
        version => ?SCHEMA_VERSION
    },
    gen_api_schema_json_iodata(emqx_bridge_api, SchemaInfo).

actions_schema_json() ->
    SchemaInfo = #{
        title => <<"Actions and Sources Schema">>,
        version => ?SCHEMA_VERSION
    },
    gen_api_schema_json_iodata(emqx_bridge_v2_api, SchemaInfo).

connectors_schema_json() ->
    SchemaInfo = #{
        title => <<"Connectors Schema">>,
        version => ?SCHEMA_VERSION
    },
    gen_api_schema_json_iodata(emqx_connector_api, SchemaInfo).

gen_api_schema_json_iodata(SchemaMod, SchemaInfo) ->
    emqx_dashboard_swagger:gen_api_schema_json_iodata(
        SchemaMod,
        SchemaInfo,
        fun hocon_schema_to_spec/2
    ).

hocon_schema_to_spec(?R_REF(Module, StructName), _LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(Module, StructName)}, [{Module, StructName}]};
hocon_schema_to_spec(?REF(StructName), LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(LocalModule, StructName)}, [{LocalModule, StructName}]};
hocon_schema_to_spec(Type, LocalModule) when ?IS_TYPEREFL(Type) ->
    {typename_to_spec(typerefl:name(Type), LocalModule), []};
hocon_schema_to_spec(?ARRAY(Item), LocalModule) ->
    {Schema, Refs} = hocon_schema_to_spec(Item, LocalModule),
    {#{type => array, items => Schema}, Refs};
hocon_schema_to_spec(?ENUM(Items), _LocalModule) ->
    {#{type => enum, symbols => Items}, []};
hocon_schema_to_spec(?MAP(Name, Type), LocalModule) ->
    {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
    {
        #{
            <<"type">> => object,
            <<"properties">> => #{<<"$", (to_bin(Name))/binary>> => Schema}
        },
        SubRefs
    };
hocon_schema_to_spec(?UNION(Types, _DisplayName), LocalModule) ->
    {OneOf, Refs} = lists:foldl(
        fun(Type, {Acc, RefsAcc}) ->
            {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
            {[Schema | Acc], SubRefs ++ RefsAcc}
        end,
        {[], []},
        hoconsc:union_members(Types)
    ),
    {#{<<"oneOf">> => OneOf}, Refs};
hocon_schema_to_spec(Atom, _LocalModule) when is_atom(Atom) ->
    {#{type => enum, symbols => [Atom]}, []}.

typename_to_spec(TypeStr, Module) ->
    emqx_conf_schema_types:readable_dashboard(Module, TypeStr).

to_bin(List) when is_list(List) -> iolist_to_binary(List);
to_bin(Boolean) when is_boolean(Boolean) -> Boolean;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_bin(X) -> X.
