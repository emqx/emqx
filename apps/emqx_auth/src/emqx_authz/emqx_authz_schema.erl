%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_schema).

-include("emqx_authz.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    desc/1,
    namespace/0
]).

-export([
    authz_fields/0,
    api_authz_fields/0,
    api_source_type/0,
    source_types/0
]).

-export([
    injected_fields/1
]).

-export([
    default_authz/0,
    authz_common_fields/1
]).

-ifdef(TEST).
-export([source_schema_mods/0]).
-endif.

-define(AUTHZ_MODS_PT_KEY, {?MODULE, authz_schema_mods}).

%%--------------------------------------------------------------------
%% Authz Source Schema Behaviour
%%--------------------------------------------------------------------

-type schema_ref() :: ?R_REF(module(), hocon_schema:name()).
-type source_refs_type() :: source_refs | api_source_refs.
-callback type() -> emqx_authz_source:source_type().
-callback source_refs() -> [schema_ref()].
-callback select_union_member(emqx_config:raw_config(), source_refs_type()) ->
    schema_ref() | undefined | no_return().
-callback fields(hocon_schema:name()) -> [hocon_schema:field()].
-callback api_source_refs() -> [schema_ref()].

-optional_callbacks([
    api_source_refs/0
]).

%%--------------------------------------------------------------------
%% Hocon Schema
%%--------------------------------------------------------------------

roots() -> [].

namespace() -> undefined.

%% "authorization"
fields(?CONF_NS) ->
    emqx_schema:authz_fields() ++ authz_fields();
fields("metrics_status_fields") ->
    [
        {"resource_metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})},
        {"node_resource_metrics", array("node_resource_metrics", "node_metrics")},
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})},
        {"node_metrics", array("node_metrics")},
        {"status", ?HOCON(cluster_status(), #{desc => ?DESC("status")})},
        {"node_status", array("node_status")},
        {"node_error", array("node_error")}
    ];
fields("metrics") ->
    [
        {"total", ?HOCON(integer(), #{desc => ?DESC("metrics_total")})},
        {"ignore", ?HOCON(integer(), #{desc => ?DESC("ignore")})},
        {"allow", ?HOCON(integer(), #{desc => ?DESC("allow")})},
        {"deny", ?HOCON(integer(), #{desc => ?DESC("deny")})},
        {"nomatch", ?HOCON(float(), #{desc => ?DESC("nomatch")})}
    ] ++ common_rate_field();
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("resource_metrics") ->
    common_field();
fields("node_resource_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", ?HOCON(status(), #{desc => ?DESC("node_status")})}
    ];
fields("node_error") ->
    [
        node_name(),
        {"error", ?HOCON(string(), #{desc => ?DESC("node_error")})}
    ].

desc(?CONF_NS) ->
    ?DESC(?CONF_NS);
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% emqx_schema_hooks behaviour
%%--------------------------------------------------------------------

injected_fields(AuthzSchemaMods) ->
    persistent_term:put(?AUTHZ_MODS_PT_KEY, AuthzSchemaMods),
    #{
        'roots.high' => [
            {?CONF_NS_ATOM, ?HOCON(?R_REF(?CONF_NS), #{desc => ?DESC(?CONF_NS)})}
        ]
    }.

authz_fields() ->
    AuthzSchemaMods = source_schema_mods(),
    AllTypes = lists:concat([Mod:source_refs() || Mod <- AuthzSchemaMods]),
    UnionMemberSelector =
        fun
            (all_union_members) ->
                AllTypes;
            %% must return list
            ({value, Value}) ->
                [
                    select_union_member(
                        Value,
                        AuthzSchemaMods,
                        source_refs
                    )
                ]
        end,
    [
        {sources,
            ?HOCON(
                ?ARRAY(hoconsc:union(UnionMemberSelector)),
                #{
                    default => [default_authz()],
                    desc => ?DESC(sources),
                    %% doc_lift is force a root level reference instead of nesting sub-structs
                    extra => #{doc_lift => true},
                    %% it is recommended to configure authz sources from dashboard
                    %% hence the importance level for config is low
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {node_cache,
            ?HOCON(
                ?R_REF(emqx_auth_cache_schema, config),
                #{
                    desc => ?DESC("node_cache"),
                    importance => ?IMPORTANCE_LOW,
                    default => emqx_auth_cache_schema:default_config()
                }
            )}
    ].

api_authz_fields() ->
    [
        {sources, ?HOCON(?ARRAY(api_source_type()), #{desc => ?DESC(sources)})},
        {node_cache, ?HOCON(?R_REF(emqx_auth_cache_schema, config), #{desc => ?DESC("node_cache")})}
    ].

api_source_type() ->
    AuthzSchemaMods = source_schema_mods(),
    AllTypes = lists:concat([api_source_refs(Mod) || Mod <- AuthzSchemaMods]),
    UnionMemberSelector =
        fun
            (all_union_members) ->
                AllTypes;
            %% must return list
            ({value, Value}) ->
                [
                    select_union_member(
                        Value,
                        AuthzSchemaMods,
                        api_source_refs
                    )
                ]
        end,
    hoconsc:union(UnionMemberSelector).

authz_common_fields(Type) ->
    [
        {type, ?HOCON(Type, #{required => true, desc => ?DESC(type)})},
        {enable,
            ?HOCON(boolean(), #{
                default => true,
                importance => ?IMPORTANCE_NO_DOC,
                desc => ?DESC(enable)
            })}
    ].

source_types() ->
    [Mod:type() || Mod <- source_schema_mods()].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

api_source_refs(Mod) ->
    case erlang:function_exported(Mod, api_source_refs, 0) of
        true ->
            Mod:api_source_refs();
        _ ->
            Mod:source_refs()
    end.

source_schema_mods() ->
    try
        persistent_term:get(?AUTHZ_MODS_PT_KEY)
    catch
        error:badarg ->
            %% This may happen only in tests.
            %% emqx_conf provides the schema mods for emqx_authz_schema
            %% and injects it into the full app's schema.
            %% Please check if emqx_conf is properly started.
            error(emqx_authz_schema_not_injected)
    end.

common_rate_field() ->
    [
        {"rate", ?HOCON(float(), #{desc => ?DESC("rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("rate_last5m")})}
    ].

array(Ref) -> array(Ref, Ref).

array(Ref, DescId) ->
    ?HOCON(?ARRAY(?R_REF(Ref)), #{desc => ?DESC(DescId)}).

select_union_member(#{<<"type">> := Type}, [], _Type) ->
    throw(#{
        reason => "unknown_authz_type",
        got => Type
    });
select_union_member(#{<<"type">> := _} = Value, [Mod | Mods], Type) ->
    case Mod:select_union_member(Value, Type) of
        undefined ->
            select_union_member(Value, Mods, Type);
        Member ->
            Member
    end;
select_union_member(_Value, _Mods, _Type) ->
    throw("missing_type_field").

default_authz() ->
    #{
        <<"type">> => <<"file">>,
        <<"enable">> => true,
        <<"path">> => <<"${EMQX_ETC_DIR}/acl.conf">>
    }.

common_field() ->
    [
        {"matched", ?HOCON(integer(), #{desc => ?DESC("matched")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("failed")})}
    ] ++ common_rate_field().

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

cluster_status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", ?HOCON(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})}.
