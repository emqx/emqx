%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_schema).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_authn.hrl").
-include("emqx_authn_chains.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(emqx_schema_hooks).
-export([
    injected_fields/1
]).

-export([
    common_fields/0,
    roots/0,
    tags/0,
    fields/1,
    authenticator_type/0,
    authenticator_type/1,
    authenticator_type_without/1,
    authenticator_type_without/2,
    mechanism/1,
    backend/1,
    namespace/0
]).

-export([
    global_auth_fields/0
]).

-export_type([shema_kind/0]).

-define(AUTHN_MODS_PT_KEY, {?MODULE, authn_schema_mods}).
-define(DEFAULT_SCHEMA_KIND, config).

%%--------------------------------------------------------------------
%% Authn Source Schema Behaviour
%%--------------------------------------------------------------------

-type schema_ref() :: ?R_REF(module(), hocon_schema:name()).
-type shema_kind() ::
    %% api_write: schema for mutating API request validation
    api_write
    %% config: schema for config validation
    | config.
-callback namespace() -> string().
-callback refs() -> [schema_ref()].
-callback refs(shema_kind()) -> [schema_ref()].
-callback select_union_member(emqx_config:raw_config()) -> [schema_ref()] | undefined | no_return().
-callback select_union_member(shema_kind(), emqx_config:raw_config()) ->
    [schema_ref()] | undefined | no_return().
-callback fields(hocon_schema:name()) -> [hocon_schema:field()].

-optional_callbacks([
    select_union_member/1,
    select_union_member/2,
    refs/0,
    refs/1
]).

namespace() -> "authn".

roots() -> [].

injected_fields(AuthnSchemaMods) ->
    persistent_term:put(?AUTHN_MODS_PT_KEY, AuthnSchemaMods),
    #{
        'mqtt.listener' => mqtt_listener_auth_fields(),
        'roots.high' => global_auth_fields()
    }.

tags() ->
    [<<"Authentication">>].

authenticator_type() ->
    authenticator_type(?DEFAULT_SCHEMA_KIND).

authenticator_type(Kind) ->
    hoconsc:union(union_member_selector(Kind, provider_schema_mods())).

authenticator_type_without(ProviderSchemaMods) ->
    authenticator_type_without(?DEFAULT_SCHEMA_KIND, ProviderSchemaMods).

authenticator_type_without(Kind, ProviderSchemaMods) ->
    hoconsc:union(
        union_member_selector(Kind, provider_schema_mods() -- ProviderSchemaMods)
    ).

union_member_selector(Kind, Mods) ->
    AllTypes = config_refs(Kind, Mods),
    fun
        (all_union_members) -> AllTypes;
        ({value, Value}) -> select_union_member(Kind, Value, Mods)
    end.

select_union_member(_Kind, #{<<"mechanism">> := Mechanism, <<"backend">> := Backend}, []) ->
    throw(#{
        reason => "unsupported_mechanism",
        mechanism => Mechanism,
        backend => Backend
    });
select_union_member(_Kind, #{<<"mechanism">> := Mechanism}, []) ->
    throw(#{
        reason => "unsupported_mechanism",
        mechanism => Mechanism
    });
select_union_member(Kind, #{<<"mechanism">> := _} = Value, [Mod | Mods]) ->
    case mod_select_union_member(Kind, Value, Mod) of
        undefined ->
            select_union_member(Kind, Value, Mods);
        Member ->
            Member
    end;
select_union_member(_Kind, #{} = _Value, _Mods) ->
    throw(#{reason => "missing_mechanism_field"});
select_union_member(_Kind, Value, _Mods) ->
    throw(#{reason => "not_a_struct", value => Value}).

mod_select_union_member(Kind, Value, Mod) ->
    Args1 = [Kind, Value],
    Args2 = [Value],
    ArgsL = [Args1, Args2],
    emqx_utils:call_first_defined(Mod, select_union_member, ArgsL).

config_refs(Kind, Mods) ->
    lists:append([mod_refs(Kind, Mod) || Mod <- Mods]).

mod_refs(Kind, Mod) ->
    emqx_utils:call_first_defined(Mod, refs, [[Kind], []]).

root_type() ->
    hoconsc:array(authenticator_type()).

global_auth_fields() ->
    [
        {?CONF_NS_ATOM,
            hoconsc:mk(root_type(), #{
                desc => ?DESC(global_authentication),
                converter => fun ensure_array/2,
                default => [],
                validator => validator(),
                importance => ?IMPORTANCE_LOW
            })},
        {authentication_cache,
            ?HOCON(
                ?R_REF(emqx_auth_cache_schema, config),
                #{
                    desc => ?DESC(authentication_cache),
                    importance => ?IMPORTANCE_LOW,
                    default => emqx_auth_cache_schema:default_config()
                }
            )}
    ].

mqtt_listener_auth_fields() ->
    [
        {?CONF_NS_ATOM,
            hoconsc:mk(root_type(), #{
                desc => ?DESC(listener_authentication),
                converter => fun ensure_array/2,
                default => [],
                validator => validator(),
                importance => ?IMPORTANCE_HIDDEN
            })}
    ].

%% the older version schema allows individual element (instead of a chain) in config
ensure_array(undefined, _) -> undefined;
ensure_array(L, _) when is_list(L) -> L;
ensure_array(M, _) -> [M].

mechanism(Name) ->
    ?HOCON(
        Name,
        #{
            required => true,
            desc => ?DESC("mechanism")
        }
    ).

backend(Name) ->
    ?HOCON(Name, #{
        required => true,
        desc => ?DESC("backend")
    }).

common_fields() ->
    [{enable, fun enable/1}].

enable(type) -> boolean();
enable(default) -> true;
enable(importance) -> ?IMPORTANCE_NO_DOC;
enable(desc) -> ?DESC(?FUNCTION_NAME);
enable(_) -> undefined.

fields("metrics_status_fields") ->
    [
        {"resource_metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})},
        array("node_resource_metrics", "node_metrics"),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})},
        array("node_metrics"),
        {"status", ?HOCON(cluster_status(), #{desc => ?DESC("status")})},
        array("node_status"),
        array("node_error")
    ];
fields("metrics") ->
    [
        {"nomatch", ?HOCON(integer(), #{desc => ?DESC("metrics_nomatch")})},
        {"total", ?HOCON(integer(), #{desc => ?DESC("metrics_total")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("metrics_success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("metrics_failed")})},
        {"rate", ?HOCON(float(), #{desc => ?DESC("metrics_rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("metrics_rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("metrics_rate_last5m")})}
    ];
fields("resource_metrics") ->
    common_field();
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})}
    ];
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

common_field() ->
    [
        {"matched", ?HOCON(integer(), #{desc => ?DESC("matched")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("failed")})},
        {"rate", ?HOCON(float(), #{desc => ?DESC("rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("rate_last5m")})}
    ].

validator() ->
    Validations = lists:flatmap(
        fun validations/1,
        provider_schema_mods()
    ),
    fun(AuthConf) ->
        lists:foreach(
            fun(Conf) ->
                lists:foreach(
                    fun({_Name, Validation}) ->
                        Validation(Conf)
                    end,
                    Validations
                )
            end,
            wrap_list(AuthConf)
        )
    end.

validations(Mod) ->
    case erlang:function_exported(Mod, validations, 0) of
        true ->
            Mod:validations();
        false ->
            []
    end.

provider_schema_mods() ->
    try
        persistent_term:get(?AUTHN_MODS_PT_KEY)
    catch
        error:badarg ->
            %% This may happen only in tests.
            %% emqx_conf provides the schema mods for emqx_authn_schema
            %% and injects it into the full app's schema.
            %% Please check if emqx_conf is properly started.
            error(emqx_authn_schema_not_injected)
    end.

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

cluster_status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", ?HOCON(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})}.

array(Name) ->
    array(Name, Name).

array(Name, DescId) ->
    {Name, ?HOCON(?R_REF(Name), #{desc => ?DESC(DescId)})}.

wrap_list(Map) when is_map(Map) ->
    [Map];
wrap_list(L) when is_list(L) ->
    L.
