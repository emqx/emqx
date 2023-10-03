%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_authentication.hrl").

-behaviour(emqx_schema_hooks).
-export([
    injected_fields/0
]).

-export([
    common_fields/0,
    roots/0,
    validations/0,
    tags/0,
    fields/1,
    authenticator_type/0,
    authenticator_type_without_scram/0,
    mechanism/1,
    backend/1
]).

roots() -> [].

injected_fields() ->
    #{
        'mqtt.listener' => mqtt_listener_auth_fields(),
        'roots.high' => global_auth_fields()
    }.

tags() ->
    [<<"Authentication">>].

common_fields() ->
    [{enable, fun enable/1}].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> ?DESC(?FUNCTION_NAME);
enable(_) -> undefined.

authenticator_type() ->
    hoconsc:union(union_member_selector(emqx_authn:providers())).

authenticator_type_without_scram() ->
    Providers = lists:filtermap(
        fun
            ({{scram, _Backend}, _Mod}) ->
                false;
            (_) ->
                true
        end,
        emqx_authn:providers()
    ),
    hoconsc:union(union_member_selector(Providers)).

config_refs(Providers) ->
    lists:append([Module:refs() || {_, Module} <- Providers]).

union_member_selector(Providers) ->
    Types = config_refs(Providers),
    fun
        (all_union_members) -> Types;
        ({value, Value}) -> select_union_member(Value, Providers)
    end.

select_union_member(#{<<"mechanism">> := _} = Value, Providers0) ->
    BackendVal = maps:get(<<"backend">>, Value, undefined),
    MechanismVal = maps:get(<<"mechanism">>, Value),
    BackendFilterFn = fun
        ({{_Mec, Backend}, _Mod}) ->
            BackendVal =:= atom_to_binary(Backend);
        (_) ->
            BackendVal =:= undefined
    end,
    MechanismFilterFn = fun
        ({{Mechanism, _Backend}, _Mod}) ->
            MechanismVal =:= atom_to_binary(Mechanism);
        ({Mechanism, _Mod}) ->
            MechanismVal =:= atom_to_binary(Mechanism)
    end,
    case lists:filter(BackendFilterFn, Providers0) of
        [] ->
            throw(#{reason => "unknown_backend", backend => BackendVal});
        Providers1 ->
            case lists:filter(MechanismFilterFn, Providers1) of
                [] ->
                    throw(#{
                        reason => "unsupported_mechanism",
                        mechanism => MechanismVal,
                        backend => BackendVal
                    });
                [{_, Module}] ->
                    try_select_union_member(Module, Value)
            end
    end;
select_union_member(Value, _Providers) when is_map(Value) ->
    throw(#{reason => "missing_mechanism_field"});
select_union_member(Value, _Providers) ->
    throw(#{reason => "not_a_struct", value => Value}).

try_select_union_member(Module, Value) ->
    %% some modules have `union_member_selector/1' exported to help selecting
    %% the sub-types, they are:
    %%   emqx_authn_http
    %%   emqx_authn_jwt
    %%   emqx_authn_mongodb
    %%   emqx_authn_redis
    try
        Module:union_member_selector({value, Value})
    catch
        error:undef ->
            %% otherwise expect only one member from this module
            Module:refs()
    end.

root_type() ->
    hoconsc:array(authenticator_type()).

global_auth_fields() ->
    [
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM,
            hoconsc:mk(root_type(), #{
                desc => ?DESC(global_authentication),
                converter => fun ensure_array/2,
                default => [],
                importance => ?IMPORTANCE_LOW
            })}
    ].

mqtt_listener_auth_fields() ->
    [
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM,
            hoconsc:mk(root_type(), #{
                desc => ?DESC(listener_authentication),
                converter => fun ensure_array/2,
                default => [],
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

validations() ->
    [
        {check_http_ssl_opts, fun(Conf) ->
            CheckFun = fun emqx_authn_http:check_ssl_opts/1,
            validation(Conf, CheckFun)
        end},
        {check_http_headers, fun(Conf) ->
            CheckFun = fun emqx_authn_http:check_headers/1,
            validation(Conf, CheckFun)
        end}
    ].

validation(Conf, CheckFun) when is_map(Conf) ->
    validation(hocon_maps:get(?CONF_NS, Conf), CheckFun);
validation(undefined, _) ->
    ok;
validation([], _) ->
    ok;
validation([AuthN | Tail], CheckFun) ->
    case CheckFun(#{?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY => AuthN}) of
        ok -> validation(Tail, CheckFun);
        Error -> Error
    end.
