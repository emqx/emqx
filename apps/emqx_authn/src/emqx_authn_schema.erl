%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-import(hoconsc, [mk/2, ref/2]).

-export([
    common_fields/0,
    roots/0,
    fields/1,
    authenticator_type/0,
    authenticator_type_without_scram/0,
    root_type/0,
    mechanism/1,
    backend/1
]).

roots() -> [].

common_fields() ->
    [{enable, fun enable/1}].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> ?DESC(?FUNCTION_NAME);
enable(_) -> undefined.

authenticator_type() ->
    hoconsc:union(config_refs([Module || {_AuthnType, Module} <- emqx_authn:providers()])).

authenticator_type_without_scram() ->
    Providers = lists:filter(
        fun
            ({{password_based, _Backend}, _Mod}) ->
                true;
            ({jwt, _Mod}) ->
                true;
            ({{scram, _Backend}, _Mod}) ->
                false
        end,
        emqx_authn:providers()
    ),
    hoconsc:union(
        config_refs([Module || {_AuthnType, Module} <- Providers])
    ).

config_refs(Modules) ->
    lists:append([Module:refs() || Module <- Modules]).

%% authn is a core functionality however implemented outside of emqx app
%% in emqx_schema, 'authentication' is a map() type which is to allow
%% EMQX more pluggable.
root_type() ->
    hoconsc:array(authenticator_type()).

mechanism(Name) ->
    hoconsc:mk(
        hoconsc:enum([Name]),
        #{
            required => true,
            desc => ?DESC("mechanism")
        }
    ).

backend(Name) ->
    hoconsc:mk(
        hoconsc:enum([Name]),
        #{
            required => true,
            desc => ?DESC("backend")
        }
    ).

fields("metrics_status_fields") ->
    [
        {"metrics", mk(ref(?MODULE, "metrics"), #{desc => ?DESC("metrics")})},
        {"node_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_metrics")),
                #{desc => ?DESC("node_metrics")}
            )}
    ] ++ common_metrics_field();
fields("metrics_status_fields_authz") ->
    [
        {"metrics", mk(ref(?MODULE, "metrics_authz"), #{desc => ?DESC("metrics")})},
        {"node_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_metrics_authz")),
                #{desc => ?DESC("node_metrics")}
            )}
    ] ++ common_metrics_field();
fields("metrics") ->
    [
        {"ignore", mk(integer(), #{desc => ?DESC("failed")})}
    ] ++ common_field();
fields("metrics_authz") ->
    [
        {"matched", mk(integer(), #{desc => ?DESC("matched")})},
        {"allow", mk(integer(), #{desc => ?DESC("allow")})},
        {"deny", mk(integer(), #{desc => ?DESC("deny")})},
        {"ignore", mk(float(), #{desc => ?DESC("ignore")})}
    ];
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_metrics_authz") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "metrics_authz"), #{desc => ?DESC("metrics")})}
    ];
fields("resource_metrics") ->
    common_field();
fields("node_resource_metrics") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "resource_metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", mk(status(), #{desc => ?DESC("node_status")})}
    ];
fields("node_error") ->
    [
        node_name(),
        {"error", mk(string(), #{desc => ?DESC("node_error")})}
    ].

common_field() ->
    [
        {"matched", mk(integer(), #{desc => ?DESC("matched")})},
        {"success", mk(integer(), #{desc => ?DESC("success")})},
        {"failed", mk(integer(), #{desc => ?DESC("failed")})},
        {"rate", mk(float(), #{desc => ?DESC("rate")})},
        {"rate_max", mk(float(), #{desc => ?DESC("rate_max")})},
        {"rate_last5m", mk(float(), #{desc => ?DESC("rate_last5m")})}
    ].

common_metrics_field() ->
    [
        {"resource_metrics", mk(ref(?MODULE, "resource_metrics"), #{desc => ?DESC("metrics")})},
        {"node_resource_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_resource_metrics")),
                #{desc => ?DESC("node_metrics")}
            )},
        {"status", mk(cluster_status(), #{desc => ?DESC("status")})},
        {"node_status",
            mk(
                hoconsc:array(ref(?MODULE, "node_status")),
                #{desc => ?DESC("node_status")}
            )},
        {"node_error",
            mk(
                hoconsc:array(ref(?MODULE, "node_error")),
                #{desc => ?DESC("node_error")}
            )}
    ].

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

cluster_status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", mk(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})}.
