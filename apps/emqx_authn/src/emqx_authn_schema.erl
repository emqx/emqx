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
    Providers = lists:filtermap(
        fun
            ({{password_based, _Backend}, Mod}) ->
                {true, Mod};
            ({jwt, Mod}) ->
                {true, Mod};
            ({{scram, _Backend}, _Mod}) ->
                false
        end,
        emqx_authn:providers()
    ),
    hoconsc:union(config_refs(Providers)).

config_refs(Modules) ->
    lists:append([Module:refs() || Module <- Modules]).

%% authn is a core functionality however implemented outside of emqx app
%% in emqx_schema, 'authentication' is a map() type which is to allow
%% EMQX more pluggable.
root_type() ->
    hoconsc:array(authenticator_type()).

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
