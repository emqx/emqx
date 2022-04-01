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
-include_lib("typerefl/include/types.hrl").
-import(hoconsc, [mk/2, ref/2]).

-export([
    common_fields/0,
    roots/0,
    fields/1,
    authenticator_type/0,
    root_type/0,
    mechanism/1,
    backend/1
]).

roots() -> [].

common_fields() ->
    [{enable, fun enable/1}].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> "Set to <code>false</code> to disable this auth provider";
enable(_) -> undefined.

authenticator_type() ->
    hoconsc:union(config_refs([Module || {_AuthnType, Module} <- emqx_authn:providers()])).

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
            desc => "Authentication mechanism."
        }
    ).

backend(Name) ->
    hoconsc:mk(
        hoconsc:enum([Name]),
        #{
            required => true,
            desc => "Backend type."
        }
    ).

fields("metrics_status_fields") ->
    [
        {"metrics", mk(ref(?MODULE, "metrics"), #{desc => "The metrics of the resource"})},
        {"node_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_metrics")),
                #{desc => "The metrics of the resource for each node"}
            )},
        {"status", mk(status(), #{desc => "The status of the resource"})},
        {"node_status",
            mk(
                hoconsc:array(ref(?MODULE, "node_status")),
                #{desc => "The status of the resource for each node"}
            )}
    ];
fields("metrics") ->
    [
        {"matched", mk(integer(), #{desc => "Count of this resource is queried"})},
        {"success", mk(integer(), #{desc => "Count of query success"})},
        {"failed", mk(integer(), #{desc => "Count of query failed"})},
        {"rate", mk(float(), #{desc => "The rate of matched, times/second"})},
        {"rate_max", mk(float(), #{desc => "The max rate of matched, times/second"})},
        {"rate_last5m",
            mk(
                float(),
                #{desc => "The average rate of matched in the last 5 minutes, times/second"}
            )}
    ];
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "metrics"), #{})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", mk(status(), #{desc => "Status of the node."})}
    ].

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

node_name() ->
    {"node", mk(binary(), #{desc => "The node name", example => "emqx@127.0.0.1"})}.
