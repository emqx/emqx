%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([
    get_response/0,
    put_request/0,
    post_request/0
]).

-export([
    common_bridge_fields/0,
    status_fields/0,
    metrics_fields/0
]).

%%======================================================================================
%% Hocon Schema Definitions

%%======================================================================================
%% For HTTP APIs
get_response() ->
    api_schema("get").

put_request() ->
    api_schema("put").

post_request() ->
    api_schema("post").

api_schema(Method) ->
    Broker = [
        ref(Mod, Method)
     || Mod <- [emqx_bridge_webhook_schema, emqx_bridge_mqtt_schema]
    ],
    EE = ee_api_schemas(Method),
    hoconsc:union(Broker ++ EE).

ee_api_schemas(Method) ->
    %% must ensure the app is loaded before checking if fn is defined.
    ensure_loaded(emqx_ee_bridge, emqx_ee_bridge),
    case erlang:function_exported(emqx_ee_bridge, api_schemas, 1) of
        true -> emqx_ee_bridge:api_schemas(Method);
        false -> []
    end.

ee_fields_bridges() ->
    %% must ensure the app is loaded before checking if fn is defined.
    ensure_loaded(emqx_ee_bridge, emqx_ee_bridge),
    case erlang:function_exported(emqx_ee_bridge, fields, 1) of
        true -> emqx_ee_bridge:fields(bridges);
        false -> []
    end.

common_bridge_fields() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("desc_enable"),
                    default => true
                }
            )}
    ].

status_fields() ->
    [
        {"status", mk(status(), #{desc => ?DESC("desc_status")})},
        {"node_status",
            mk(
                hoconsc:array(ref(?MODULE, "node_status")),
                #{desc => ?DESC("desc_node_status")}
            )}
    ].

metrics_fields() ->
    [
        {"metrics", mk(ref(?MODULE, "metrics"), #{desc => ?DESC("desc_metrics")})},
        {"node_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_metrics")),
                #{desc => ?DESC("desc_node_metrics")}
            )}
    ].

%%======================================================================================
%% For config files

namespace() -> "bridge".

tags() ->
    [<<"Bridge">>].

roots() -> [bridges].

fields(bridges) ->
    [
        {webhook,
            mk(
                hoconsc:map(name, ref(emqx_bridge_webhook_schema, "config")),
                #{
                    desc => ?DESC("bridges_webhook"),
                    required => false,
                    converter => fun(X, _HoconOpts) ->
                        emqx_bridge_compatible_config:upgrade_pre_ee(
                            X, fun emqx_bridge_compatible_config:webhook_maybe_upgrade/1
                        )
                    end
                }
            )},
        {mqtt,
            mk(
                hoconsc:map(name, ref(emqx_bridge_mqtt_schema, "config")),
                #{
                    desc => ?DESC("bridges_mqtt"),
                    required => false,
                    converter => fun(X, _HoconOpts) ->
                        emqx_bridge_compatible_config:upgrade_pre_ee(
                            X, fun emqx_bridge_compatible_config:maybe_upgrade/1
                        )
                    end
                }
            )}
    ] ++ ee_fields_bridges();
fields("metrics") ->
    [
        {"dropped", mk(integer(), #{desc => ?DESC("metric_dropped")})},
        {"dropped.other", mk(integer(), #{desc => ?DESC("metric_dropped_other")})},
        {"dropped.queue_full", mk(integer(), #{desc => ?DESC("metric_dropped_queue_full")})},
        {"dropped.resource_not_found",
            mk(integer(), #{desc => ?DESC("metric_dropped_resource_not_found")})},
        {"dropped.resource_stopped",
            mk(integer(), #{desc => ?DESC("metric_dropped_resource_stopped")})},
        {"matched", mk(integer(), #{desc => ?DESC("metric_matched")})},
        {"queuing", mk(integer(), #{desc => ?DESC("metric_queuing")})},
        {"retried", mk(integer(), #{desc => ?DESC("metric_retried")})},
        {"failed", mk(integer(), #{desc => ?DESC("metric_sent_failed")})},
        {"inflight", mk(integer(), #{desc => ?DESC("metric_inflight")})},
        {"success", mk(integer(), #{desc => ?DESC("metric_sent_success")})},
        {"rate", mk(float(), #{desc => ?DESC("metric_rate")})},
        {"rate_max", mk(float(), #{desc => ?DESC("metric_rate_max")})},
        {"rate_last5m",
            mk(
                float(),
                #{desc => ?DESC("metric_rate_last5m")}
            )},
        {"received", mk(float(), #{desc => ?DESC("metric_received")})}
    ];
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "metrics"), #{})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", mk(status(), #{})}
    ].

desc(bridges) ->
    ?DESC("desc_bridges");
desc("metrics") ->
    ?DESC("desc_metrics");
desc("node_metrics") ->
    ?DESC("desc_node_metrics");
desc("node_status") ->
    ?DESC("desc_node_status");
desc(_) ->
    undefined.

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

node_name() ->
    {"node", mk(binary(), #{desc => ?DESC("desc_node_name"), example => "emqx@127.0.0.1"})}.

%%=================================================================================================
%% Internal fns
%%=================================================================================================

ensure_loaded(App, Mod) ->
    try
        _ = application:load(App),
        _ = Mod:module_info(),
        ok
    catch
        _:_ ->
            ok
    end.
