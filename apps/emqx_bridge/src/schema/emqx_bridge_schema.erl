%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([
    get_response/0,
    put_request/0,
    post_request/0
]).

-export([
    common_bridge_fields/0,
    metrics_fields/0,
    status_fields/0,
    type_and_name_fields/1
]).

%% for testing only
-export([enterprise_api_schemas/1, enterprise_fields_bridges/0]).

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
        {Type, ref(Mod, Method)}
     || {Type, Mod} <- [
            {<<"webhook">>, emqx_bridge_http_schema},
            {<<"mqtt">>, emqx_bridge_mqtt_schema}
        ]
    ],
    EE = ?MODULE:enterprise_api_schemas(Method),
    hoconsc:union(bridge_api_union(Broker ++ EE)).

bridge_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                reason => <<"unknown bridge type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    throw(#{
                        field_name => type,
                        reason => <<"unknown bridge type">>
                    })
            end
    end.

-if(?EMQX_RELEASE_EDITION == ee).
enterprise_api_schemas(Method) ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    ok = emqx_utils:interactive_load(emqx_bridge_enterprise),
    case erlang:function_exported(emqx_bridge_enterprise, api_schemas, 1) of
        true -> emqx_bridge_enterprise:api_schemas(Method);
        false -> []
    end.

enterprise_fields_bridges() ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    ok = emqx_utils:interactive_load(emqx_bridge_enterprise),
    case erlang:function_exported(emqx_bridge_enterprise, fields, 1) of
        true -> emqx_bridge_enterprise:fields(bridges);
        false -> []
    end.

-else.

enterprise_api_schemas(_) -> [].

enterprise_fields_bridges() -> [].

-endif.

common_bridge_fields() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("desc_enable"),
                    importance => ?IMPORTANCE_NO_DOC,
                    default => true
                }
            )},
        {tags, emqx_schema:tags_schema()},
        %% Create v2 connector then usr v1 /bridges_probe api to test connector
        %% /bridges_probe should pass through v2 connector's description.
        {description, emqx_schema:description_schema()}
    ].

status_fields() ->
    [
        {"status", mk(status(), #{desc => ?DESC("desc_status")})},
        {"status_reason",
            mk(binary(), #{
                required => false,
                desc => ?DESC("desc_status_reason"),
                example => <<"Connection refused">>
            })},
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

type_and_name_fields(ConnectorType) ->
    [
        {type, mk(ConnectorType, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

%%======================================================================================
%% For config files

namespace() -> "bridge".

tags() ->
    [<<"Bridge">>].

roots() -> [{bridges, ?HOCON(?R_REF(bridges), #{importance => ?IMPORTANCE_HIDDEN})}].

fields(bridges) ->
    [
        {webhook,
            mk(
                hoconsc:map(name, ref(emqx_bridge_http_schema, "config")),
                #{
                    desc => ?DESC("bridges_webhook"),
                    required => false,
                    converter => fun http_bridge_converter/2
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
    ] ++ ?MODULE:enterprise_fields_bridges();
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
        {"status", mk(status(), #{})},
        {"status_reason",
            mk(binary(), #{
                required => false,
                desc => ?DESC("desc_status_reason"),
                example => <<"Connection refused">>
            })}
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
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", mk(binary(), #{desc => ?DESC("desc_node_name"), example => "emqx@127.0.0.1"})}.

http_bridge_converter(Conf0, _HoconOpts) ->
    emqx_bridge_compatible_config:upgrade_pre_ee(
        Conf0, fun emqx_bridge_compatible_config:http_maybe_upgrade/1
    ).
