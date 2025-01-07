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

-define(APP, emqx_prometheus).
-define(PROMETHEUS, [prometheus]).

-define(PROMETHEUS_DEFAULT_REGISTRY, default).
-define(PROMETHEUS_AUTH_REGISTRY, '/prometheus/auth').
-define(PROMETHEUS_AUTH_COLLECTOR, emqx_prometheus_auth).
-define(PROMETHEUS_DATA_INTEGRATION_REGISTRY, '/prometheus/data_integration').
-define(PROMETHEUS_DATA_INTEGRATION_COLLECTOR, emqx_prometheus_data_integration).
-define(PROMETHEUS_SCHEMA_VALIDATION_REGISTRY, '/prometheus/schema_validation').
-define(PROMETHEUS_SCHEMA_VALIDATION_COLLECTOR, emqx_prometheus_schema_validation).
-define(PROMETHEUS_MESSAGE_TRANSFORMATION_REGISTRY, '/prometheus/message_transformation').
-define(PROMETHEUS_MESSAGE_TRANSFORMATION_COLLECTOR, emqx_prometheus_message_transformation).

-if(?EMQX_RELEASE_EDITION == ee).
-define(PROMETHEUS_EE_REGISTRIES, [
    ?PROMETHEUS_SCHEMA_VALIDATION_REGISTRY,
    ?PROMETHEUS_MESSAGE_TRANSFORMATION_REGISTRY
]).
%% ELSE if(?EMQX_RELEASE_EDITION == ee).
-else.
-define(PROMETHEUS_EE_REGISTRIES, []).
%% END if(?EMQX_RELEASE_EDITION == ee).
-endif.

-define(PROMETHEUS_ALL_REGISTRIES,
    ?PROMETHEUS_EE_REGISTRIES ++
        [
            ?PROMETHEUS_DEFAULT_REGISTRY,
            ?PROMETHEUS_AUTH_REGISTRY,
            ?PROMETHEUS_DATA_INTEGRATION_REGISTRY
        ]
).

-define(PROM_DATA_MODE__NODE, node).
-define(PROM_DATA_MODE__ALL_NODES_AGGREGATED, all_nodes_aggregated).
-define(PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, all_nodes_unaggregated).

-define(PROM_DATA_MODES, [
    ?PROM_DATA_MODE__NODE,
    ?PROM_DATA_MODE__ALL_NODES_AGGREGATED,
    ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED
]).

-define(PROM_DATA_MODE_KEY__, prom_data_mode).

-define(PUT_PROM_DATA_MODE(MODE__), erlang:put(?PROM_DATA_MODE_KEY__, MODE__)).
-define(GET_PROM_DATA_MODE(), erlang:get(?PROM_DATA_MODE_KEY__)).
