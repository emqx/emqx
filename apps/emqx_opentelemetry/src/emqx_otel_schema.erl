%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_otel_schema).

-include("emqx_otel_trace.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

namespace() -> opentelemetry.

roots() ->
    [
        {"opentelemetry",
            ?HOCON(?R_REF("opentelemetry"), #{
                converter => fun legacy_metrics_converter/2
            })}
    ].

fields("opentelemetry") ->
    [
        {metrics,
            ?HOCON(
                ?R_REF("otel_metrics"),
                #{
                    desc => ?DESC(otel_metrics)
                }
            )},
        {logs,
            ?HOCON(
                ?R_REF("otel_logs"),
                #{
                    desc => ?DESC(otel_logs)
                }
            )},
        {traces,
            ?HOCON(
                ?R_REF("otel_traces"),
                #{
                    desc => ?DESC(otel_traces)
                }
            )},
        {exporter,
            ?HOCON(
                ?R_REF("otel_exporter"),
                #{
                    desc => ?DESC(otel_exporter)
                }
            )}
    ];
fields("otel_metrics") ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    %% importance => ?IMPORTANCE_NO_DOC,
                    required => true,
                    desc => ?DESC(enable)
                }
            )},
        {interval,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    aliases => [scheduled_delay],
                    default => <<"10s">>,
                    desc => ?DESC(scheduled_delay),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("otel_logs") ->
    [
        {level,
            ?HOCON(
                emqx_conf_schema:log_level(),
                #{
                    default => warning,
                    desc => ?DESC(otel_log_handler_level),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(enable),
                    %% importance => ?IMPORTANCE_NO_DOC
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {max_queue_size,
            ?HOCON(
                pos_integer(),
                #{
                    default => 2048,
                    desc => ?DESC(max_queue_size),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {exporting_timeout,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(exporting_timeout),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {scheduled_delay,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"1s">>,
                    desc => ?DESC(scheduled_delay),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("otel_traces") ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(enable),
                    %% importance => ?IMPORTANCE_NO_DOC
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {max_queue_size,
            ?HOCON(
                pos_integer(),
                #{
                    default => 2048,
                    desc => ?DESC(max_queue_size),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {exporting_timeout,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(exporting_timeout),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {scheduled_delay,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"5s">>,
                    desc => ?DESC(scheduled_delay),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {filter,
            ?HOCON(
                ?R_REF("trace_filter"),
                #{
                    desc => ?DESC(trace_filter),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("otel_exporter") ->
    [
        {endpoint,
            ?HOCON(
                emqx_schema:url(),
                #{
                    default => "http://localhost:4317",
                    desc => ?DESC(exporter_endpoint),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {protocol,
            ?HOCON(
                %% http protobuf/json may be added in future
                ?ENUM([grpc]),
                #{
                    default => grpc,
                    desc => ?DESC(exporter_protocol),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {ssl_options,
            ?HOCON(
                ?R_REF(emqx_schema, "ssl_client_opts"),
                #{
                    desc => ?DESC(exporter_ssl),
                    default => #{<<"enable">> => false},
                    importance => ?IMPORTANCE_LOW
                }
            )}
    ];
fields("trace_filter") ->
    %% More filters can be implemented in future, e.g. topic, clientid
    [
        {trace_mode,
            ?HOCON(
                ?ENUM([legacy, e2e]),
                #{
                    default => legacy,
                    desc => ?DESC(trace_mode),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {trace_all,
            %% Only takes effect when trace_mode set to `legacy`
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(trace_all),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {e2e_tracing_options,
            ?HOCON(
                %% Only takes effect when trace_mode set to `e2e`
                ?R_REF("e2e_tracing_options"),
                #{
                    desc => ?DESC(e2e_tracing_options),
                    default => #{},
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("e2e_tracing_options") ->
    [
        {attribute_meta,
            ?HOCON(
                string(),
                #{
                    default => emqxcl,
                    desc => ?DESC(e2e_attribute_meta),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {publish_response_trace_level,
            ?HOCON(
                emqx_schema:qos(),
                #{
                    default => 0,
                    desc => ?DESC(publish_response_trace_level),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {samplers,
            ?HOCON(
                ?R_REF("e2e_samplers"),
                #{
                    desc => ?DESC(e2e_samplers),
                    default => #{},
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("e2e_samplers") ->
    [
        {whitelist_based_sampler,
            ?HOCON(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(whitelist_based_sampler),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {event_based_samplers,
            ?HOCON(
                ?ARRAY(?R_REF("event_based_samplers")),
                #{
                    default => [],
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ];
fields("event_based_samplers") ->
    [
        {name,
            ?HOCON(
                ?ENUM(root_span_names()),
                #{
                    required => ture,
                    desc => ?DESC(event_type),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {ratio,
            ?HOCON(
                emqx_schema:percent(),
                #{
                    default => <<"10%">>,
                    desc => ?DESC(ratio),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ].

desc("opentelemetry") ->
    ?DESC(opentelemetry);
desc("otel_exporter") ->
    ?DESC(otel_exporter);
desc("otel_logs") ->
    ?DESC(otel_logs);
desc("otel_metrics") ->
    ?DESC(otel_metrics);
desc("otel_traces") ->
    ?DESC(otel_traces);
desc("trace_filter") ->
    ?DESC(trace_filter);
desc(_) ->
    undefined.

root_span_names() ->
    [
        ?CLIENT_CONNECT_SPAN_NAME,
        ?CLIENT_DISCONNECT_SPAN_NAME,
        ?CLIENT_SUBSCRIBE_SPAN_NAME,
        ?CLIENT_UNSUBSCRIBE_SPAN_NAME,
        ?CLIENT_PUBLISH_SPAN_NAME
    ].

%% Compatibility with the previous schema that defined only metrics fields
legacy_metrics_converter(OtelConf, _Opts) when is_map(OtelConf) ->
    Otel1 =
        case maps:take(<<"enable">>, OtelConf) of
            {MetricsEnable, OtelConf1} ->
                emqx_utils_maps:deep_put(
                    [<<"metrics">>, <<"enable">>], OtelConf1, MetricsEnable
                );
            error ->
                OtelConf
        end,
    case Otel1 of
        #{<<"exporter">> := #{<<"interval">> := Interval} = Exporter} ->
            emqx_utils_maps:deep_put(
                [<<"metrics">>, <<"interval">>],
                Otel1#{<<"exporter">> => maps:remove(<<"interval">>, Exporter)},
                Interval
            );
        _ ->
            Otel1
    end;
legacy_metrics_converter(Conf, _Opts) ->
    Conf.
