%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-export([upgrade_legacy_metrics/1]).

%% Compatibility with the previous schema that defined only metric fields
upgrade_legacy_metrics(RawConf) ->
    case RawConf of
        #{<<"opentelemetry">> := Otel} ->
            LegacyMetricsFields = [<<"enable">>, <<"exporter">>],
            Otel1 = maps:without(LegacyMetricsFields, Otel),
            Metrics = maps:with(LegacyMetricsFields, Otel),
            case Metrics =:= #{} of
                true ->
                    RawConf;
                false ->
                    RawConf#{<<"opentelemetry">> => Otel1#{<<"metrics">> => Metrics}}
            end;
        _ ->
            RawConf
    end.

namespace() -> opentelemetry.

roots() -> ["opentelemetry"].

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
            )}
    ];
fields("otel_metrics") ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    desc => ?DESC(enable)
                }
            )},
        {exporter,
            ?HOCON(
                ?R_REF("otel_metrics_exporter"),
                #{desc => ?DESC(exporter)}
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
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {exporter,
            ?HOCON(
                ?R_REF("otel_logs_exporter"),
                #{
                    desc => ?DESC(exporter),
                    importance => ?IMPORTANCE_HIGH
                }
            )}
    ];
fields("otel_metrics_exporter") ->
    exporter_fields(metrics);
fields("otel_logs_exporter") ->
    exporter_fields(logs);
fields("ssl_opts") ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete("enable", 1, Schema).

desc("opentelemetry") -> ?DESC(opentelemetry);
desc("exporter") -> ?DESC(exporter);
desc("otel_logs_exporter") -> ?DESC(exporter);
desc("otel_metrics_exporter") -> ?DESC(exporter);
desc("otel_logs") -> ?DESC(otel_logs);
desc("otel_metrics") -> ?DESC(otel_metrics);
desc("ssl_opts") -> ?DESC(exporter_ssl);
desc(_) -> undefined.

exporter_fields(OtelSignal) ->
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
                ?R_REF("ssl_opts"),
                #{
                    desc => ?DESC(exporter_ssl),
                    importance => ?IMPORTANCE_LOW
                }
            )}
    ] ++ exporter_extra_fields(OtelSignal).

%% Let's keep it in exporter config for metrics, as it is different from
%% scheduled_delay_ms opt used for otel traces and logs
exporter_extra_fields(metrics) ->
    [
        {interval,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"10s">>,
                    required => true,
                    desc => ?DESC(scheduled_delay)
                }
            )}
    ];
exporter_extra_fields(_OtelSignal) ->
    [].
