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

namespace() -> opentelemetry.
roots() -> ["opentelemetry"].

fields("opentelemetry") ->
    [
        {exporter,
            ?HOCON(
                ?R_REF("exporter"),
                #{desc => ?DESC(exporter)}
            )},
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    desc => ?DESC(enable)
                }
            )}
    ];
fields("exporter") ->
    [
        {"protocol",
            ?HOCON(
                %% http_protobuf is not support for metrics yet.
                ?ENUM([grpc]),
                #{
                    mapping => "opentelemetry_exporter.otlp_protocol",
                    desc => ?DESC(protocol),
                    default => grpc,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"endpoint",
            ?HOCON(
                emqx_schema:url(),
                #{
                    mapping => "opentelemetry_exporter.otlp_endpoint",
                    default => <<"http://localhost:4317">>,
                    desc => ?DESC(endpoint)
                }
            )},
        {"interval",
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"10s">>,
                    required => true,
                    desc => ?DESC(interval)
                }
            )}
    ].

desc("opentelemetry") -> ?DESC(opentelemetry);
desc("exporter") -> ?DESC(exporter);
desc(_) -> undefined.
