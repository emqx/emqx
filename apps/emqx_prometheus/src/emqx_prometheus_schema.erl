%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_prometheus_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ namespace/0
        , roots/0
        , fields/1
        , desc/1
        ]).

namespace() -> "prometheus".

roots() -> ["prometheus"].

fields("prometheus") ->
    [ {push_gateway_server, sc(string(),
                               #{ default => "http://127.0.0.1:9091"
                                , required => true
                                , desc => "URL of Prometheus pushgateway."
                                })}
    , {interval, sc(emqx_schema:duration_ms(),
                    #{ default => "15s"
                     , required => true
                     , desc => "Data reporting interval in milliseconds."
                     })}
    , {enable, sc(boolean(),
                  #{ default => false
                   , required => true
                   , desc => "Enable reporting of metrics via Prometheus Pushgateway."
                   })}
    ].

desc("prometheus") ->
    "Settings for reporting metrics to Prometheus pushgateway.";
desc(_) ->
    undefined.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
