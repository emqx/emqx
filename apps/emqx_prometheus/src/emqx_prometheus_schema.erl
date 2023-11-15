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
-module(emqx_prometheus_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    translation/1,
    convert_headers/2,
    validate_push_gateway_server/1
]).

namespace() -> "prometheus".

roots() -> [{"prometheus", ?HOCON(?R_REF("prometheus"), #{translate_to => ["prometheus"]})}].

fields("prometheus") ->
    [
        {push_gateway_server,
            ?HOCON(
                string(),
                #{
                    default => <<"http://127.0.0.1:9091">>,
                    required => true,
                    validator => fun ?MODULE:validate_push_gateway_server/1,
                    desc => ?DESC(push_gateway_server)
                }
            )},
        {interval,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    required => true,
                    desc => ?DESC(interval)
                }
            )},
        {headers,
            ?HOCON(
                typerefl:alias("map", list({string(), string()}), #{}, [string(), string()]),
                #{
                    default => #{},
                    required => false,
                    converter => fun ?MODULE:convert_headers/2,
                    desc => ?DESC(headers)
                }
            )},
        {job_name,
            ?HOCON(
                binary(),
                #{
                    default => <<"${name}/instance/${name}~${host}">>,
                    required => true,
                    desc => ?DESC(job_name)
                }
            )},

        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    desc => ?DESC(enable)
                }
            )},
        {vm_dist_collector,
            ?HOCON(
                hoconsc:enum([disabled, enabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(vm_dist_collector)
                }
            )},
        %% Mnesia metrics mainly using mnesia:system_info/1
        {mnesia_collector,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(mnesia_collector)
                }
            )},
        %% Collects Erlang VM metrics using erlang:statistics/1.
        {vm_statistics_collector,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(vm_statistics_collector)
                }
            )},
        %% Collects Erlang VM metrics using erlang:system_info/1.
        {vm_system_info_collector,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(vm_system_info_collector)
                }
            )},
        %% Collects information about memory dynamically allocated by the Erlang VM using erlang:memory/0,
        %% it also provides basic (D)ETS statistics.
        {vm_memory_collector,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(vm_memory_collector)
                }
            )},
        %% Collects microstate accounting metrics using erlang:statistics(microstate_accounting).
        {vm_msacc_collector,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(vm_msacc_collector)
                }
            )}
    ].

desc("prometheus") -> ?DESC(prometheus);
desc(_) -> undefined.

convert_headers(Headers, #{make_serializable := true}) ->
    Headers;
convert_headers(<<>>, _Opts) ->
    [];
convert_headers(Headers, _Opts) when is_map(Headers) ->
    maps:fold(
        fun(K, V, Acc) ->
            [{binary_to_list(K), binary_to_list(V)} | Acc]
        end,
        [],
        Headers
    );
convert_headers(Headers, _Opts) when is_list(Headers) ->
    Headers.

validate_push_gateway_server(Url) ->
    case uri_string:parse(Url) of
        #{scheme := S} when S =:= "https" orelse S =:= "http" -> ok;
        _ -> {error, "Invalid url"}
    end.

%% for CI test, CI don't load the whole emqx_conf_schema.
translation(Name) ->
    %% translate 'vm_dist_collector', 'mnesia_collector', 'vm_statistics_collector',
    %% 'vm_system_info_collector', 'vm_memory_collector', 'vm_msacc_collector'
    %% to prometheus environments
    emqx_conf_schema:translation(Name).
