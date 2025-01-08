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
    validate_url/1,
    is_recommend_type/1
]).

namespace() -> prometheus.

roots() ->
    [
        {prometheus,
            ?HOCON(
                ?UNION(setting_union_schema()),
                #{translate_to => ["prometheus"], default => #{}}
            )}
    ].

fields(recommend_setting) ->
    [
        {enable_basic_auth,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    importance => ?IMPORTANCE_HIGH,
                    desc => ?DESC(enable_basic_auth)
                }
            )},
        {push_gateway,
            ?HOCON(
                ?R_REF(push_gateway),
                #{
                    required => false,
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(push_gateway)
                }
            )},
        {collectors,
            ?HOCON(?R_REF(collectors), #{
                required => false,
                importance => ?IMPORTANCE_LOW,
                desc => ?DESC(collectors)
            })}
    ];
fields(push_gateway) ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    %% importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(push_gateway_enable)
                }
            )},
        {url,
            ?HOCON(
                string(),
                #{
                    required => false,
                    default => <<"http://127.0.0.1:9091">>,
                    validator => fun ?MODULE:validate_url/1,
                    desc => ?DESC(push_gateway_url)
                }
            )},
        {interval,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    required => false,
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
                    required => false,
                    desc => ?DESC(job_name)
                }
            )}
    ];
fields(collectors) ->
    [
        {vm_dist,
            ?HOCON(
                hoconsc:enum([disabled, enabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(vm_dist_collector)
                }
            )},
        %% Mnesia metrics mainly using mnesia:system_info/1
        {mnesia,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(mnesia_collector)
                }
            )},
        %% Collects Erlang VM metrics using erlang:statistics/1.
        {vm_statistics,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(vm_statistics_collector)
                }
            )},
        %% Collects Erlang VM metrics using erlang:system_info/1.
        {vm_system_info,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(vm_system_info_collector)
                }
            )},
        %% Collects information about memory dynamically allocated by the Erlang VM using erlang:memory/0,
        %% it also provides basic (D)ETS statistics.
        {vm_memory,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(vm_memory_collector)
                }
            )},
        %% Collects microstate accounting metrics using erlang:statistics(microstate_accounting).
        {vm_msacc,
            ?HOCON(
                hoconsc:enum([enabled, disabled]),
                #{
                    default => disabled,
                    required => true,
                    desc => ?DESC(vm_msacc_collector)
                }
            )}
    ];
fields(legacy_deprecated_setting) ->
    [
        {push_gateway_server,
            ?HOCON(
                string(),
                #{
                    default => <<"http://127.0.0.1:9091">>,
                    required => true,
                    validator => fun ?MODULE:validate_url/1,
                    desc => ?DESC(legacy_push_gateway_server)
                }
            )},
        {interval,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    required => true,
                    desc => ?DESC(legacy_interval)
                }
            )},
        {headers,
            ?HOCON(
                typerefl:alias("map", list({string(), string()}), #{}, [string(), string()]),
                #{
                    default => #{},
                    required => false,
                    converter => fun ?MODULE:convert_headers/2,
                    desc => ?DESC(legacy_headers)
                }
            )},
        {job_name,
            ?HOCON(
                binary(),
                #{
                    default => <<"${name}/instance/${name}~${host}">>,
                    required => true,
                    desc => ?DESC(legacy_job_name)
                }
            )},
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    required => true,
                    %% importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(legacy_enable)
                }
            )},
        {vm_dist_collector,
            ?HOCON(
                hoconsc:enum([disabled, enabled]),
                #{
                    default => disabled,
                    required => true,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(legacy_vm_dist_collector)
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
                    desc => ?DESC(legacy_mnesia_collector)
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
                    desc => ?DESC(legacy_vm_statistics_collector)
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
                    desc => ?DESC(legacy_vm_system_info_collector)
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
                    desc => ?DESC(legacy_vm_memory_collector)
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
                    desc => ?DESC(legacy_vm_msacc_collector)
                }
            )}
    ].

setting_union_schema() ->
    RecommendSetting = ?R_REF(recommend_setting),
    LegacySetting = ?R_REF(legacy_deprecated_setting),
    fun
        (all_union_members) ->
            [RecommendSetting, LegacySetting];
        ({value, Setting}) ->
            case is_recommend_type(Setting) of
                true -> [RecommendSetting];
                false -> [LegacySetting]
            end
    end.

%% For it to be considered as new schema,
%% all keys must be included in the new configuration.
is_recommend_type(Setting) ->
    case maps:keys(Setting) of
        [] ->
            true;
        Keys ->
            NewKeys = fields(recommend_setting),
            Fun = fun(Key0) ->
                Key = binary_to_existing_atom(Key0),
                lists:keymember(Key, 1, NewKeys)
            end,
            lists:all(Fun, Keys)
    end.

desc(prometheus) -> ?DESC(prometheus);
desc(collectors) -> ?DESC(collectors);
desc(legacy_deprecated_setting) -> ?DESC(legacy_deprecated_setting);
desc(recommend_setting) -> ?DESC(recommend_setting);
desc(push_gateway) -> ?DESC(push_gateway);
desc(_) -> undefined.

convert_headers(undefined, _) ->
    undefined;
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

validate_url(Url) ->
    case uri_string:parse(Url) of
        #{scheme := S} when
            S =:= "https";
            S =:= "http";
            S =:= <<"https">>;
            S =:= <<"http">>
        ->
            ok;
        _ ->
            {error, "Invalid url"}
    end.

%% for CI test, CI don't load the whole emqx_conf_schema.
translation(Name) ->
    %% translate 'vm_dist_collector', 'mnesia_collector', 'vm_statistics_collector',
    %% 'vm_system_info_collector', 'vm_memory_collector', 'vm_msacc_collector'
    %% to prometheus environments
    emqx_conf_schema:translation(Name).
