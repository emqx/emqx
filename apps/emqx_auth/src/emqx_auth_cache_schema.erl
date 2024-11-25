%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_cache_schema).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    fill_defaults/1,
    default_config/0
]).

namespace() -> auth_cache.

%% @doc auth cache schema is not exported but directly used
roots() -> [].

fields(config) ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("enable"), default => false})},
        {cache_ttl,
            mk(emqx_schema:timeout_duration_ms(), #{
                desc => ?DESC("cache_ttl"), default => <<"1m">>
            })},
        {cleanup_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                desc => ?DESC("cleanup_interval"),
                default => <<"1m">>,
                importance => ?IMPORTANCE_HIDDEN
            })},
        {stat_update_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                desc => ?DESC("cleanup_interval"),
                default => <<"1m">>,
                importance => ?IMPORTANCE_HIDDEN
            })},
        {max_size,
            mk(hoconsc:union([unlimited, non_neg_integer()]), #{
                desc => ?DESC("max_size"),
                default => 1000000
            })},
        {max_memory,
            mk(hoconsc:union([unlimited, emqx_schema:bytesize()]), #{
                desc => ?DESC("max_memory"),
                default => <<"100MB">>
            })}
    ];
fields(rate) ->
    [
        {rate, ?HOCON(float(), #{desc => ?DESC("rate")})},
        {rate_max, ?HOCON(float(), #{desc => ?DESC("rate_max")})},
        {rate_last5m, ?HOCON(float(), #{desc => ?DESC("rate_last5m")})}
    ];
fields(counter) ->
    [
        {value, ?HOCON(integer(), #{desc => ?DESC("counter_value")})},
        {rate, ?HOCON(?R_REF(rate), #{desc => ?DESC("counter_rate")})}
    ];
fields(metrics) ->
    [
        {hits, ?HOCON(?R_REF(counter), #{desc => ?DESC("metrics_hits")})},
        {misses, ?HOCON(?R_REF(counter), #{desc => ?DESC("metrics_hits")})},
        {inserts, ?HOCON(?R_REF(counter), #{desc => ?DESC("metrics_hits")})},
        {size, ?HOCON(integer(), #{desc => ?DESC("metric_size")})},
        {memory, ?HOCON(integer(), #{desc => ?DESC("metric_memory")})}
    ];
fields(node_metrics) ->
    [
        {node, ?HOCON(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})},
        {metrics, ?HOCON(?R_REF(metrics), #{desc => ?DESC("metrics")})}
    ];
fields(status) ->
    [
        {metrics, ?HOCON(?R_REF(metrics), #{desc => ?DESC("status_metrics")})},
        {node_metrics,
            ?HOCON(?ARRAY(?R_REF(node_metrics)), #{desc => ?DESC("status_node_metrics")})}
    ].

desc(config) -> ?DESC("auth_cache_config");
desc(metrics) -> ?DESC("auth_cache_metrics").

fill_defaults(Config) ->
    WithRoot = #{<<"auth_cache">> => Config},
    Schema = #{roots => [{auth_cache, hoconsc:mk(?R_REF(config), #{})}]},
    case emqx_hocon:check(Schema, WithRoot, #{make_serializable => true}) of
        {ok, #{<<"auth_cache">> := WithDefaults}} ->
            WithDefaults;
        {error, Reason} ->
            throw(Reason)
    end.

default_config() ->
    #{
        <<"enable">> => false
    }.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
