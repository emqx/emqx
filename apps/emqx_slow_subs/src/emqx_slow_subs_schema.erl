-module(emqx_slow_subs_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).

namespace() -> "slow_subs".

roots() -> ["slow_subs"].

fields("slow_subs") ->
    [
        {enable, sc(boolean(), false, "Enable this feature.")},
        {threshold,
            sc(
                emqx_schema:duration_ms(),
                "500ms",
                "The latency threshold for statistics, the minimum value is 100ms."
            )},
        {expire_interval,
            sc(
                emqx_schema:duration_ms(),
                "300s",
                "The eviction time of the record, which in the statistics record table."
            )},
        {top_k_num,
            sc(
                pos_integer(),
                10,
                "The maximum number of records in the slow subscription statistics record table."
            )},
        {stats_type,
            sc(
                hoconsc:union([whole, internal, response]),
                whole,
                "The method to calculate the latency."
            )}
    ].

desc("slow_subs") ->
    "Configuration for `slow_subs` feature.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Default, Desc) ->
    hoconsc:mk(Type, #{default => Default, desc => Desc}).
