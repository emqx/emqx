-module(emqx_slow_subs_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).

namespace() -> "slow_subs".

roots() -> ["slow_subs"].

fields("slow_subs") ->
    [
        {enable, sc(boolean(), false, enable)},
        {threshold,
            sc(
                emqx_schema:duration_ms(),
                "500ms",
                threshold
            )},
        {expire_interval,
            sc(
                emqx_schema:duration_ms(),
                "300s",
                expire_interval
            )},
        {top_k_num,
            sc(
                pos_integer(),
                10,
                top_k_num
            )},
        {stats_type,
            sc(
                ?ENUM([whole, internal, response]),
                whole,
                stats_type
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
    ?HOCON(Type, #{default => Default, desc => ?DESC(Desc)}).
