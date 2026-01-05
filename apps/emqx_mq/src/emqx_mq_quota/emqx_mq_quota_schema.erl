%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_schema).

-export([
    quota_fields/0
]).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% Schema
%%------------------------------------------------------------------------------

quota_fields() ->
    [
        {threshold_percentage,
            mk(range(1, 100), #{
                required => true,
                desc => ?DESC(threshold_percentage),
                default => 10,
                importance => ?IMPORTANCE_HIDDEN
            })},
        {buffer_max_size,
            mk(pos_integer(), #{
                required => true,
                desc => ?DESC(buffer_max_size),
                default => 100,
                importance => ?IMPORTANCE_HIDDEN
            })},
        {buffer_flush_interval,
            mk(emqx_schema:duration_ms(), #{
                required => true,
                desc => ?DESC(buffer_flush_interval),
                default => 1000,
                importance => ?IMPORTANCE_HIDDEN
            })},
        {buffer_pool_size,
            mk(pos_integer(), #{
                required => true,
                desc => ?DESC(buffer_pool_size),
                default => 16,
                importance => ?IMPORTANCE_HIDDEN
            })}
    ].

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).
