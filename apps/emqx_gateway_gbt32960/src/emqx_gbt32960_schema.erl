%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gbt32960_schema).

-behaviour(hocon_schema).

-include("emqx_gbt32960.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, roots/0, fields/1, desc/1]).

namespace() -> "gateway_gbt32960".

roots() -> [].

fields(gbt32960) ->
    [
        {mountpoint, emqx_gateway_schema:mountpoint(?DEFAULT_MOUNTPOINT)},
        {retry_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => <<"8s">>,
                    desc => ?DESC(retry_interval)
                }
            )},
        {max_retry_times,
            sc(
                non_neg_integer(),
                #{
                    default => 3,
                    desc => ?DESC(max_retry_times)
                }
            )},
        {message_queue_len,
            sc(
                non_neg_integer(),
                #{
                    default => 10,
                    desc => ?DESC(message_queue_len)
                }
            )},
        {listeners, sc(ref(emqx_gateway_schema, tcp_listeners), #{desc => ?DESC(tcp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options().

desc(gbt32960) ->
    "The GBT-32960 gateway";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
