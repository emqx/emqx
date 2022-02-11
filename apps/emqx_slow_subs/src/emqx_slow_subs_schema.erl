-module(emqx_slow_subs_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1, namespace/0]).

namespace() -> "slow_subs".

roots() -> ["slow_subs"].

fields("slow_subs") ->
    [ {enable, sc(boolean(), false, "switch of this function")}
    , {threshold,
       sc(emqx_schema:duration_ms(),
          "500ms",
          "The latency threshold for statistics, the minimum value is 100ms")}
    , {expire_interval,
       sc(emqx_schema:duration_ms(),
          "300s",
          "The eviction time of the record, which in the statistics record table")}
    , {top_k_num,
       sc(integer(),
          10,
          "The maximum number of records in the slow subscription statistics record table")}
    , {notice_interval,
       sc(emqx_schema:duration_ms(),
          "0s",
          "The interval for pushing statistics table records to the system topic. "
          "publish top-k list to $SYS/brokers/${node}/slow_subs per notice_interval. "
          "publish is disabled if set to 0s."
         )}
    , {notice_qos,
       sc(emqx_schema:qos(),
          0,
          "QoS of notification message in notice topic")}
    , {notice_batch_size,
       sc(integer(),
          100,
          "Maximum information number in one notification")}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Default, Desc) ->
    hoconsc:mk(Type, #{default => Default, desc => Desc}).
