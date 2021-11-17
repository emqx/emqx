-module(emqx_st_statistics_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

roots() -> ["emqx_st_statistics"].

fields("emqx_st_statistics") ->
    [ {threshold_time,
       sc(emqx_schema:duration_ms(),
          "10s",
          "threshold time of slow topics statistics")}
    , {ignore_before_create,
       sc(boolean(),
          true,
          "ignore the messages that before than session created")}
    , {time_window,
       sc(emqx_schema:duration_ms(),
          "5m",
          "time window of slow topics statistics")}
    , {max_log_num,
       sc(integer(),
          500,
          "maximum number of slow topics log per time window,"
          "log will clear when enter new time window")}
    , {top_k_num,
       sc(integer(),
          500,
          "maximum number of top-K record for slow topics, update from logs"         )}
    , {notice_topic,
       sc(string(),
          "$slow_topics",
          "topic for notification")}
    , {notice_qos, sc(range(0, 2), 0, "QoS of notification message")}
    , {notice_batch_size,
       sc(integer(),
          500,
          "maximum information number in one notification")}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Default, Desc) ->
    hoconsc:mk(Type, #{default => Default, desc => Desc}).
