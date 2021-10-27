-module(emqx_st_statistics_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

roots() -> ["emqx_st_statistics"].

fields("emqx_st_statistics") ->
    [ {threshold_time, sc(emqx_schema:duration_ms(), "10s")}
    , {ignore_before_create, sc(boolean(), true)}
    , {time_window, sc(emqx_schema:duration_ms(), "5m")}
    , {max_log_num, sc(integer(), 500)}
    , {top_k_num, sc(integer(), 500)}
    , {notice_topic, sc(string(), "$slow_topics")}
    , {notice_qos, sc(range(0, 2), 0)}
    , {notice_batch_size, sc(integer(), 500)}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Default) ->
    hoconsc:mk(Type, #{default => Default}).
