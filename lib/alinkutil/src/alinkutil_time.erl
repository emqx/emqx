%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 上午11:18
%%%-------------------------------------------------------------------
-module(alinkutil_time).
-author("yqfclid").

%% API
-export([
    timestamp/0,
    timestamp/1,
    flush_interval/0,
    flush_interval/1
]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(timestamp() -> integer()).
timestamp() ->
    erlang:system_time(second).

-spec(timestamp(Type :: nano | micro | milli | second) -> integer()).
timestamp(nano) ->
    erlang:system_time();
timestamp(micro) ->
    erlang:system_time(microsecond);
timestamp(milli) ->
    erlang:system_time(millisecond);
timestamp(second) ->
    erlang:system_time(second).

%% return integer of the rest milliseconds of the day.
-spec(flush_interval() -> integer()).
flush_interval() ->
    flush_interval(1).

-spec(flush_interval(D :: integer()) -> integer()).
flush_interval(D) ->
    {H, M, S} = erlang:time(),
    D * 24 * 3600 - H * 3600 - M * 60 - S.


%%%===================================================================
%%% Internal functions
%%%===================================================================