%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 下午3:23
%%%-------------------------------------------------------------------
-module(alinkutil_timer).
-author("yqfclid").

%% API
-export([
    start_timer/2,
    start_timer/3,
    cancel_timer/1
]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_timer(integer(), term()) -> reference()).
start_timer(Interval, Msg) ->
    start_timer(Interval, self(), Msg).

-spec(start_timer(integer(), pid() | atom(), term()) -> reference()).
start_timer(Interval, Dest, Msg) ->
    erlang:start_timer(erlang:ceil(Interval), Dest, Msg).

-spec(cancel_timer(reference()) -> ok).
cancel_timer(Timer) when is_reference(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive {timeout, Timer, _} -> ok after 0 -> ok end;
        _ -> ok
    end;
cancel_timer(_) -> ok.
%%%===================================================================
%%% Internal functions
%%%===================================================================