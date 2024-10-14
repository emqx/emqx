%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 12. 6月 2023 下午10:36
%%%-------------------------------------------------------------------
-module(alinkcore_healthy).

%% API
-export([
    check_oom/0,
    check_oom/1
]).

%%%===================================================================
%%% API
%%%===================================================================
check_oom() ->
    check_oom(self()).

check_oom(Pid) ->
    case process_info(Pid, [message_queue_len, total_heap_size]) of
        undefined -> ok;
        [{message_queue_len, QLen}, {total_heap_size, _HeapSize}] ->
            case QLen < 1000 of
                true ->
                    ok;
                _ ->
                    shutdown
            end
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================