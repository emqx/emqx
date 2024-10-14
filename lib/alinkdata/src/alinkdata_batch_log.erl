%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 06. 4月 2023 上午11:18
%%%-------------------------------------------------------------------
-module(alinkdata_batch_log).

-behaviour(gen_server).

-export([insert/1, batch/0]).
-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(FLUSH_SECOND, 30).

-record(state, {}).

insert(LogSql) ->
    ets:insert(?MODULE, {erlang:system_time(), LogSql}).


batch() ->
    Sqls = lists:map(fun({_, S}) -> S end, ets:tab2list(?MODULE)),
    ets:delete_all_objects(?MODULE),
    do_batch(Sqls, []).
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?MODULE, [named_table, set, public]),
    erlang:start_timer(?FLUSH_SECOND * 1000, self(), batch),
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({timeout, _, batch}, State) ->
    catch case batch() of
        {'EXIT', Reason} ->
            logger:error("batch failed:~p", [Reason]);
        _ ->
            ok
    end,
    erlang:start_timer(?FLUSH_SECOND * 1000, self(), batch),
    {noreply,State};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_batch([], []) ->
    ok;
do_batch([], BatchSqls) ->
    batch_to_db(BatchSqls);
do_batch([S|Ss], BatchSqls) ->
    case length(BatchSqls) of
        Len when Len < 50 ->
            do_batch(Ss, [S|BatchSqls]);
        _ ->
            batch_to_db(BatchSqls),
            do_batch(Ss, [])
    end.


batch_to_db(BatchSqls) ->
    InsertSqls =
        lists:foldl(
            fun(BatchSql, Acc) ->
                <<Acc/binary, BatchSql/binary, ";">>
            end, <<>>, BatchSqls),
    case alinkdata:query_mysql_format_map(default, InsertSqls) of
        ok ->
            ok;
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("insert log failed:~p", [Reason])
    end.