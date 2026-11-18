%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_cleaner).

-moduledoc """
A worker of the `emqx_cm_pool` pool. It cleans up after channels that are down.

`emqx_cm` sends the pids of the down channels to a random worker of the pool. A
worker takes the pids that are queued in its mailbox, up to its batch size, and
calls `emqx_cm:clean_down/1` once for all of them. That function removes the
channels from the global registry with one call, which is a round trip to a core
node on a replicant.

The batch size is `emqx_cm:batch_size()`. A worker reads it when it starts.
""".

-behaviour(gen_batch_server).

-include("emqx_cm.hrl").
-include("logger.hrl").

-export([start_link/2, async_clean_down/1]).

-ifdef(TEST).
-export([sync/0]).
-endif.

%% gen_batch_server callbacks
-export([init/1, handle_batch/2, terminate/2]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(Pool, Id) ->
    BatchSize = emqx_cm:batch_size(),
    gen_batch_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        %% gen_batch_server takes its own options only from the head of the list.
        %% The same minimum and maximum keep the batch size constant. The worker
        %% never waits for a batch to fill: it stops when its mailbox is empty.
        [{min_batch_size, BatchSize}, {max_batch_size, BatchSize}, {hibernate_after, 1000}]
    ).

-doc """
Clean up after the channels asynchronously, in one worker of the pool.

The pids are one message for the worker, so a batch of the worker can exceed its
batch size by the length of the last list that it takes.
""".
-spec async_clean_down([pid()]) -> ok.
async_clean_down(Pids) ->
    gen_batch_server:cast_batch(gproc_pool:pick_worker(?CM_POOL), Pids).

-ifdef(TEST).
%% Return when every worker has handled what was queued before this call.
sync() ->
    lists:foreach(
        fun({_, Worker}) -> ok = gen_batch_server:call(Worker, sync, infinity) end,
        gproc_pool:active_workers(?CM_POOL)
    ).
-endif.

%%--------------------------------------------------------------------
%% gen_batch_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_batch(Ops, State) ->
    {Pids, Actions} = lists:foldr(fun handle_op/2, {[], []}, Ops),
    ok = clean_down(Pids),
    {ok, Actions, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_op({cast, Pid}, {Pids, Actions}) when is_pid(Pid) ->
    {[Pid | Pids], Actions};
handle_op({call, From, sync}, {Pids, Actions}) ->
    %% gen_batch_server sends the replies after `handle_batch/2` returns.
    {Pids, [{reply, From, ok} | Actions]};
handle_op({call, From, Req}, {Pids, Actions}) ->
    ?SLOG(error, #{msg => "emqx_cm_cleaner_unexpected_call", call => Req}),
    {Pids, [{reply, From, ignored} | Actions]};
handle_op(Op, Acc) ->
    ?SLOG(error, #{msg => "emqx_cm_cleaner_unexpected_message", message => Op}),
    Acc.

clean_down([]) ->
    ok;
clean_down(Pids) ->
    try
        ok = emqx_cm:clean_down(Pids)
    catch
        Class:Reason:Stacktrace ->
            %% Keep the worker alive. Its exit would lose the pids that are
            %% queued in its mailbox, and their channels would stay in the tables.
            ?SLOG(error, #{
                msg => "failed_to_clean_down_channels",
                count => length(Pids),
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end.
