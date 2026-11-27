%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cm_worker).

-moduledoc """
A worker of the `emqx_cm_pool` pool. The workers monitor the channel processes
of this node, and clean up after the channels that are down.

`emqx_cm:register_channel/3` sends the pid of a new channel to one worker, which
monitors it. The pid selects the worker, so a channel always belongs to the same
worker. When channels exit, the worker takes the `DOWN` messages that are queued
in its mailbox, up to its batch size, and calls `emqx_cm:clean_down/1` once for
all of them. That function removes the channels from the global registry with
one call, which is a round trip to a core node on a replicant.

The batch size is `node.channel_cleanup_batch_size`, at most 1000. A worker reads
it when it starts.

A worker that starts again monitors its channels again: it finds them in the
channel table. So it also cleans up after the channels that exited while it was
down, and after the channels whose `DOWN` message was lost with the old process.
""".

-behaviour(gen_batch_server).

-include("emqx_cm.hrl").
-include("logger.hrl").

-export([start_link/2, pool_size/0, monitor_channel/1]).

-ifdef(TEST).
-export([sync/0, worker_id/1, monitor_channel/2]).
-endif.

%% gen_batch_server callbacks
-export([init/1, handle_batch/2, terminate/2]).

%% One batch is one call to a core node on a replicant, so a batch must stay
%% small enough for that call. A larger configured value, such as the default
%% of an older release, is capped.
-define(MAX_BATCH_SIZE, 1000).

-define(WORKER_NAMES, {?MODULE, worker_names}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(?CM_POOL = Pool, Id) ->
    BatchSize = batch_size(),
    gen_batch_server:start_link(
        {local, worker_name(Id)},
        ?MODULE,
        [Pool, Id],
        %% gen_batch_server takes its own options only from the head of the list.
        %% The same minimum and maximum keep the batch size constant. The worker
        %% never waits for a batch to fill: it stops when its mailbox is empty.
        [
            {min_batch_size, BatchSize},
            {max_batch_size, BatchSize},
            {hibernate_after, 1000},
            {spawn_opt, [{message_queue_data, off_heap}]}
        ]
    ).

-doc "The number of workers. `monitor_channel/1` relies on it to select a worker.".
-spec pool_size() -> pos_integer().
pool_size() ->
    emqx_vm:schedulers().

-doc """
Monitor the channel process, and clean up after it when it exits.

Call it after the insert of the channel into `?CHAN_CONN_TAB`. When the worker is
not running, the message is lost, and the worker finds the channel in that table
when it starts.
""".
-spec monitor_channel(pid()) -> ok.
monitor_channel(ChanPid) ->
    monitor_channel(worker_id(ChanPid), ChanPid).

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
    ok = monitor_registered_channels(Id),
    {ok, #{pool => Pool, id => Id}}.

handle_batch(Ops, State) ->
    {Down, Actions} = lists:foldl(fun handle_op/2, {[], []}, Ops),
    %% A channel that was monitored two times is in the batch two times.
    ok = clean_down(lists:usort(Down)),
    {ok, Actions, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

monitor_channel(WorkerId, ChanPid) ->
    gen_batch_server:cast(worker_name(WorkerId), {monitor, ChanPid}).

worker_id(ChanPid) ->
    erlang:phash2(ChanPid, pool_size()) + 1.

worker_name(Id) ->
    element(Id, worker_names()).

%% The registered names of the workers, by worker id, in a persistent term:
%% `monitor_channel/1` needs a name for each new channel. The names stay the
%% same while the node runs. The first worker that starts makes them.
worker_names() ->
    case persistent_term:get(?WORKER_NAMES, undefined) of
        undefined ->
            Names = list_to_tuple([
                emqx_utils:proc_name(?CM_POOL, Id)
             || Id <- lists:seq(1, pool_size())
            ]),
            ok = persistent_term:put(?WORKER_NAMES, Names),
            Names;
        Names ->
            Names
    end.

batch_size() ->
    min(emqx:get_config([node, channel_cleanup_batch_size], ?MAX_BATCH_SIZE), ?MAX_BATCH_SIZE).

%% Monitor the channels of this worker that are in the channel table. There are
%% some only when the worker starts again. A monitor of a channel that is
%% already down gives its `DOWN` message at once.
monitor_registered_channels(Id) ->
    PoolSize = pool_size(),
    try
        ets:foldl(
            fun(#chan_conn{pid = Pid}, ok) ->
                _ = erlang:phash2(Pid, PoolSize) + 1 =:= Id andalso erlang:monitor(process, Pid),
                ok
            end,
            ok,
            ?CHAN_CONN_TAB
        )
    catch
        %% No table: `emqx_cm`, which owns it, is not running. No channel is registered.
        error:badarg -> ok
    end.

handle_op({cast, {monitor, Pid}}, Acc) when is_pid(Pid) ->
    _ = erlang:monitor(process, Pid),
    Acc;
handle_op({info, {'DOWN', _MRef, process, Pid, _Reason}}, {Down, Actions}) ->
    {[Pid | Down], Actions};
handle_op({call, From, sync}, {Down, Actions}) ->
    %% gen_batch_server sends the replies after `handle_batch/2` returns.
    {Down, [{reply, From, ok} | Actions]};
handle_op({call, From, Req}, {Down, Actions}) ->
    ?SLOG(error, #{msg => "emqx_cm_worker_unexpected_call", call => Req}),
    {Down, [{reply, From, ignored} | Actions]};
handle_op(Op, Acc) ->
    ?SLOG(error, #{msg => "emqx_cm_worker_unexpected_message", message => Op}),
    Acc.

clean_down([]) ->
    ok;
clean_down(Pids) ->
    try
        ok = emqx_cm:clean_down(Pids)
    catch
        Class:Reason:Stacktrace ->
            %% Keep the worker alive: an exit drops the monitors and the mailbox.
            %% The next start finds only the channels that are still in the table.
            ?SLOG(error, #{
                msg => "failed_to_clean_down_channels",
                count => length(Pids),
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end.
