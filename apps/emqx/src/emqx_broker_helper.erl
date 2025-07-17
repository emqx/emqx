%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker_helper).

-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_router.hrl").
-include("emqx_shared_sub.hrl").
-include("logger.hrl").
-include("types.hrl").

-export([start_link/0]).

%% APIs
-export([
    register_sub/2,
    lookup_subid/1,
    lookup_subpid/1,
    assign_sub_shard/1,
    assigned_sub_shards/1,
    unassign_sub_shard/2,
    shard_capacity/0
]).

%% Stats fun
-export([stats_fun/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Internal APIs
-export([clean_down/1]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(HELPER, ?MODULE).
-define(SUBID, emqx_subid).
-define(SUBMON, emqx_submon).

-define(SHARD_CAPACITY, 1024).
-define(SHARD_REFILL, 768).

-define(BATCH_SIZE, 1000).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?HELPER}, ?MODULE, [], []).

-spec register_sub(pid(), emqx_types:subid()) -> ok.
register_sub(SubPid, SubId) when is_pid(SubPid) ->
    case ets:lookup(?SUBMON, SubPid) of
        [] ->
            _ = erlang:send(?HELPER, {register_sub, SubPid, SubId}),
            ok;
        [{_, SubId}] ->
            ok;
        _Other ->
            error(subid_conflict)
    end.

-spec lookup_subid(pid()) -> option(emqx_types:subid()).
lookup_subid(SubPid) when is_pid(SubPid) ->
    emqx_utils_ets:lookup_value(?SUBMON, SubPid).

-spec lookup_subpid(emqx_types:subid()) -> option(pid()).
lookup_subpid(SubId) ->
    emqx_utils_ets:lookup_value(?SUBID, SubId).

-doc """
Assign `Topic` subscriber to some shard, where shard is simply a number between
0 and positive infinity. Assignment strives to be optimal: for the given number
of `Topic` subscribers (i.e. `assign_sub_shard(Topic)` calls) there should be as
few shards assigned as possible, where each shard has fixed limited capacity
(see `shard_capacity/0`). This limit is a soft limit, meaning going over capacity
a bit is allowed, however it's expected to happen only under extremely high load.

This operation is not idempotent.

Shard numbers are used to partition subscribers into separate records in
`emqx_subscriber` bag table, to smooth out O(N) insertion cost and O(N) memory
requirement during record lookup.
""".
-spec assign_sub_shard(emqx_types:topic()) -> non_neg_integer().
assign_sub_shard(Topic) ->
    %% Lookup current shard:
    %% Current shard is the shard that's being filled up now, _each_ assignment
    %% goes into it.
    Ix = ets:lookup_element(?HELPER, Topic, 2, 0),
    Shard = {Topic, Ix},
    %% Make assignment in the current shard:
    case ets:update_counter(?HELPER, Shard, {2, 1}, {'_', 0}) of
        N when N =< ?SHARD_CAPACITY ->
            Ix;
        _ ->
            %% Current shard is over capacity. Switch up to the next shard.
            %% Subject to races.
            _ = ets:update_counter(?HELPER, Topic, {2, 1, Ix + 1, Ix + 1}, Shard),
            Ix
    end.

-doc """
Unassign `Topic` from the given shard `Ix`, where `Ix` was obtained before by
calling `assign_sub_shard(Topic)`.

\"Unassignment\" is performed in such a way that further assignments will tend
towards optimality, as much as practically possible. This is why the logic is so
complex.

This operation is not idempotent.
""".
-spec unassign_sub_shard(emqx_types:topic(), non_neg_integer()) -> _Left :: non_neg_integer().
unassign_sub_shard(Topic, ShardIx) when is_integer(ShardIx) ->
    Shard = {Topic, ShardIx},
    %% Unassign subscriber from the given shard:
    case ets:update_counter(?HELPER, Shard, {2, -1, 0, 0}, {'_', 1}) of
        N when N =< ?SHARD_REFILL ->
            %% After unassigment, shard has number of susbcribers fewer than threshold.
            %% It is now a candidate for refilling. To keep assignments close to being
            %% optimal, current shard can only switched down but not up, because it
            %% makes "switch-up" to a new, never seen before shard less likely during
            %% assign.
            case ets:lookup_element(?HELPER, Topic, 2, 0) of
                Ix when Ix > ShardIx ->
                    %% Current shard is higher.
                    %% Switch down to this shard atomically.
                    Spec = [{{Topic, '$1'}, [{'>', '$1', ShardIx}], [{const, Shard}]}],
                    _ = ets:select_replace(?HELPER, Spec),
                    ok;
                Ix when Ix < ShardIx andalso N =:= 0 ->
                    %% Current shard is lower, this one is empty.
                    %% Likely not going to be filled soon. Delete the shard counter
                    %% record atomically.
                    _ = ets:delete_object(?HELPER, {Shard, 0}),
                    ok;
                ShardIx when ShardIx > 0 andalso N =:= 0 ->
                    %% This is the current shard, and is now empty.
                    %% It's also not the zeroth shard, so switch-down is possible.
                    %% Switch down to previous shard atomically and delete the shard
                    %% counter record atomically.
                    Spec = [{Shard, [], [{const, {Topic, ShardIx - 1}}]}],
                    _ = ets:select_replace(?HELPER, Spec),
                    _ = ets:delete_object(?HELPER, {Shard, 0}),
                    ok;
                _ ->
                    %% Current shard is either this or lower.
                    ok
            end,
            N;
        N ->
            N
    end.

-spec assigned_sub_shards(emqx_types:topic()) ->
    [{_Shard :: non_neg_integer(), non_neg_integer()}].
assigned_sub_shards(Topic) ->
    ets:select(?HELPER, ets:fun2ms(fun({{T, ShardIx}, C}) when T == Topic -> {ShardIx, C} end)).

-spec shard_capacity() -> pos_integer().
shard_capacity() ->
    ?SHARD_CAPACITY.

%%--------------------------------------------------------------------
%% Stats fun
%%--------------------------------------------------------------------

stats_fun() ->
    safe_update_stats(subscriber_val(), 'subscribers.count', 'subscribers.max'),
    safe_update_stats(subscription_count(), 'subscriptions.count', 'subscriptions.max'),
    safe_update_stats(
        durable_subscription_count(),
        'durable_subscriptions.count',
        'durable_subscriptions.max'
    ),
    safe_update_stats(table_size(?SUBOPTION), 'suboptions.count', 'suboptions.max').

safe_update_stats(undefined, _Stat, _MaxStat) ->
    ok;
safe_update_stats(Val, Stat, MaxStat) when is_integer(Val) ->
    emqx_stats:setstat(Stat, MaxStat, Val).

%% N.B.: subscriptions from durable sessions are not tied to any particular node.
%% Therefore, do not sum them with node-local subscriptions.
subscription_count() ->
    table_size(?SUBSCRIPTION).

durable_subscription_count() ->
    emqx_persistent_session_bookkeeper:get_subscription_count().

subscriber_val() ->
    sum_subscriber(table_size(?SUBSCRIBER), table_size(?SHARED_SUBSCRIBER)).

sum_subscriber(undefined, undefined) -> undefined;
sum_subscriber(undefined, V2) when is_integer(V2) -> V2;
sum_subscriber(V1, undefined) when is_integer(V1) -> V1;
sum_subscriber(V1, V2) when is_integer(V1), is_integer(V2) -> V1 + V2.

table_size(Tab) when is_atom(Tab) -> ets:info(Tab, size).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(message_queue_data, off_heap),
    %% Helper table
    ok = emqx_utils_ets:new(?HELPER, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% SubId: SubId -> SubPid
    ok = emqx_utils_ets:new(?SUBID, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% SubMon: SubPid -> SubId
    ok = emqx_utils_ets:new(?SUBMON, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% Stats timer
    ok = emqx_stats:update_interval(broker_stats, fun ?MODULE:stats_fun/0),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "emqx_broker_helper_unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "emqx_broker_helper_unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({register_sub, SubPid, SubId}, State) ->
    ok = collect_and_handle([{SubId, SubPid}], []),
    {noreply, State};
handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State) ->
    ok = collect_and_handle([], [SubPid]),
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "emqx_broker_helper_unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(broker_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

collect_and_handle(Reg0, Down0) ->
    {Regs, Down} = collect_messages(Reg0, Down0),
    %% handle register before handle down to avoid race condition
    %% because down message is always the last one from a process
    ok = handle_registrations(Regs),
    ok = handle_down(Down).

collect_messages(Reg, Down) ->
    collect_messages(Reg, Down, ?BATCH_SIZE).

%% Collect both register_sub and 'DOWN' messages in a loop.
%% There is no other message sent to this process, so the
%% 'receive' should not have to scan the mailbox.
collect_messages(Reg, Down, 0) ->
    {Reg, Down};
collect_messages(Reg, Down, N) ->
    receive
        {register_sub, Pid, Id} ->
            collect_messages([{Id, Pid} | Reg], Down, N - 1);
        {'DOWN', _MRef, process, Pid, _Reason} ->
            collect_messages(Reg, [Pid | Down], N - 1)
    after 0 ->
        {Reg, Down}
    end.

handle_registrations(Regs) ->
    lists:foreach(fun handle_register/1, Regs).

handle_register({SubId, SubPid}) ->
    record_subscription(SubId, SubPid),
    monitor_subscriber(SubId, SubPid).

record_subscription(undefined, _) ->
    true;
record_subscription(SubId, SubPid) ->
    ets:insert(?SUBID, {SubId, SubPid}).

monitor_subscriber(SubId, SubPid) ->
    case ets:member(?SUBMON, SubPid) of
        false ->
            _MRef = erlang:monitor(process, SubPid),
            ets:insert(?SUBMON, {SubPid, SubId});
        true ->
            true
    end.

handle_down(SubPids) ->
    ok = emqx_pool:async_submit(
        fun lists:foreach/2, [fun ?MODULE:clean_down/1, SubPids]
    ).

clean_down(SubPid) ->
    try
        case ets:lookup(?SUBMON, SubPid) of
            [{_, SubId}] ->
                true = ets:delete(?SUBMON, SubPid),
                true =
                    (SubId =:= undefined) orelse
                        ets:delete_object(?SUBID, {SubId, SubPid}),
                emqx_broker:subscriber_down(SubPid);
            [] ->
                ok
        end
    catch
        error:badarg -> ok
    end.
