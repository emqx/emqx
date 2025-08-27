%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_shared_sub_leader).
-moduledoc """
This module implements logic responsible for allocation of streams
to the members of shared subscription group.

## State machine

```

              ,-->(standby)<----------<-----------.
             /       |                            |            x                        x
           yes       |                            |            ^                        ^
          /          |                            |            |                        |
*--has---{     {DOWN from leader}--normal-->x    yes           |                        |
 leader?  \          |                            |       idle timeout  .---->----#call_destroy{}
           no        |                            |            |       /                |
            \        v                            |            |      /                 |
             '--->(candidate)---random-----has----+---no--(leader,idle)---client--->(leader,!idle)
                                sleep    leader?      .        ^          joined        |
                                                      .        |                        |
                                                   register    `----<--last client -----'
                                                    global               left
                                                     name
```
""".

-behaviour(gen_statem).
-behaviour(emqx_ds_client).

%% API:
-export([start_link/2, wait_leader/1, whereis_leader/1, destroy/1, cfg_heartbeat_interval/0]).

%% behavior callbacks:
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5
]).

%% internal exports:
-export([]).

-export_type([
    stream_state/0,
    borrowers/0,
    allocations/0,
    alloc_plan/0
]).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_format.hrl").
-include("../gen_src/DSSharedSub.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type stream_state() :: #'StreamState'{}.

-define(name(SHARE), {?MODULE, SHARE}).

-type borrower_id() :: emqx_ds_shared_sub_proto:borrower_id().

%% Stream statuses. Stream with states other than `stream_granted` are
%% considered "transient".

%% Stream is being assigned to a borrower, a borrower should confirm the assignment
%% by sending progress or reject the assignment.
-define(stream_granting, stream_granting).
%% Stream is assigned to a borrower, the borrower confirmed the assignment.
-define(stream_granted, stream_granted).
%% Stream is being revoked from a borrower, a borrower should send us final progress
%% for the stream.
-define(stream_revoking, stream_revoking).
%% Stream is revoked from a borrower, the borrower sent us final progress.
%% We wait till the borrower confirms that it has cleaned the data.
-define(stream_revoked, stream_revoked).

-type stream_status() :: ?stream_granting | ?stream_granted | ?stream_revoking | ?stream_revoked.

-record(alloc, {
    status :: stream_status(),
    slab :: emqx_ds:slab(),
    owner :: borrower_id(),
    next_owner :: borrower_id() | undefined,
    deadline :: integer() | undefined
}).
-type alloc() :: #alloc{}.

%% Borrower state:
-record(bs, {
    validity_deadline :: integer()
}).
-type borrower_state() :: #bs{}.

%% FSM states:
-record(st_candidate, {gr :: emqx_types:share()}).
-record(st_standby, {gr :: emqx_types:share(), mref :: reference()}).
-record(st_leader, {gr :: emqx_types:share(), idle = true :: boolean()}).
-record(st_cleanup, {gr :: emqx_types:share(), from :: gen_statem:from()}).

-type state() :: #st_candidate{} | #st_standby{} | #st_leader{} | #st_cleanup{}.

%% Calls/casts:
-record(call_get_leader, {}).
-record(call_destroy, {}).

%% Timeouts:
-record(to_become, {}).
-record(to_checkpoint, {}).
-record(to_borrowers, {}).
-record(to_realloc, {}).
-record(to_idle_stop, {}).

%% Leader data:
-type borrowers() :: #{emqx_ds_shared_sub_proto:borrower_id() => borrower_state()}.
-type allocations() :: #{emqx_ds:stream() => alloc()}.

%% Host state (used in the client callbacks)
-record(hs, {
    s :: emqx_ds_shared_sub_dl:t(),
    realloc_timer :: reference() | now | undefined,
    checkpoint_timer :: reference() | undefined,
    borrowers = #{} :: borrowers(),
    allocations = #{} :: allocations()
}).

-type hs() :: #hs{}.

%% Leader state: client state * host state
-record(ls, {
    c :: emqx_ds_client:t(),
    h :: hs()
}).
-type ls() :: #ls{}.
-type data() :: ls() | emqx_ds_shared_sub:options().

%% Leader uses `emqx_ds_client' to manage streams that belong to the
%% shared topic. This is the client subscription ID:
-define(subid, subid).

-type alloc_plan() :: [{emqx_ds:slab(), emqx_ds:stream(), emqx_ds_shared_sub_proto:borrower_id()}].

-callback stream_alloction_strategy(emqx_ds_shared_sub_dl:t(), borrowers(), allocations()) ->
    alloc_plan().

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_types:share(), emqx_ds_shared_sub:options()) -> {ok, pid()}.
start_link(Share = #share{group = Grp, topic = Topic}, Options) ->
    maybe
        true ?= is_binary(Grp),
        true ?= is_binary(Topic),
        true ?= is_integer(maps:get(start_time, Options, 0)),
        true ?= is_valid_strategy(Options),
        gen_statem:start_link(?MODULE, {Share, Options}, [])
    else
        _ -> {error, badarg}
    end.

-doc """
Query global about pid of the leader.
It doesn't wait for the leader election.
""".
-spec whereis_leader(emqx_types:share()) -> {ok, pid()} | undefined.
whereis_leader(Share) ->
    case global:whereis_name(?name(Share)) of
        undefined ->
            undefined;
        Pid when is_pid(Pid) ->
            {ok, Pid}
    end.

-doc """
Query pid of the cluster leader from the local shared sub worker.
This function waits until election is finished.
""".
-spec wait_leader(pid()) -> {ok, pid()} | emqx_ds:error(_).
wait_leader(Pid) ->
    try
        gen_statem:call(Pid, #call_get_leader{}, 5_000)
    catch
        exit:timeout ->
            {error, recoverable, timeout}
    end.

-doc """
Send a command to the shared sub leader to destroy all data associated
with the shared sub group.
""".
-spec destroy(pid()) -> boolean().
destroy(Pid) ->
    try
        gen_statem:call(Pid, #call_destroy{}, 5_000)
    catch
        exit:timeout ->
            {error, recoverable, timeout}
    end.

-spec cfg_heartbeat_interval() -> pos_integer().
cfg_heartbeat_interval() ->
    emqx_config:get([durable_sessions, shared_subs, heartbeat_interval]).

%%================================================================================
%% behavior callbacks
%%================================================================================

%% gen_statem

callback_mode() ->
    [handle_event_function, state_enter].

-spec init({emqx_types:share(), emqx_ds_shared_sub:options()}) -> {ok, state(), data()}.
init({ShareTF, Options}) ->
    process_flag(trap_exit, true),
    case global:whereis_name(?name(ShareTF)) of
        Pid when is_pid(Pid) ->
            {ok, st_standby(ShareTF, Pid), Options};
        undefined ->
            {ok, #st_candidate{gr = ShareTF}, Options}
    end.

%% Leader:
handle_event(enter, _, #st_leader{gr = Group, idle = Idle}, Data) ->
    enter_leader(Group, Idle, Data);
handle_event(info, #to_checkpoint{}, #st_leader{}, Data) ->
    handle_checkpoint(Data);
handle_event(info, #to_realloc{}, #st_leader{}, Data) ->
    {keep_state, handle_realloc(Data)};
handle_event(state_timeout, #to_borrowers{}, #st_leader{gr = Gr}, Data) ->
    leader_loop(Gr, handle_borrower_timeouts(Data));
handle_event({call, From}, #call_get_leader{}, #st_leader{gr = Gr}, _) ->
    {keep_state_and_data, handle_get_leader(From, Gr)};
handle_event(state_timeout, #to_idle_stop{}, #st_leader{gr = Gr}, _) ->
    ?tp(info, ds_shared_sub_leader_idle_stop, #{group => Gr}),
    {stop, normal};
%%    Borrower messages:
handle_event(
    info, ?borrower_connect_match(BorrowerId, _ShareTopicFilter), #st_leader{gr = Gr}, Data
) ->
    leader_loop(Gr, handle_borrower_connect(Gr, BorrowerId, Data));
handle_event(info, ?borrower_ping_match(BorrowerId), #st_leader{gr = Gr}, Data) ->
    leader_loop(Gr, handle_borrower_ping(Gr, BorrowerId, Data));
handle_event(
    info, ?borrower_update_progress_match(BorrowerId, StreamProgress), #st_leader{gr = Gr}, Data
) ->
    leader_loop(Gr, handle_borrower_progress(BorrowerId, StreamProgress, Data));
handle_event(info, ?borrower_revoke_finished_match(BorrowerId, Stream), #st_leader{gr = Gr}, Data) ->
    leader_loop(Gr, handle_borrower_revoke_finished(BorrowerId, Stream, Data));
handle_event(
    info, ?borrower_disconnect_match(BorrowerId, StreamProgresses), #st_leader{gr = Gr}, Data
) ->
    leader_loop(Gr, handle_borrower_disconnect(BorrowerId, StreamProgresses, Data));
%%    DS client messages:
handle_event(info, Event, #st_leader{gr = Group}, Data) ->
    handle_client_msg(Event, Group, Data);
%% Candidate:
handle_event(enter, _, #st_candidate{gr = Group}, _Options) ->
    enter_candidate(Group);
handle_event(state_timeout, #to_become{}, #st_candidate{gr = Group}, Options) ->
    try_become_leader(Group, Options);
%% Standby:
handle_event(enter, _, #st_standby{}, _) ->
    keep_state_and_data;
handle_event({call, From}, #call_get_leader{}, #st_standby{gr = Gr}, _) ->
    {keep_state_and_data, handle_get_leader(From, Gr)};
handle_event(info, {'DOWN', MRef, _, _, Reason}, #st_standby{gr = Group, mref = MRef}, Data) ->
    case Reason of
        normal ->
            %% Previous leader stopped with Reason `normal`. This can
            %% only happen due to idle timeout. So we shouldn't take
            %% over either.
            {stop, normal};
        _ ->
            {next_state, #st_candidate{gr = Group}, Data}
    end;
%% Cleanup:
handle_event(enter, _, #st_cleanup{gr = Group, from = From}, Data) ->
    do_cleanup(Group, From, Data);
%% Common:
handle_event({call, From}, #call_destroy{}, State, Data) ->
    handle_destroy(From, State, Data);
handle_event(info, {'EXIT', _, shutdown}, _State, _Data) ->
    {stop, shutdown};
handle_event({call, _}, #call_get_leader{}, _State, _Data) ->
    {keep_state_and_data, [postpone]};
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(Reason, State, Data) ->
    ?tp(debug, ?tp_leader_terminate, #{reason => Reason, state => State}),
    case State of
        #st_leader{} ->
            _ = handle_checkpoint(Data),
            ok;
        _ ->
            ok
    end.

%% ds_client

get_current_generation(?subid, Shard, #hs{s = S}) ->
    case emqx_ds_shared_sub_dl:get_current_generation(Shard, S) of
        undefined ->
            0;
        Gen ->
            Gen
    end.

on_advance_generation(?subid, Shard, Generation, HS = #hs{s = S0}) ->
    S = drop_old_streams(
        Shard, Generation, emqx_ds_shared_sub_dl:set_current_generation(Shard, Generation, S0)
    ),
    schedule_checkpoint(later, HS#hs{s = S}).

get_iterator(?subid, Slab, Stream, #hs{s = S}) ->
    case emqx_ds_shared_sub_dl:get_stream_state(Slab, Stream, S) of
        undefined ->
            undefined;
        #'StreamState'{iterator = It} ->
            {ok, It}
    end.

on_new_iterator(?subid, Slab, Stream, Iterator, HS0 = #hs{s = S0}) ->
    StreamState = #'StreamState'{iterator = Iterator},
    HS = HS0#hs{
        s = emqx_ds_shared_sub_dl:set_stream_state(Slab, Stream, StreamState, S0)
    },
    %% Note: we schedule reallocation of tasks immediately upon
    %% discovery of new streams, so when the group is stable its
    %% members will make progress without delay.
    {ignore, schedule_checkpoint(later, schedule_realloc(now, HS))}.

on_unrecoverable_error(?subid, _Slab, _Stream, _Err, HS) ->
    %% This happens when the client fails to make an iterator. Such
    %% iterators are unseen by the borrowrs, so nothing should be done
    %% here:
    HS.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Cleanup
%%--------------------------------------------------------------------------------

handle_destroy(From, #st_standby{}, _) ->
    Reply = {error, recoverable, not_the_leader},
    {keep_state_and_data, {reply, From, Reply}};
handle_destroy(From, #st_leader{gr = Gr}, Data) ->
    {next_state, #st_cleanup{gr = Gr, from = From}, Data};
handle_destroy(_, #st_candidate{}, _) ->
    {keep_state_and_data, postpone}.

do_cleanup(Group = #share{topic = Topic}, From, #ls{h = HS = #hs{borrowers = B}}) ->
    ?tp(info, ds_shared_sub_leader_destroy, #{group => Group}),
    Id = emqx_ds_shared_sub_dl:mk_id(Group),
    %% Notify borrowers:
    maps:foreach(
        fun(BorrowerId, _) ->
            invalidate_subscriber(HS, BorrowerId, destroy)
        end,
        B
    ),
    %% Remove route:
    %% TODO
    %% Potentially broken ordering assumptions? This delete op is not serialized with
    %% respective add op, it's possible (yet extremely unlikely) that they will arrive
    %% to the external broker out-of-order.
    _ = emqx_external_broker:delete_persistent_route(Topic, Id),
    _ = emqx_persistent_session_ds_router:do_delete_route(Topic, Id),
    %% Remove data:
    _ = emqx_ds_shared_sub_dl:destroy(Id),
    {stop_and_reply, normal, [{reply, From, true}]}.

%%--------------------------------------------------------------------------------
%% Reallocation
%%--------------------------------------------------------------------------------

-spec handle_realloc(ls()) -> ls().
handle_realloc(Data = #ls{h = HS0}) ->
    #hs{s = S, borrowers = Borrowers, allocations = Allocs0} = HS0,
    Mod = emqx_ds_shared_sub:strategy_module(cfg_strategy(S)),
    Plan = Mod:stream_reallocation_strategy(S, Borrowers, Allocs0),
    ?tp(debug, ?tp_leader_realloc, #{b => Borrowers, pl => Plan}),
    %% Put plan into action.
    Allocs1 = realloc_notify_revoked(S, Allocs0, Plan),
    Allocs = realloc_notify_granted(S, Borrowers, Allocs1, Plan),
    %%
    HS = HS0#hs{allocations = Allocs, realloc_timer = undefined},
    Data#ls{h = HS}.

-spec realloc_notify_revoked(emqx_ds_shared_sub_dl:t(), allocations(), alloc_plan()) ->
    allocations().
realloc_notify_revoked(S, Allocs0, Plan) ->
    %% Fold over planned items and compare the new stream owners with
    %% the old ones, currently recorded in the state. Send revoke
    %% notification to the owner that don't match with the new plan:
    lists:foldl(
        fun({_Slab, Stream, BorrowerId}, Allocs) ->
            case Allocs of
                #{Stream := A0 = #alloc{status = Status, owner = Owner}} when
                    Owner =/= BorrowerId,
                    Status =:= ?stream_granting orelse Status =:= ?stream_granted
                ->
                    %% This stream has been reallocated to a different owner:
                    ?tp(ds_shared_sub_leader_notify_revoke, #{
                        borrower_id => Owner,
                        stream => Stream
                    }),
                    emqx_ds_shared_sub_proto:send_to_borrower(
                        Owner,
                        ?leader_revoke(leader(), Stream)
                    ),
                    A = A0#alloc{
                        status = ?stream_revoking,
                        deadline = now_ms_monotonic() + cfg_revocation_timeout(S)
                    },
                    Allocs#{Stream => A};
                #{} ->
                    Allocs
            end
        end,
        Allocs0,
        Plan
    ).

-spec realloc_notify_granted(emqx_ds_shared_sub_dl:t(), borrowers(), allocations(), alloc_plan()) ->
    allocations().
realloc_notify_granted(S, Borrowers, Allocs0, Plan) ->
    lists:foldl(
        fun({Slab, Stream, BorrowerId}, Allocs) ->
            maybe
                true ?= maps:is_key(BorrowerId, Borrowers),
                false ?= maps:is_key(Stream, Allocs),
                %% This stream is not owned by anyone and is
                %% scheduled to be sent to a valid borrower. We
                %% can send it to the new borrower:
                #'StreamState'{iterator = It} = emqx_ds_shared_sub_dl:get_stream_state(
                    Slab, Stream, S
                ),
                Progress = #{
                    stream => Stream,
                    progress => #{iterator => It}
                },
                emqx_ds_shared_sub_proto:send_to_borrower(
                    BorrowerId, ?leader_grant(leader(), Progress)
                ),
                Allocs#{
                    Stream => #alloc{
                        status = ?stream_granting,
                        slab = Slab,
                        owner = BorrowerId
                    }
                }
            else
                _ ->
                    Allocs
            end
        end,
        Allocs0,
        Plan
    ).

%%--------------------------------------------------------------------------------
%% Handling of borrower protocol messages
%%--------------------------------------------------------------------------------

-spec leader_loop(emqx_types:share(), ls()) -> {keep_state, ls(), [gen_statem:action()]}.
leader_loop(Group, Data = #ls{h = #hs{borrowers = B}}) ->
    case maps:size(B) of
        0 ->
            IdleTimeout = cfg_leader_max_idle_time(),
            Action = {state_timeout, IdleTimeout, #to_idle_stop{}},
            {next_state, #st_leader{gr = Group, idle = true}, Data, [Action]};
        _ ->
            Action = {state_timeout, cfg_heartbeat_interval(), #to_borrowers{}},
            {keep_state, Data, [Action]}
    end.

-spec handle_borrower_connect(emqx_types:share(), emqx_ds_shared_sub_proto:borrower_id(), ls()) ->
    ls().
handle_borrower_connect(GroupId, BorrowerId, Data = #ls{h = HS0}) ->
    #hs{borrowers = Borrowers0} = HS0,
    ?tp(debug, ?tp_leader_borrower_connect, #{
        borrower_id => BorrowerId,
        group_id => GroupId,
        borrower_ids => maps:keys(Borrowers0)
    }),
    %% Heuristic: we don't immediately assign streams to the new
    %% borrowers, because there's a chance that borrowers will join or
    %% leave in packs (e.g. due to node restart or traffic
    %% disturbance). Instead we perform all reallocations in one go
    %% during `#to_realloc{}' timeout.
    HS =
        case Borrowers0 of
            #{BorrowerId := _} -> HS0;
            _ -> set_borrower_state(HS0, BorrowerId, new_borrower_data())
        end,
    emqx_ds_shared_sub_proto:send_to_borrower(BorrowerId, ?leader_connect_response(leader())),
    %%
    Data#ls{h = schedule_realloc(later, HS)}.

-spec handle_borrower_ping(emqx_types:share(), emqx_ds_shared_sub_proto:borrower_id(), ls()) ->
    ls().
handle_borrower_ping(_GroupId, BorrowerId, Data = #ls{h = HS0}) ->
    ?tp(debug, ds_shared_sub_leader_borrower_ping, #{
        borrower_id => BorrowerId
    }),
    HS = update_borrower_deadline(BorrowerId, HS0),
    emqx_ds_shared_sub_proto:send_to_borrower(BorrowerId, ?leader_ping_response(leader())),
    Data#ls{h = HS}.

-spec handle_borrower_progress(
    emqx_ds_shared_sub_proto:borrower_id(),
    emqx_ds_shared_sub_proto:agent_stream_progress(),
    ls()
) -> ls().
handle_borrower_progress(BorrowerId, Progress, Data0 = #ls{h = HS0}) ->
    case maybe_handle_stream_progress(Data0, BorrowerId, Progress) of
        {true, Data} ->
            Data;
        false ->
            %% This borrower is not the owner according to
            %% our data. Kick it:
            HS = drop_invalidate_borrower(HS0, BorrowerId, invalid_stream_progress),
            Data0#ls{h = HS}
    end.

-spec handle_borrower_revoke_finished(
    emqx_ds_shared_sub_proto:borrower_id(),
    emqx_ds:stream(),
    ls()
) -> ls().
handle_borrower_revoke_finished(BorrowerId, Stream, Data = #ls{h = HS0}) ->
    ?tp(debug, ds_shared_sub_leader_complete_revoke, #{
        borrower_id => BorrowerId, stream => Stream
    }),
    case HS0#hs.allocations of
        Allocs = #{Stream := #alloc{owner = BorrowerId}} ->
            HS1 = HS0#hs{
                allocations = maps:remove(Stream, Allocs)
            },
            HS = schedule_realloc(now, HS1),
            Data#ls{h = HS};
        #{} ->
            HS = drop_invalidate_borrower(HS0, BorrowerId, invalid_stream_revoke),
            Data#ls{h = HS}
    end.

-spec handle_borrower_disconnect(
    emqx_ds_shared_sub_proto:borrower_id(),
    emqx_ds_shared_sub_proto:agent_stream_progresses(),
    ls()
) -> ls().
handle_borrower_disconnect(BorrowerId, StreamProgresses, Data0) ->
    ?tp(info, ?tp_leader_disconnect_borrower, #{
        borrower_id => BorrowerId,
        stream_progress => StreamProgresses
    }),
    %% Save progress:
    Data = lists:foldl(
        fun(Progress, Acc) ->
            case maybe_handle_stream_progress(Acc, BorrowerId, Progress) of
                {true, Data} ->
                    Data;
                false ->
                    Acc
            end
        end,
        Data0,
        StreamProgresses
    ),
    %% Drop borrower and schedule realloc:
    #ls{h = HS0} = Data,
    HS = schedule_realloc(later, drop_borrower(HS0, BorrowerId)),
    Data#ls{h = HS}.

-doc """
This function checks if the progress reported by a borrower is valid,
that is, the borrower is the current owner of the stream.

If borrower reports progress for a stream owned by someone else, this
function returns `false`.

On success it saves the state of the stream to the durable storage
and optionally reports `end_of_stream` to the DS client.
""".
maybe_handle_stream_progress(
    Data0 = #ls{c = CS0, h = HS0 = #hs{s = S0, allocations = Allocs0}},
    BorrowerId,
    Progress
) ->
    #{stream := Stream, progress := #{iterator := It}, use_finished := Finished} = Progress,
    case Allocs0 of
        #{Stream := A = #alloc{slab = Slab, owner = BorrowerId}} ->
            %% 1. Update the borrower's deadline:
            HS1 = update_borrower_deadline(BorrowerId, HS0),
            %% 2. Save progress to DB:
            S = emqx_ds_shared_sub_dl:set_stream_state(
                Slab,
                Stream,
                #'StreamState'{iterator = It},
                S0
            ),
            %% 3. Is borrower done with the stream?
            Allocs =
                case Finished of
                    false ->
                        Realloc = false,
                        Allocs0#{Stream := A#alloc{status = ?stream_granted}};
                    true ->
                        Realloc = true,
                        emqx_ds_shared_sub_proto:send_to_borrower(
                            BorrowerId,
                            ?leader_revoked(leader(), Stream)
                        ),
                        maps:remove(Stream, Allocs0)
                end,
            HS2 = HS1#hs{s = S, allocations = Allocs},
            HS3 =
                case Realloc of
                    true -> schedule_realloc(now, HS2);
                    false -> HS2
                end,
            %% 4. Handle end of stream:
            {CS, HS} =
                case It of
                    end_of_stream ->
                        emqx_ds_client:complete_stream(CS0, ?subid, Slab, Stream, HS3);
                    _ ->
                        {CS0, HS3}
                end,
            Data = Data0#ls{c = CS, h = schedule_checkpoint(later, HS)},
            {true, Data};
        #{Stream := _} ->
            false;
        #{} ->
            {true, Data0}
    end.

-spec new_borrower_data() -> borrower_state().
new_borrower_data() ->
    Deadline = ping_deadline(),
    #bs{validity_deadline = Deadline}.

-spec drop_invalidate_borrower(hs(), emqx_ds_shared_sub_proto:borrower_id(), _Reason) -> hs().
drop_invalidate_borrower(HS0, BorrowerId, Reason) ->
    HS = drop_borrower(HS0, BorrowerId),
    ok = invalidate_subscriber(HS, BorrowerId, Reason),
    schedule_realloc(later, HS).

-spec invalidate_subscriber(hs(), emqx_ds_shared_sub_proto:borrower_id(), _Reason) -> ok.
invalidate_subscriber(_, BorrowerId, Reason) ->
    ?tp(debug, ds_shared_sub_leader_invalidate_sub, #{id => BorrowerId, reason => Reason}),
    ok = emqx_ds_shared_sub_proto:send_to_borrower(
        BorrowerId,
        ?leader_invalidate(leader())
    ).

-doc """
Forget a bout a borrower and remove its allocations.
""".
-spec drop_borrower(hs(), emqx_ds_shared_sub_proto:borrower_id()) -> hs().
drop_borrower(
    #hs{borrowers = Borrowers0, allocations = Allocs0} = HS, DropId
) ->
    ?tp(debug, ds_shared_sub_leader_drop_borrower, #{
        borrower_id => DropId
    }),
    Borrowers = maps:remove(DropId, Borrowers0),
    Allocs = maps:filter(
        fun(_Stream, #alloc{owner = Id}) ->
            Id =/= DropId
        end,
        Allocs0
    ),
    HS#hs{
        borrowers = Borrowers,
        allocations = Allocs
    }.

-spec update_borrower_deadline(emqx_ds_shared_sub_proto:borrower_id(), hs()) -> hs().
update_borrower_deadline(BorrowerId, #hs{borrowers = Borrowers0} = HS) ->
    case Borrowers0 of
        #{BorrowerId := BS0} ->
            NewDeadline = ping_deadline(),
            BS = BS0#bs{validity_deadline = NewDeadline},
            set_borrower_state(HS, BorrowerId, BS);
        #{} ->
            HS
    end.

ping_deadline() ->
    now_ms_monotonic() + cfg_heartbeat_interval() * 2.

-spec set_borrower_state(hs(), emqx_ds_shared_sub_proto:borrower_id(), borrower_state()) -> hs().
set_borrower_state(HS = #hs{borrowers = Map}, Id, State) ->
    HS#hs{borrowers = Map#{Id => State}}.

-spec handle_borrower_timeouts(ls()) -> ls().
handle_borrower_timeouts(Data = #ls{h = HS0}) ->
    #hs{borrowers = Borrowers, allocations = Allocs} = HS0,
    Now = now_ms_monotonic(),
    %% Drop borrowers that didn't send ping message in time:
    HS1 = maps:fold(
        fun(BorrowerId, #bs{validity_deadline = Deadline}, Acc) ->
            case Now < Deadline of
                true ->
                    %% Borrower sent ping in time. Keep it.
                    Acc;
                false ->
                    %% Borrower didn't sent ping in time.
                    drop_invalidate_borrower(Acc, BorrowerId, ping_timeout)
            end
        end,
        HS0,
        Borrowers
    ),
    %% Verify deadline for stream reallocation operations:
    HS = maps:fold(
        fun
            (
                _Stream,
                #alloc{status = ?stream_revoking, deadline = Deadline, owner = BorrowerId},
                Acc
            ) when Now > Deadline ->
                %% Borrower didn't release the revoked stream in
                %% time. Kick it:
                drop_invalidate_borrower(Acc, BorrowerId, revoke_timeout);
            (_Stream, #alloc{}, Acc) ->
                Acc
        end,
        HS1,
        Allocs
    ),
    Data#ls{h = HS}.

%%--------------------------------------------------------------------------------
%% Leader's own logic
%%--------------------------------------------------------------------------------

enter_leader(ShareTF = #share{group = Group, topic = Topic}, Idle, Options = #{}) ->
    ?tp(debug, ds_shared_sub_become_leader, Options#{group => Group, topic => Topic}),
    SId = emqx_ds_shared_sub_dl:mk_id(ShareTF),
    %% Open durable state, or create a new one:
    case emqx_ds_shared_sub_dl:open(SId) of
        {ok, S2} ->
            ok;
        undefined ->
            Now = erlang:system_time(millisecond),
            S0 = emqx_ds_shared_sub_dl:create_new(SId),
            S1 = emqx_ds_shared_sub_dl:set_start_time(maps:get(start_time, Options, Now), S0),
            S2 = emqx_ds_shared_sub_dl:set_created_at(Now, S1)
    end,
    S = emqx_ds_shared_sub_dl:commit(new, S2),
    StartTime = emqx_ds_shared_sub_dl:get_start_time(S),
    SubOpts = #{
        id => ?subid,
        db => ?PERSISTENT_MESSAGE_DB,
        topic => emqx_ds:topic_words(Topic),
        start_time => StartTime
    },
    %% Create route:
    _ = emqx_persistent_session_ds_router:do_add_route(Topic, SId),
    _ = emqx_external_broker:add_persistent_route(Topic, SId),
    %% Create the client:
    {ok, Client, HS} = emqx_ds_client:subscribe(
        emqx_ds_client:new(?MODULE, #{}),
        SubOpts,
        #hs{s = S}
    ),
    Data = #ls{c = Client, h = HS},
    ?tp(debug, ?tp_leader_started, #{group => Group, topic => Topic}),
    enter_leader(ShareTF, Idle, Data);
enter_leader(_, _Idle, #ls{h = HS0} = Data0) ->
    HS = schedule_checkpoint(now, HS0),
    Data = Data0#ls{h = HS},
    {keep_state, Data, [{state_timeout, cfg_heartbeat_interval(), #to_borrowers{}}]}.

handle_checkpoint(Data0 = #ls{h = HS}) ->
    #hs{s = S0} = HS,
    S = emqx_ds_shared_sub_dl:commit(up, S0),
    Data = Data0#ls{h = HS#hs{s = S, checkpoint_timer = undefined}},
    {keep_state, Data}.

-doc """
This function removes data related to streams that belong to generations older than the current one.
""".
-spec drop_old_streams(emqx_ds:shard(), emqx_ds:generation(), emqx_ds_shared_sub_dl:t()) ->
    emqx_ds_shared_sub_dl:t().
drop_old_streams(Shard, CurrentGen, S0) ->
    emqx_ds_shared_sub_dl:fold_stream_states(
        fun(Slab, Stream, _, S1) ->
            case Slab of
                {Shard, Gen} when Gen < CurrentGen ->
                    emqx_ds_shared_sub_dl:del_stream_state(Slab, Stream, S1);
                {_, _} ->
                    S1
            end
        end,
        S0,
        S0
    ).

-spec schedule_realloc(now | later, hs()) -> hs().
schedule_realloc(now, HS = #hs{realloc_timer = now}) ->
    HS;
schedule_realloc(now, HS = #hs{realloc_timer = undefined}) ->
    self() ! #to_realloc{},
    HS#hs{realloc_timer = now};
schedule_realloc(now, HS = #hs{realloc_timer = Ref}) when is_reference(Ref) ->
    %% Re-schedule the timer immediately:
    _ = erlang:cancel_timer(Ref),
    self() ! #to_realloc{},
    HS#hs{realloc_timer = now};
schedule_realloc(later, HS = #hs{s = S, realloc_timer = undefined}) ->
    Ref = erlang:send_after(cfg_realloc_timeout(S), self(), #to_realloc{}),
    HS#hs{realloc_timer = Ref};
schedule_realloc(later, HS) ->
    HS.

-spec schedule_checkpoint(now | later, hs()) -> hs().
schedule_checkpoint(_, HS = #hs{checkpoint_timer = Ref}) when is_reference(Ref) ->
    HS;
schedule_checkpoint(When, HS = #hs{s = S, checkpoint_timer = undefined}) ->
    case emqx_ds_shared_sub_dl:is_dirty(S) of
        true ->
            Timeout =
                case When of
                    now -> 0;
                    later -> cfg_checkpoint_interval()
                end,
            Ref = erlang:send_after(Timeout, self(), #to_checkpoint{}),
            HS#hs{checkpoint_timer = Ref};
        false ->
            HS
    end.

%%--------------------------------------------------------------------------------
%% Candidate/standby
%%--------------------------------------------------------------------------------

handle_client_msg(Message, _Group, #ls{c = CS0, h = HS0}) ->
    case emqx_ds_client:dispatch_message(Message, CS0, HS0) of
        ignore ->
            keep_state_and_data;
        {CS, HS} ->
            {keep_state, #ls{c = CS, h = HS}}
    end.

enter_candidate(Group) ->
    ?tp(debug, emqx_ds_shared_sub_become_candidate, #{group => Group}),
    Timeout = rand:uniform(1_000),
    {keep_state_and_data, {state_timeout, Timeout, #to_become{}}}.

try_become_leader(Group, Data) ->
    case global:whereis_name(?name(Group)) of
        Pid when is_pid(Pid) ->
            {next_state, st_standby(Group, Pid), Data};
        undefined ->
            case global:register_name(?name(Group), self()) of
                yes ->
                    {next_state, #st_leader{gr = Group}, Data};
                no ->
                    {next_state, #st_candidate{gr = Group}, Data}
            end
    end.

st_standby(ShareTF, Pid) ->
    #st_standby{
        gr = ShareTF,
        mref = monitor(process, Pid)
    }.

%%--------------------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------------------

handle_get_leader(From, Group) ->
    {reply, From, {ok, global:whereis_name(?name(Group))}}.

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

leader() ->
    self().

-spec cfg_strategy(emqx_ds_shared_sub_dl:t()) -> emqx_ds_shared_sub:strategy().
cfg_strategy(S) ->
    case emqx_ds_shared_sub_dl:get_strategy(S) of
        undefined ->
            %% TODO: specify in configuration schema
            shard;
        CBM ->
            CBM
    end.

-spec cfg_checkpoint_interval() -> non_neg_integer().
cfg_checkpoint_interval() ->
    emqx_config:get([durable_sessions, shared_subs, checkpoint_interval]).

-spec cfg_realloc_timeout(emqx_ds_shared_sub_dl:t()) -> non_neg_integer().
cfg_realloc_timeout(S) ->
    case emqx_ds_shared_sub_dl:get_realloc_timeout(S) of
        undefined ->
            emqx_config:get([durable_sessions, shared_subs, realloc_interval]);
        N when is_integer(N) ->
            N
    end.

-spec cfg_revocation_timeout(emqx_ds_shared_sub_dl:t()) -> non_neg_integer().
cfg_revocation_timeout(S) ->
    case emqx_ds_shared_sub_dl:get_revocation_timeout(S) of
        undefined ->
            emqx_config:get([durable_sessions, shared_subs, revocation_timeout]);
        N when is_integer(N) ->
            N
    end.

-spec cfg_leader_max_idle_time() -> non_neg_integer().
cfg_leader_max_idle_time() ->
    emqx_config:get([durable_sessions, shared_subs, max_idle_time]).

-spec is_valid_strategy(emqx_ds_shared_sub:options()) -> boolean().
is_valid_strategy(#{strategy := Strat}) ->
    Strat =:= shard;
is_valid_strategy(#{}) ->
    true.
