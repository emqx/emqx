%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_connector_foreman).

-behaviour(gen_statem).

-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    start_link/1,
    ensure_pg_scope_started/2,

    current_members/2,

    get_assignments/1,
    stage_assignments/3,
    ack_assignments/3,
    nack_assignments/2,
    commit_assignments/2
]).

%% `gen_statem' API
-export([
    callback_mode/0,

    init/1,

    handle_event/4
]).

%% `global' API
-export([resolve_name_clash/3]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Also the leader name in `global'.
-define(GROUP(NAME), {?MODULE, NAME}).
-define(gproc_ref(NAME), {n, l, {?MODULE, NAME}}).
-define(via(NAME), {via, gproc, ?gproc_ref(NAME)}).

%% States
-define(leader, leader).
-define(follower, follower).
-define(candidate, candidate).

-type state() :: ?leader | ?follower | ?candidate.

-type leader_data() :: #{
    %% common fields
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    resources := resources(),
    retry_election_timeout := pos_integer(),
    %% leader-only fields
    pg_ref := reference(),
    allocation := allocation(),
    pending_acks := #{node() => _}
}.
-type follower_data() :: #{
    %% common fields
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    resources := resources(),
    retry_election_timeout := pos_integer(),
    %% follower-only fields
    leader := pid(),
    leader_mon := reference()
}.
-type candidate_data() :: #{
    %% common fields
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    resources := resources(),
    retry_election_timeout := pos_integer()
}.
-type data() :: leader_data() | follower_data() | candidate_data().

-type name() :: term().
-type scope() :: atom().

-type init_opts() :: #{
    name := name(),
    scope := scope(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    retry_election_timeout => pos_integer()
}.

-type compute_allocation_fn() :: fun((group_context()) -> allocation()).
-type allocation() :: #{node() => [resource()]}.
-type on_stage_fn() :: fun((allocation_context()) -> ok | {error, term()}).
-type on_commit_fn() :: fun((allocation_context()) -> ok | {error, term()}).
-type group_context() :: #{gen_id := gen_id(), members := [node()]}.
-type allocation_context() :: #{resources := [resource()], gen_id := gen_id()}.
-type resource() :: term().
-type resources() :: {committed | staged, [resource()]}.
-type gen_id() :: integer().

-type handler_result(State, Data) :: gen_statem:event_handler_result(State, Data).
-type handler_result() ::
    handler_result(?leader, leader_data())
    | handler_result(?follower, follower_data())
    | handler_result(?candidate, candidate_data()).

%% Events
-record(try_election, {}).
-record(allocate, {}).
-record(trigger_commit, {}).

%% external calls/casts/infos
-record(get_assignments, {}).
-record(new_assignments, {gen_id :: gen_id(), resources :: [resource()]}).
-record(ack_assignments, {gen_id :: gen_id(), member :: node()}).
-record(nack_assignments, {current_gen_id :: gen_id()}).
-record(commit_assignments, {gen_id :: gen_id()}).

-export_type([
    gen_id/0,
    resource/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(init_opts()) -> gen_statem:start_ret().
start_link(Opts = #{name := Name}) ->
    gen_statem:start_link(?via(Name), ?MODULE, Opts, []).

%% N.B.: There's the `supervisor:sup_ref()' type, which is more appropriate here, but that
%% type is not exported in OTP 26, so dialyzer complains if we try to use it here...  In
%% OTP 27, `supervisor:sup_ref()' is correctly exported.
-spec ensure_pg_scope_started(gen_server:server_ref(), scope()) -> ok.
ensure_pg_scope_started(Sup, Scope) ->
    ChildSpec = #{
        id => {scope, Scope},
        start => {pg, start_link, [Scope]},
        restart => permanent,
        type => worker,
        shutdown => 5_000
    },
    case supervisor:start_child(Sup, ChildSpec) of
        {error, {already_started, _}} ->
            ok;
        {ok, _} ->
            ok
    end.

-spec current_members(scope(), name()) -> [node()].
current_members(Scope, Name) ->
    lists:usort([node(P) || P <- pg:get_members(Scope, ?GROUP(Name))]).

-spec get_assignments(name()) -> {committed | staged, [resource()]} | undefined.
get_assignments(Name) ->
    try
        gen_statem:call(?via(Name), #get_assignments{})
    catch
        exit:{noproc, _} ->
            undefined
    end.

%% @doc Called by the leader on group members to stage the resources for a given
%% generation id.  Members should "release" the resources from previous generations and
%% prepare to use the received resources
-spec stage_assignments(name(), gen_id(), [resource()]) -> ok.
stage_assignments(Name, GenId, Assignments) ->
    gen_statem:cast(?via(Name), #new_assignments{gen_id = GenId, resources = Assignments}).

%% @doc Called by the group member on leader as a positive acknowledgement to received
%% resources, in response to `new_assignments'.
-spec ack_assignments(name(), gen_id(), node()) -> ok.
ack_assignments(Name, GenId, Member) ->
    gen_statem:cast(?via(Name), #ack_assignments{gen_id = GenId, member = Member}).

%% @doc Called by the group member on leader as a negative acknowledgement to received
%% resources, in response to `new_assignments', along with the member's last seen
%% generation id so the leader may bump its counter.
-spec nack_assignments(name(), gen_id()) -> ok.
nack_assignments(Name, CurrentGenId) ->
    gen_statem:cast(?via(Name), #nack_assignments{current_gen_id = CurrentGenId}).

%% @doc Called by the leader on group members to commit the resources for a given
%% generation id.  Sent after all members reply with positive acknowledgements.
-spec commit_assignments(name(), gen_id()) -> ok.
commit_assignments(Name, GenId) ->
    gen_statem:cast(?via(Name), #commit_assignments{gen_id = GenId}).

%%------------------------------------------------------------------------------
%% `gen_statem' API
%%------------------------------------------------------------------------------

callback_mode() ->
    [handle_event_function, state_enter].

-spec init(init_opts()) -> {ok, ?candidate, candidate_data()}.
init(Opts) ->
    #{
        name := Name,
        scope := Scope,
        compute_allocation_fn := ComputeAllocationFn,
        on_stage_fn := OnStageFn,
        on_commit_fn := OnCommitFn
    } = Opts,
    RetryElectionTimeout = maps:get(retry_election_timeout, Opts, 5_000),
    ok = pg:join(Scope, ?GROUP(Name), self()),
    CData = #{
        name => Name,
        gen_id => 0,
        compute_allocation_fn => ComputeAllocationFn,
        on_stage_fn => OnStageFn,
        on_commit_fn => OnCommitFn,
        resources => {staged, []},
        retry_election_timeout => RetryElectionTimeout,
        scope => Scope
    },
    logger:set_process_metadata(#{
        name => Name,
        scope => Scope,
        domain => [emqx, connector, foreman]
    }),
    {ok, ?candidate, CData}.

handle_event(enter, OldState, NewState, Data) ->
    ?tp(
        debug,
        "foreman_state_enter",
        #{
            previous_state => OldState,
            current_state => NewState,
            data => Data
        }
    ),
    handle_state_enter(OldState, NewState, Data);
%% `?candidate' state
handle_event(state_timeout, #try_election{}, ?candidate, CData) ->
    try_election(CData);
%% `?follower' state
handle_event(
    info,
    {'DOWN', MRef, process, Pid, _Reason},
    ?follower,
    FData = #{leader := Pid, leader_mon := MRef}
) ->
    CData = follower_to_candidate_data(FData),
    {next_state, ?candidate, CData};
%% `?leader' state
handle_event(info, {Ref, JoinOrLeave, _Group, Pids}, ?leader, LData = #{pg_ref := Ref}) ->
    handle_pg_event(JoinOrLeave, Pids, LData);
handle_event(state_timeout, #allocate{}, ?leader, LData) ->
    ?tp("foreman_allocate", #{}),
    handle_allocate(LData);
handle_event(internal, #trigger_commit{}, ?leader, LData) ->
    ?tp("foreman_trigger_commit", #{}),
    handle_trigger_commit(LData);
%% Misc events
handle_event(info, {global_name_conflict, ?GROUP(Name), _OthePid}, State, Data = #{name := Name}) ->
    handle_name_clash(State, Data);
handle_event({call, From}, #get_assignments{}, _State, Data) ->
    handle_get_assignments(From, Data);
handle_event(cast, #new_assignments{} = Event, State, Data) ->
    handle_new_assignments(Event, State, Data);
handle_event(cast, #ack_assignments{} = Event, State, Data) ->
    handle_ack_assignments(Event, State, Data);
handle_event(cast, #commit_assignments{} = Event, State, Data) ->
    handle_commit_assignments(Event, State, Data);
handle_event(EventType, EventContent, State, Data) ->
    ?tp(
        info,
        "unexpected_event",
        #{
            event_type => EventType,
            event_content => EventContent,
            state => State,
            data => Data
        }
    ),
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% `global' API
%%------------------------------------------------------------------------------

resolve_name_clash(Name, Pid1, Pid2) ->
    global:notify_all_name(Name, Pid1, Pid2).

%%------------------------------------------------------------------------------
%% State handlers
%%------------------------------------------------------------------------------

-spec handle_state_enter(state(), state(), data()) -> handler_result().
handle_state_enter(_OldState, NewState, Data) ->
    case NewState of
        ?candidate ->
            {keep_state_and_data, [{state_timeout, _Now = 0, #try_election{}}]};
        ?follower ->
            keep_state_and_data;
        ?leader ->
            %% Assert
            core = mria_rlog:role(),
            %% Bump generation id
            #{gen_id := GenId0} = Data,
            %% Fixme: wait a while in case multiple nodes are starting at the same time?
            Delay = 500,
            {keep_state, Data#{gen_id := GenId0 + 1}, [{state_timeout, Delay, #allocate{}}]}
    end.

-spec try_election(candidate_data()) -> handler_result().
try_election(CData) ->
    case mria_rlog:role() of
        core ->
            do_try_election(CData);
        replicant ->
            #{name := Name} = CData,
            ?tp_span(
                foreman_replicant_try_follow_leader,
                #{name => Name},
                try_follow_leader(CData)
            )
    end.

-spec do_try_election(candidate_data()) -> handler_result().
do_try_election(CData) ->
    #{
        name := Name,
        retry_election_timeout := RetryElectionTimeout,
        scope := Scope
    } = CData,
    case global:register_name(?GROUP(Name), self(), fun ?MODULE:resolve_name_clash/3) of
        yes ->
            {Ref, _} = pg:monitor(Scope, ?GROUP(Name)),
            LData = CData#{
                pg_ref => Ref,
                allocation => #{},
                pending_acks => #{}
            },
            ?tp(debug, "foreman_elected", #{leader => node()}),
            {next_state, ?leader, LData};
        no ->
            case global:whereis_name(?GROUP(Name)) of
                undefined ->
                    %% race condition: leader died / network partition?
                    {keep_state_and_data, [{state_timeout, RetryElectionTimeout, #try_election{}}]};
                LeaderPid when LeaderPid =:= self() ->
                    %% `global' name clash resolve function picked this process?
                    %% currently, we make the leaders shutdown, so this should be
                    %% unreachable until that behavior changes.
                    {Ref, _} = pg:monitor(Scope, ?GROUP(Name)),
                    LData = CData#{
                        pg_ref => Ref,
                        allocation => #{},
                        pending_acks => #{}
                    },
                    ?tp(debug, "foreman_elected", #{leader => node()}),
                    {next_state, ?leader, LData};
                LeaderPid when is_pid(LeaderPid) ->
                    MRef = monitor(process, LeaderPid),
                    FData = CData#{leader => LeaderPid, leader_mon => MRef},
                    ?tp(debug, "foreman_elected", #{leader => node(LeaderPid)}),
                    {next_state, ?follower, FData}
            end
    end.

-spec try_follow_leader(candidate_data()) -> handler_result().
try_follow_leader(CData) ->
    #{
        name := Name,
        retry_election_timeout := RetryElectionTimeout
    } = CData,
    case global:whereis_name(?GROUP(Name)) of
        undefined ->
            {keep_state_and_data, [{state_timeout, RetryElectionTimeout, #try_election{}}]};
        LeaderPid when is_pid(LeaderPid) ->
            %% Assert
            false = LeaderPid =:= self(),
            MRef = monitor(process, LeaderPid),
            FData = CData#{leader => LeaderPid, leader_mon => MRef},
            ?tp(debug, "foreman_elected", #{leader => node(LeaderPid)}),
            {next_state, ?follower, FData}
    end.

-spec handle_pg_event(join | leave, [pid()], leader_data()) ->
    handler_result(?leader, leader_data()).
handle_pg_event(_JoinOrLeave, _Pids, _LData) ->
    ?tp("foreman_pg_event", #{event => _JoinOrLeave, pids => _Pids, data => _LData}),
    %% TODO: check allocations?
    keep_state_and_data.

-spec handle_name_clash(state(), data()) -> handler_result().
handle_name_clash(?leader, LData) ->
    %% If we are a leader, we shutdown so that our followers receive a `DOWN' signal and
    %% start an election.
    #{name := Name} = LData,
    ?tp(debug, "foreman_step_down", #{name => Name}),
    {stop, {shutdown, step_down}};
handle_name_clash(_State, _Data) ->
    keep_state_and_data.

handle_get_assignments(From, Data) ->
    #{resources := Resources} = Data,
    {keep_state_and_data, [{reply, From, Resources}]}.

handle_allocate(LData0) ->
    #{
        name := Name,
        scope := Scope,
        gen_id := GenId,
        allocation := PrevAllocation,
        compute_allocation_fn := ComputeAllocationFn
    } = LData0,
    MemberNodes = current_members(Scope, Name),
    GroupContext = #{
        gen_id => GenId,
        members => MemberNodes
    },
    Allocation = #{} = ComputeAllocationFn(GroupContext),
    case Allocation of
        PrevAllocation ->
            keep_state_and_data;
        _ ->
            maps:foreach(
                fun(N, Rs) ->
                    ok = emqx_connector_foreman_proto_v1:stage_assignments(N, Name, GenId, Rs)
                end,
                Allocation
            ),
            LData = LData0#{
                allocation := Allocation,
                pending_acks := maps:from_keys(MemberNodes, true)
            },
            %% TODO: timeout to ping lagging members?
            {keep_state, LData}
    end.

-spec handle_new_assignments(#new_assignments{}, state(), data()) -> handler_result().
handle_new_assignments(#new_assignments{gen_id = GenId, resources = _}, _State, #{gen_id := MyGenId}) when
    GenId < MyGenId
->
    %% Stale message from old leader.
    keep_state_and_data;
handle_new_assignments(#new_assignments{gen_id = _, resources = _}, ?candidate, _CData) ->
    {keep_state_and_data, [postpone]};
handle_new_assignments(
    #new_assignments{gen_id = GenId0, resources = _}, ?follower, Data = #{gen_id := MyGenId}
) when
    GenId0 < MyGenId
->
    %% New started and elected leaders start at generationd id = 0.  Reply with our
    %% generation id so it may catch up faster.
    #{
        name := Name,
        leader := LeaderPid
    } = Data,
    ok = emqx_connector_foreman_proto_v1:nack_assignments(node(LeaderPid), Name, MyGenId),
    keep_state_and_data;
handle_new_assignments(
    #new_assignments{gen_id = GenId0, resources = Resources},
    State,
    Data0 = #{gen_id := MyGenId}
) when
    GenId0 >= MyGenId
->
    #{
        name := Name,
        on_stage_fn := OnStageFn
    } = Data0,
    {GenId, LeaderPid} =
        case State of
            ?leader -> {MyGenId, self()};
            ?follower -> {GenId0, maps:get(leader, Data0)}
        end,
    AllocationContext = #{
        gen_id => GenId,
        resources => Resources
    },
    %% TODO: handle errors
    ok = OnStageFn(AllocationContext),
    Member = node(),
    ok = emqx_connector_foreman_proto_v1:ack_assignments(node(LeaderPid), Name, GenId, Member),
    ?tp("foreman_staged_assignments", #{gen_id => GenId}),
    Data = Data0#{
        gen_id := GenId,
        resources := {staged, Resources}
    },
    {keep_state, Data}.

-spec handle_ack_assignments(#ack_assignments{}, state(), data()) -> handler_result().
handle_ack_assignments(#ack_assignments{gen_id = GenId, member = _}, _State, #{gen_id := MyGenId}) when
    MyGenId > GenId
->
    %% Stale message
    keep_state_and_data;
handle_ack_assignments(
    #ack_assignments{gen_id = GenId, member = Member}, ?leader, #{gen_id := GenId} = LData0
) ->
    #{pending_acks := PendingAcks0} = LData0,
    PendingAcks = maps:remove(Member, PendingAcks0),
    LData = LData0#{pending_acks := PendingAcks},
    case map_size(PendingAcks) of
        0 ->
            {keep_state, LData, [{next_event, internal, #trigger_commit{}}]};
        _ ->
            {keep_state, LData}
    end;
handle_ack_assignments(#ack_assignments{gen_id = _, member = _}, _State, _Data) ->
    %% Stale message?
    keep_state_and_data.

-spec handle_trigger_commit(leader_data()) -> handler_result(?leader, leader_data()).
handle_trigger_commit(LData) ->
    #{
        gen_id := GenId,
        name := Name,
        scope := Scope
    } = LData,
    MemberNodes = current_members(Scope, Name),
    ok = emqx_connector_foreman_proto_v1:commit_assignments(MemberNodes, Name, GenId),
    %% TODO: make configurable and larger default
    Delay = 1_000,
    {keep_state_and_data, [{state_timeout, Delay, #allocate{}}]}.

-spec handle_commit_assignments(#commit_assignments{}, state(), data()) -> handler_result().
handle_commit_assignments(#commit_assignments{gen_id = GenId}, _State, #{gen_id := GenId} = Data0) ->
    #{
        on_commit_fn := OnCommitFn,
        resources := {PrevResState, Resources}
    } = Data0,
    Data = Data0#{resources := {committed, Resources}},
    case PrevResState of
        staged ->
            AllocationContext = #{
                gen_id => GenId,
                resources => Resources
            },
            %% TODO: handle errors
            ok = OnCommitFn(AllocationContext),
            ?tp("foreman_committed_assignments", #{gen_id => GenId}),
            ok;
        committed ->
            ok
    end,
    {keep_state, Data};
handle_commit_assignments(#commit_assignments{gen_id = _}, ?candidate, _CData) ->
    {keep_state_and_data, [postpone]};
handle_commit_assignments(#commit_assignments{gen_id = _}, _State, _Data) ->
    %% Stale message
    keep_state_and_data.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec follower_to_candidate_data(follower_data()) -> candidate_data().
follower_to_candidate_data(FData) ->
    %% Using explicit construction and deconstruction instead of `maps:with' to have
    %% dialyzer actually make a helpful analysis and avoid missing fields.
    #{
        name := Name,
        scope := Scope,
        gen_id := GenId,
        compute_allocation_fn := ComputeAllocationFn,
        on_stage_fn := OnStageFn,
        on_commit_fn := OnCommitFn,
        resources := Resources,
        retry_election_timeout := RetryElectionTimeout
    } = FData,
    #{
        name => Name,
        scope => Scope,
        gen_id => GenId,
        compute_allocation_fn => ComputeAllocationFn,
        on_stage_fn => OnStageFn,
        on_commit_fn => OnCommitFn,
        resources => Resources,
        retry_election_timeout => RetryElectionTimeout
    }.
