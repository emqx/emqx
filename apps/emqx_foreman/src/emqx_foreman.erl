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

-module(emqx_foreman).

%%========================================================================================
%% @doc This process implements leader election on top of `global' to distribute
%% "resources" to members of the EMQX cluster.
%%
%% This process can be in one of 3 states: `leader', `follower', `candidate'.
%%
%% The process starts in the `candidate' state and attempts to elect itself by claiming a
%% `global' name based on the input name.  If it manages to claim it, it's the
%% `leader'.  Otherwise, it monitors the leader pid and enters the `follower' state.  A
%% `follower' will transition back to a `candidate' if it deems that the leader is dead.
%%
%% If a name clash is detected by `global' (e.g.: after cliques in a netsplit merge back),
%% the leaders in each clique will shut themselves down to signal their followers to
%% trigger a new election.
%%
%% Once a leader is elected, it'll evaluate an allocation of resources based on the
%% current running nodes and an input callback function, and then "stage" these
%% allocations by casting a message to each member if it's a new allocation (i.e.:
%% different from previously computed).
%%
%% After the leader receives an ack from each member, it then commits this allocation by
%% sending a commit message to each one.
%%
%% Callbacks are evaluated when staging and committing an allocation, so that previously
%% allocated resources may be released and new ones acquired/initialized when staged, and
%% then effectively "started" when committed.
%%
%% Recently started followers attempt to consult the leader for their own allocations, so
%% that they may catch up faster if they were restarted.  The leader also tries to stage
%% the same allocations multiple times to lagging members.  The leader does not track
%% commit success.
%%========================================================================================

-feature(maybe_expr, enable).

-behaviour(gen_statem).

-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    start_link/1,

    current_members/0,
    get_allocation/2,

    get_assignments/1,
    stage_assignments/2,
    ack_assignments/2,
    commit_assignments/1
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

-define(LEADER_NAME(NAME), {?MODULE, NAME}).
-define(gproc_ref(NAME), {n, l, {?MODULE, NAME}}).
-define(via(NAME), {via, gproc, ?gproc_ref(NAME)}).

%% States
-define(leader, leader).
-define(follower, follower).
-define(candidate, candidate).

%% TODO: make configurable
-define(DEFAULT_ALLOCATION_TRIGGER_TIMEOUT, 5_000).

-type state() :: ?leader | ?follower | ?candidate.

-type leader_data() :: #{
    %% common fields
    name := name(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    resources := resources(),
    retry_election_timeout := pos_integer(),
    %% leader-only fields
    allocation := undefined | allocation(),
    allocation_status := allocation_status(),
    pending_acks := #{node() => _}
}.
-type follower_data() :: #{
    %% common fields
    name := name(),
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
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    resources := resources(),
    retry_election_timeout := pos_integer()
}.
-type data() :: leader_data() | follower_data() | candidate_data().

-type name() :: term().

-type init_opts() :: #{
    name := name(),
    compute_allocation_fn := compute_allocation_fn(),
    on_stage_fn := on_stage_fn(),
    on_commit_fn := on_commit_fn(),
    retry_election_timeout => pos_integer()
}.

-type compute_allocation_fn() :: fun((group_context()) -> allocation()).
-type allocation() :: #{node() => [resource()]}.
-type on_stage_fn() :: fun((allocation_context()) -> ok | {error, term()}).
-type on_commit_fn() :: fun((allocation_context()) -> ok | {error, term()}).
-type group_context() :: #{members := [node()]}.
-type allocation_context() :: #{
    previous_resources := [resource()],
    resources := [resource()]
}.
-type resource() :: term().
-type resources() :: {allocation_status(), [resource()]}.
-type allocation_status() :: committed | staged.

-type handler_result(State, Data) :: gen_statem:event_handler_result(State, Data).
-type handler_result() ::
    handler_result(?leader, leader_data())
    | handler_result(?follower, follower_data())
    | handler_result(?candidate, candidate_data()).

%% Events
-record(try_election, {}).
-record(allocate, {}).
-record(trigger_commit, {}).
-record(ping_lagging_members, {}).
-record(consult_leader, {}).

%% external calls/casts/infos
-record(get_assignments, {}).
-record(get_allocation, {member :: node()}).
-record(new_assignments, {resources :: [resource()]}).
-record(ack_assignments, {member :: node()}).
-record(commit_assignments, {}).

-type new_assignments_event() :: #new_assignments{}.
-type ack_assignments_event() :: #ack_assignments{}.
-type commit_assignments_event() :: #commit_assignments{}.

-export_type([
    resource/0,
    allocation_status/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(init_opts()) -> gen_statem:start_ret().
start_link(Opts = #{name := Name}) ->
    gen_statem:start_link(?via(Name), ?MODULE, Opts, []).

-spec current_members() -> [node()].
current_members() ->
    emqx:running_nodes().

-spec get_assignments(name()) -> {committed | staged, [resource()]} | undefined.
get_assignments(Name) ->
    try
        gen_statem:call(?via(Name), #get_assignments{})
    catch
        exit:{noproc, _} ->
            undefined
    end.

-spec get_allocation(name(), node()) ->
    {ok, #{
        status => allocation_status(),
        resources => [resource()]
    }}
    | {error, not_leader}
    | {error, noproc}.
get_allocation(Name, Member) ->
    try
        gen_statem:call(?via(Name), #get_allocation{member = Member})
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.

%% @doc Called by the leader on group members to stage the resources from the input.
%% Members should "release" the resources from previous generations and prepare to use the
%% received resources
-spec stage_assignments(name(), [resource()]) -> ok.
stage_assignments(Name, Assignments) ->
    gen_statem:cast(?via(Name), #new_assignments{resources = Assignments}).

%% @doc Called by the group member on leader as a positive acknowledgement to received
%% resources, in response to `new_assignments'.
-spec ack_assignments(name(), node()) -> ok.
ack_assignments(Name, Member) ->
    gen_statem:cast(?via(Name), #ack_assignments{member = Member}).

%% @doc Called by the leader on group members to commit the resources already received.
%% Sent after all members reply with positive acknowledgements.
-spec commit_assignments(name()) -> ok.
commit_assignments(Name) ->
    gen_statem:cast(?via(Name), #commit_assignments{}).

%%------------------------------------------------------------------------------
%% `gen_statem' API
%%------------------------------------------------------------------------------

callback_mode() ->
    [handle_event_function, state_enter].

-spec init(init_opts()) -> {ok, ?candidate, candidate_data()}.
init(Opts) ->
    #{
        name := Name,
        compute_allocation_fn := ComputeAllocationFn,
        on_stage_fn := OnStageFn,
        on_commit_fn := OnCommitFn
    } = Opts,
    RetryElectionTimeout = maps:get(retry_election_timeout, Opts, 5_000),
    CData = #{
        name => Name,
        compute_allocation_fn => ComputeAllocationFn,
        on_stage_fn => OnStageFn,
        on_commit_fn => OnCommitFn,
        resources => {staged, []},
        retry_election_timeout => RetryElectionTimeout
    },
    logger:set_process_metadata(#{
        name => Name,
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
    ?tp("foreman_leader_down", #{}),
    CData = follower_to_candidate_data(FData),
    {next_state, ?candidate, CData};
handle_event(state_timeout, #consult_leader{}, ?follower, FData) ->
    ?tp("foreman_consult_leader", #{}),
    handle_consult_leader(FData);
%% `?leader' state
handle_event(state_timeout, #allocate{}, ?leader, LData) ->
    ?tp("foreman_allocate", #{}),
    handle_allocate(LData);
handle_event(state_timeout, #ping_lagging_members{}, ?leader, LData) ->
    ?tp("foreman_ping_lagging_members", #{}),
    handle_ping_lagging_members(LData);
handle_event(internal, #allocate{}, ?leader, LData) ->
    ?tp("foreman_allocate", #{}),
    handle_allocate(LData);
handle_event(internal, #trigger_commit{}, ?leader, LData) ->
    ?tp("foreman_trigger_commit", #{}),
    handle_trigger_commit(LData);
%% Misc events
handle_event(
    info,
    {global_name_conflict, ?LEADER_NAME(Name), _OthePid},
    State,
    Data = #{name := Name}
) ->
    handle_name_clash(State, Data);
handle_event({call, From}, #get_assignments{}, _State, Data) ->
    handle_get_assignments(From, Data);
handle_event({call, From}, #get_allocation{member = Member}, State, Data) ->
    handle_get_allocation(From, Member, State, Data);
handle_event(cast, #new_assignments{} = Event, State, Data) ->
    ?tp("foreman_new_assignments", #{state => State}),
    handle_new_assignments(Event, State, Data);
handle_event(cast, #ack_assignments{} = Event, State, Data) ->
    ?tp("foreman_ack_assignments", #{state => State}),
    handle_ack_assignments(Event, State, Data);
handle_event(cast, #commit_assignments{} = Event, State, Data) ->
    ?tp("foreman_commit_assignments", #{state => State}),
    handle_commit_assignments(Event, State, Data);
handle_event(EventType, EventContent, State, Data) ->
    ?tp(
        info,
        "foreman_unexpected_event",
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
            {keep_state_and_data, [{state_timeout, _Now = 0, #consult_leader{}}]};
        ?leader ->
            %% Assert
            core = mria_rlog:role(),
            %% Fixme: wait a while in case multiple nodes are starting at the same time?
            Delay = 500,
            {keep_state, Data, [{state_timeout, Delay, #allocate{}}]}
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
        retry_election_timeout := RetryElectionTimeout
    } = CData,
    case global:register_name(?LEADER_NAME(Name), self(), fun ?MODULE:resolve_name_clash/3) of
        yes ->
            LData = CData#{
                allocation => undefined,
                allocation_status => staged,
                pending_acks => #{}
            },
            ?tp(debug, "foreman_elected", #{leader => node()}),
            {next_state, ?leader, LData};
        no ->
            case global:whereis_name(?LEADER_NAME(Name)) of
                undefined ->
                    %% race condition: leader died / network partition?
                    {keep_state_and_data, [{state_timeout, RetryElectionTimeout, #try_election{}}]};
                LeaderPid when LeaderPid =:= self() ->
                    %% `global' name clash resolve function picked this process?
                    %% currently, we make the leaders shutdown, so this should be
                    %% unreachable until that behavior changes.
                    LData = CData#{
                        allocation => undefined,
                        allocation_status => staged,
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
    case global:whereis_name(?LEADER_NAME(Name)) of
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

handle_get_allocation(From, Member, ?leader, LData) ->
    #{
        allocation := Allocation,
        allocation_status := AllocationStatus
    } = LData,
    Resources =
        maybe
            #{} ?= Allocation,
            maps:get(Member, Allocation, undefined)
        else
            _ -> undefined
        end,
    Reply =
        {ok, #{
            status => AllocationStatus,
            resources => Resources
        }},
    {keep_state_and_data, [{reply, From, Reply}]};
handle_get_allocation(From, _Member, _State, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_leader}}]}.

-spec handle_allocate(leader_data()) -> handler_result(?leader, leader_data()).
handle_allocate(LData0) ->
    #{
        name := Name,
        allocation := PrevAllocation,
        compute_allocation_fn := ComputeAllocationFn
    } = LData0,
    MemberNodes = current_members(),
    GroupContext = #{members => MemberNodes},
    Allocation = #{} = ComputeAllocationFn(GroupContext),
    case Allocation of
        PrevAllocation ->
            {keep_state_and_data, [
                {state_timeout, ?DEFAULT_ALLOCATION_TRIGGER_TIMEOUT, #allocate{}}
            ]};
        _ ->
            maps:foreach(
                fun(N, Rs) ->
                    ok = emqx_foreman_proto_v1:stage_assignments(N, Name, Rs)
                end,
                Allocation
            ),
            LData = LData0#{
                allocation := Allocation,
                allocation_status := staged,
                pending_acks := maps:from_keys(MemberNodes, true)
            },
            %% TODO: make delay configurable
            Delay = 1_000,
            {keep_state, LData, [{state_timeout, Delay, #ping_lagging_members{}}]}
    end.

-spec handle_ping_lagging_members(leader_data()) -> handler_result(?leader, leader_data()).
handle_ping_lagging_members(LData) ->
    #{
        name := Name,
        %% FIXME: check this before crashing...
        allocation := #{} = Allocation,
        pending_acks := PendingAcks
    } = LData,
    lists:foreach(
        fun(N) ->
            Rs = maps:get(N, Allocation),
            ok = emqx_foreman_proto_v1:stage_assignments(N, Name, Rs)
        end,
        maps:keys(PendingAcks)
    ),
    %% TODO: make delay configurable
    Delay = 1_000,
    {keep_state_and_data, [{state_timeout, Delay, #ping_lagging_members{}}]}.

-spec handle_consult_leader(follower_data()) -> handler_result(?follower, follower_data()).
handle_consult_leader(FData0) ->
    #{
        leader := LeaderPid,
        name := Name,
        on_stage_fn := OnStageFn,
        on_commit_fn := OnCommitFn,
        resources := {PrevAllocationStatus, PrevResources}
    } = FData0,
    try emqx_foreman_proto_v1:get_allocation(node(LeaderPid), Name, node()) of
        {error, noproc} ->
            {keep_state_and_data, [{state_timeout, 1_000, #consult_leader{}}]};
        {error, not_leader} ->
            %% Race condition?  Should receive `DOWN' signal any moment, then.
            keep_state_and_data;
        {ok, #{resources := undefined}} ->
            %% Leader didn't compute allocation yet.
            keep_state_and_data;
        {ok, #{resources := PrevResources, status := PrevAllocationStatus}} ->
            keep_state_and_data;
        {ok, #{resources := Resources, status := AllocationStatus}} ->
            AllocationContext = allocation_context(Resources, PrevResources),
            %% TODO: handle errors
            ok = OnStageFn(AllocationContext),
            Member = node(),
            ok = emqx_foreman_proto_v1:ack_assignments(
                node(LeaderPid), Name, Member
            ),
            maybe
                committed ?= AllocationStatus,
                %% TODO: handle errors
                ok = OnCommitFn(AllocationContext),
                ?tp("foreman_committed_assignments", #{}),
                ok
            end,
            FData = FData0#{resources := {AllocationStatus, Resources}},
            {keep_state, FData}
    catch
        error:{erpc, _} ->
            {keep_state_and_data, [{state_timeout, 1_000, #consult_leader{}}]}
    end.

-spec handle_new_assignments(new_assignments_event(), state(), data()) -> handler_result().
handle_new_assignments(#new_assignments{resources = _}, ?candidate, _CData) ->
    {keep_state_and_data, [postpone]};
handle_new_assignments(#new_assignments{resources = Resources}, State, Data0) ->
    #{
        name := Name,
        on_stage_fn := OnStageFn,
        resources := {_PrevAllocationStatus, PrevResources}
    } = Data0,
    LeaderPid =
        case State of
            ?leader -> self();
            ?follower -> maps:get(leader, Data0)
        end,
    AllocationContext = allocation_context(Resources, PrevResources),
    %% TODO: handle errors
    ok = OnStageFn(AllocationContext),
    Member = node(),
    ok = emqx_foreman_proto_v1:ack_assignments(node(LeaderPid), Name, Member),
    ?tp("foreman_staged_assignments", #{}),
    Data = Data0#{resources := {staged, Resources}},
    {keep_state, Data}.

%% Received by the leader when a member acknowledges an assignment.
-spec handle_ack_assignments(ack_assignments_event(), state(), data()) -> handler_result().
handle_ack_assignments(#ack_assignments{member = Member}, ?leader, LData0) ->
    #{pending_acks := PendingAcks0} = LData0,
    PendingAcks = maps:remove(Member, PendingAcks0),
    LData = LData0#{pending_acks := PendingAcks},
    WasMember = is_map_key(Member, PendingAcks0),
    case map_size(PendingAcks) =:= 0 andalso WasMember of
        true ->
            {keep_state, LData, [{next_event, internal, #trigger_commit{}}]};
        false ->
            {keep_state, LData}
    end;
handle_ack_assignments(#ack_assignments{member = _}, _State, _Data) ->
    %% Stale message?
    keep_state_and_data.

-spec handle_trigger_commit(leader_data()) -> handler_result(?leader, leader_data()).
handle_trigger_commit(LData0) ->
    #{name := Name} = LData0,
    MemberNodes = current_members(),
    ok = emqx_foreman_proto_v1:commit_assignments(MemberNodes, Name),
    LData = LData0#{allocation_status := committed},
    {keep_state, LData, [{state_timeout, ?DEFAULT_ALLOCATION_TRIGGER_TIMEOUT, #allocate{}}]}.

-spec handle_commit_assignments(commit_assignments_event(), state(), data()) -> handler_result().
handle_commit_assignments(#commit_assignments{}, ?candidate, _CData) ->
    {keep_state_and_data, [postpone]};
handle_commit_assignments(#commit_assignments{}, _State, Data0) ->
    #{
        on_commit_fn := OnCommitFn,
        resources := {PrevResState, Resources}
    } = Data0,
    Data = Data0#{resources := {committed, Resources}},
    case PrevResState of
        staged ->
            AllocationContext = allocation_context(Resources, Resources),
            %% TODO: handle errors
            ok = OnCommitFn(AllocationContext),
            ?tp("foreman_committed_assignments", #{}),
            ok;
        committed ->
            ok
    end,
    {keep_state, Data}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec follower_to_candidate_data(follower_data()) -> candidate_data().
follower_to_candidate_data(FData) ->
    %% Using explicit construction and deconstruction instead of `maps:with' to have
    %% dialyzer actually make a helpful analysis and avoid missing fields.
    #{
        name := Name,
        compute_allocation_fn := ComputeAllocationFn,
        on_stage_fn := OnStageFn,
        on_commit_fn := OnCommitFn,
        resources := Resources,
        retry_election_timeout := RetryElectionTimeout
    } = FData,
    #{
        name => Name,
        compute_allocation_fn => ComputeAllocationFn,
        on_stage_fn => OnStageFn,
        on_commit_fn => OnCommitFn,
        resources => Resources,
        retry_election_timeout => RetryElectionTimeout
    }.

allocation_context(NewResources, PrevResources) ->
    #{
        resources => NewResources,
        previous_resources => PrevResources
    }.
