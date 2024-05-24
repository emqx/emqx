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

    stage_assignments/3,
    ack_assignments/2,
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
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    retry_election_timeout := pos_integer(),
    pg_ref := reference()
}.
-type follower_data() :: #{
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    retry_election_timeout := pos_integer(),
    leader := pid(),
    leader_mon := reference()
}.
-type candidate_data() :: #{
    name := name(),
    scope := scope(),
    gen_id := gen_id(),
    compute_allocation_fn := compute_allocation_fn(),
    retry_election_timeout := pos_integer()
}.
-type data() :: leader_data() | follower_data() | candidate_data().

-type name() :: term().
-type scope() :: atom().

-type init_opts() :: #{
    name := name(),
    scope := scope(),
    compute_allocation_fn := compute_allocation_fn(),
    retry_election_timeout => pos_integer()
}.

-type compute_allocation_fn() :: fun((group_context()) -> #{pid() => [resource()]}).
-type group_context() :: #{members := [pid()]}.
-type resource() :: term().
-type gen_id() :: integer().

-type handler_result(State, Data) :: gen_statem:event_handler_result(State, Data).
-type handler_result() ::
    handler_result(?leader, leader_data())
    | handler_result(?follower, follower_data())
    | handler_result(?candidate, candidate_data()).

%% Events
-record(try_election, {}).
-record(new_assignments, {gen_id :: gen_id(), resources :: [resource()]}).
-record(ack_assignments, {gen_id :: gen_id()}).
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

%% @doc Called by the leader on group members to stage the resources for a given
%% generation id.  Members should "release" the resources from previous generations and
%% prepare to use the received resources
-spec stage_assignments(name(), gen_id(), [resource()]) -> ok.
stage_assignments(Name, GenId, Assignments) ->
    gen_statem:cast(?via(Name), #new_assignments{gen_id = GenId, resources = Assignments}).

%% @doc Called by the group member on leader as a positive acknowledgement to received
%% resources, in response to `new_assignments'.
-spec ack_assignments(name(), gen_id()) -> ok.
ack_assignments(Name, GenId) ->
    gen_statem:cast(?via(Name), #ack_assignments{gen_id = GenId}).

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
        compute_allocation_fn := ComputeAllocationFn
    } = Opts,
    RetryElectionTimeout = maps:get(retry_election_timeout, Opts, 5_000),
    ok = pg:join(Scope, ?GROUP(Name), self()),
    CData = #{
        name => Name,
        gen_id => 0,
        compute_allocation_fn => ComputeAllocationFn,
        retry_election_timeout => RetryElectionTimeout,
        scope => Scope
    },
    {ok, ?candidate, CData}.

handle_event(enter, OldState, NewState, Data) ->
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
%% Misc events
handle_event(info, {global_name_conflict, ?GROUP(Name), _OthePid}, State, Data = #{name := Name}) ->
    handle_name_clash(State, Data);
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
handle_state_enter(OldState, NewState, Data) ->
    ?tp(
        debug,
        "foreman_state_enter",
        #{
            previous_state => OldState,
            current_state => NewState,
            data => Data
        }
    ),
    case NewState of
        ?candidate ->
            {keep_state_and_data, [{state_timeout, _Now = 0, #try_election{}}]};
        ?follower ->
            keep_state_and_data;
        ?leader ->
            %% Assert
            core = mria_rlog:role(),
            keep_state_and_data
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
            LData = CData#{pg_ref => Ref},
            ?tp(debug, "foreman_elected", #{leader => self()}),
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
                    LData = CData#{pg_ref => Ref},
                    ?tp(debug, "foreman_elected", #{leader => self()}),
                    {next_state, ?leader, LData};
                LeaderPid when is_pid(LeaderPid) ->
                    MRef = monitor(process, LeaderPid),
                    FData = CData#{leader => LeaderPid, leader_mon => MRef},
                    ?tp(debug, "foreman_elected", #{leader => LeaderPid}),
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
            ?tp(debug, "foreman_elected", #{leader => LeaderPid}),
            {next_state, ?follower, FData}
    end.

-spec handle_pg_event(join | leave, [pid()], leader_data()) ->
    handler_result(?leader, leader_data()).
handle_pg_event(_JoinOrLeave, _Pids, _LData) ->
    ?tp("foreman_pg_event", #{event => _JoinOrLeave, pids => _Pids, data => _LData}),
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

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% current_members(Scope, Name) ->
%%     pg:get_members(Scope, ?GROUP(Name)).

-spec follower_to_candidate_data(follower_data()) -> candidate_data().
follower_to_candidate_data(FData) ->
    maps:with(
        [
            gen_id,
            name,
            scope,
            compute_allocation_fn,
            retry_election_timeout
        ],
        FData
    ).
