%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_consumer_coordinator).

%%========================================================================================
%% @doc This process is used by the Kinesis Consumer bridge to (re)distribute shards among
%% nodes.
%%
%% See also the original RFC:
%% https://emqx.atlassian.net/wiki/spaces/P/pages/692092942/Consumer+data+bridge
%%
%% # Election process
%%
%% 0) Each node that has the bridge configured starts this process.
%%
%% 1) The process always starts in the `candidate' state.  It then immediately attempts to
%%    elect itself as `leader' by registering a name that should be unique per bridge.
%%
%% 2) If the registration is successful, the process transitions to the `leader' state and
%%    then distributes shards among the cluster nodes.  Such assignments are stored in
%%    Mnesia.  It also subscribes to cluster membership change events using `ekka:monitor'
%%    API.
%%
%% 3) If the registration is unsuccessful (i.e.: another process already claimed the
%%    name), then it transitions to the `follower' state and will simply read the
%%    decisions taken by the leader.
%%
%% 4) Both `leader' and `follower' processes should then start one worker process for each
%%    locally assigned shard to start consuming the stream.  (TODO)
%%
%% 5) Each coordinator process may periodically check its assignments to stop or start new
%%    worker processes if assignments changed.  Also, the `leader' may send a message to
%%    specific nodes alerting them to changes in assignments.  (TODO)
%%
%% 6) The `follower' coordinators monitor the `leader' process.  If that process dies,
%%    each `follower' will enter the `candidate' state and begin the election process from
%%    (1).
%%
%% 7) In the event that the EMQX cluster is partitioned into 2 or more cliques, each with
%%    its `leader', and then the partitioning is resolved, `global' will signal the
%%    conflict to all leaders, which will step down by stopping and being restarted by
%%    their supervisor.  It's important that they terminate so that `follower' processes
%%    will be notified and update their monitors to the new leader which will be elected
%%    accordingly.
%%
%% # Shard distribution process
%%
%% Once elected, the leader then proceeds to list the existing Kinesis stream shard list,
%% the existing shard assignments per node (if any) and the current cluster members.
%%
%% ## Redistribution of shards (`?redistribute_stale` event)
%%
%% Unassigned shards, shards assigned to non-cluster nodes (removed nodes/nodes that left)
%% and shards assigned to nodes that are currently down are distributed among the running
%% nodes.  This process is performed by the leader when it enters such state, and also
%% periodically.  Even if there are not enough shards to be assigned to all nodes, an
%% empty shard list is assigned to the surplus nodes to indicate that they've been
%% accounted for.
%%
%% ## Nodes added (`{timeout, ?add_nodes}' event)
%%
%% When new nodes join the cluster, the `leader' process is notified and buffers the new
%% nodes during a configurable amount of time.  After this time elapses, it then proceeds
%% to distribute shards to those new nodes by moving assignments from nodes with more
%% shards to the new ones.  The simple heuristic to distribute shards is as follows.
%%
%% The minimum desired number of shards per node is calculated by dividing the total
%% number of shards by the total number of nodes (including new ones),
%% `DesiredMinPerNode'.  Using that number, it then defines the potential donor nodes as
%% those that have more shards assigned to them than this minimum number.
%%
%% Then, for each new node, surplus shards are taken from the donor nodes and assigned to
%% the new nodes so that they have _at most_ `DesiredMinPerNode' shards.
%%
%% ## Nodes removed (`{timeout, ?remove_nodes}' event)
%%
%% When nodes leave the cluster or become down, the `leader' process is notified and
%% buffers the dead nodes during a configurable amount of time.  After this time elapses,
%% it then proceeds to move shards that were assigned to those dead nodes to the other
%% running nodes.  It distributes those dangling shards in a round-robin fashion to each
%% surviving node.
%%========================================================================================

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([start_link/1]).

%% `gen_statem' API
-export([
    callback_mode/0,
    init/1,
    terminate/3,
    handle_event/4
]).

%% `global' callback
-export([resolve_name_clash/3]).

-ifdef(TEST).
-export([compute_donation_plan/3]).
-endif.

%% states
-define(candidate, candidate).
-define(leader, leader).
-define(follower, follower).

%% events
-define(try_election, try_election).
-define(redistribute_stale, redistribute_stale).
-define(remove_nodes, remove_nodes).
-define(add_nodes, add_nodes).

-type leader_name() :: term().
-type stream_name() :: binary().
-type candidate_data() ::
    #{
        leader_name := leader_name(),
        stream_name := stream_name(),
        membership_change_timeout := timer:time(),
        redistribute_stale_timeout := timer:time()
        %% specific fields:
    }.
-type leader_data() ::
    #{
        leader_name := leader_name(),
        stream_name := stream_name(),
        membership_change_timeout := timer:time(),
        redistribute_stale_timeout := timer:time(),
        %% specific fields:
        is_leader := true,
        nodes_to_remove := #{node() => true},
        nodes_to_add := #{node() => true}
    }.
-type follower_data() ::
    #{
        leader_name := leader_name(),
        stream_name := stream_name(),
        membership_change_timeout := timer:time(),
        redistribute_stale_timeout := timer:time(),
        %% specific fields:
        leader := pid(),
        leader_mon := reference()
    }.

-type membership_event() ::
    {node, up | down | leaving | joining, node()}
    | {mnesia, up | down, node()}.

-type shard_id() :: emqx_bridge_kinesis_consumer:shard_id().
-type state() :: ?candidate | ?follower | ?leader.
-type data() :: candidate_data() | follower_data() | leader_data().
-type handler_result(State, Data) :: gen_statem:event_handler_result(State, Data).
-type handler_result() ::
    handler_result(?leader, leader_data())
    | handler_result(?follower, follower_data())
    | handler_result(?candidate, candidate_data()).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_statem:start_link(?MODULE, Opts, []).

%%--------------------------------------------------------------------
%% `gen_statem' API
%%--------------------------------------------------------------------

callback_mode() ->
    [handle_event_function, state_enter].

init(Opts = #{}) ->
    process_flag(trap_exit, true),
    ok = emqx_bridge_kinesis_consumer:create_tables(),
    CData = #{
        leader_name => maps:get(leader_name, Opts),
        stream_name => maps:get(stream_name, Opts),
        membership_change_timeout => maps:get(membership_change_timeout, Opts),
        redistribute_stale_timeout => maps:get(redistribute_stale_timeout, Opts)
    },
    {ok, ?candidate, CData}.

terminate(_Reason, _State, _Data) ->
    ?tp(
        debug,
        "kinesis_coordinator_terminating",
        #{reason => _Reason, state => _State, data => _Data}
    ),
    ok.

handle_event(enter, OldState, State, Data) ->
    handle_enter_state(OldState, State, Data);
handle_event(state_timeout, ?try_election, ?candidate, CData) ->
    try_election(CData);
%% --- follower events
handle_event(
    info,
    {'DOWN', MRef, process, LeaderPid, _Reason},
    ?follower,
    FData = #{leader_mon := MRef, leader := LeaderPid}
) ->
    ?tp(debug, "kinesis_coordinator_leader_down", #{reason => _Reason, leader_pid => LeaderPid}),
    handle_leader_down(FData);
%% --- leader events
handle_event(state_timeout, ?redistribute_stale, ?leader, LData) ->
    handle_redistribute_stale(LData);
handle_event(
    info,
    {global_name_conflict, LeaderName, _OtherPid},
    ?leader,
    LData = #{leader_name := LeaderName}
) ->
    ?tp(
        debug,
        "kinesis_coordinator_step_down",
        #{
            current_state => ?leader,
            data => LData,
            other_pid => _OtherPid,
            pid => self()
        }
    ),
    handle_step_down(LData);
handle_event(info, {membership, MembershipEvent}, ?leader, LData) ->
    ?tp(debug, "kinesis_coordinator_membership_event", #{event => MembershipEvent}),
    handle_membership_event(LData, MembershipEvent);
handle_event({timeout, ?remove_nodes}, ?remove_nodes, ?leader, LData) ->
    handle_remove_nodes(LData);
handle_event({timeout, ?add_nodes}, ?add_nodes, ?leader, LData) ->
    handle_add_nodes(LData);
%% ---
handle_event(_Event, _EventContent, _State, _Data) ->
    ?tp(
        debug,
        "kinesis_coordinator_unknown_event",
        #{
            current_state => _State,
            data => _Data,
            event => _Event,
            event_content => _EventContent,
            pid => self()
        }
    ),
    keep_state_and_data.

%%--------------------------------------------------------------------
%% Handler fns
%%--------------------------------------------------------------------

-spec handle_enter_state(state(), S, D) -> handler_result(S, D) when
    S :: state(), D :: data().
handle_enter_state(OldState, State, Data) ->
    ?tp(
        debug,
        "kinesis_coordinator_state_enter",
        #{
            previous_state => OldState,
            current_state => State,
            data => Data,
            pid => self()
        }
    ),
    case State of
        ?candidate ->
            {keep_state_and_data, [{state_timeout, _Now = 0, ?try_election}]};
        ?leader ->
            ok = ekka:monitor(membership),
            {keep_state_and_data, [{state_timeout, _Now = 0, ?redistribute_stale}]};
        ?follower ->
            keep_state_and_data
    end.

-spec try_election(candidate_data()) -> handler_result().
try_election(Data0) ->
    #{leader_name := LeaderName} = Data0,
    case global:register_name(LeaderName, self(), fun ?MODULE:resolve_name_clash/3) of
        yes ->
            ?tp(debug, "kinesis_coordinator_elected", #{leader => self()}),
            LData = Data0#{
                is_leader => true,
                nodes_to_add => #{},
                nodes_to_remove => #{}
            },
            {next_state, ?leader, LData};
        no ->
            case global:whereis_name(LeaderName) of
                undefined ->
                    %% race condition: leader died / network partition?
                    RetryElectionTimeout = 5_000,
                    {keep_state_and_data, [{state_timeout, RetryElectionTimeout, ?try_election}]};
                LeaderPid when LeaderPid =:= self() ->
                    %% `global' name clash resolve function picked this process?
                    %% currently, we make the leaders shutdown, so this should be
                    %% unreachable until that behavior changes.
                    ?tp(debug, "kinesis_coordinator_elected", #{leader => self()}),
                    LData = Data0#{is_leader => true},
                    {next_state, ?leader, LData};
                LeaderPid when is_pid(LeaderPid) ->
                    ?tp(debug, "kinesis_coordinator_elected", #{leader => LeaderPid}),
                    MRef = monitor(process, LeaderPid),
                    FData = Data0#{leader => LeaderPid, leader_mon => MRef},
                    {next_state, ?follower, FData}
            end
    end.

resolve_name_clash(Name, Pid1, Pid2) ->
    global:notify_all_name(Name, Pid1, Pid2).

-spec handle_leader_down(follower_data()) -> handler_result(?candidate, candidate_data()).
handle_leader_down(FData0) ->
    %% todo: better handling of keys
    CData = maps:without([leader, leader_mon], FData0),
    {next_state, ?candidate, CData}.

%% --- leader events
-spec handle_redistribute_stale(leader_data()) -> handler_result(?leader, leader_data()).
handle_redistribute_stale(LData) ->
    #{
        leader_name := LeaderName,
        stream_name := StreamName,
        membership_change_timeout := MembershipChangeTimeout,
        redistribute_stale_timeout := RedistributeStaleTimeout
    } = LData,
    ?tp(debug, "kinesis_coordinator_redistributing", #{}),
    case emqx_bridge_kinesis_consumer:list_shard_ids(LeaderName, StreamName) of
        {ok, ShardIds} ->
            #{
                unassigned := Unassigned,
                with_down_nodes := WithDownNodes,
                with_unknown_nodes := WithUnknownNodes,
                unexpected_shards := UnexpectedShards,
                nodes_without_assignments := NodesWithoutAssignments
            } = Issues = emqx_bridge_kinesis_consumer:get_shard_issues(ShardIds, LeaderName),
            {atomic, ok} = emqx_bridge_kinesis_consumer:transaction(fun() ->
                DeadNodes = maps:values(WithDownNodes) ++ maps:values(WithUnknownNodes),
                ToAssign = maps:keys(WithDownNodes) ++ maps:keys(WithUnknownNodes) ++ Unassigned,
                ok = delete_multi_shard_assignments(LeaderName, DeadNodes),
                Nodes = emqx_bridge_kinesis_consumer:all_nodes() -- DeadNodes,
                NodeAssignments = distribute(ToAssign, Nodes),
                ok = emqx_bridge_kinesis_consumer:merge_shard_assignments(
                    LeaderName, NodeAssignments
                ),
                %% remove unexpected shard ids
                NodeAssignments1 = emqx_bridge_kinesis_consumer:get_all_shard_assignments(
                    LeaderName
                ),
                FinalNodeAssignments =
                    maps:fold(
                        fun(Node, UnexpectedSIds, Acc) ->
                            maps:update_with(Node, fun(SIds) -> SIds -- UnexpectedSIds end, [], Acc)
                        end,
                        NodeAssignments1,
                        UnexpectedShards
                    ),
                ok = emqx_bridge_kinesis_consumer:set_shard_assignments(
                    LeaderName, FinalNodeAssignments
                ),
                ok
            end),
            ?tp(debug, "kinesis_coordinator_redistributed", #{issues => Issues}),
            case NodesWithoutAssignments of
                [] ->
                    {keep_state_and_data, [
                        {state_timeout, RedistributeStaleTimeout, ?redistribute_stale}
                    ]};
                [_ | _] ->
                    NewLData = lists:foldl(fun stage_add_node/2, LData, NodesWithoutAssignments),
                    {keep_state, NewLData, [
                        {{timeout, ?add_nodes}, MembershipChangeTimeout, ?add_nodes},
                        {state_timeout, RedistributeStaleTimeout, ?redistribute_stale}
                    ]}
            end;
        Error ->
            ?tp(warning, "kinesis_coordinator_error_listing_shards", #{reason => Error}),
            RetryTimeout = 10_000,
            {keep_state_and_data, [{state_timeout, RetryTimeout, ?redistribute_stale}]}
    end.

-spec handle_step_down(leader_data()) -> handler_result(?candidate, candidate_data()).
handle_step_down(_LData) ->
    %% It's important that the leaders die when stepping down so that monitors and pid
    %% references are refreshed by their followers.
    {stop, step_down}.

-spec handle_membership_event(leader_data(), membership_event()) ->
    handler_result(?leader, leader_data()).
handle_membership_event(LData0, {node, up, NewNode}) ->
    #{membership_change_timeout := Timeout} = LData0,
    LData = stage_add_node(NewNode, LData0),
    {keep_state, LData, [{{timeout, ?add_nodes}, Timeout, ?add_nodes}]};
handle_membership_event(LData0, {mnesia, up, NewNode}) ->
    %% When a node leaves and re-joins the cluster without fully halting, only this event
    %% is emitted: `{node, up, node()}' doesn't happen again currently.
    %% This event is only triggered by core nodes.
    #{membership_change_timeout := Timeout} = LData0,
    LData = stage_add_node(NewNode, LData0),
    {keep_state, LData, [{{timeout, ?add_nodes}, Timeout, ?add_nodes}]};
handle_membership_event(LData0, {node, down, DeadNode}) ->
    #{membership_change_timeout := Timeout} = LData0,
    LData = stage_remove_node(DeadNode, LData0),
    {keep_state, LData, [{{timeout, ?remove_nodes}, Timeout, ?remove_nodes}]};
handle_membership_event(LData0, {mnesia, down, DeadNode}) ->
    %% This event is only triggered by core nodes.
    #{membership_change_timeout := Timeout} = LData0,
    LData = stage_remove_node(DeadNode, LData0),
    {keep_state, LData, [{{timeout, ?remove_nodes}, Timeout, ?remove_nodes}]};
handle_membership_event(LData0, {node, leaving, DeadNode}) ->
    #{membership_change_timeout := Timeout} = LData0,
    LData = stage_remove_node(DeadNode, LData0),
    {keep_state, LData, [{{timeout, ?remove_nodes}, Timeout, ?remove_nodes}]};
handle_membership_event(_LData, _Event) ->
    %% Possible membership events:
    %% - `{node, up | down | joining | healing, node()}'
    %%   + `joining' is not triggered when a node re-joins a cluster that has seen it.
    %%   + `leaving' can only be observed by replicants when the core it's connected to
    %%     joins another cluster.
    %% - `{mnesia, up | down, node()}'
    keep_state_and_data.

%%--------------------------------------------------------------------
%% Membership change-specific helpers
%%--------------------------------------------------------------------

-spec handle_remove_nodes(leader_data()) -> handler_result(?leader, leader_data()).
handle_remove_nodes(_LData = #{nodes_to_remove := DeadNodes}) when map_size(DeadNodes) =:= 0 ->
    keep_state_and_data;
handle_remove_nodes(LData) ->
    #{leader_name := LeaderName, nodes_to_remove := DeadNodes0} = LData,
    DeadNodes = maps:keys(DeadNodes0),
    AllNodes = emqx_bridge_kinesis_consumer:all_nodes(),
    %% repeated down events for a node should be idempotent: it's already unassigned.
    Nodes = AllNodes -- DeadNodes,
    {atomic, Status} = emqx_bridge_kinesis_consumer:transaction(fun() ->
        NodeAssignments = emqx_bridge_kinesis_consumer:get_all_shard_assignments(LeaderName),
        ShardIds = get_multi_shard_assignments(LeaderName, DeadNodes),
        ok = delete_multi_shard_assignments(LeaderName, DeadNodes),
        Assignments = distribute(ShardIds, Nodes),
        emqx_bridge_kinesis_consumer:merge_shard_assignments(LeaderName, Assignments),
        FinalNodeAssignments = emqx_bridge_kinesis_consumer:get_all_shard_assignments(LeaderName),
        #{
            final_assignments => FinalNodeAssignments,
            initial_assignments => NodeAssignments,
            all_nodes => AllNodes,
            remaining_nodes => Nodes,
            event_nodes => DeadNodes
        }
    end),
    ?tp(debug, "kinesis_coordinator_nodes_removed", Status),
    NewLData = LData#{nodes_to_remove := #{}},
    {keep_state, NewLData}.

-spec handle_add_nodes(leader_data()) -> handler_result(?leader, leader_data()).
handle_add_nodes(_LData = #{nodes_to_add := NewNodes}) when map_size(NewNodes) =:= 0 ->
    keep_state_and_data;
handle_add_nodes(LData) ->
    #{leader_name := LeaderName, nodes_to_add := NewNodes0} = LData,
    AllNodes = emqx_bridge_kinesis_consumer:all_nodes(),
    {atomic, Status} = emqx_bridge_kinesis_consumer:transaction(fun() ->
        NodeAssignments = emqx_bridge_kinesis_consumer:get_all_shard_assignments(LeaderName),
        DonorNodes = maps:keys(NodeAssignments),
        %% ensure that repeated up events won't treat an assigned node as new.
        NewNodes1 = maps:keys(NewNodes0),
        NewNodes = NewNodes1 -- DonorNodes,
        ShardIds = lists:concat(maps:values(NodeAssignments)),
        DesiredMinPerNode = length(ShardIds) div length(AllNodes),
        Plan = compute_donation_plan(NewNodes, DesiredMinPerNode, NodeAssignments),
        ok = emqx_bridge_kinesis_consumer:set_shard_assignments(LeaderName, Plan),
        FinalNodeAssignments = emqx_bridge_kinesis_consumer:get_all_shard_assignments(LeaderName),
        #{
            final_assignments => FinalNodeAssignments,
            initial_assignments => NodeAssignments,
            plan => Plan,
            all_nodes => AllNodes,
            donor_nodes => DonorNodes,
            event_nodes => NewNodes1,
            new_nodes => NewNodes
        }
    end),
    ?tp(debug, "kinesis_coordinator_nodes_added", Status),
    NewLData = LData#{nodes_to_add := #{}},
    {keep_state, NewLData}.

%%--------------------------------------------------------------------
%% Internal helper fns
%%--------------------------------------------------------------------

-spec distribute([shard_id()], [node()]) -> #{node() => [shard_id()]}.
distribute(ShardIds, Nodes) ->
    NodesIndex = maps:from_list(lists:enumerate(Nodes)),
    NumNodes = map_size(NodesIndex),
    lists:foldl(
        fun({N, ShardId}, Acc) ->
            Index = 1 + N rem NumNodes,
            Node = maps:get(Index, NodesIndex),
            maps:update_with(Node, fun(SIds) -> [ShardId | SIds] end, [ShardId], Acc)
        end,
        #{},
        lists:enumerate(ShardIds)
    ).

-spec get_multi_shard_assignments(leader_name(), [node()]) -> [shard_id()].
get_multi_shard_assignments(LeaderName, Nodes) ->
    lists:usort(
        [
            SId
         || Node <- Nodes,
            SId <- emqx_bridge_kinesis_consumer:get_shard_assignments(LeaderName, Node)
        ]
    ).

-spec delete_multi_shard_assignments(leader_name(), [node()]) -> ok.
delete_multi_shard_assignments(LeaderName, Nodes) ->
    lists:foreach(
        fun(N) -> emqx_bridge_kinesis_consumer:delete_shard_assignments(LeaderName, N) end,
        Nodes
    ).

-spec stage_add_node(node(), leader_data()) -> leader_data().
stage_add_node(NewNode, LData0) ->
    LData1 = maps:update_with(
        nodes_to_add,
        fun(Old) -> Old#{NewNode => true} end,
        #{NewNode => true},
        LData0
    ),
    maps:update_with(nodes_to_remove, fun(Old) -> maps:remove(NewNode, Old) end, #{}, LData1).

-spec stage_remove_node(node(), leader_data()) -> leader_data().
stage_remove_node(DeadNode, LData0) ->
    LData1 = maps:update_with(
        nodes_to_remove,
        fun(Old) -> Old#{DeadNode => true} end,
        #{DeadNode => true},
        LData0
    ),
    maps:update_with(nodes_to_add, fun(Old) -> maps:remove(DeadNode, Old) end, #{}, LData1).

-spec compute_donation_plan(
    RecipientNodes :: [node()],
    Desired :: non_neg_integer(),
    NodeAssignments :: #{node() => [shard_id()]}
) -> Plan :: #{node() => [shard_id()]}.
compute_donation_plan(RecipientNodes, DesiredMinPerNode, NodeAssignments) ->
    SurplusAssignments =
        maps:filter(
            fun(_N, SIds) -> length(SIds) > DesiredMinPerNode end,
            NodeAssignments
        ),
    Stock = maps:to_list(SurplusAssignments),
    compute_donation_plan(RecipientNodes, DesiredMinPerNode, Stock, _Plan = #{}).

-spec compute_donation_plan(
    RecipientNodes :: [node()],
    Desired :: non_neg_integer(),
    Stock :: [{node(), [shard_id()]}],
    Plan
) -> Plan when
    Plan :: #{node() => [shard_id()]}.
compute_donation_plan(_RecipientNodes = [], _DesiredMinPerNode, Stock, Plan) when is_list(Stock) ->
    Plan;
compute_donation_plan(RecipientNodes, _DesiredMinPerNode = 0, Stock, Plan) when is_list(Stock) ->
    %% important to mark nodes without shards as assigned
    maps:merge(Plan, maps:from_keys(RecipientNodes, []));
compute_donation_plan(RecipientNodes, _DesiredMinPerNode, _Stock = [], Plan) ->
    %% important to mark nodes without shards as assigned
    maps:merge(Plan, maps:from_keys(RecipientNodes, []));
compute_donation_plan([Recipient | Rest], DesiredMinPerNode, Stock = [_ | _], Plan0) ->
    {NewStock, Donations, DepletedDonors} = compute_donations1(DesiredMinPerNode, Stock),
    Plan1 = maps:merge(Plan0, DepletedDonors),
    NewPlan = Plan1#{Recipient => Donations},
    compute_donation_plan(Rest, DesiredMinPerNode, NewStock, NewPlan).

-spec compute_donations1(Desired :: pos_integer(), Stock) ->
    {Stock, Donations, DepletedDonors}
when
    Stock :: [{node(), [shard_id()]}, ...],
    Donations :: [shard_id()],
    DepletedDonors :: #{node() => [shard_id()]}.
compute_donations1(DesiredMinPerNode, Stock = [_ | _]) ->
    Remaining = DesiredMinPerNode,
    compute_donations1(DesiredMinPerNode, Remaining, Stock, _Donations = [], _DepletedDonors = #{}).

%% Invariant: each donor in stock has more than `DesiredMinPerNode'.
-spec compute_donations1(
    Desired :: pos_integer(),
    Remaining :: non_neg_integer(),
    Stock,
    Donations,
    DepletedDonors
) -> {Stock, Donations, DepletedDonors} when
    Stock :: [{node(), [shard_id()]}],
    Donations :: [shard_id()],
    DepletedDonors :: #{node() => [shard_id()]}.
compute_donations1(_DesiredMinPerNode, _Remaining, Stock = [], Donations, DepletedDonors) ->
    {Stock, Donations, DepletedDonors};
compute_donations1(_DesiredMinPerNode, Remaining, Stock, Donations, DepletedDonors) when
    Remaining =< 0
->
    {Stock, Donations, DepletedDonors};
compute_donations1(DesiredMinPerNode, Remaining, [{Donor, SIds} | Rest], Donations, DepletedDonors) ->
    NumToTake = min(DesiredMinPerNode, Remaining),
    {NSIds0, NSIds1} = lists:split(NumToTake, SIds),
    %% minimizing the number of transferred shards
    {ToKeep, ToGive} =
        case length(NSIds0) > length(NSIds1) of
            true -> {NSIds0, NSIds1};
            false -> {NSIds1, NSIds0}
        end,
    %% remember to update donors, even if not depleted
    NewDepletedDonors = DepletedDonors#{Donor => ToKeep},
    NewStock =
        case length(ToKeep) > DesiredMinPerNode of
            true ->
                %% still has some juice left
                [{Donor, ToKeep} | Rest];
            false ->
                %% this donor is depleted
                Rest
        end,
    NewDonations = ToGive ++ Donations,
    NewRemaining = Remaining - length(ToGive),
    compute_donations1(DesiredMinPerNode, NewRemaining, NewStock, NewDonations, NewDepletedDonors).
