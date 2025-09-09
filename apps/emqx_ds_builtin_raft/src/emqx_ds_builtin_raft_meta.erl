%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Metadata storage for the builtin sharded database.
%%
%% Currently metadata is stored in mria; that's not ideal, but
%% eventually we'll replace it, so it's important not to leak
%% implementation details from this module.
-module(emqx_ds_builtin_raft_meta).

-compile(inline).

-export([print_status/3]).

-behaviour(gen_server).

%% API:
-export([
    shards/1,
    my_shards/1,
    shard_info/2,
    allocate_shards/1,
    replica_set/2,
    sites/0,
    sites/1,
    node/1,
    node_status/1,
    this_site/0,
    forget_site/1,
    print_status/0
]).

%% DB API:
-export([
    open_db/2,
    db_config/1,
    update_db_config/2,
    drop_db/1,
    dbs/0
]).

%% Site / shard allocation:
-export([
    join_db_site/2,
    leave_db_site/2,
    forget_node/1,
    assign_db_sites/2,
    modify_db_sites/2,
    db_sites/1,
    db_target_sites/1,
    replica_set_transitions/2,
    claim_transition/3,
    update_replica_set/3,
    target_set/2
]).

%% Subscriptions to changes:
-export([
    subscribe/2,
    unsubscribe/1
]).

%% gen_server
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    open_db_trans/2,
    allocate_shards_trans/2,
    assign_db_sites_trans/2,
    modify_db_sites_trans/2,
    claim_transition_trans/3,
    update_replica_set_trans/3,
    update_db_config_trans/2,
    drop_db_trans/1,
    claim_site_trans/2,
    forget_site_trans/1,
    n_shards/1
]).

%% Migrations:
-export([]).

-export_type([
    site/0,
    transition/0,
    subscription_event/0,
    update_cluster_result/0
]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(RLOG_SHARD, emqx_ds_builtin_metadata_shard).
%% DS database metadata:
-define(META_TAB, emqx_ds_builtin_metadata_tab2).
%% Mapping from Site to the actual Erlang node:
-define(NODE_TAB, emqx_ds_builtin_node_tab3).
%% Shard metadata:
-define(SHARD_TAB, emqx_ds_builtin_shard_tab3).
%% Membership transitions:
-define(TRANSITION_TAB, emqx_ds_builtin_trans_tab).

-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_builtin_raft:db_opts()
}).

-record(?NODE_TAB, {
    site :: site(),
    node :: node(),
    misc = #{} :: map()
}).

-record(?SHARD_TAB, {
    shard :: {emqx_ds:db(), emqx_ds:shard()},
    %% Sites that currently contain the data:
    replica_set :: [site()],
    %% Sites that should contain the data when the cluster is in the
    %% stable state (no nodes are being added or removed from it):
    target_set :: [site()] | undefined,
    misc = #{} :: map()
}).

-record(?TRANSITION_TAB, {
    shard :: {emqx_ds:db(), emqx_ds:shard()},
    transition :: transition(),
    misc = #{} :: map()
}).

%% Persistent ID of the node (independent from the IP/FQDN):
-type site() :: binary().

%% Membership transition of shard's replica set:
-type transition() :: {add | del, site()}.

-type site_status() :: up | down | lost.
-type site_info() :: #{status := site_status()}.
-type shard_info() :: #{
    replica_set := #{site() => site_info()},
    target_set => #{site() => site_info()},
    transitions => [#{site() => add | del}]
}.

-type update_cluster_result() ::
    {ok, unchanged | [site()]}
    | {error, {nonexistent_db, emqx_ds:db()}}
    | {error, {nonexistent_sites | lost_sites, [site()]}}
    | {error, {too_few_sites, [site()]}}
    | {error, _}.

%% Subject of the subscription:
-type subject() :: emqx_ds:db().

%% Event for the subscription:
-type subscription_event() ::
    {changed, {shard, emqx_ds:db(), emqx_ds:shard()}}.

%% Peristent term key:
-define(emqx_ds_builtin_site, emqx_ds_builtin_site).

%% Make Dialyzer happy
-define(NODE_PAT(),
    %% Equivalent of `#?NODE_TAB{_ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_')
).

-define(NODE_PAT(NODE),
    %% Equivalent of `#?NODE_TAB{node = NODE, _ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_', [{#?NODE_TAB.node, NODE}])
).

-define(SHARD_PAT(SHARD),
    %% Equivalent of `#?SHARD_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?SHARD_TAB), '_', [{#?SHARD_TAB.shard, SHARD}])
).

-define(TRANSITION_PAT(SHARD),
    %% Equivalent of `#?TRANSITION_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?TRANSITION_TAB), '_', [{#?TRANSITION_TAB.shard, SHARD}])
).

%%================================================================================
%% API functions
%%================================================================================

-spec this_site() -> site().
this_site() ->
    persistent_term:get(?emqx_ds_builtin_site).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    [#?META_TAB{db_props = #{n_shards := NShards}}] = mnesia:dirty_read(?META_TAB, DB),
    NShards.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec shards(emqx_ds:db()) -> [emqx_ds:shard()].
shards(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}} <- Recs].

-spec shard_info(emqx_ds:db(), emqx_ds:shard()) ->
    shard_info() | undefined.
shard_info(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [] ->
            undefined;
        [ShardRecord] ->
            ReplicaSet = get_shard_sites(ShardRecord),
            TargetSet = get_shard_target_sites(ShardRecord),
            Transitions = pending_transitions(ShardRecord),
            Info1 = #{
                replica_set => maps:from_list([{S, site_info(S)} || S <- ReplicaSet])
            },
            case TargetSet of
                ReplicaSet ->
                    Info2 = Info1;
                _ ->
                    Info2 = Info1#{
                        target_set => maps:from_list([{S, site_info(S)} || S <- TargetSet])
                    }
            end,
            case Transitions of
                [] ->
                    Info = Info2;
                _ ->
                    Info = Info2#{
                        transitions => [#{S => T} || {T, S} <- Transitions]
                    }
            end,
            Info
    end.

site_info(Site) ->
    #{status => node_status(?MODULE:node(Site))}.

-spec my_shards(emqx_ds:db()) -> [emqx_ds:shard()].
my_shards(DB) ->
    Site = this_site(),
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}, replica_set = RS} <- Recs, lists:member(Site, RS)].

allocate_shards(DB) ->
    case mria:transaction(?RLOG_SHARD, fun ?MODULE:allocate_shards_trans/2, [this_site(), DB]) of
        {atomic, Shards} ->
            {ok, Shards};
        {aborted, {shards_already_allocated, Shards}} ->
            {ok, Shards};
        {aborted, {insufficient_sites_online, Needed, Sites}} ->
            {error, #{reason => insufficient_sites_online, needed => Needed, sites => Sites}};
        {aborted, this_site_is_gone} ->
            exit(restart)
    end.

%% @doc List all sites.
-spec sites() -> [site()].
sites() ->
    sites(all).

%% @doc List sites.
%% * `all`: all sites.
%% * `cluster`: sites that are considered part of the cluster.
%% * `lost`: sites that are no longer considered part of the cluster.
-spec sites(all | cluster | lost) -> [site()].
sites(all) ->
    [S || #?NODE_TAB{site = S} <- all_nodes()];
sites(cluster) ->
    [S || #?NODE_TAB{site = S, node = N} <- all_nodes(), node_status(N) =/= lost];
sites(lost) ->
    [S || #?NODE_TAB{site = S, node = N} <- all_nodes(), node_status(N) == lost].

-spec node(site()) -> node() | undefined.
node(Site) ->
    case mnesia:dirty_read(?NODE_TAB, Site) of
        [#?NODE_TAB{node = Node}] ->
            Node;
        [] ->
            undefined
    end.

-spec node_status(node()) -> up | down | lost.
node_status(Node) when is_atom(Node) ->
    case mria:cluster_status(Node) of
        running -> up;
        stopped -> down;
        false -> lost
    end.

-spec forget_site(site()) -> ok | {error, _Reason}.
forget_site(Site) ->
    maybe
        [Record] ?= mnesia:dirty_read(?NODE_TAB, Site),
        lost ?= node_status(Record#?NODE_TAB.node),
        %% Node is lost, proceed.
        transaction(fun ?MODULE:forget_site_trans/1, [Record])
    else
        [] ->
            {error, nonexistent_site};
        up ->
            {error, site_up};
        down ->
            %% Node is stopped, reject the request.
            %% If it's gone, it should leave the cluster first.
            {error, site_temporarily_down}
    end.

%%===============================================================================

-spec print_status() -> ok.
print_status() ->
    %% TODO: Consistent view of state.
    Nodes = all_nodes(),
    Shards = all_shards(),
    Transitions = all_transitions(),
    print_status(Nodes, Shards, Transitions).

print_status(Nodes, Shards, Transitions) ->
    PendingTransitions = lists:filtermap(
        fun(Record = #?SHARD_TAB{shard = DBShard}) ->
            ClaimedTs = [T || T = #?TRANSITION_TAB{shard = S} <- Transitions, S == DBShard],
            case pending_transitions(Record, ClaimedTs) of
                [] -> false;
                ShardTransitions -> {true, {DBShard, ShardTransitions}}
            end
        end,
        Shards
    ),
    %% This site
    io:format("THIS SITE:~n"),
    try this_site() of
        Site -> io:format("~s~n", [Site])
    catch
        error:badarg ->
            io:format(
                "(!) UNCLAIMED~n"
                "(!) Likely this node's name is already known as another site in the cluster.~n"
                "(!) Please resolve conflicts manually.~n"
            )
    end,
    %% Sites information
    io:format("~nSITES:~n", []),
    print_table(
        ["Site", "Node", "Status"],
        [
            [Site, Node, format_node_status(node_status(Node))]
         || #?NODE_TAB{site = Site, node = Node} <- Nodes
        ]
    ),
    NodesLost = [Node || #?NODE_TAB{node = Node} <- Nodes, node_status(Node) == lost],
    NodesLost =/= [] andalso
        io:format(
            "(!) ATTENTION~n"
            "(!) One or more sites are lost, replicas under their ownership are gone.~n"
            "(!) Availability may be compromised.~n"
            "(!) Please take actions to bring the cluster back to healthy state.~n"
        ),
    %% Shards information
    io:format("~nSHARDS:~n"),
    print_table(
        ["DB/Shard", "Replicas", "Transitions"],
        [
            [
                format_shard(DBShard),
                {group, [
                    {subcolumns, [
                        format_replicas(RS, Nodes),
                        format_transitions(ShardTransitions, Nodes)
                    ]}
                ]}
            ]
         || #?SHARD_TAB{shard = DBShard, replica_set = RS} <- Shards,
            ShardTransitions <- [proplists:get_value(DBShard, PendingTransitions, [])]
        ]
    ),
    TransitionsStuck = [
        DBShard
     || #?SHARD_TAB{shard = DBShard, replica_set = RS} <- Shards,
        RSLost <- [[Site || Site <- RS, site_status(Site, Nodes) == lost]],
        length(RSLost) * 2 >= length(RS)
    ],
    TransitionsStuck =/= [] andalso
        io:format(
            "(!) ATTENTION~n"
            "(!) One or more shards have replica sets where majority of replicas are gone.~n"
            "(!) Membership changes are compromised.~n"
            "(!) Please take necessary steps to deal with lost sites.~n"
            "(!) Prepare for the possibility of data loss.~n"
        ),
    ok.

format_shard({DB, Shard}) ->
    io_lib:format("~p/~s", [DB, Shard]).

format_replicas(RS, Nodes) ->
    [format_replica(R, Nodes) || R <- RS].

format_replica(Site, Nodes) ->
    [Site, format_node_marker(site_status(Site, Nodes))].

site_status(Site, Nodes) ->
    [Node] = [N || #?NODE_TAB{site = S, node = N} <- Nodes, S == Site],
    node_status(Node).

format_transitions(Transitions, Nodes) ->
    [format_transition(T, Nodes) || T <- Transitions].

format_transition({add, Site}, Nodes) ->
    ["+ ", format_replica(Site, Nodes)];
format_transition({del, Site}, Nodes) ->
    ["- ", format_replica(Site, Nodes)].

format_node_status(Status) ->
    case Status of
        up -> "    up";
        down -> "(x) down";
        lost -> "(!) LOST"
    end.

format_node_marker(Status) ->
    case Status of
        up -> "";
        down -> " (x)";
        lost -> " (!)"
    end.

print_table(Header, Rows) ->
    io:put_chars(emqx_utils_fmt:table(Header, Rows)).

%%===============================================================================
%% DB API
%%===============================================================================

-spec db_config(emqx_ds:db()) -> emqx_ds_builtin_raft:db_opts() | #{}.
db_config(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Opts}] ->
            Opts;
        [] ->
            #{}
    end.

-spec open_db(emqx_ds:db(), emqx_ds_builtin_raft:db_opts()) ->
    emqx_ds_builtin_raft:db_opts().
open_db(DB, DefaultOpts) ->
    transaction(fun ?MODULE:open_db_trans/2, [DB, DefaultOpts]).

-spec update_db_config(emqx_ds:db(), emqx_ds_builtin_raft:db_opts()) ->
    emqx_ds_builtin_raft:db_opts() | {error, nonexistent_db}.
update_db_config(DB, DefaultOpts) ->
    transaction(fun ?MODULE:update_db_config_trans/2, [DB, DefaultOpts]).

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    transaction(fun ?MODULE:drop_db_trans/1, [DB]).

-spec dbs() -> [emqx_ds:db()].
dbs() ->
    mnesia:dirty_all_keys(?META_TAB).

%%===============================================================================
%% Site / shard allocation API
%%===============================================================================

%% @doc Join a site to the set of sites the DB is replicated across.
-spec join_db_site(emqx_ds:db(), site()) -> update_cluster_result().
join_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{add, Site}]]).

%% @doc Make a site leave the set of sites the DB is replicated across.
-spec leave_db_site(emqx_ds:db(), site()) -> update_cluster_result().
leave_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{del, Site}]]).

%% @doc Assign a set of sites to the DB for replication.
-spec assign_db_sites(emqx_ds:db(), [site()]) -> update_cluster_result().
assign_db_sites(DB, Sites) ->
    transaction(fun ?MODULE:assign_db_sites_trans/2, [DB, Sites]).

%% @doc Assign a set of sites to the DB for replication.
-spec modify_db_sites(emqx_ds:db(), [transition()]) -> update_cluster_result().
modify_db_sites(DB, Transitions) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, Transitions]).

%% @doc List the sites the DB is currently replicated across.
-spec db_sites(emqx_ds:db()) -> [site()].
db_sites(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    list_sites(Recs).

%% @doc List the sites the DB should be replicated across, once transitions
%% are completed. If no transitions are pending, equivalent to `db_sites/1`.
-spec db_target_sites(emqx_ds:db()) -> [site()].
db_target_sites(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    list_target_sites(Recs).

%% @doc List the sequence of transitions that should be conducted in order to
%% bring the set of replicas for a DB shard in line with the target set.
-spec replica_set_transitions(emqx_ds:db(), emqx_ds:shard()) ->
    [transition()] | undefined.
replica_set_transitions(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [Record] ->
            pending_transitions(Record);
        [] ->
            undefined
    end.

%% @doc Claim the intention to start the replica set transition for the given shard.
%% To be called before starting acting on transition, so that information about this
%% will not get lost. Once it finishes, call `update_replica_set/3`.
-spec claim_transition(emqx_ds:db(), emqx_ds:shard(), transition()) ->
    ok | {error, {conflict, transition()} | {outdated, _Expected :: [transition()]}}.
claim_transition(DB, Shard, Trans) ->
    transaction(fun ?MODULE:claim_transition_trans/3, [DB, Shard, Trans]).

%% @doc Update the set of replication sites for a shard.
%% To be called after a `transition()` has been conducted successfully.
-spec update_replica_set(emqx_ds:db(), emqx_ds:shard(), transition()) -> ok.
update_replica_set(DB, Shard, Trans) ->
    transaction(fun ?MODULE:update_replica_set_trans/3, [DB, Shard, Trans]).

%% @doc Get the current set of replication sites for a shard.
-spec replica_set(emqx_ds:db(), emqx_ds:shard()) ->
    [site()] | undefined.
replica_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{replica_set = ReplicaSet}] ->
            ReplicaSet;
        [] ->
            undefined
    end.

%% @doc Get the target set of replication sites for a DB shard.
%% Target set is updated every time the set of replication sites for the DB changes.
%% See `join_db_site/2`, `leave_db_site/2`, `assign_db_sites/2`.
-spec target_set(emqx_ds:db(), emqx_ds:shard()) ->
    [site()] | undefined.
target_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{target_set = TargetSet}] ->
            TargetSet;
        [] ->
            undefined
    end.

%%================================================================================

subscribe(Pid, Subject) ->
    gen_server:call(?SERVER, {subscribe, Pid, Subject}, infinity).

unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    subs = #{} :: #{pid() => {subject(), _Monitor :: reference()}}
}).

init([]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [ds, meta]}),
    ok = ekka:monitor(membership),
    ensure_tables(),
    run_migrations(),
    ensure_site(),
    {ok, _Node} = mnesia:subscribe({table, ?SHARD_TAB, simple}),
    {ok, #s{}}.

handle_call({subscribe, Pid, Subject}, _From, S) ->
    {reply, ok, handle_subscribe(Pid, Subject, S)};
handle_call({unsubscribe, Pid}, _From, S) ->
    {reply, ok, handle_unsubscribe(Pid, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({mnesia_table_event, {write, #?SHARD_TAB{shard = {DB, Shard}}, _}}, S) ->
    ok = notify_subscribers(DB, {shard, DB, Shard}, S),
    {noreply, S};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, S) ->
    {noreply, handle_unsubscribe(Pid, S)};
handle_info({membership, {node, leaving, _Node}}, S) ->
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{}) ->
    persistent_term:erase(?emqx_ds_builtin_site),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec open_db_trans(emqx_ds:db(), emqx_ds_builtin_raft:db_opts()) ->
    emqx_ds_builtin_raft:db_opts().
open_db_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [] ->
            mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
            CreateOpts;
        [#?META_TAB{db_props = Opts}] ->
            case maps:merge(CreateOpts, Opts) of
                Opts ->
                    Opts;
                UpdatedOpts ->
                    %% NOTE
                    %% Preserve any new options not yet present in the DB. This is
                    %% most likely because `Opts` is outdated, written by earlier
                    %% EMQX version.
                    mnesia:write(#?META_TAB{db = DB, db_props = UpdatedOpts}),
                    UpdatedOpts
            end
    end.

-spec allocate_shards_trans(site(), emqx_ds:db()) -> [emqx_ds:shard()].
allocate_shards_trans(Site, DB) ->
    case mnesia:read(?NODE_TAB, Site) of
        [_] ->
            ok;
        [] ->
            %% Workaround for a race condition that may occur on a
            %% replicant in a cluster with 2 or more core nodes.
            %%
            %% Current site may be unexpectedly gone if the following
            %% happens:
            %%
            %% 1. Replicant connects to the core A.
            %%
            %% 2. This module initializes and starts waiting for the
            %% quorum.
            %%
            %% 3. In the meanwhile, A runs autocluster and decides to
            %% join core node B (or the operator commands to do that)
            %%
            %% 4. During join, A wipes its data and replaces it with
            %% B's data. Since replicant nodes DON'T restart business
            %% apps during cluster join, this may go undetected.
            %%
            %% 5. Replicant that registered itself on A ends up in the
            %% situation where its site registration is gone.
            %%
            %% To combat that we just restart the server.
            mnesia:abort(this_site_is_gone)
    end,
    Opts = #{n_shards := NShards, n_sites := NSites} = db_config_trans(DB),
    case mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write) of
        [] ->
            ok;
        Records ->
            ShardsAllocated = [Shard || #?SHARD_TAB{shard = {_DB, Shard}} <- Records],
            mnesia:abort({shards_already_allocated, ShardsAllocated})
    end,
    Sites = sites(cluster),
    case length(Sites) of
        N when N >= NSites ->
            ok;
        _ ->
            mnesia:abort({insufficient_sites_online, NSites, Sites})
    end,
    Shards = gen_shards(NShards),
    Allocation = compute_allocation(Shards, Sites, Opts),
    lists:map(
        fun({Shard, ReplicaSet}) ->
            Record = #?SHARD_TAB{
                shard = {DB, Shard},
                replica_set = ReplicaSet
            },
            ok = mnesia:write(Record),
            Shard
        end,
        Allocation
    ).

-spec assign_db_sites_trans(emqx_ds:db(), [site()]) -> {ok, [site()]}.
assign_db_sites_trans(DB, Sites) ->
    Opts = db_config_trans(DB),
    case length(Sites) of
        0 ->
            mnesia:abort({too_few_sites, Sites});
        _ ->
            ok
    end,
    SiteRecords = [R || S <- Sites, R <- mnesia:read(?NODE_TAB, S, read)],
    NonexistentSites = [S || S <- Sites, not lists:keymember(S, #?NODE_TAB.site, SiteRecords)],
    case NonexistentSites of
        [] ->
            ok;
        [_ | _] ->
            mnesia:abort({nonexistent_sites, NonexistentSites})
    end,
    LostSites = [S || #?NODE_TAB{site = S, node = N} <- SiteRecords, node_status(N) == lost],
    case LostSites of
        [] ->
            ok;
        [_ | _] ->
            mnesia:abort({lost_sites, LostSites})
    end,
    %% TODO
    %% Optimize reallocation. The goals are:
    %% 1. Minimize the number of membership transitions.
    %% 2. Ensure that sites are responsible for roughly the same number of shards.
    Shards = db_shards_trans(DB),
    Reallocation = compute_allocation(Shards, Sites, Opts),
    ok = lists:foreach(
        fun({Record, ReplicaSet}) ->
            ok = mnesia:write(Record#?SHARD_TAB{target_set = ReplicaSet})
        end,
        Reallocation
    ),
    {ok, Sites}.

-spec modify_db_sites_trans(emqx_ds:db(), [transition()]) -> {ok, unchanged | [site()]}.
modify_db_sites_trans(DB, Modifications) ->
    Shards = db_shards_trans(DB),
    Sites0 = list_target_sites(Shards),
    Sites = lists:foldl(fun apply_transition/2, Sites0, Modifications),
    case Sites of
        Sites0 ->
            {ok, unchanged};
        _Changed ->
            assign_db_sites_trans(DB, Sites)
    end.

claim_transition_trans(DB, Shard, Trans) ->
    ShardRecord =
        case mnesia:read(?SHARD_TAB, {DB, Shard}, read) of
            [Record] ->
                Record;
            [] ->
                mnesia:abort({nonexistent_shard, {DB, Shard}})
        end,
    case shard_transition_trans(ShardRecord) of
        [#?TRANSITION_TAB{transition = Trans}] ->
            ok;
        [#?TRANSITION_TAB{transition = Conflict}] ->
            mnesia:abort({conflict, Conflict});
        [] ->
            case compute_transition_plan(ShardRecord) of
                [Trans | _] ->
                    mnesia:write(#?TRANSITION_TAB{shard = {DB, Shard}, transition = Trans});
                Expected ->
                    mnesia:abort({outdated, Expected})
            end
    end.

update_replica_set_trans(DB, Shard, Trans) ->
    case mnesia:read(?SHARD_TAB, {DB, Shard}, write) of
        [Record = #?SHARD_TAB{replica_set = ReplicaSet0, target_set = TargetSet0}] ->
            %% NOTE
            %% It's possible to complete a transition that's no longer planned. We
            %% should anticipate that we may stray _away_ from the target set.
            TargetSet1 = emqx_maybe:define(TargetSet0, ReplicaSet0),
            ReplicaSet = apply_transition(Trans, ReplicaSet0),
            case lists:usort(TargetSet1) of
                ReplicaSet ->
                    TargetSet = undefined;
                TS ->
                    TargetSet = TS
            end,
            %% NOTE: Not enforcing existence on that level, makes little sense.
            mnesia:delete_object(#?TRANSITION_TAB{shard = {DB, Shard}, transition = Trans}),
            mnesia:write(Record#?SHARD_TAB{replica_set = ReplicaSet, target_set = TargetSet});
        [] ->
            mnesia:abort({nonexistent_shard, {DB, Shard}})
    end.

-spec update_db_config_trans(emqx_ds:db(), emqx_ds_builtin_raft:db_opts()) ->
    emqx_ds_builtin_raft:db_opts().
update_db_config_trans(DB, UpdateOpts) ->
    Opts = db_config_trans(DB, write),
    %% Since this is an update and not a reopen,
    %% we should keep the shard number and replication factor
    %% and not create a new shard server
    ChangeableOpts = maps:without([n_shards, n_sites, replication_factor], UpdateOpts),
    EffectiveOpts = maps:merge(Opts, ChangeableOpts),
    ok = mnesia:write(#?META_TAB{
        db = DB,
        db_props = EffectiveOpts
    }),
    EffectiveOpts.

-spec db_config_trans(emqx_ds:db()) -> emqx_ds_builtin_raft:db_opts().
db_config_trans(DB) ->
    db_config_trans(DB, read).

db_config_trans(DB, LockType) ->
    case mnesia:read(?META_TAB, DB, LockType) of
        [#?META_TAB{db_props = Config}] ->
            Config;
        [] ->
            mnesia:abort({nonexistent_db, DB})
    end.

db_shards_trans(DB) ->
    mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write).

shard_transition_trans(ShardRecord) ->
    shard_transition_trans(ShardRecord, write).

shard_transition_trans(#?SHARD_TAB{shard = DBShard}, LockType) ->
    mnesia:read(?TRANSITION_TAB, DBShard, LockType).

-spec drop_db_trans(emqx_ds:db()) -> ok.
drop_db_trans(DB) ->
    mnesia:delete({?META_TAB, DB}),
    [mnesia:delete({?SHARD_TAB, Shard}) || Shard <- shards(DB)],
    ok.

-spec claim_site_trans(site(), node()) -> ok.
claim_site_trans(Site, Node) ->
    case node_sites_trans(Node) of
        [] ->
            mnesia:write(#?NODE_TAB{site = Site, node = Node});
        [#?NODE_TAB{site = Site}] ->
            ok;
        Records ->
            ExistingSites = [S || #?NODE_TAB{site = S} <- Records],
            mnesia:abort({conflicting_node_site, ExistingSites})
    end.

-spec forget_site_trans(_Record :: tuple()) -> ok.
forget_site_trans(Record = #?NODE_TAB{site = Site}) ->
    %% Safeguards.
    SiteShards = site_shards_trans(Site),
    %% 1. Compute which DBs has this site as a replica for any of the shards.
    SiteDBs = lists:usort([DB || {current, {DB, _Shard}} <- SiteShards]),
    %% 2. Compute which DBs has this site in a membership transition, for any of the shards.
    SiteTargetDBs = lists:usort([DB || {target, {DB, _Shard}} <- SiteShards]),
    case SiteDBs of
        [] when SiteTargetDBs == [] ->
            mnesia:delete_object(?NODE_TAB, Record, write);
        [_ | _] ->
            mnesia:abort({member_of_replica_sets, SiteDBs});
        [] ->
            mnesia:abort({member_of_target_sets, SiteTargetDBs})
    end.

site_shards_trans(Site) ->
    ShardRecords = all_shards_trans(),
    Current = [
        {current, R#?SHARD_TAB.shard}
     || R <- ShardRecords,
        S <- get_shard_sites(R),
        S == Site
    ],
    Target = [
        {target, R#?SHARD_TAB.shard}
     || R <- ShardRecords,
        {_T, S} <- pending_transitions(R, shard_transition_trans(R, read)),
        S == Site
    ],
    Current ++ Target.

node_sites_trans(Node) ->
    mnesia:match_object(?NODE_TAB, ?NODE_PAT(Node), write).

all_nodes() ->
    mnesia:dirty_match_object(?NODE_TAB, ?NODE_PAT()).

all_shards() ->
    mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT('_')).

all_transitions() ->
    mnesia:dirty_match_object(?TRANSITION_TAB, ?TRANSITION_PAT('_')).

all_shards_trans() ->
    mnesia:match_object(?SHARD_TAB, ?SHARD_PAT('_'), read).

%%================================================================================
%% Internal functions
%%================================================================================

ensure_tables() ->
    ok = mria:create_table(?META_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?NODE_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?NODE_TAB},
        {attributes, record_info(fields, ?NODE_TAB)}
    ]),
    ok = mria:create_table(?SHARD_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?SHARD_TAB},
        {attributes, record_info(fields, ?SHARD_TAB)}
    ]),
    ok = mria:create_table(?TRANSITION_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, bag},
        {storage, disc_copies},
        {record_name, ?TRANSITION_TAB},
        {attributes, record_info(fields, ?TRANSITION_TAB)}
    ]),
    ok = mria:wait_for_tables([?META_TAB, ?NODE_TAB, ?SHARD_TAB]).

run_migrations() ->
    run_migrations(emqx_release:version()).

run_migrations(_Version = "6." ++ _) ->
    ok.

ensure_site() ->
    Site = emqx_dsch:this_site(),
    case transaction(fun ?MODULE:claim_site_trans/2, [Site, node()]) of
        ok ->
            persistent_term:put(?emqx_ds_builtin_site, Site);
        {error, Reason} ->
            logger:error("Attempt to claim site with ID=~s failed: ~p", [Site, Reason])
    end.

forget_node(Node) ->
    Sites = node_sites(Node),
    Result = transaction(fun lists:map/2, [fun ?MODULE:forget_site_trans/1, Sites]),
    case Result of
        Ok when is_list(Ok) ->
            ok;
        {error, Reason} ->
            logger:error("Failed to forget leaving node ~p: ~p", [Node, Reason])
    end.

node_sites(Node) ->
    mnesia:dirty_match_object(?NODE_TAB, ?NODE_PAT(Node)).

%% @doc Returns sorted list of sites shards are replicated across.
-spec list_sites([_Shard]) -> [site()].
list_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_sites/1, Shards).

%% @doc Returns sorted list of sites shards are _going to be_ replicated across.
-spec list_target_sites([_Shard]) -> [site()].
list_target_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_target_sites/1, Shards).

-spec get_shard_sites(_Shard) -> [site()].
get_shard_sites(#?SHARD_TAB{replica_set = ReplicaSet}) ->
    ReplicaSet.

-spec get_shard_target_sites(_Shard) -> [site()].
get_shard_target_sites(#?SHARD_TAB{target_set = Sites}) when is_list(Sites) ->
    Sites;
get_shard_target_sites(#?SHARD_TAB{target_set = undefined} = Shard) ->
    get_shard_sites(Shard).

-spec compute_allocation([Shard], [Site], emqx_ds_builtin_raft:db_opts()) ->
    [{Shard, [Site, ...]}].
compute_allocation(Shards, Sites, Opts) ->
    NSites = length(Sites),
    ReplicationFactor = maps:get(replication_factor, Opts),
    NReplicas = min(NSites, ReplicationFactor),
    ShardsSorted = lists:sort(Shards),
    SitesSorted = lists:sort(Sites),
    {Allocation, _} = lists:mapfoldl(
        fun(Shard, SSites) ->
            {ReplicaSet, _} = emqx_utils_stream:consume(NReplicas, SSites),
            {_, SRest} = emqx_utils_stream:consume(1, SSites),
            {{Shard, ReplicaSet}, SRest}
        end,
        emqx_utils_stream:repeat(emqx_utils_stream:list(SitesSorted)),
        ShardsSorted
    ),
    Allocation.

pending_transitions(ShardRecord = #?SHARD_TAB{shard = Shard}) ->
    PendingTransitions = mnesia:dirty_read(?TRANSITION_TAB, Shard),
    pending_transitions(ShardRecord, PendingTransitions).

pending_transitions(ShardRecord, PendingTransitions) ->
    prepend_pending_transitions(compute_transition_plan(ShardRecord), PendingTransitions).

prepend_pending_transitions(Transitions, []) ->
    Transitions;
prepend_pending_transitions(Transitions, [#?TRANSITION_TAB{transition = Trans}]) ->
    [Trans | lists:delete(Trans, Transitions)].

compute_transition_plan(#?SHARD_TAB{target_set = TargetSet, replica_set = ReplicaSet}) ->
    compute_transition_plan(TargetSet, ReplicaSet).

compute_transition_plan(undefined, _ReplicaSet) ->
    [];
compute_transition_plan(TargetSet, ReplicaSet) ->
    SitesAdded = TargetSet -- ReplicaSet,
    SitesDeleted = ReplicaSet -- TargetSet,
    SitesLost = sites(lost),
    SitesDeletedLost = [S || S <- SitesDeleted, lists:member(S, SitesLost)],
    Additions = [{add, S} || S <- SitesAdded],
    Deletions = [{del, S} || S <- SitesDeleted -- SitesDeletedLost],
    LostDeletions = [{del, S} || S <- SitesDeletedLost],
    lists:append([
        %% 1. We need to remove lost replicas first.
        %%    They don't contribute to availability and redundancy anyway, and pose
        %%    a risk to make any further additions inoperable, depending on if quorum
        %%    is reachable or not.
        LostDeletions,
        %% 2. We need to alternate additions and deletions, starting from additions
        %%    (if any). This way we won't compromise redundancy, and on the other hand
        %%    won't temporarily increase quorum size and replication factor too much
        %%    to effectively compromise availability.
        intersperse(Additions, Deletions)
    ]).

%% @doc Apply a transition to a list of sites, preserving sort order.
-spec apply_transition(transition(), [site()]) -> [site()].
apply_transition({add, S}, Sites) ->
    lists:usort([S | Sites]);
apply_transition({del, S}, Sites) ->
    lists:delete(S, Sites).

gen_shards(NShards) ->
    [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)].

transaction(Fun, Args) ->
    case mria:transaction(?RLOG_SHARD, Fun, Args) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%====================================================================

handle_subscribe(Pid, Subject, S = #s{subs = Subs0}) ->
    case maps:is_key(Pid, Subs0) of
        false ->
            MRef = erlang:monitor(process, Pid),
            Subs = Subs0#{Pid => {Subject, MRef}},
            S#s{subs = Subs};
        true ->
            S
    end.

handle_unsubscribe(Pid, S = #s{subs = Subs0}) ->
    case maps:take(Pid, Subs0) of
        {{_Subject, MRef}, Subs} ->
            _ = erlang:demonitor(MRef, [flush]),
            S#s{subs = Subs};
        error ->
            S
    end.

notify_subscribers(EventSubject, Event, #s{subs = Subs}) ->
    maps:foreach(
        fun(Pid, {Subject, _MRef}) ->
            Subject == EventSubject andalso
                erlang:send(Pid, {changed, Event})
        end,
        Subs
    ).

%%====================================================================

%% @doc Intersperse elements of two lists.
%% Example: intersperse([1, 2], [3, 4, 5]) -> [1, 3, 2, 4, 5].
-spec intersperse([X], [Y]) -> [X | Y].
intersperse(L1, []) ->
    L1;
intersperse([], L2) ->
    L2;
intersperse([H1 | T1], L2) ->
    [H1 | intersperse(L2, T1)].

%% @doc Map list into a list of sets and return union, as a sorted list.
-spec flatmap_sorted_set(fun((X) -> [Y]), [X]) -> [Y].
flatmap_sorted_set(Fun, L) ->
    ordsets:to_list(
        lists:foldl(
            fun(X, Acc) -> ordsets:union(ordsets:from_list(Fun(X)), Acc) end,
            ordsets:new(),
            L
        )
    ).
