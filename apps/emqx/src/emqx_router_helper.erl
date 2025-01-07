%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Router helper process.
%%
%% Responsibility is twofold:
%% 1. Cleaning own portion of the global routing table when restarted.
%%    The assumption is that the node has crashed (worst-case), so the
%%    previous incarnation's routes are still present upon restart.
%% 2. Managing portions of global routing table belonging to dead / "left"
%%    cluster members, i.e. members that are not supposed to come back
%%    online again.
%%
%% Only core nodes are responsible for the latter task. Moreover, helper
%% adopts the following operational model:
%% 1. Core nodes are supposed to be explicitly evicted (or "left") from
%%    the cluster. Even if a core node is marked down for several hours,
%%    helper won't attempt to purge its portion of the global routing
%%    table.
%% 2. Replicant nodes are considered dead (or "left") once they are down
%%    for a specific timespan. Currently hardcoded as `?PURGE_DEAD_TIMEOUT`.
%%    Ideally it should reflect amount of time it takes for a connectivity
%%    failure between cores and replicants to heal worst-case.
%%
%% TODO
%% While cores purge unreachable replicants' routes after a timeout,
%% replicants _do nothing_ on connectivity loss, regardless of how long
%% it is. Coupled with the fact that replicants are not affected by
%% "autoheal" mechanism, this may still lead to routing inconsistencies.

-module(emqx_router_helper).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_router.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% Mnesia bootstrap
-export([create_tables/0]).

%% API
-export([
    start_link/0,
    post_start/0,
    monitor/1,
    is_routable/1,
    schedule_purge/0,
    schedule_force_purge/0
]).

%% Internal export
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

-record(routing_node, {name, const = unused}).

-define(TAB_STATUS, ?MODULE).

-define(LOCK(RESOURCE), {?MODULE, RESOURCE}).

%% How often to reconcile nodes state? (ms)
%% Introduce some jitter to avoid concerted firing on different nodes.
-define(RECONCILE_INTERVAL, {2 * 60_000, '±', 15_000}).
%% How soon should a dead node be purged? (ms)
-define(PURGE_DEAD_TIMEOUT, 15 * 60_000).
%% How soon should a left node be purged? (ms)
%% This is a fallback, left node is expected to be purged right away.
-define(PURGE_LEFT_TIMEOUT, 15_000).

-ifdef(TEST).
-undef(RECONCILE_INTERVAL).
-undef(PURGE_DEAD_TIMEOUT).
-undef(PURGE_LEFT_TIMEOUT).
-define(RECONCILE_INTERVAL, {2_000, '±', 500}).
-define(PURGE_DEAD_TIMEOUT, 3_000).
-define(PURGE_LEFT_TIMEOUT, 1_000).
-endif.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?ROUTING_NODE, [
        {type, set},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, ram_copies},
        {record_name, routing_node},
        {attributes, record_info(fields, routing_node)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?ROUTING_NODE].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Starts the router helper
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec post_start() -> ignore.
post_start() ->
    %% Cleanup any routes left by old incarnations of this node (if any).
    %% Depending on the size of routing tables this can take signicant amount of time.
    _ = mria:wait_for_tables([?ROUTING_NODE]),
    _ = purge_this_node(),
    ignore.

%% @doc Monitor routing node
-spec monitor(node() | {binary(), node()}) -> ok.
monitor({_Group, Node}) ->
    monitor(Node);
monitor(Node) when is_atom(Node) ->
    case ets:member(?ROUTING_NODE, Node) of
        true -> ok;
        false -> mria:dirty_write(?ROUTING_NODE, #routing_node{name = Node})
    end.

%% @doc Is given node considered routable?
%% I.e. should the broker attempt to forward messages there, even if there are
%% routes to this node in the routing table?
-spec is_routable(node()) -> boolean().
is_routable(Node) when Node == node() ->
    true;
is_routable(Node) ->
    try
        lookup_node_reachable(Node)
    catch
        error:badarg -> true
    end.

%% @doc Schedule dead node purges.
-spec schedule_purge() -> scheduled.
schedule_purge() ->
    TS = erlang:monotonic_time(millisecond),
    gen_server:call(?MODULE, {purge, TS}).

%% @doc Force dead node purges, regardless of for how long nodes are down.
%% Mostly for testing purposes.
-spec schedule_force_purge() -> scheduled.
schedule_force_purge() ->
    TS = erlang:monotonic_time(millisecond),
    gen_server:call(?MODULE, {purge, TS + ?PURGE_DEAD_TIMEOUT * 2}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-record(ns, {
    node :: node(),
    status :: down | left,
    since :: _MonotonicTimestamp :: integer(),
    reachable = false :: boolean()
}).

init([]) ->
    process_flag(trap_exit, true),
    %% Initialize a table to cache node status.
    Tab = ets:new(?TAB_STATUS, [
        protected,
        {keypos, #ns.node},
        named_table,
        set,
        {read_concurrency, true}
    ]),
    %% Monitor nodes lifecycle events.
    ok = ekka:monitor(membership),
    %% Setup periodic stats reporting.
    ok = emqx_stats:update_interval(route_stats, fun ?MODULE:stats_fun/0),
    TRef = schedule_task(reconcile, ?RECONCILE_INTERVAL),
    State = #{
        last_membership => emqx_maybe:define(cores(), []),
        tab_node_status => Tab,
        timer_reconcile => TRef
    },
    {ok, State, hibernate}.

handle_call({purge, TS}, _From, State) ->
    NState = schedule_purges(TS, State),
    {reply, scheduled, NState};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({membership, Event}, State) ->
    NState = handle_membership_event(Event, State),
    {noreply, NState};
handle_info({timeout, _TRef, {start, Task}}, State) ->
    NState = handle_task(Task, State),
    {noreply, NState};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(route_stats),
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_membership_event({node, down, Node}, State) ->
    _ = record_node_down(Node),
    State;
handle_membership_event({node, leaving, Node}, State) ->
    _ = schedule_purge_left(Node),
    _ = record_node_left(Node),
    State;
handle_membership_event({node, up, Node}, State) ->
    _ = record_node_alive(Node),
    State;
handle_membership_event({mnesia, up, Node}, State = #{last_membership := Membership}) ->
    State#{last_membership := lists:usort([Node | Membership])};
handle_membership_event(_Event, State) ->
    State.

record_node_down(Node) ->
    NS = #ns{
        node = Node,
        status = down,
        since = erlang:monotonic_time(millisecond)
    },
    case ets:lookup(?TAB_STATUS, Node) of
        [#ns{status = left}] ->
            %% Node is still marked as left, keep it that way.
            false;
        [#ns{status = down}] ->
            %% Duplicate.
            ets:insert(?TAB_STATUS, NS);
        [] ->
            ets:insert(?TAB_STATUS, NS)
    end.

record_node_left(Node) ->
    NS = #ns{
        node = Node,
        status = left,
        since = erlang:monotonic_time(millisecond)
    },
    ets:insert(?TAB_STATUS, NS).

record_node_alive(Node) ->
    forget_node(Node).

forget_node(Node) ->
    ets:delete(?TAB_STATUS, Node).

lookup_node_status(Node) ->
    ets:lookup_element(?TAB_STATUS, Node, #ns.status, _Default = notfound).

lookup_node_reachable(Node) ->
    ets:lookup_element(?TAB_STATUS, Node, #ns.reachable, _Default = true).

handle_reconcile(State) ->
    TRef = schedule_task(reconcile, ?RECONCILE_INTERVAL),
    NState = State#{timer_reconcile := TRef},
    TS = erlang:monotonic_time(millisecond),
    schedule_purges(TS, reconcile(NState)).

reconcile(State = #{last_membership := MembersLast}) ->
    %% Find if there are discrepancies in membership.
    %% Missing core nodes must have been "force-left" from the cluster.
    %% On replicants, `cores()` may return `undefined` under severe connectivity loss.
    Members = emqx_maybe:define(cores(), MembersLast),
    ok = lists:foreach(fun(Node) -> record_node_left(Node) end, MembersLast -- Members),
    %% This is a fallback mechanism.
    %% Avoid purging live nodes, if missed lifecycle events for whatever reason.
    ok = lists:foreach(fun record_node_alive/1, mria:running_nodes()),
    State#{last_membership := Members}.

select(Status) ->
    MS = ets:fun2ms(fun(#ns{node = Node, status = S}) when S == Status -> Node end),
    ets:select(?TAB_STATUS, MS).

select_outdated(Status, Since0) ->
    MS = ets:fun2ms(
        fun(#ns{node = Node, status = S, since = Since}) when S == Status andalso Since < Since0 ->
            Node
        end
    ),
    ets:select(?TAB_STATUS, MS).

filter_replicants(Nodes) ->
    Replicants = mria_membership:replicant_nodelist(),
    [RN || RN <- Nodes, lists:member(RN, Replicants)].

schedule_purges(TS, State) ->
    %% Safety measure: purge only dead replicants.
    %% Assuming a dead / offline core node should:
    %% 1. Either come back online, and potentially reboot itself when Mria autoheal
    %%    will kick in.
    %% 2. ...Or leave the cluster (be "force-left" to be precise), in which case
    %%    `reconcile/1` should notice a discrepancy and schedule a purge.
    ok = lists:foreach(
        fun(Node) -> schedule_purge(Node, {replicant_down_for, ?PURGE_DEAD_TIMEOUT}) end,
        filter_replicants(select_outdated(down, TS - ?PURGE_DEAD_TIMEOUT))
    ),
    %% Trigger purges for "force-left" nodes found during reconcile, if resposible.
    ok = lists:foreach(
        fun schedule_purge_left/1,
        select(left)
    ),
    %% Otherwise, purge nodes marked left for a while.
    ok = lists:foreach(
        fun(Node) -> schedule_purge(Node, left) end,
        select_outdated(left, TS - ?PURGE_LEFT_TIMEOUT)
    ),
    State.

schedule_purge(Node, Why) ->
    case am_core() of
        true ->
            schedule_task({purge, Node, Why}, 0);
        false ->
            false
    end.

schedule_purge_left(Node) ->
    case am_core() andalso pick_responsible({purge, Node}) of
        Responsible when Responsible == node() ->
            %% Schedule purge on responsible node first, to avoid racing for a global lock.
            schedule_task({purge, Node, left}, 0);
        _ ->
            %% Replicant / not responsible.
            %% In the latter case try to purge on the next reconcile, as a fallback.
            false
    end.

handle_purge(Node, Why, State) ->
    try purge_dead_node_trans(Node) of
        true ->
            ?tp(warning, router_node_routing_table_purged, #{
                node => Node,
                reason => Why,
                hint => "Ignore if the node in question went offline due to cluster maintenance"
            }),
            forget_node(Node);
        false ->
            ?tp(debug, router_node_purge_skipped, #{node => Node}),
            forget_node(Node);
        aborted ->
            ?tp(notice, router_node_purge_aborted, #{node => Node})
    catch
        Kind:Error ->
            ?tp(warning, router_node_purge_error, #{
                node => Node,
                kind => Kind,
                error => Error
            })
    end,
    State.

purge_dead_node(Node) ->
    case node_has_routes(Node) of
        true ->
            ok = cleanup_routes(Node),
            true;
        false ->
            false
    end.

purge_dead_node_trans(Node) ->
    StillKnown = lookup_node_status(Node) =/= notfound,
    case StillKnown andalso node_has_routes(Node) of
        true ->
            case cores() of
                Nodes = [_ | _] ->
                    global:trans(
                        {?LOCK(Node), self()},
                        fun() ->
                            ok = cleanup_routes(Node),
                            true
                        end,
                        Nodes,
                        _Retries = 3
                    );
                undefined ->
                    error(no_core_nodes)
            end;
        false ->
            false
    end.

purge_this_node() ->
    %% TODO: Guard against possible `emqx_router_helper` restarts?
    purge_dead_node(node()).

node_has_routes(Node) ->
    ets:member(?ROUTING_NODE, Node).

cleanup_routes(Node) ->
    emqx_router:cleanup_routes(Node),
    mria:dirty_delete(?ROUTING_NODE, Node).

%%

schedule_task(Task, Timeout) ->
    emqx_utils:start_timer(choose_timeout(Timeout), {start, Task}).

handle_task({purge, Node, Why}, State) ->
    handle_purge(Node, Why, State);
handle_task(reconcile, State) ->
    handle_reconcile(State).

choose_timeout({Baseline, '±', Jitter}) ->
    Baseline + rand:uniform(Jitter * 2) - Jitter;
choose_timeout(Baseline) ->
    Baseline.

%%

-spec am_core() -> boolean().
am_core() ->
    mria_config:whoami() =/= replicant.

-spec cores() -> [node()] | undefined.
cores() ->
    %% Include stopped nodes as well.
    try
        mria:cluster_nodes(cores)
    catch
        error:_Timeout ->
            undefined
    end.

-spec pick_responsible(_Task) -> node().
pick_responsible(Task) ->
    %% Pick a responsible core node.
    %% We expect the same node to be picked as responsible across the cluster (unless
    %% the cluster is highly turbulent).
    Nodes = lists:sort(mria_mnesia:running_nodes()),
    case length(Nodes) of
        0 -> node();
        N -> lists:nth(1 + erlang:phash2(Task, N), Nodes)
    end.

%%

stats_fun() ->
    PSRouteCount = persistent_route_count(),
    NonPSRouteCount = emqx_router:stats(n_routes),
    emqx_stats:setstat('topics.count', 'topics.max', PSRouteCount + NonPSRouteCount).

persistent_route_count() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            emqx_persistent_session_ds_router:stats(n_routes);
        false ->
            0
    end.
