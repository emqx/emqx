%%--------------------------------------------------------------------
%% Copyright (c) 2018-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    monitor/1,
    is_routable/1,
    purge/0,
    purge_force/0
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
-define(RECONCILE_INTERVAL, {10 * 60_000, '±', 30_000}).
%% How soon should a dead node be purged? (ms)
-define(PURGE_DEAD_TIMEOUT, 60 * 60_000).
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
-spec purge() -> scheduled.
purge() ->
    TS = erlang:monotonic_time(millisecond),
    gen_server:call(?MODULE, {purge, TS}).

%% @doc Force dead node purges, regardless of for how long nodes are down.
%% Mostly for testing purposes.
-spec purge_force() -> scheduled.
purge_force() ->
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
    %% Cleanup any routes left by old incarnations of this node (if any).
    %% Depending on the size of routing tables this can take signicant amount of time.
    _ = mria:wait_for_tables([?ROUTING_NODE]),
    _ = purge_this_node(),
    %% Setup periodic stats reporting.
    ok = emqx_stats:update_interval(route_stats, fun ?MODULE:stats_fun/0),
    TRef = schedule_task(reconcile, ?RECONCILE_INTERVAL),
    State = #{
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

lookup_node_reachable(Node) ->
    ets:lookup_element(?TAB_STATUS, Node, #ns.reachable, _Default = true).

handle_reconcile(State) ->
    TRef = schedule_task(reconcile, ?RECONCILE_INTERVAL),
    NState = State#{timer_reconcile := TRef},
    TS = erlang:monotonic_time(millisecond),
    schedule_purges(TS, reconcile(NState)).

reconcile(State) ->
    %% NOTE
    %% This is a fallback mechanism. Avoid purging / ignoring live nodes, if missed
    %% lifecycle events for whatever reason.
    ok = lists:foreach(fun record_node_alive/1, mria:running_nodes() ++ nodes()),
    State.

select_outdated(Status, Since0) ->
    MS = ets:fun2ms(
        fun(#ns{node = Node, status = S, since = Since}) when S == Status andalso Since < Since0 ->
            Node
        end
    ),
    ets:select(?TAB_STATUS, MS).

schedule_purges(TS, State) ->
    ok = lists:foreach(fun schedule_purge/1, select_outdated(down, TS - ?PURGE_DEAD_TIMEOUT)),
    ok = lists:foreach(fun schedule_purge/1, select_outdated(left, TS - ?PURGE_LEFT_TIMEOUT)),
    State.

schedule_purge(Node) ->
    case am_core() of
        true ->
            schedule_task({purge, Node}, 0);
        false ->
            false
    end.

schedule_purge_left(Node) ->
    case am_core() andalso pick_responsible({purge, Node}) of
        Responsible when Responsible == node() ->
            %% Schedule purge on responsible node first, to avoid racing for a global lock.
            schedule_task({purge, Node}, 0);
        _ ->
            %% Replicant / not responsible.
            %% In the latter case try to purge on the next reconcile, as a fallback.
            false
    end.

handle_purge(Node, State) ->
    try purge_dead_node_trans(Node) of
        true ->
            ?tp(debug, emqx_router_node_purged, #{node => Node}),
            forget_node(Node);
        false ->
            ?tp(debug, emqx_router_node_purge_skipped, #{node => Node}),
            forget_node(Node);
        aborted ->
            ?tp(notice, emqx_router_node_purge_aborted, #{node => Node})
    catch
        Kind:Error ->
            ?tp(warning, emqx_router_node_purge_error, #{
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
    case node_has_routes(Node) of
        true ->
            global:trans(
                {?LOCK(Node), self()},
                fun() ->
                    ok = cleanup_routes(Node),
                    true
                end,
                cores(),
                _Retries = 3
            );
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

handle_task({purge, Node}, State) ->
    handle_purge(Node, State);
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

-spec cores() -> [node()].
cores() ->
    %% Include stopped nodes as well.
    mria_membership:nodelist().

-spec pick_responsible(_Task) -> node().
pick_responsible(Task) ->
    %% Pick a responsible core node.
    %% We expect the same node to be picked as responsible across the cluster (unless
    %% the cluster is highly turbulent).
    Nodes = lists:sort(mria_membership:running_core_nodelist()),
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
