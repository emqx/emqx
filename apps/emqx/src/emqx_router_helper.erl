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

%% Mnesia bootstrap
-export([create_tables/0]).

%% API
-export([
    start_link/0,
    monitor/1,
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

-define(LOCK(RESOURCE), {?MODULE, RESOURCE}).

%% How often to reconcile nodes state? (ms)
-define(RECONCILE_INTERVAL, 10 * 60_000).
%% How soon should a dead node be purged? (ms)
-define(PURGE_DEAD_TIMEOUT, 60 * 60_000).
%% How soon should a left node be purged? (ms)
%% This is a fallback, left node is expected to be purged right away.
-define(PURGE_LEFT_TIMEOUT, 15_000).

% -define(PURGE_LEFT_FALLBACK_TIMEOUT, {15_000, '±', 2_500}).

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

init([]) ->
    process_flag(trap_exit, true),
    %% Monitor nodes lifecycle events.
    ok = ekka:monitor(membership),
    %% Cleanup any routes left by old incarnations of this node (if any).
    %% Depending on the size of routing tables this can take signicant amount of time.
    _ = mria:wait_for_tables([?ROUTING_NODE]),
    _ = purge_this_node(),
    %% Setup periodic stats reporting.
    ok = emqx_stats:update_interval(route_stats, fun ?MODULE:stats_fun/0),
    {ok, TRef} = timer:send_interval(?RECONCILE_INTERVAL, reconcile),
    State = #{
        down => #{},
        left => #{},
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
handle_info(reconcile, State) ->
    TS = erlang:monotonic_time(millisecond),
    NState = schedule_purges(TS, reconcile(State)),
    {noreply, NState};
handle_info({timeout, _TRef, {start, Task}}, State) ->
    NState = handle_task(Task, State),
    {noreply, NState};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State = #{timer_reconcile := TRef}) ->
    timer:cancel(TRef),
    emqx_stats:cancel_update(route_stats),
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_membership_event({node, down, Node}, State) ->
    record_node_down(Node, State);
handle_membership_event({node, leaving, Node}, State) ->
    _ = schedule_purge_left(Node),
    record_node_left(Node, State);
handle_membership_event({node, up, Node}, State) ->
    record_node_alive(Node, State);
handle_membership_event(_Event, State) ->
    State.

record_node_down(Node, State = #{down := Down}) ->
    Record = #{since => erlang:monotonic_time(millisecond)},
    State#{down := Down#{Node => Record}}.

record_node_left(Node, State = #{left := Left}) ->
    Record = #{since => erlang:monotonic_time(millisecond)},
    State#{left := Left#{Node => Record}}.

record_node_alive(Node, State) ->
    forget_node(Node, State).

forget_node(Node, State = #{down := Down, left := Left}) ->
    State#{
        down := maps:remove(Node, Down),
        left := maps:remove(Node, Left)
    }.

reconcile(State) ->
    %% Fallback: avoid purging live nodes, if missed lifecycle events for whatever reason.
    lists:foldl(fun record_node_alive/2, State, mria:running_nodes()).

select_outdated(NodesInfo, Since0) ->
    maps:keys(maps:filter(fun(_, #{since := Since}) -> Since < Since0 end, NodesInfo)).

schedule_purges(TS, State = #{down := Down, left := Left}) ->
    ok = lists:foreach(fun schedule_purge/1, select_outdated(Down, TS - ?PURGE_DEAD_TIMEOUT)),
    ok = lists:foreach(fun schedule_purge/1, select_outdated(Left, TS - ?PURGE_LEFT_TIMEOUT)),
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
            forget_node(Node, State);
        false ->
            ?tp(debug, emqx_router_node_purge_skipped, #{node => Node}),
            forget_node(Node, State);
        aborted ->
            ?tp(notice, emqx_router_node_purge_aborted, #{node => Node}),
            State
    catch
        Kind:Error ->
            ?tp(warning, emqx_router_node_purge_error, #{
                node => Node,
                kind => Kind,
                error => Error
            }),
            State
    end.

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
    handle_purge(Node, State).

choose_timeout({Baseline, '±', Jitter}) ->
    Baseline + rand:uniform(Jitter * 2) - Jitter;
choose_timeout(Baseline) ->
    Baseline.

%%

am_core() ->
    mria_config:whoami() =/= replicant.

cores() ->
    %% Include stopped nodes as well.
    mria_membership:nodelist().

pick_responsible(Task) ->
    %% Pick a responsible core node.
    %% We expect the same node to be picked as responsible across the cluster (unless
    %% the cluster is highly turbulent).
    Nodes = lists:sort(mria_membership:running_core_nodelist()),
    N = length(Nodes),
    N > 0 andalso lists:nth(1 + erlang:phash2(Task, N), Nodes).

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
