%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0
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

-ifdef(TEST).
-define(DRAIN_TIMEOUT, 100).
-else.
-define(DRAIN_TIMEOUT, 3000).
-endif.

-define(LOCK(NODE), {?MODULE, cleanup_routes, NODE}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Starts the router helper
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    %% Make sure no other node can do late routes cleanup when this node is up.
    %% Despite Retries = infinity, it is to set the lock eventually,
    %% even if some of the nodes are not responding (down).
    %% In the worst case, the time needed to detect a down node should be:
    %% `NetTickTime + NetTickTime / NetTickIntensity`.
    true = global:set_lock({?LOCK(node()), self()}, locking_nodes(), infinity),
    %% Cleaning routes makes sense here if this node is being restarted
    %% and somehow manages to win the lock first, disallowing other nodes
    %% to cleanup its old routes when they detected it was down.
    %% Should be done on both core and replicant nodes and must not be run within global trans,
    %% since it will release the previously acquired lock.
    _ = emqx_router:cleanup_routes(node()),
    %% NOTE: it solely relies on `ekka:monitor/1` (which uses `mria:monitor/3`)
    %% to properly get notified about down nodes (both cores and replicants).
    ok = ekka:monitor(membership),
    ok = emqx_stats:update_interval(route_stats, fun ?MODULE:stats_fun/0),
    {ok, #{}, hibernate}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({membership, {What, down, Node}}, State) when
    What =:= node; What =:= mnesia; What =:= mria
->
    ok = maybe_drain_nodedown_msg(What, Node),
    _ = cleanup_routes(Node),
    {noreply, State, hibernate};
handle_info({membership, _Event}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    try
        ok = ekka:unmonitor(membership),
        emqx_stats:cancel_update(route_stats)
    catch
        exit:{noproc, {gen_server, call, [mria_membership, _]}} ->
            ?SLOG(warning, #{msg => "mria_membership_down"}),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

stats_fun() ->
    emqx_stats:setstat('topics.count', 'topics.max', emqx_router:stats(n_routes)).

%% This is just to avoid cleaning up routes twice in case the node is down,
%% as we can expect {mria, down}, {node, down} under normal circumstances
%%  when a replicant node is being stopped.
maybe_drain_nodedown_msg(What, DownNode) when DownNode =/= node(), What =/= node ->
    receive
        {membership, {node, down, DownNode}} -> ok
    after ?DRAIN_TIMEOUT -> ok
    end;
maybe_drain_nodedown_msg(_, _) ->
    ok.

locking_nodes() ->
    locking_nodes(mria_rlog:role()).

locking_nodes(core) ->
    mria_mnesia:running_nodes();
locking_nodes(replicant) ->
    %% No reliable source to get the list of core nodes, unfortunately.
    %% mria_membership:running_core_nodelist/0` is not absolutely suitable,
    %% because it inserts nodes asynchronously (one by one after doing pings).
    %% So it can return empty/not full, especially when it's called in `emqx_router_helper:init/1`.
    %% Setting the lock on all the nodes for now, until Mria has a better API.
    [node() | nodes()].

cleanup_routes(Node) ->
    case mria_rlog:role() of
        core ->
            global:trans(
                {?LOCK(Node), self()},
                fun() -> emqx_router:cleanup_routes(Node) end,
                %% Can acquire the lock only on running cores,
                %% The node being cleaned is anyway either already down or is expected
                %% to release its lock. And if the node is back, it will try to re-acquire
                %% the lock on all the core nodes, preventing them from re-cleaning its routes
                %% when it is up and running.
                locking_nodes(core),
                %% It's important to do a limited number of retries (not infinity).
                %% Otherwise, if a down node comes back in a short time
                %% and wins the lock in init/1,
                %% emqx_router_helper on another node can desperately retry it indefinitely.
                %% Moreover, as we currently set prevent_overlapping_partitions = false,
                %% the connection loss between two nodes may not be visible to other nodes.
                %% For example, Node A is down for Node B but not for Node C,
                %% Node B tries to cleanup A routes, but cannot acquire the lock,
                %% as A's lock is still present on node C (C and A still can communicate).
                %% If retries were set to infinity, there will be a risk for emqx_router_helper
                %% to be blocked forever on node B, trying to acquire the lock and do cleanup.
                2
            );
        replicant when Node =:= node() ->
            %% Mnesia (or Mria) is down locally,
            %% but emqx_router_helper is still running and holds the lock,
            %% remove it so that other healthy nodes can do cleanup.
            global:del_lock({?LOCK(Node), self()}, locking_nodes(replicant));
        _ ->
            ok
    end,
    ?tp(emqx_router_helper_cleanup_done, #{node => Node}).
