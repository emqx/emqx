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
-include("emqx_router.hrl").
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
    true = global:set_lock({?LOCK(node()), self()}),
    %% Cleaning routes makes sense here if this node is being restarted
    %% and somehow manages to win the lock first, disallowing other nodes
    %% to cleanup its old routes when they detected it was down.
    _ = cleanup_routes(node()),
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

handle_info({nodedown, Node}, State) ->
    case mria_rlog:role() of
        core ->
            global:trans(
                {?LOCK(Node), self()},
                fun() -> cleanup_routes(Node) end,
                %% Need to acquire the lock on replicants too,
                %% since replicants participate in locking (see init/1).
                [node() | nodes()],
                %% It's important to do a limited number of retries (not infinity).
                %% Otherwise, if a down node comes back in a short time
                %% and wins the lock in init/1,
                %% emqx_router_helepr on another node can desparetly retry it indefinitely.
                2
            );
        replicant ->
            ok
    end,
    ?tp(emqx_router_helper_cleanup_done, #{node => Node}),
    {noreply, State, hibernate};
handle_info({membership, {mnesia, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
handle_info({membership, {node, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
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

cleanup_routes(Node) ->
    emqx_router:cleanup_routes(Node).
