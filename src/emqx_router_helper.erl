%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

%% Mnesia Bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API
-export([start_link/1, monitor/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(routing_node, {name, ts}).

-record(state, {nodes = [], stats_fun, stats_timer}).

-compile({no_auto_import, [monitor/1]}).

-define(SERVER, ?MODULE).

-define(TAB, emqx_routing_node).

-define(LOCK, {?MODULE, clean_routes}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {ram_copies, [node()]},
                {record_name, routing_node},
                {attributes, record_info(fields, routing_node)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Starts the router helper
-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, any()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [StatsFun], []).

%% @doc Monitor routing node
-spec(monitor(node()) -> ok).
monitor({_Group,  Node}) ->
    monitor(Node);
monitor(Node) when is_atom(Node) ->
    case ekka:is_member(Node) orelse ets:member(?TAB, Node) of
        true  -> ok;
        false ->
            mnesia:dirty_write(?TAB, #routing_node{name = Node, ts = os:timestamp()})
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    ekka:monitor(membership),
    mnesia:subscribe({table, ?TAB, simple}),
    Nodes = lists:foldl(
              fun(Node, Acc) ->
                  case ekka:is_member(Node) of
                      true  -> Acc;
                      false -> _ = erlang:monitor_node(Node, true),
                               [Node | Acc]
                  end
              end, [], mnesia:dirty_all_keys(?TAB)),
    {ok, TRef} = timer:send_interval(timer:seconds(1), stats),
    {ok, #state{nodes = Nodes, stats_fun = StatsFun, stats_timer = TRef}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[RouterHelper] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_log:error("[RouterHelper] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, #routing_node{name = Node}, _}},
            State = #state{nodes = Nodes}) ->
    emqx_log:info("[RouterHelper] New routing node: ~s", [Node]),
    case ekka:is_member(Node) orelse lists:member(Node, Nodes) of
        true  -> {noreply, State};
        false -> _ = erlang:monitor_node(Node, true),
                 {noreply, State#state{nodes = [Node | Nodes]}}
    end;

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info({nodedown, Node}, State = #state{nodes = Nodes}) ->
    global:trans({?LOCK, self()},
                 fun() ->
                     mnesia:transaction(fun cleanup_routes/1, [Node])
                 end),
    mnesia:dirty_delete(?TAB, Node),
    handle_info(stats, State#state{nodes = lists:delete(Node, Nodes)});

handle_info({membership, {mnesia, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    ok = StatsFun(mnesia:table_info(emqx_route, size)),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    emqx_log:error("[RouteHelper] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef}) ->
    timer:cancel(TRef),
    ekka:unmonitor(membership),
    mnesia:unsubscribe({table, ?TAB, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_routes(Node) ->
    Patterns = [#route{_ = '_', dest = Node},
                #route{_ = '_', dest = {'_', Node}}],
    [mnesia:delete_object(?TAB, R, write)
     || Pat <- Patterns, R <- mnesia:match_object(?TAB, Pat, write)].

