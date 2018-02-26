%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_router).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-include("emqx.hrl").

%% Mnesia Bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/1]).

%% For eunit tests
-export([start/0, stop/0]).

%% Topics
-export([topics/0, local_topics/0]).

%% Route APIs
-export([add_route/1, get_routes/1, del_route/1, has_route/1]).

%% Match and print
-export([match/1, print/1]).

%% Local Route API
-export([get_local_routes/0, add_local_route/1, match_local/1,
         del_local_route/1, clean_local_routes/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([dump/0]).

-record(state, {stats_fun, stats_timer}).

-define(ROUTER, ?MODULE).

-define(LOCK, {?ROUTER, clean_routes}).

%%--------------------------------------------------------------------
%% Mnesia Bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(route, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, route},
                {attributes, record_info(fields, route)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(route, ram_copies).

%%--------------------------------------------------------------------
%% Start the Router
%%--------------------------------------------------------------------

start_link(StatsFun) ->
    gen_server:start_link({local, ?ROUTER}, ?MODULE, [StatsFun], []).

%%--------------------------------------------------------------------
%% Topics
%%--------------------------------------------------------------------

-spec(topics() -> list(binary())).
topics() ->
    mnesia:dirty_all_keys(route).

-spec(local_topics() -> list(binary())).
local_topics() ->
    ets:select(local_route, [{{'$1', '_'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% Match API
%%--------------------------------------------------------------------

%% @doc Match Routes.
-spec(match(Topic:: binary()) -> [route()]).
match(Topic) when is_binary(Topic) ->
    %% Optimize: ets???
    Matched = mnesia:ets(fun emqx_trie:match/1, [Topic]),
    %% Optimize: route table will be replicated to all nodes.
    lists:append([ets:lookup(route, To) || To <- [Topic | Matched]]).

%% @doc Print Routes.
-spec(print(Topic :: binary()) -> [ok]).
print(Topic) ->
    [io:format("~s -> ~s~n", [To, Node]) ||
        #route{topic = To, node = Node} <- match(Topic)].

%%--------------------------------------------------------------------
%% Route Management API
%%--------------------------------------------------------------------

%% @doc Add Route.
-spec(add_route(binary() | route()) -> ok | {error, Reason :: term()}).
add_route(Topic) when is_binary(Topic) ->
    add_route(#route{topic = Topic, node = node()});
add_route(Route = #route{topic = Topic}) ->
    case emqx_topic:wildcard(Topic) of
        true  -> case mnesia:is_transaction() of
                     true  -> add_trie_route(Route);
                     false -> trans(fun add_trie_route/1, [Route])
                 end;
        false -> add_direct_route(Route)
    end.

add_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:write/1, [Route]).

add_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({route, Topic}) of
        [] -> emqx_trie:insert(Topic);
        _  -> ok
    end,
    mnesia:write(Route).

%% @doc Lookup Routes
-spec(get_routes(binary()) -> [route()]).
get_routes(Topic) ->
    ets:lookup(route, Topic).

%% @doc Delete Route
-spec(del_route(binary() | route()) -> ok | {error, Reason :: term()}).
del_route(Topic) when is_binary(Topic) ->
    del_route(#route{topic = Topic, node = node()});
del_route(Route = #route{topic = Topic}) ->
    case emqx_topic:wildcard(Topic) of
        true  -> case mnesia:is_transaction() of
                     true  -> del_trie_route(Route);
                     false -> trans(fun del_trie_route/1, [Route])
                 end;
        false -> del_direct_route(Route)
    end.

del_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:delete_object/1, [Route]).

del_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({route, Topic}) of
        [Route] -> %% Remove route and trie
                   mnesia:delete_object(Route),
                   emqx_trie:delete(Topic);
        [_|_]   -> %% Remove route only
                   mnesia:delete_object(Route);
        []      -> ok
    end.

%% @doc Has route?
-spec(has_route(binary()) -> boolean()).
has_route(Topic) when is_binary(Topic) ->
    ets:member(route, Topic).

%% @private
-spec(trans(function(), list(any())) -> ok | {error, term()}).
trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, _}      -> ok;
        {aborted, Error} -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% Local Route API
%%--------------------------------------------------------------------

-spec(get_local_routes() -> list({binary(), node()})).
get_local_routes() ->
    ets:tab2list(local_route).

-spec(add_local_route(binary()) -> ok).
add_local_route(Topic) ->
    gen_server:call(?ROUTER, {add_local_route, Topic}).
    
-spec(del_local_route(binary()) -> ok).
del_local_route(Topic) ->
    gen_server:call(?ROUTER, {del_local_route, Topic}).
    
-spec(match_local(binary()) -> [mqtt_route()]).
match_local(Name) ->
    case ets:info(local_route, size) of
        0 -> [];
        _ -> ets:foldl(
               fun({Filter, Node}, Matched) ->
                   case emqx_topic:match(Name, Filter) of
                       true  -> [#route{topic = {local, Filter}, node = Node} | Matched];
                       false -> Matched
                   end
               end, [], local_route)
    end.

-spec(clean_local_routes() -> ok).
clean_local_routes() ->
    gen_server:call(?ROUTER, clean_local_routes).

dump() ->
    [{route, ets:tab2list(route)}, {local_route, ets:tab2list(local_route)}].

%% For unit test.
start() ->
    gen_server:start({local, ?ROUTER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?ROUTER, stop).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    ekka:monitor(membership),
    ets:new(local_route, [set, named_table, protected]),
    {ok, TRef} = timer:send_interval(timer:seconds(1), stats),
    {ok, #state{stats_fun = StatsFun, stats_timer = TRef}}.

handle_call({add_local_route, Topic}, _From, State) ->
    %% why node()...?
    ets:insert(local_route, {Topic, node()}),
    {reply, ok, State};

handle_call({del_local_route, Topic}, _From, State) ->
    ets:delete(local_route, Topic),
    {reply, ok, State};

handle_call(clean_local_routes, _From, State) ->
    ets:delete_all_objects(local_route),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Req, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    global:trans({?LOCK, self()}, fun() -> clean_routes_(Node) end),
    handle_info(stats, State);

handle_info({membership, _Event}, State) ->
    %% ignore
    {noreply, State};

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(mnesia:table_info(route, size)),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef}) ->
    timer:cancel(TRef),
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% Clean routes on the down node.
clean_routes_(Node) ->
    Pattern = #route{_ = '_', node = Node},
    Clean = fun() ->
                [mnesia:delete_object(route, R, write) ||
                    R <- mnesia:match_object(route, Pattern, write)]
            end,
    mnesia:transaction(Clean).

