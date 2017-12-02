%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_router).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-include("emqttd.hrl").

%% Mnesia Bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/0, topics/0, local_topics/0]).

%% For eunit tests
-export([start/0, stop/0]).

%% Route APIs
-export([add_route/1, del_route/1, match/1, print/1, has_route/1]).

%% Local Route API
-export([get_local_routes/0, add_local_route/1, match_local/1,
         del_local_route/1, clean_local_routes/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([dump/0]).

-record(state, {stats_timer}).

-define(ROUTER, ?MODULE).

-define(LOCK, {?ROUTER, clean_routes}).

%%--------------------------------------------------------------------
%% Mnesia Bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(mqtt_route, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_route},
                {attributes, record_info(fields, mqtt_route)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(mqtt_route, ram_copies).

%%--------------------------------------------------------------------
%% Start the Router
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?ROUTER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Topics
%%--------------------------------------------------------------------

-spec(topics() -> list(binary())).
topics() ->
    mnesia:dirty_all_keys(mqtt_route).

-spec(local_topics() -> list(binary())).
local_topics() ->
    ets:select(mqtt_local_route, [{{'$1', '_'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% Match API
%%--------------------------------------------------------------------

%% @doc Match Routes.
-spec(match(Topic:: binary()) -> [mqtt_route()]).
match(Topic) when is_binary(Topic) ->
    %% Optimize: ets???
    Matched = mnesia:ets(fun emqttd_trie:match/1, [Topic]),
    %% Optimize: route table will be replicated to all nodes.
    lists:append([ets:lookup(mqtt_route, To) || To <- [Topic | Matched]]).

%% @doc Print Routes.
-spec(print(Topic :: binary()) -> [ok]).
print(Topic) ->
    [io:format("~s -> ~s~n", [To, Node]) ||
        #mqtt_route{topic = To, node = Node} <- match(Topic)].

%%--------------------------------------------------------------------
%% Route Management API
%%--------------------------------------------------------------------

%% @doc Add Route.
-spec(add_route(binary() | mqtt_route()) -> ok | {error, Reason :: term()}).
add_route(Topic) when is_binary(Topic) ->
    add_route(#mqtt_route{topic = Topic, node = node()});
add_route(Route = #mqtt_route{topic = Topic}) ->
    case emqttd_topic:wildcard(Topic) of
        true  -> case mnesia:is_transaction() of
                     true  -> add_trie_route(Route);
                     false -> trans(fun add_trie_route/1, [Route])
                 end;
        false -> add_direct_route(Route)
    end.

add_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:write/1, [Route]).

add_trie_route(Route = #mqtt_route{topic = Topic}) ->
    case mnesia:wread({mqtt_route, Topic}) of
        [] -> emqttd_trie:insert(Topic);
        _  -> ok
    end,
    mnesia:write(Route).

%% @doc Delete Route
-spec(del_route(binary() | mqtt_route()) -> ok | {error, Reason :: term()}).
del_route(Topic) when is_binary(Topic) ->
    del_route(#mqtt_route{topic = Topic, node = node()});
del_route(Route = #mqtt_route{topic = Topic}) ->
    case emqttd_topic:wildcard(Topic) of
        true  -> case mnesia:is_transaction() of
                     true  -> del_trie_route(Route);
                     false -> trans(fun del_trie_route/1, [Route])
                 end;
        false -> del_direct_route(Route)
    end.

del_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:delete_object/1, [Route]).

del_trie_route(Route = #mqtt_route{topic = Topic}) ->
    case mnesia:wread({mqtt_route, Topic}) of
        [Route] -> %% Remove route and trie
                   mnesia:delete_object(Route),
                   emqttd_trie:delete(Topic);
        [_|_]   -> %% Remove route only
                   mnesia:delete_object(Route);
        []      -> ok
    end.

%% @doc Has route?
-spec(has_route(binary()) -> boolean()).
has_route(Topic) when is_binary(Topic) ->
    ets:member(mqtt_route, Topic).

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
    ets:tab2list(mqtt_local_route).

-spec(add_local_route(binary()) -> ok).
add_local_route(Topic) ->
    gen_server:call(?ROUTER, {add_local_route, Topic}).
    
-spec(del_local_route(binary()) -> ok).
del_local_route(Topic) ->
    gen_server:call(?ROUTER, {del_local_route, Topic}).
    
-spec(match_local(binary()) -> [mqtt_route()]).
match_local(Name) ->
    case ets:info(mqtt_local_route, size) of
        0 -> [];
        _ -> ets:foldl(
               fun({Filter, Node}, Matched) ->
                   case emqttd_topic:match(Name, Filter) of
                       true  -> [#mqtt_route{topic = {local, Filter}, node = Node} | Matched];
                       false -> Matched
                   end
               end, [], mqtt_local_route)
    end.

-spec(clean_local_routes() -> ok).
clean_local_routes() ->
    gen_server:call(?ROUTER, clean_local_routes).

dump() ->
    [{route, ets:tab2list(mqtt_route)}, {local_route, ets:tab2list(mqtt_local_route)}].

%% For unit test.
start() ->
    gen_server:start({local, ?ROUTER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?ROUTER, stop).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    ekka:monitor(membership),
    ets:new(mqtt_local_route, [set, named_table, protected]),
    {ok, TRef} = timer:send_interval(timer:seconds(1), stats),
    {ok, #state{stats_timer = TRef}}.

handle_call({add_local_route, Topic}, _From, State) ->
    %% why node()...?
    ets:insert(mqtt_local_route, {Topic, node()}),
    {reply, ok, State};

handle_call({del_local_route, Topic}, _From, State) ->
    ets:delete(mqtt_local_route, Topic),
    {reply, ok, State};

handle_call(clean_local_routes, _From, State) ->
    ets:delete_all_objects(mqtt_local_route),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Req, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    global:trans({?LOCK, self()},
                 fun() ->
                     clean_routes_(Node),
                     update_stats_()
                 end),
    {noreply, State, hibernate};

handle_info({membership, _Event}, State) ->
    %% ignore
    {noreply, State};

handle_info(stats, State) ->
    update_stats_(),
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

%% Clean Routes on Node
clean_routes_(Node) ->
    Pattern = #mqtt_route{_ = '_', node = Node},
    Clean = fun() ->
                [mnesia:delete_object(mqtt_route, R, write) ||
                    R <- mnesia:match_object(mqtt_route, Pattern, write)]
            end,
    mnesia:transaction(Clean).

update_stats_() ->
    Size = mnesia:table_info(mqtt_route, size),
    emqttd_stats:setstats('routes/count', 'routes/max', Size),
    emqttd_stats:setstats('topics/count', 'topics/max', Size).

