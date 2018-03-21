%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-behaviour(gen_server).

-include("emqx.hrl").

%% Mnesia Bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Start
-export([start_link/2]).

%% Topics
-export([topics/0]).

%% Route Management APIs
-export([add_route/2, add_route/3, get_routes/1, del_route/2, del_route/3]).

%% Match, print routes
-export([has_routes/1, match_routes/1, print_routes/1]).

-export([dump/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id}).

-type(destination() :: node() | {binary(), node()}).

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
    ok = ekka_mnesia:copy_table(route).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

start_link(Pool, Id) ->
    gen_server:start_link(?MODULE, [Pool, Id], [{hibernate_after, 1000}]).

%%--------------------------------------------------------------------
%% Add/Del Routes
%%--------------------------------------------------------------------

%% @doc Add a route
-spec(add_route(topic(), destination()) -> ok).
add_route(Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {add_route, #route{topic = Topic, dest = Dest}}).

-spec(add_route({pid(), reference()}, topic(), destination()) -> ok).
add_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {add_route, From, #route{topic = Topic, dest = Dest}}).

%% @doc Get routes
-spec(get_routes(topic()) -> [route()]).
get_routes(Topic) ->
    ets:lookup(route, Topic).

%% @doc Delete a route
-spec(del_route(topic(), destination()) -> ok).
del_route(Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {del_route, #route{topic = Topic, dest = Dest}}).

-spec(del_route({pid(), reference()}, topic(), destination()) -> ok).
del_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {del_route, From, #route{topic = Topic, dest = Dest}}).

%% @doc Has routes?
-spec(has_routes(topic()) -> boolean()).
has_routes(Topic) when is_binary(Topic) ->
    ets:member(route, Topic).

%%--------------------------------------------------------------------
%% Topics
%%--------------------------------------------------------------------

-spec(topics() -> list(binary())).
topics() ->
    mnesia:dirty_all_keys(route).

%%--------------------------------------------------------------------
%% Match Routes
%%--------------------------------------------------------------------

%% @doc Match routes
-spec(match_routes(Topic:: topic()) -> [{topic(), binary() | node()}]).
match_routes(Topic) when is_binary(Topic) ->
    %% Optimize: ets???
    Matched = mnesia:ets(fun emqx_trie:match/1, [Topic]),
    %% Optimize: route table will be replicated to all nodes.
    aggre(lists:append([ets:lookup(route, To) || To <- [Topic | Matched]])).

%% Aggregate routes
aggre([]) ->
    [];
aggre([#route{topic = To, dest = Node}]) when is_atom(Node) ->
    [{To, Node}];
aggre([#route{topic = To, dest = {Group, _Node}}]) ->
    [{To, Group}];
aggre(Routes) ->
    lists:foldl(
      fun(#route{topic = To, dest = Node}, Acc) when is_atom(Node) ->
        [{To, Node} | Acc];
        (#route{topic = To, dest = {Group, _}}, Acc) ->
        lists:usort([{To, Group} | Acc])
      end, [], Routes).

%%--------------------------------------------------------------------
%% Print Routes
%%--------------------------------------------------------------------

%% @doc Print routes to a topic
-spec(print_routes(topic()) -> ok).
print_routes(Topic) ->
    lists:foreach(fun({To, Dest}) ->
                      io:format("~s -> ~s~n", [To, Dest])
                  end, match_routes(Topic)).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(router, Topic).

%%FIXME: OOM?
dump() ->
    [{route, [{To, Dest} || #route{topic = To, dest = Dest} <- ets:tab2list(route)]}].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[Router] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({add_route, From, Route}, State) ->
    _ = handle_cast({add_route, Route}, State),
    gen_server:reply(From, ok),
    {noreply, State};

handle_cast({add_route, Route = #route{topic = Topic, dest = Dest}}, State) ->
    case lists:member(Route, ets:lookup(route, Topic)) of
        true  -> ok;
        false ->
            ok = emqx_router_helper:monitor(Dest),
            case emqx_topic:wildcard(Topic) of
                true  -> trans(fun add_trie_route/1, [Route]);
                false -> add_direct_route(Route)
            end
    end,
    {noreply, State};

handle_cast({del_route, From, Route}, State) ->
    _ = handle_cast({del_route, Route}, State),
    gen_server:reply(From, ok),
    {noreply, State};

handle_cast({del_route, Route = #route{topic = Topic}}, State) ->
    %% Confirm if there are still subscribers...
    case ets:member(subscriber, Topic) of
        true  -> ok;
        false ->
            case emqx_topic:wildcard(Topic) of
                true  -> trans(fun del_trie_route/1, [Route]);
                false -> del_direct_route(Route)
            end
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_log:error("[Router] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

add_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:write/1, [Route]).

add_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({route, Topic}) of
        [] -> emqx_trie:insert(Topic);
        _  -> ok
    end,
    mnesia:write(Route).

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

%% @private
-spec(trans(function(), list(any())) -> ok | {error, term()}).
trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, _}      -> ok;
        {aborted, Error} ->
            emqx_log:error("[Router] Mnesia aborted: ~p", [Error]),
            {error, Error}
    end.

