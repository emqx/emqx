%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("ekka/include/ekka.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/2]).

%% Route APIs
-export([add_route/1, add_route/2, add_route/3]).
-export([get_routes/1]).
-export([del_route/1, del_route/2, del_route/3]).
-export([has_routes/1, match_routes/1, print_routes/1]).
-export([topics/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type(destination() :: node() | {binary(), node()}).

-record(batch, {enabled, timer, pending}).
-record(state, {pool, id, batch :: #batch{}}).

-define(ROUTE, emqx_route).
-define(BATCH(Enabled), #batch{enabled = Enabled}).
-define(BATCH(Enabled, Pending), #batch{enabled = Enabled, pending = Pending}).

%%-----------------------------------------------------------------------------
%% Mnesia bootstrap
%%-----------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ROUTE, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, route},
                {attributes, record_info(fields, route)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ROUTE).

%%------------------------------------------------------------------------------
%% Strat a router
%%------------------------------------------------------------------------------

-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], [{hibernate_after, 2000}]).

%%------------------------------------------------------------------------------
%% Route APIs
%%------------------------------------------------------------------------------

-spec(add_route(topic() | route()) -> ok).
add_route(Topic) when is_binary(Topic) ->
    add_route(#route{topic = Topic, dest = node()});
add_route(Route = #route{topic = Topic}) ->
    cast(pick(Topic), {add_route, Route}).

-spec(add_route(topic(), destination()) -> ok).
add_route(Topic, Dest) when is_binary(Topic) ->
    add_route(#route{topic = Topic, dest = Dest}).

-spec(add_route({pid(), reference()}, topic(), destination()) -> ok).
add_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {add_route, From, #route{topic = Topic, dest = Dest}}).

-spec(get_routes(topic()) -> [route()]).
get_routes(Topic) ->
    ets:lookup(?ROUTE, Topic).

-spec(del_route(topic() | route()) -> ok).
del_route(Topic) when is_binary(Topic) ->
    del_route(#route{topic = Topic, dest = node()});
del_route(Route = #route{topic = Topic}) ->
    cast(pick(Topic), {del_route, Route}).

-spec(del_route(topic(), destination()) -> ok).
del_route(Topic, Dest) when is_binary(Topic) ->
    del_route(#route{topic = Topic, dest = Dest}).

-spec(del_route({pid(), reference()}, topic(), destination()) -> ok).
del_route(From, Topic, Dest) when is_binary(Topic) ->
    cast(pick(Topic), {del_route, From, #route{topic = Topic, dest = Dest}}).

-spec(has_routes(topic()) -> boolean()).
has_routes(Topic) when is_binary(Topic) ->
    ets:member(?ROUTE, Topic).

-spec(topics() -> list(topic())).
topics() -> mnesia:dirty_all_keys(?ROUTE).

%% @doc Match routes
%% Optimize: routing table will be replicated to all router nodes.
-spec(match_routes(topic()) -> [route()]).
match_routes(Topic) when is_binary(Topic) ->
    Matched = mnesia:ets(fun emqx_trie:match/1, [Topic]),
    lists:append([get_routes(To) || To <- [Topic | Matched]]).

%% @doc Print routes to a topic
-spec(print_routes(topic()) -> ok).
print_routes(Topic) ->
    lists:foreach(fun(#route{topic = To, dest = Dest}) ->
                      io:format("~s -> ~s~n", [To, Dest])
                  end, match_routes(Topic)).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(router, Topic).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([Pool, Id]) ->
    rand:seed(exsplus, erlang:timestamp()),
    gproc_pool:connect_worker(Pool, {Pool, Id}),
    Batch = #batch{enabled = emqx_config:get_env(route_batch_delete, false),
                   pending = sets:new()},
    {ok, ensure_batch_timer(#state{pool = Pool, id = Id, batch = Batch})}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Router] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({add_route, From, Route}, State) ->
    {noreply, NewState} = handle_cast({add_route, Route}, State),
    _ = gen_server:reply(From, ok),
    {noreply, NewState};

handle_cast({add_route, Route = #route{topic = Topic, dest = Dest}}, State) ->
    case lists:member(Route, get_routes(Topic)) of
        true  -> ok;
        false ->
            ok = emqx_router_helper:monitor(Dest),
            case emqx_topic:wildcard(Topic) of
                true  -> log(trans(fun add_trie_route/1, [Route]));
                false -> add_direct_route(Route)
            end
    end,
    {noreply, State};

handle_cast({del_route, From, Route}, State) ->
    {noreply, NewState} = handle_cast({del_route, Route}, State),
    _ = gen_server:reply(From, ok),
    {noreply, NewState};

handle_cast({del_route, Route = #route{topic = Topic}}, State) ->
    %% Confirm if there are still subscribers...
    {noreply, case ets:member(emqx_subscriber, Topic) of
                  true  -> State;
                  false ->
                      case emqx_topic:wildcard(Topic) of
                          true  -> log(trans(fun del_trie_route/1, [Route])),
                                   State;
                          false -> del_direct_route(Route, State)
                      end
              end};

handle_cast(Msg, State) ->
    emqx_logger:error("[Router] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, _TRef, batch_delete}, State = #state{batch = Batch}) ->
    _ = del_direct_routes(Batch#batch.pending),
    {noreply, ensure_batch_timer(State#state{batch = ?BATCH(true, sets:new())}), hibernate};

handle_info(Info, State) ->
    emqx_logger:error("[Router] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id, batch = Batch}) ->
    _ = cacel_batch_timer(Batch),
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

ensure_batch_timer(State = #state{batch = #batch{enabled = false}}) ->
    State;
ensure_batch_timer(State = #state{batch = Batch}) ->
    TRef = erlang:start_timer(50 + rand:uniform(50), self(), batch_delete),
    State#state{batch = Batch#batch{timer = TRef}}.

cacel_batch_timer(#batch{enabled = false}) ->
    ok;
cacel_batch_timer(#batch{enabled = true, timer = TRef}) ->
    catch erlang:cancel_timer(TRef).

add_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:write/3, [?ROUTE, Route, sticky_write]).

add_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({?ROUTE, Topic}) of
        [] -> emqx_trie:insert(Topic);
        _  -> ok
    end,
    mnesia:write(?ROUTE, Route, sticky_write).

del_direct_route(Route, State = #state{batch = ?BATCH(false)}) ->
    del_direct_route(Route), State;
del_direct_route(Route, State = #state{batch = Batch = ?BATCH(true, Pending)}) ->
    State#state{batch = Batch#batch{pending = sets:add_element(Route, Pending)}}.

del_direct_route(Route) ->
    mnesia:async_dirty(fun mnesia:delete_object/3, [?ROUTE, Route, sticky_write]).

del_direct_routes([]) ->
    ok;
del_direct_routes(Routes) ->
    DelFun = fun(R = #route{topic = Topic}) ->
                 case ets:member(emqx_subscriber, Topic) of
                     true  -> ok;
                     false -> mnesia:delete_object(?ROUTE, R, sticky_write)
                 end
             end,
    mnesia:async_dirty(fun lists:foreach/2, [DelFun, Routes]).

del_trie_route(Route = #route{topic = Topic}) ->
    case mnesia:wread({?ROUTE, Topic}) of
        [Route] -> %% Remove route and trie
                   mnesia:delete_object(?ROUTE, Route, sticky_write),
                   emqx_trie:delete(Topic);
        [_|_]   -> %% Remove route only
                   mnesia:delete_object(?ROUTE, Route, sticky_write);
        []      -> ok
    end.

%% @private
-spec(trans(function(), list(any())) -> ok | {error, term()}).
trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, _}      -> ok;
        {aborted, Error} -> {error, Error}
    end.

log(ok) -> ok;
log({error, Reason}) ->
    emqx_logger:error("[Router] mnesia aborted: ~p", [Reason]).

