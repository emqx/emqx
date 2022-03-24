%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").
-include("types.hrl").
-include_lib("mria/include/mria.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-export([start_link/2]).

%% Route APIs
-export([
    add_route/1,
    add_route/2,
    do_add_route/1,
    do_add_route/2
]).

-export([
    delete_route/1,
    delete_route/2,
    do_delete_route/1,
    do_delete_route/2
]).

-export([
    match_routes/1,
    lookup_routes/1,
    has_routes/1
]).

-export([print_routes/1]).

-export([topics/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type group() :: binary().

-type dest() :: node() | {group(), node()}.

-define(ROUTE_TAB, emqx_route).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    mria_config:set_dirty_shard(?ROUTE_SHARD, true),
    ok = mria:create_table(?ROUTE_TAB, [
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, ram_copies},
        {record_name, route},
        {attributes, record_info(fields, route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> startlink_ret().
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_misc:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec add_route(emqx_types:topic()) -> ok | {error, term()}.
add_route(Topic) when is_binary(Topic) ->
    add_route(Topic, node()).

-spec add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
add_route(Topic, Dest) when is_binary(Topic) ->
    call(pick(Topic), {add_route, Topic, Dest}).

-spec do_add_route(emqx_types:topic()) -> ok | {error, term()}.
do_add_route(Topic) when is_binary(Topic) ->
    do_add_route(Topic, node()).

-spec do_add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_add_route(Topic, Dest) when is_binary(Topic) ->
    Route = #route{topic = Topic, dest = Dest},
    case lists:member(Route, lookup_routes(Topic)) of
        true ->
            ok;
        false ->
            ok = emqx_router_helper:monitor(Dest),
            case emqx_topic:wildcard(Topic) of
                true ->
                    Fun = fun emqx_router_utils:insert_trie_route/2,
                    emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route], ?ROUTE_SHARD);
                false ->
                    emqx_router_utils:insert_direct_route(?ROUTE_TAB, Route)
            end
    end.

%% @doc Match routes
-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched -> lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.

%% Optimize: routing table will be replicated to all router nodes.
match_trie(Topic) ->
    case emqx_trie:empty() of
        true -> [];
        false -> emqx_trie:match(Topic)
    end.

-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

-spec has_routes(emqx_types:topic()) -> boolean().
has_routes(Topic) when is_binary(Topic) ->
    ets:member(?ROUTE_TAB, Topic).

-spec delete_route(emqx_types:topic()) -> ok | {error, term()}.
delete_route(Topic) when is_binary(Topic) ->
    delete_route(Topic, node()).

-spec delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
delete_route(Topic, Dest) when is_binary(Topic) ->
    call(pick(Topic), {delete_route, Topic, Dest}).

-spec do_delete_route(emqx_types:topic()) -> ok | {error, term()}.
do_delete_route(Topic) when is_binary(Topic) ->
    do_delete_route(Topic, node()).

-spec do_delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_delete_route(Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true ->
            Fun = fun emqx_router_utils:delete_trie_route/2,
            emqx_router_utils:maybe_trans(Fun, [?ROUTE_TAB, Route], ?ROUTE_SHARD);
        false ->
            emqx_router_utils:delete_direct_route(?ROUTE_TAB, Route)
    end.

-spec topics() -> list(emqx_types:topic()).
topics() ->
    mnesia:dirty_all_keys(?ROUTE_TAB).

%% @doc Print routes to a topic
-spec print_routes(emqx_types:topic()) -> ok.
print_routes(Topic) ->
    lists:foreach(
        fun(#route{topic = To, dest = Dest}) ->
            io:format("~ts -> ~ts~n", [To, Dest])
        end,
        match_routes(Topic)
    ).

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

pick(Topic) ->
    gproc_pool:pick_worker(router_pool, Topic).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({add_route, Topic, Dest}, _From, State) ->
    Ok = do_add_route(Topic, Dest),
    {reply, Ok, State};
handle_call({delete_route, Topic, Dest}, _From, State) ->
    Ok = do_delete_route(Topic, Dest),
    {reply, Ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
