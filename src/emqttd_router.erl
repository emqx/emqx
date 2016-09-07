%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% Start/Stop
-export([start_link/0, topics/0, local_topics/0, stop/0]).

%% Route APIs
-export([add_route/1, add_route/2, add_routes/1, match/1, print/1,
         del_route/1, del_route/2, del_routes/1, has_route/1]).

%% Local Route API
-export([add_local_route/1, del_local_route/1, match_local/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([dump/0]).

-record(state, {stats_timer}).

-define(ROUTER, ?MODULE).

%%--------------------------------------------------------------------
%% Mnesia Bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(mqtt_topic, [
                {ram_copies, [node()]},
                {record_name, mqtt_topic},
                {attributes, record_info(fields, mqtt_topic)}]),
    ok = emqttd_mnesia:create_table(mqtt_route, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_route},
                {attributes, record_info(fields, mqtt_route)}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(mqtt_topic),
    ok = emqttd_mnesia:copy_table(mqtt_route, ram_copies).

%%--------------------------------------------------------------------
%% Start the Router
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?ROUTER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

topics() ->
    mnesia:dirty_all_keys(mqtt_route).

local_topics() ->
    ets:select(mqtt_local_route, [{{'$1', '_'}, [], ['$1']}]).

%% @doc Match Routes.
-spec(match(Topic:: binary()) -> [mqtt_route()]).
match(Topic) when is_binary(Topic) ->
    Matched = mnesia:async_dirty(fun emqttd_trie:match/1, [Topic]),
    %% Optimize: route table will be replicated to all nodes.
    lists:append([ets:lookup(mqtt_route, To) || To <- [Topic | Matched]]).

%% @doc Print Routes.
-spec(print(Topic :: binary()) -> [ok]).
print(Topic) ->
    [io:format("~s -> ~s~n", [To, Node]) ||
        #mqtt_route{topic = To, node = Node} <- match(Topic)].

%% @doc Add Route
-spec(add_route(binary() | mqtt_route()) -> ok | {error, Reason :: any()}).
add_route(Topic) when is_binary(Topic) ->
    add_route(#mqtt_route{topic = Topic, node = node()});
add_route(Route) when is_record(Route, mqtt_route) ->
    add_routes([Route]).

-spec(add_route(Topic :: binary(), Node :: node()) -> ok | {error, Reason :: any()}).
add_route(Topic, Node) when is_binary(Topic), is_atom(Node) ->
    add_route(#mqtt_route{topic = Topic, node = Node}).

%% @doc Add Routes
-spec(add_routes([mqtt_route()]) -> ok | {errory, Reason :: any()}).
add_routes(Routes) ->
    AddFun = fun() -> [add_route_(Route) || Route <- Routes] end,
    case mnesia:is_transaction() of
        true  -> AddFun();
        false -> trans(AddFun)
    end.

%% @private
add_route_(Route = #mqtt_route{topic = Topic}) ->
    case mnesia:wread({mqtt_route, Topic}) of
        [] ->
            case emqttd_topic:wildcard(Topic) of
                true  -> emqttd_trie:insert(Topic);
                false -> ok
            end,
            mnesia:write(Route),
            mnesia:write(#mqtt_topic{topic = Topic});
        Records ->
            case lists:member(Route, Records) of
                true  -> ok;
                false -> mnesia:write(Route)
            end
    end.

%% @doc Delete Route
-spec(del_route(binary() | mqtt_route()) -> ok | {error, Reason :: any()}).
del_route(Topic) when is_binary(Topic) ->
    del_route(#mqtt_route{topic = Topic, node = node()});
del_route(Route) when is_record(Route, mqtt_route) ->
    del_routes([Route]).

-spec(del_route(Topic :: binary(), Node :: node()) -> ok | {error, Reason :: any()}).
del_route(Topic, Node) when is_binary(Topic), is_atom(Node) ->
    del_route(#mqtt_route{topic = Topic, node = Node}).

%% @doc Delete Routes
-spec(del_routes([mqtt_route()]) -> ok | {error, any()}).
del_routes(Routes) ->
    DelFun = fun() -> [del_route_(Route) || Route <- Routes] end,
    case mnesia:is_transaction() of
        true  -> DelFun();
        false -> trans(DelFun)
    end.

del_route_(Route = #mqtt_route{topic = Topic}) ->
    case mnesia:wread({mqtt_route, Topic}) of
        [] ->
            ok;
        [Route] ->
            %% Remove route and trie
            mnesia:delete_object(Route),
            case emqttd_topic:wildcard(Topic) of
                true  -> emqttd_trie:delete(Topic);
                false -> ok
            end,
            mnesia:delete({mqtt_topic, Topic});
        _More ->
            %% Remove route only
            mnesia:delete_object(Route)
    end.

%% @doc Has Route?
-spec(has_route(binary()) -> boolean()).
has_route(Topic) ->
    Routes = case mnesia:is_transaction() of
                 true  -> mnesia:read(mqtt_route, Topic);
                 false -> mnesia:dirty_read(mqtt_route, Topic)
             end,
    length(Routes) > 0.

%% @private
-spec(trans(function()) -> ok | {error, any()}).
trans(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, _}      -> ok;
        {aborted, Error} -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% Local Route API
%%--------------------------------------------------------------------

-spec(add_local_route(binary()) -> ok).
add_local_route(Topic) ->
    gen_server:cast(?ROUTER, {add_local_route, Topic}).
    
-spec(del_local_route(binary()) -> ok).
del_local_route(Topic) ->
    gen_server:cast(?ROUTER, {del_local_route, Topic}).
    
-spec(match_local(binary()) -> [mqtt_route()]).
match_local(Name) ->
    [#mqtt_route{topic = {local, Filter}, node = Node}
        || {Filter, Node} <- ets:tab2list(mqtt_local_route),
           emqttd_topic:match(Name, Filter)].

dump() ->
    [{route, ets:tab2list(mqtt_route)}, {local_route, ets:tab2list(mqtt_local_route)}].

stop() -> gen_server:call(?ROUTER, stop).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    mnesia:subscribe(system),
    ets:new(mqtt_local_route, [set, named_table, protected]),
    {ok, TRef}  = timer:send_interval(timer:seconds(1), stats),
    {ok, #state{stats_timer = TRef}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Req, _From, State) ->
    {reply, ignore, State}.

handle_cast({add_local_route, Topic}, State) ->
    %% why node()...?
    ets:insert(mqtt_local_route, {Topic, node()}),
    {noreply, State};
    
handle_cast({del_local_route, Topic}, State) ->
    ets:delete(mqtt_local_route, Topic),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({mnesia_system_event, {mnesia_up, Node}}, State) ->
    lager:error("Mnesia up: ~p~n", [Node]),
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    lager:error("Mnesia down: ~p~n", [Node]),
    clean_routes_(Node),
    update_stats_(),
    {noreply, State, hibernate};

handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) ->
    %% 1. Backup and restart
    %% 2. Set master nodes
    lager:critical("Mnesia inconsistent_database event: ~p, ~p~n", [Context, Node]),
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_overload, Details}}, State) ->
    lager:critical("Mnesia overload: ~p~n", [Details]),
    {noreply, State};

handle_info({mnesia_system_event, _Event}, State) ->
    {noreply, State};

handle_info(stats, State) ->
    update_stats_(),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef}) ->
    timer:cancel(TRef),
    mnesia:unsubscribe(system).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% Clean Routes on Node
clean_routes_(Node) ->
    Pattern = #mqtt_route{_ = '_', node = Node},
    Clean = fun() ->
                [mnesia:delete_object(route, R, write) ||
                    R <- mnesia:match_object(route, Pattern, write)]
            end,
    mnesia:transaction(Clean).

update_stats_() ->
    emqttd_stats:setstats('routes/count', 'routes/max', mnesia:table_info(route, size)).

