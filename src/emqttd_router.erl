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

%% @doc MQTT Message Router
-module(emqttd_router).

-behaviour(gen_server2).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-export([start_link/4]).

%% Route API
-export([route/2]).

%% Route Admin API
-export([add_route/2, lookup_routes/1, has_route/1, delete_route/2]).

%% Batch API
-export([add_routes/2, delete_routes/2]).

%% For Test
-export([stop/1]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(aging, {topics, time, tref}).

-record(state, {pool, id, statsfun, aging :: #aging{}}).

%% @doc Start a local router.
-spec start_link(atom(), pos_integer(), fun((atom()) -> ok), list()) -> {ok, pid()} | {error, any()}.
start_link(Pool, Id, StatsFun, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)},
                           ?MODULE, [Pool, Id, StatsFun, Env], []).

%% @doc Route Message on the local node.
-spec route(emqttd_topic:topic(), mqtt_message()) -> any().
route(Queue = <<"$Q/", _Q>>, Msg) ->
    case lookup_routes(Queue) of
        [] ->
            emqttd_metrics:inc('messages/dropped');
        [SubPid] ->
            SubPid ! {dispatch, Queue, Msg};
        Routes ->
            Idx = crypto:rand_uniform(1, length(Routes) + 1),
            SubPid = lists:nth(Idx, Routes),
            SubPid ! {dispatch, Queue, Msg}
    end;

route(Topic, Msg) ->
    case lookup_routes(Topic) of
        [] ->
            emqttd_metrics:inc('messages/dropped');
        [SubPid] -> %% optimize
            SubPid ! {dispatch, Topic, Msg};
        Routes ->
            lists:foreach(fun(SubPid) ->
                SubPid ! {dispatch, Topic, Msg}
            end, Routes)
    end.

%% @doc Has Route?
-spec has_route(emqttd_topic:topic()) -> boolean().
has_route(Topic) ->
    ets:member(route, Topic).

%% @doc Lookup Routes
-spec lookup_routes(emqttd_topic:topic()) -> list(pid()).
lookup_routes(Topic) when is_binary(Topic) ->
    case ets:member(route, Topic) of
        true  ->
            try ets:lookup_element(route, Topic, 2) catch error:badarg -> [] end;
        false ->
            []
    end.

%% @doc Add Route.
-spec add_route(emqttd_topic:topic(), pid()) -> ok.
add_route(Topic, Pid) when is_pid(Pid) ->
    call(pick(Topic), {add_route, Topic, Pid}).

%% @doc Add Routes.
-spec add_routes(list(emqttd_topic:topic()), pid()) -> ok.
add_routes([], _Pid) ->
    ok;
add_routes([Topic], Pid) ->
    add_route(Topic, Pid);

add_routes(Topics, Pid) ->
    lists:foreach(fun({Router, Slice}) ->
                call(Router, {add_routes, Slice, Pid})
        end, slice(Topics)).

%% @doc Delete Route.
-spec delete_route(emqttd_topic:topic(), pid()) -> ok.
delete_route(Topic, Pid) ->
    cast(pick(Topic), {delete_route, Topic, Pid}).

%% @doc Delete Routes.
-spec delete_routes(list(emqttd_topic:topic()), pid()) -> ok.
delete_routes([Topic], Pid) ->
    delete_route(Topic, Pid);

delete_routes(Topics, Pid) ->
    lists:foreach(fun({Router, Slice}) ->
                cast(Router, {delete_routes, Slice, Pid})
        end, slice(Topics)).

%% @private Slice topics.
slice(Topics) ->
    dict:to_list(lists:foldl(fun(Topic, Dict) ->
                    dict:append(pick(Topic), Topic, Dict)
            end, dict:new(), Topics)).

%% @private Pick a router.
pick(Topic) ->
    gproc_pool:pick_worker(router, Topic).

stop(Id) when is_integer(Id) ->
    gen_server2:call(?PROC_NAME(?MODULE, Id), stop).

call(Router, Request) ->
    gen_server2:call(Router, Request, infinity).

cast(Router, Msg) ->
    gen_server2:cast(Router, Msg).

init([Pool, Id, StatsFun, Opts]) ->

    emqttd_time:seed(),

    %% Calls from pubsub should be scheduled first?
    process_flag(priority, high),

    ?GPROC_POOL(join, Pool, Id),

    AgingSecs = proplists:get_value(route_aging, Opts, 5),

    %% Aging Timer
    {ok, AgingTref} = start_tick(AgingSecs + random:uniform(AgingSecs)),

    Aging = #aging{topics = dict:new(), time = AgingSecs, tref = AgingTref},

    {ok, #state{pool = Pool, id = Id, statsfun = StatsFun, aging = Aging}}.

start_tick(Secs) ->
    timer:send_interval(timer:seconds(Secs), {clean, aged}).

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({add_route, Topic, Pid}, _From, State) ->
    ets:insert(route, {Topic, Pid}),
    {reply, ok, setstats(State)};

handle_call({add_routes, Topics, Pid}, _From, State) ->
    ets:insert(route, [{Topic, Pid} || Topic <- Topics]),
    {reply, ok, setstats(State)};

handle_call(Req, _From, State) ->
   ?UNEXPECTED_REQ(Req, State).

handle_cast({delete_route, Topic, Pid}, State = #state{aging = Aging}) ->
    ets:delete_object(route, {Topic, Pid}),
    NewState =
    case has_route(Topic) of
        false -> State#state{aging = store_aged(Topic, Aging)};
        true  -> State
    end,
    {noreply, setstats(NewState)};

handle_cast({delete_routes, Topics, Pid}, State) ->
    NewAging =
    lists:foldl(fun(Topic, Aging) ->
                    ets:delete_object(route, {Topic, Pid}),
                    case has_route(Topic) of
                        false -> store_aged(Topic, Aging);
                        true  -> Aging
                    end
            end, State#state.aging, Topics),
    {noreply, setstats(State#state{aging = NewAging})};

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({clean, aged}, State = #state{aging = Aging}) ->

    #aging{topics = Dict, time = Time} = Aging,

    ByTime  = emqttd_time:now_to_secs() - Time,

    Dict1 = try_clean(ByTime, dict:to_list(Dict)),

    NewAging = Aging#aging{topics = dict:from_list(Dict1)},

    {noreply, State#state{aging = NewAging}, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id, aging = #aging{tref = TRef}}) ->
    timer:cancel(TRef),
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

try_clean(ByTime, List) ->
    try_clean(ByTime, List, []).

try_clean(_ByTime, [], Acc) ->
    Acc;

try_clean(ByTime, [{Topic, TS} | Left], Acc) ->
    case has_route(Topic) of
        false ->
            try_clean2(ByTime, {Topic, TS}, Left, Acc);
        true  ->
            try_clean(ByTime, Left, Acc)
    end.

try_clean2(ByTime, {Topic, TS}, Left, Acc) when TS > ByTime ->
    try_clean(ByTime, Left, [{Topic, TS} | Acc]);

try_clean2(ByTime, {Topic, _TS}, Left, Acc) ->
    TopicR = #mqtt_topic{topic = Topic, node = node()},
    case mnesia:transaction(fun try_remove_topic/1, [TopicR]) of
        {atomic, _}      -> ok;
        {aborted, Error} -> lager:error("Clean Topic '~s' Error: ~p", [Topic, Error])
    end,
    try_clean(ByTime, Left, Acc).

try_remove_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    %% Lock topic first
    case mnesia:wread({topic, Topic}) of
        [] ->
            ok; %% mnesia:abort(not_found);
        [TopicR] ->
            %% Remove topic and trie
            delete_topic(TopicR),
            emqttd_trie:delete(Topic);
        _More ->
            %% Remove topic only
            delete_topic(TopicR)
    end.

delete_topic(TopicR) ->
    mnesia:delete_object(topic, TopicR, write).

store_aged(Topic, Aging = #aging{topics = Dict}) ->
    Now = emqttd_time:now_to_secs(),
    Aging#aging{topics = dict:store(Topic, Now, Dict)}.

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(route), State.

