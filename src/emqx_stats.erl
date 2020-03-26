%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stats).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Stats]").

%% APIs
-export([ start_link/0
        , start_link/1
        , stop/0
        ]).

%% Stats API.
-export([ getstats/0
        , getstat/1
        , setstat/2
        , setstat/3
        , statsfun/1
        , statsfun/2
        ]).

-export([ update_interval/2
        , update_interval/3
        , cancel_update/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([stats/0]).

-record(update, {name, countdown, interval, func}).

-record(state, {
          timer   :: reference(),
          updates :: [#update{}],
          tick_ms :: timeout()
         }).

-type(stats() :: list({atom(), non_neg_integer()})).

%% Connection stats
-define(CONNECTION_STATS,
        ['connections.count', %% Count of Concurrent Connections
         'connections.max'    %% Maximum Number of Concurrent Connections
        ]).

%% Channel stats
-define(CHANNEL_STATS,
        ['channels.count', %% Count of Concurrent Channels
         'channels.max'    %% Maximum Number of Concurrent Channels
        ]).

%% Session stats
-define(SESSION_STATS,
        ['sessions.count', %% Count of Concurrent Sessions
         'sessions.max'    %% Maximum Number of Concurrent Sessions
        ]).

%% PubSub stats
-define(PUBSUB_STATS,
        ['topics.count',
         'topics.max',
         'suboptions.count',
         'suboptions.max',
         'subscribers.count',
         'subscribers.max',
         'subscriptions.count',
         'subscriptions.max',
         'subscriptions.shared.count',
         'subscriptions.shared.max'
        ]).

%% Route stats
-define(ROUTE_STATS,
        ['routes.count',
         'routes.max'
        ]).

%% Retained stats
-define(RETAINED_STATS,
        ['retained.count',
         'retained.max'
        ]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-type(opts() :: #{tick_ms := timeout()}).

%% @doc Start stats server
-spec(start_link() -> startlink_ret()).
start_link() ->
    start_link(#{tick_ms => timer:seconds(1)}).

-spec(start_link(opts()) -> startlink_ret()).
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?SERVER, stop, infinity).

%% @doc Generate stats fun.
-spec(statsfun(Stat :: atom()) -> fun()).
statsfun(Stat) ->
    fun(Val) -> setstat(Stat, Val) end.

-spec(statsfun(Stat :: atom(), MaxStat :: atom()) -> fun()).
statsfun(Stat, MaxStat) ->
    fun(Val) -> setstat(Stat, MaxStat, Val) end.

%% @doc Get all statistics.
-spec(getstats() -> stats()).
getstats() ->
    case ets:info(?TAB, name) of
        undefined -> [];
        _ -> ets:tab2list(?TAB)
    end.

%% @doc Get stats by name.
-spec(getstat(atom()) -> maybe(non_neg_integer())).
getstat(Name) ->
    case ets:lookup(?TAB, Name) of
        [{Name, Val}] -> Val;
        [] -> undefined
    end.

%% @doc Set stats
-spec(setstat(Stat :: atom(), Val :: pos_integer()) -> boolean()).
setstat(Stat, Val) when is_integer(Val) ->
    safe_update_element(Stat, Val).

%% @doc Set stats with max value.
-spec(setstat(Stat :: atom(), MaxStat :: atom(),
              Val :: pos_integer()) -> boolean()).
setstat(Stat, MaxStat, Val) when is_integer(Val) ->
    cast({setstat, Stat, MaxStat, Val}).

-spec(update_interval(atom(), fun()) -> ok).
update_interval(Name, UpFun) ->
    update_interval(Name, 1, UpFun).

-spec(update_interval(atom(), pos_integer(), fun()) -> ok).
update_interval(Name, Secs, UpFun) when is_integer(Secs), Secs >= 1 ->
    cast({update_interval, rec(Name, Secs, UpFun)}).

-spec(cancel_update(atom()) -> ok).
cancel_update(Name) ->
    cast({cancel_update, Name}).

rec(Name, Secs, UpFun) ->
    #update{name = Name, countdown = Secs, interval = Secs, func = UpFun}.

cast(Msg) -> gen_server:cast(?SERVER, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(#{tick_ms := TickMs}) ->
    ok = emqx_tables:new(?TAB, [public, set, {write_concurrency, true}]),
    Stats = lists:append([?CONNECTION_STATS,
                          ?CHANNEL_STATS,
                          ?SESSION_STATS,
                          ?PUBSUB_STATS,
                          ?ROUTE_STATS,
                          ?RETAINED_STATS
                         ]),
    true = ets:insert(?TAB, [{Name, 0} || Name <- Stats]),
    {ok, start_timer(#state{updates = [], tick_ms = TickMs}), hibernate}.

start_timer(#state{tick_ms = Ms} = State) ->
    State#state{timer = emqx_misc:start_timer(Ms, tick)}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({setstat, Stat, MaxStat, Val}, State) ->
    try ets:lookup_element(?TAB, MaxStat, 2) of
        MaxVal when Val > MaxVal ->
            ets:update_element(?TAB, MaxStat, {2, Val});
        _ -> ok
    catch
        error:badarg ->
            ets:insert(?TAB, {MaxStat, Val})
    end,
    safe_update_element(Stat, Val),
    {noreply, State};

handle_cast({update_interval, Update = #update{name = Name}},
            State = #state{updates = Updates}) ->
    NState = case lists:keyfind(Name, #update.name, Updates) of
                 #update{} ->
                     ?LOG(warning, "Duplicated update: ~s", [Name]),
                     State;
                 false -> State#state{updates = [Update|Updates]}
             end,
    {noreply, NState};

handle_cast({cancel_update, Name}, State = #state{updates = Updates}) ->
    Updates1 = lists:keydelete(Name, #update.name, Updates),
    {noreply, State#state{updates = Updates1}};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, tick}, State = #state{timer = TRef, updates = Updates}) ->
    Updates1 = lists:foldl(
                 fun(Update = #update{name = Name, countdown = C, interval = I,
                                      func = UpFun}, Acc) when C =< 0 ->
                         try UpFun()
                         catch
                             _:Error ->
                                 ?LOG(error, "Update ~s failed: ~0p", [Name, Error])
                         end,
                         [Update#update{countdown = I} | Acc];
                    (Update = #update{countdown = C}, Acc) ->
                         [Update#update{countdown = C - 1} | Acc]
                 end, [], Updates),
    {noreply, start_timer(State#state{updates = Updates1}), hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{timer = TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

safe_update_element(Key, Val) ->
    try ets:update_element(?TAB, Key, {2, Val}) of
        false ->
            ets:insert_new(?TAB, {Key, Val});
        true -> true
    catch
        error:badarg ->
            ?LOG(warning, "Failed to update ~0p to ~0p", [Key, Val])
    end.

