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

%% @doc emqttd statistics
-module(emqttd_stats).

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link/0, stop/0]).

%% statistics API.
-export([statsfun/1, statsfun/2,
         getstats/0, getstat/1,
         setstat/2, setstats/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tick_tref}).

-define(STATS_TAB, mqtt_stats).

%% $SYS Topics for Clients
-define(SYSTOP_CLIENTS, [
    'clients/count',    % clients connected current
    'clients/max'       % max clients connected
]).

%% $SYS Topics for Sessions
-define(SYSTOP_SESSIONS, [
    'sessions/count',
    'sessions/max'
]).

%% $SYS Topics for Subscribers
-define(SYSTOP_PUBSUB, [
    'routes/count',        % ...
    'routes/max',          % ...
    'topics/count',        % ...
    'topics/max',          % ...
    'subscribers/count',   % ...
    'subscribers/max',     % ...
    'subscriptions/count', % ...
    'subscriptions/max'    % ...
]).

%% $SYS Topic for retained
-define(SYSTOP_RETAINED, [
    'retained/count',
    'retained/max'
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start stats server
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

%% @doc Generate stats fun
-spec(statsfun(Stat :: atom()) -> fun()).
statsfun(Stat) ->
    fun(Val) -> setstat(Stat, Val) end.
    
-spec(statsfun(Stat :: atom(), MaxStat :: atom()) -> fun()).
statsfun(Stat, MaxStat) -> 
    fun(Val) -> setstats(Stat, MaxStat, Val) end.

%% @doc Get broker statistics
-spec(getstats() -> [{atom(), non_neg_integer()}]).
getstats() ->
    lists:sort(ets:tab2list(?STATS_TAB)).

%% @doc Get stats by name
-spec(getstat(atom()) -> non_neg_integer() | undefined).
getstat(Name) ->
    case ets:lookup(?STATS_TAB, Name) of
        [{Name, Val}] -> Val;
        [] -> undefined
    end.

%% @doc Set broker stats
-spec(setstat(Stat :: atom(), Val :: pos_integer()) -> boolean()).
setstat(Stat, Val) ->
    ets:update_element(?STATS_TAB, Stat, {2, Val}).

%% @doc Set stats with max
-spec(setstats(Stat :: atom(), MaxStat :: atom(), Val :: pos_integer()) -> boolean()).
setstats(Stat, MaxStat, Val) ->
    gen_server:cast(?MODULE, {setstats, Stat, MaxStat, Val}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqttd_time:seed(),
    ets:new(?STATS_TAB, [set, public, named_table, {write_concurrency, true}]),
    Topics = ?SYSTOP_CLIENTS ++ ?SYSTOP_SESSIONS ++ ?SYSTOP_PUBSUB ++ ?SYSTOP_RETAINED,
    ets:insert(?STATS_TAB, [{Topic, 0} || Topic <- Topics]),
    % Create $SYS Topics
    [ok = emqttd:create(topic, stats_topic(Topic)) || Topic <- Topics],
    % Tick to publish stats
    {ok, #state{tick_tref = emqttd_broker:start_tick(tick)}, hibernate}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, error, State}.

%% atomic
handle_cast({setstats, Stat, MaxStat, Val}, State) ->
    MaxVal = ets:lookup_element(?STATS_TAB, MaxStat, 2),
    if
        Val > MaxVal ->
            ets:update_element(?STATS_TAB, MaxStat, {2, Val});
        true -> ok
    end,
    ets:update_element(?STATS_TAB, Stat, {2, Val}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Interval Tick.
handle_info(tick, State) ->
    [publish(Stat, Val) || {Stat, Val} <- ets:tab2list(?STATS_TAB)],
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tick_tref = TRef}) ->
    emqttd_broker:stop_tick(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish(Stat, Val) ->
    Msg = emqttd_message:make(stats, stats_topic(Stat), bin(Val)),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)).

stats_topic(Stat) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['stats/', Stat]))).

bin(I) when is_integer(I) -> list_to_binary(integer_to_list(I)).

