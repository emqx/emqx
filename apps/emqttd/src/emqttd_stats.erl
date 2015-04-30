%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd statistics.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_stats).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd_systop.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link/0]).

%% statistics API.
-export([statsfun/1, statsfun/2,
         getstats/0, getstat/1,
         setstat/2, setstats/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(STATS_TAB, mqtt_stats).

-record(state, {tick_tref}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start stats server
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc Generate stats fun
%% @end
%%------------------------------------------------------------------------------
-spec statsfun(Stat :: atom()) -> fun().
statsfun(Stat) ->
    fun(Val) -> setstat(Stat, Val) end.
    
-spec statsfun(Stat :: atom(), MaxStat :: atom()) -> fun().
statsfun(Stat, MaxStat) -> 
    fun(Val) -> setstats(Stat, MaxStat, Val) end.

%%------------------------------------------------------------------------------
%% @doc Get broker statistics
%% @end
%%------------------------------------------------------------------------------
-spec getstats() -> [{atom(), non_neg_integer()}].
getstats() ->
    ets:tab2list(?STATS_TAB).

%%------------------------------------------------------------------------------
%% @doc Get stats by name
%% @end
%%------------------------------------------------------------------------------
-spec getstat(atom()) -> non_neg_integer() | undefined.
getstat(Name) ->
    case ets:lookup(?STATS_TAB, Name) of
        [{Name, Val}] -> Val;
        [] -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Set broker stats
%% @end
%%------------------------------------------------------------------------------
-spec setstat(Stat :: atom(), Val :: pos_integer()) -> boolean().
setstat(Stat, Val) ->
    ets:update_element(?STATS_TAB, Stat, {2, Val}).

%%------------------------------------------------------------------------------
%% @doc Set stats with max
%% @end
%%------------------------------------------------------------------------------
-spec setstats(Stat :: atom(), MaxStat :: atom(), Val :: pos_integer()) -> boolean().
setstats(Stat, MaxStat, Val) ->
    MaxVal = ets:lookup_element(?STATS_TAB, MaxStat, 2),
    if
        Val > MaxVal -> 
            ets:update_element(?STATS_TAB, MaxStat, {2, Val});
        true -> ok
    end,
    ets:update_element(?STATS_TAB, Stat, {2, Val}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    random:seed(now()),
    ets:new(?STATS_TAB, [set, public, named_table, {write_concurrency, true}]),
    Topics = ?SYSTOP_CLIENTS ++ ?SYSTOP_SESSIONS ++ ?SYSTOP_PUBSUB,
    [ets:insert(?STATS_TAB, {Topic, 0}) || Topic <- Topics],
    % Create $SYS Topics
    [ok = emqttd_pubsub:create(emqtt_topic:systop(Topic)) || Topic <- Topics],
    % Tick to publish stats
    {ok, #state{tick_tref = emqttd_broker:start_tick(tick)}, hibernate}.

handle_call(_Request, _From, State) ->
    {reply, error, State}.

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

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
publish(Stat, Val) ->
    emqttd_pubsub:publish(stats, #mqtt_message{topic   = emqtt_topic:systop(Stat),
                                               payload = emqttd_util:integer_to_binary(Val)}).

