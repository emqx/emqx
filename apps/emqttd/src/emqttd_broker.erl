%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% emqttd broker.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_broker).

-include_lib("emqtt/include/emqtt.hrl").

-include("emqttd_systop.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(BROKER_TAB, mqtt_broker).

%% API Function Exports
-export([start_link/1]).

-export([version/0, uptime/0, datetime/0, sysdescr/0]).

%% statistics API.
-export([getstats/0, getstat/1, setstat/2, setstats/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {started_at, sys_interval, tick_timer}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start emqttd broker.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link([tuple()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Options) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Options], []).

%%------------------------------------------------------------------------------
%% @doc
%% Get broker version.
%%
%% @end
%%------------------------------------------------------------------------------
-spec version() -> string().
version() ->
    {ok, Version} = application:get_key(emqttd, vsn), Version.

%%------------------------------------------------------------------------------
%% @doc
%% Get broker description.
%%
%% @end
%%------------------------------------------------------------------------------
-spec sysdescr() -> string().
sysdescr() ->
    {ok, Descr} = application:get_key(emqttd, description), Descr.

%%------------------------------------------------------------------------------
%% @doc
%% Get broker uptime.
%%
%% @end
%%------------------------------------------------------------------------------
-spec uptime() -> string().
uptime() ->
    gen_server:call(?SERVER, uptime).

%%------------------------------------------------------------------------------
%% @doc
%% Get broker datetime.
%%
%% @end
%%------------------------------------------------------------------------------
-spec datetime() -> string().
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

%%------------------------------------------------------------------------------
%% @doc
%% Get broker statistics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec getstats() -> [{atom(), non_neg_integer()}].
getstats() ->
    ets:tab2list(?BROKER_TAB).

%%------------------------------------------------------------------------------
%% @doc
%% Get stats by name.
%%
%% @end
%%------------------------------------------------------------------------------
-spec getstat(atom()) -> non_neg_integer() | undefined.
getstat(Name) ->
    case ets:lookup(?BROKER_TAB, Name) of
        [{Name, Val}] -> Val;
        [] -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Set broker stats.
%%
%% @end
%%------------------------------------------------------------------------------
-spec setstat(Stat :: atom(), Val :: pos_integer()) -> boolean().
setstat(Stat, Val) ->
    ets:update_element(?BROKER_TAB, Stat, {2, Val}).

%%------------------------------------------------------------------------------
%% @doc
%% Set stats with max.
%%
%% @end
%%------------------------------------------------------------------------------
-spec setstats(Stat :: atom(), MaxStat :: atom(), Val :: pos_integer()) -> boolean().
setstats(Stat, MaxStat, Val) ->
    MaxVal = ets:lookup_element(?BROKER_TAB, MaxStat, 2),
    if
        Val > MaxVal -> 
            ets:update_element(?BROKER_TAB, MaxStat, {2, Val});
        true -> ok
    end,
    ets:update_element(?BROKER_TAB, Stat, {2, Val}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Options]) ->
    random:seed(now()),
    ets:new(?BROKER_TAB, [set, public, named_table, {write_concurrency, true}]),
    Topics = ?SYSTOP_CLIENTS ++ ?SYSTOP_SESSIONS ++ ?SYSTOP_PUBSUB,
    [ets:insert(?BROKER_TAB, {Topic, 0}) || Topic <- Topics],
    % Create $SYS Topics
    [ok = create(systop(Topic)) || Topic <- ?SYSTOP_BROKERS],
    [ok = create(systop(Topic)) || Topic <- Topics],
    SysInterval = proplists:get_value(sys_interval, Options, 60),
    State = #state{started_at = os:timestamp(), sys_interval = SysInterval},
    Delay = if 
                SysInterval == 0 -> 0;
                true -> random:uniform(SysInterval)
            end,
    {ok, tick(Delay, State), hibernate}.

handle_call(uptime, _From, State) ->
    {reply, uptime(State), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    retain(systop(version), list_to_binary(version())),
    retain(systop(sysdescr), list_to_binary(sysdescr())),
    publish(systop(uptime), list_to_binary(uptime(State))),
    publish(systop(datetime), list_to_binary(datetime())),
    [publish(systop(Stat), i2b(Val)) 
        || {Stat, Val} <- ets:tab2list(?BROKER_TAB)],
    {noreply, tick(State), hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

systop(Name) when is_atom(Name) ->
    list_to_binary(lists:concat(["$SYS/brokers/", node(), "/", Name])).

create(Topic) ->
    emqttd_pubsub:create(Topic).

retain(Topic, Payload) when is_binary(Payload) ->
    emqttd_pubsub:publish(broker, #mqtt_message{retain = true,
                                                topic = Topic,
                                                payload = Payload}).

publish(Topic, Payload) when is_binary(Payload) ->
    emqttd_pubsub:publish(broker, #mqtt_message{topic = Topic,
                                                payload = Payload}).

uptime(#state{started_at = Ts}) ->
    Secs = timer:now_diff(os:timestamp(), Ts) div 1000000,
    lists:flatten(uptime(seconds, Secs)).

uptime(seconds, Secs) when Secs < 60 ->
    [integer_to_list(Secs), " seconds"];
uptime(seconds, Secs) ->
    [uptime(minutes, Secs div 60), integer_to_list(Secs rem 60), " seconds"];
uptime(minutes, M) when M < 60 ->
    [integer_to_list(M), " minutes, "];
uptime(minutes, M) ->
    [uptime(hours, M div 60), integer_to_list(M rem 60), " minutes, "];
uptime(hours, H) when H < 24 ->
    [integer_to_list(H), " hours, "];
uptime(hours, H) ->
    [uptime(days, H div 24), integer_to_list(H rem 24), " hours, "];
uptime(days, D) ->
    [integer_to_list(D), " days,"].

tick(State = #state{sys_interval = SysInterval}) ->
    tick(SysInterval, State).

tick(0, State) ->
    State;
tick(Delay, State) ->
    State#state{tick_timer = erlang:send_after(Delay * 1000, self(), tick)}.

i2b(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I)).


