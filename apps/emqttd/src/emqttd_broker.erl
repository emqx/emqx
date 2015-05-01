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
%%% emqttd broker.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_broker).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd_systop.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0]).

%% Event API
-export([subscribe/1, notify/2]).

%% Broker API
-export([env/1, version/0, uptime/0, datetime/0, sysdescr/0]).

%% Tick API
-export([start_tick/1, stop_tick/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(BROKER_TAB, mqtt_broker).

-record(state, {started_at, sys_interval, tick_tref}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start emqttd broker
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc Subscribe broker event
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(EventType :: any()) -> ok.
subscribe(EventType) ->
    gproc:reg({p, l, {broker, EventType}}).
    
%%------------------------------------------------------------------------------
%% @doc Notify broker event
%% @end
%%------------------------------------------------------------------------------
-spec notify(EventType :: any(), Event :: any()) -> ok.
notify(EventType, Event) ->
     Key = {broker, EventType},
     gproc:send({p, l, Key}, {self(), Key, Event}).

%%------------------------------------------------------------------------------
%% @doc Get broker env
%% @end
%%------------------------------------------------------------------------------
env(Name) ->
    proplists:get_value(Name, application:get_env(emqttd, broker, [])).

%%------------------------------------------------------------------------------
%% @doc Get broker version
%% @end
%%------------------------------------------------------------------------------
-spec version() -> string().
version() ->
    {ok, Version} = application:get_key(emqttd, vsn), Version.

%%------------------------------------------------------------------------------
%% @doc Get broker description
%% @end
%%------------------------------------------------------------------------------
-spec sysdescr() -> string().
sysdescr() ->
    {ok, Descr} = application:get_key(emqttd, description), Descr.

%%------------------------------------------------------------------------------
%% @doc Get broker uptime
%% @end
%%------------------------------------------------------------------------------
-spec uptime() -> string().
uptime() ->
    gen_server:call(?SERVER, uptime).

%%------------------------------------------------------------------------------
%% @doc Get broker datetime
%% @end
%%------------------------------------------------------------------------------
-spec datetime() -> string().
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

%%------------------------------------------------------------------------------
%% @doc Start a tick timer
%% @end
%%------------------------------------------------------------------------------
start_tick(Msg) ->
    start_tick(timer:seconds(env(sys_interval)), Msg).

start_tick(0, _Msg) ->
    undefined;
start_tick(Interval, Msg) when Interval > 0 ->
    {ok, TRef} = timer:send_interval(Interval, Msg), TRef.

%%------------------------------------------------------------------------------
%% @doc Start tick timer
%% @end
%%------------------------------------------------------------------------------
stop_tick(undefined) ->
    ok;
stop_tick(TRef) ->
    timer:cancel(TRef).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    random:seed(now()),
    ets:new(?BROKER_TAB, [set, public, named_table]),
    % Create $SYS Topics
    [ok = create_topic(Topic) || Topic <- ?SYSTOP_BROKERS],
    % Tick
    {ok, #state{started_at = os:timestamp(), tick_tref = start_tick(tick)}, hibernate}.

handle_call(uptime, _From, State) ->
    {reply, uptime(State), State};

handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    retain(version, list_to_binary(version())),
    retain(sysdescr, list_to_binary(sysdescr())),
    publish(uptime, list_to_binary(uptime(State))),
    publish(datetime, list_to_binary(datetime())),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tick_tref = TRef}) ->
    stop_tick(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

create_topic(Topic) ->
    emqttd_pubsub:create(emqtt_topic:systop(Topic)).

retain(Topic, Payload) when is_binary(Payload) ->
    publish(#mqtt_message{retain = true,
                          topic = emqtt_topic:systop(Topic),
                          payload = Payload}).

publish(Topic, Payload) when is_binary(Payload) ->
    publish( #mqtt_message{topic = emqtt_topic:systop(Topic),
                           payload = Payload}).

publish(Msg) ->
    emqttd_pubsub:publish(broker, Msg).


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

