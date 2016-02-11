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

-module(emqttd_broker).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Function Exports
-export([start_link/0]).

%% Event API
-export([subscribe/1, notify/2]).

%% Hook API
-export([hook/3, unhook/2, foreach_hooks/2, foldl_hooks/3]).

%% Broker API
-export([env/1, version/0, uptime/0, datetime/0, sysdescr/0]).

%% Tick API
-export([start_tick/1, stop_tick/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {started_at, sys_interval, heartbeat, tick_tref}).

-define(SERVER, ?MODULE).

-define(BROKER_TAB, mqtt_broker).

%% $SYS Topics of Broker
-define(SYSTOP_BROKERS, [
    version,      % Broker version
    uptime,       % Broker uptime
    datetime,     % Broker local datetime
    sysdescr      % Broker description
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start emqttd broker
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Subscribe broker event
-spec subscribe(EventType :: any()) -> ok.
subscribe(EventType) ->
    gproc:reg({p, l, {broker, EventType}}).
    
%% @doc Notify broker event
-spec notify(EventType :: any(), Event :: any()) -> ok.
notify(EventType, Event) ->
     gproc:send({p, l, {broker, EventType}}, {notify, EventType, self(), Event}).

%% @doc Get broker env
env(Name) ->
    proplists:get_value(Name, emqttd:env(broker)).

%% @doc Get broker version
-spec version() -> string().
version() ->
    {ok, Version} = application:get_key(emqttd, vsn), Version.

%% @doc Get broker description
-spec sysdescr() -> string().
sysdescr() ->
    {ok, Descr} = application:get_key(emqttd, description), Descr.

%% @doc Get broker uptime
-spec uptime() -> string().
uptime() -> gen_server:call(?SERVER, uptime).

%% @doc Get broker datetime
-spec datetime() -> string().
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

%% @doc Hook
-spec hook(Hook :: atom(), Name :: any(), MFA :: mfa()) -> ok | {error, any()}.
hook(Hook, Name, MFA) ->
    gen_server:call(?SERVER, {hook, Hook, Name, MFA}).

%% @doc Unhook
-spec unhook(Hook :: atom(), Name :: any()) -> ok | {error, any()}.
unhook(Hook, Name) ->
    gen_server:call(?SERVER, {unhook, Hook, Name}).

%% @doc Foreach hooks
-spec foreach_hooks(Hook :: atom(), Args :: list()) -> any().
foreach_hooks(Hook, Args) ->
    case ets:lookup(?BROKER_TAB, {hook, Hook}) of
        [{_, Hooks}] ->
            lists:foreach(fun({_Name, {M, F, A}}) ->
                    apply(M, F, Args++A)
                end, Hooks);
        [] ->
            ok
    end.

%% @doc Foldl hooks
-spec foldl_hooks(Hook :: atom(), Args :: list(), Acc0 :: any()) -> any().
foldl_hooks(Hook, Args, Acc0) ->
    case ets:lookup(?BROKER_TAB, {hook, Hook}) of
        [{_, Hooks}] -> 
            lists:foldl(fun({_Name, {M, F, A}}, Acc) -> 
                    apply(M, F, lists:append([Args, [Acc], A]))
                end, Acc0, Hooks);
        [] -> 
            Acc0
    end.

%% @doc Start a tick timer
start_tick(Msg) ->
    start_tick(timer:seconds(env(sys_interval)), Msg).

start_tick(0, _Msg) ->
    undefined;
start_tick(Interval, Msg) when Interval > 0 ->
    {ok, TRef} = timer:send_interval(Interval, Msg), TRef.

%% @doc Start tick timer
stop_tick(undefined) ->
    ok;
stop_tick(TRef) ->
    timer:cancel(TRef).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqttd_time:seed(),
    ets:new(?BROKER_TAB, [set, public, named_table]),
    % Create $SYS Topics
    emqttd_pubsub:create(topic, <<"$SYS/brokers">>),
    [ok = create_topic(Topic) || Topic <- ?SYSTOP_BROKERS],
    % Tick
    {ok, #state{started_at = os:timestamp(),
                heartbeat  = start_tick(1000, heartbeat),
                tick_tref  = start_tick(tick)}, hibernate}.

handle_call(uptime, _From, State) ->
    {reply, uptime(State), State};

handle_call({hook, Hook, Name, MFArgs}, _From, State) ->
    Key = {hook, Hook}, Reply =
    case ets:lookup(?BROKER_TAB, Key) of
        [{Key, Hooks}] ->
            case lists:keyfind(Name, 1, Hooks) of
                {Name, _MFArgs} ->
                    {error, existed};
                false ->
                    insert_hooks(Key, Hooks ++ [{Name, MFArgs}])
            end;
        [] ->
            insert_hooks(Key, [{Name, MFArgs}])
    end,
    {reply, Reply, State};

handle_call({unhook, Hook, Name}, _From, State) ->
    Key = {hook, Hook}, Reply =
    case ets:lookup(?BROKER_TAB, Key) of
        [{Key, Hooks}] ->
            insert_hooks(Key, lists:keydelete(Name, 1, Hooks));
        [] ->
            {error, not_found}
    end,
    {reply, Reply, State};

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(heartbeat, State) ->
    publish(uptime, list_to_binary(uptime(State))),
    publish(datetime, list_to_binary(datetime())),
    {noreply, State};

handle_info(tick, State) ->
    retain(brokers),
    retain(version,  list_to_binary(version())),
    retain(sysdescr, list_to_binary(sysdescr())),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{heartbeat = Hb, tick_tref = TRef}) ->
    stop_tick(Hb),
    stop_tick(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

insert_hooks(Key, Hooks) ->
    ets:insert(?BROKER_TAB, {Key, Hooks}), ok.

create_topic(Topic) ->
    emqttd_pubsub:create(topic, emqttd_topic:systop(Topic)).

retain(brokers) ->
    Payload = list_to_binary(string:join([atom_to_list(N) ||
                    N <- emqttd_mnesia:running_nodes()], ",")),
    Msg = emqttd_message:make(broker, <<"$SYS/brokers">>, Payload),
    emqttd_pubsub:publish(emqttd_message:set_flag(sys, Msg)).

retain(Topic, Payload) when is_binary(Payload) ->
    Msg = emqttd_message:make(broker, emqttd_topic:systop(Topic), Payload),
    emqttd_pubsub:publish(emqttd_message:set_flag(retain, Msg)).

publish(Topic, Payload) when is_binary(Payload) ->
    Msg = emqttd_message:make(broker, emqttd_topic:systop(Topic), Payload),
    emqttd_pubsub:publish(Msg).

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

