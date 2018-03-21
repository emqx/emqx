%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_sys).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).

-export([schedulers/0]).

-export([version/0, uptime/0, datetime/0, sysdescr/0, sys_interval/0]).

-export([info/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {started_at, heartbeat, sys_ticker, version, sysdescr}).

-define(APP, emqx).

-define(SERVER, ?MODULE).

%% $SYS Topics of Broker
-define(SYSTOP_BROKERS, [
    version,  % Broker version
    uptime,   % Broker uptime
    datetime, % Broker local datetime
    sysdescr  % Broker description
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Get schedulers
-spec(schedulers() -> pos_integer()).
schedulers() ->
    erlang:system_info(schedulers).

%% @doc Get sys version
-spec(version() -> string()).
version() ->
    {ok, Version} = application:get_key(?APP, vsn), Version.

%% @doc Get sys description
-spec(sysdescr() -> string()).
sysdescr() ->
    {ok, Descr} = application:get_key(?APP, description), Descr.

%% @doc Get sys uptime
-spec(uptime() -> string()).
uptime() -> gen_server:call(?SERVER, uptime).

%% @doc Get sys datetime
-spec(datetime() -> string()).
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

sys_interval() ->
    application:get_env(?APP, sys_interval, 60000).

%% @doc Get sys info
-spec(info() -> list(tuple())).
info() ->
    [{version,  version()},
     {sysdescr, sysdescr()},
     {uptime,   uptime()},
     {datetime, datetime()}].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Tick = fun(I, M) ->
               {ok, TRef} = timer:send_interval(I, M), TRef
           end,
    {ok, #state{started_at = os:timestamp(),
                heartbeat  = Tick(1000, heartbeat),
                sys_ticker = Tick(sys_interval(), tick),
                version    = iolist_to_binary(version()),
                sysdescr   = iolist_to_binary(sysdescr())}, hibernate}.

handle_call(uptime, _From, State) ->
    {reply, uptime(State), State};

handle_call(Req, _From, State) ->
    emqx_log:error("[SYS] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_log:error("[SYS] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(heartbeat, State) ->
    publish(uptime, iolist_to_binary(uptime(State))),
    publish(datetime, iolist_to_binary(datetime())),
    {noreply, State, hibernate};

handle_info(tick, State = #state{version = Version, sysdescr = Descr}) ->
    retain(brokers),
    retain(version,  Version),
    retain(sysdescr, Descr),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    emqx_log:error("[SYS] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{heartbeat = Hb, sys_ticker = TRef}) ->
    timer:cancel(Hb),
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

retain(brokers) ->
    Payload = list_to_binary(string:join([atom_to_list(N) ||
                    N <- ekka_mnesia:running_nodes()], ",")),
    Msg = emqx_message:make(broker, <<"$SYS/brokers">>, Payload),
    emqx:publish(emqx_message:set_flag(sys, emqx_message:set_flag(retain, Msg))).

retain(Topic, Payload) when is_binary(Payload) ->
    Msg = emqx_message:make(broker, emqx_topic:systop(Topic), Payload),
    emqx:publish(emqx_message:set_flag(sys, emqx_message:set_flag(retain, Msg))).

publish(Topic, Payload) when is_binary(Payload) ->
    Msg = emqx_message:make(broker, emqx_topic:systop(Topic), Payload),
    emqx:publish(emqx_message:set_flag(sys, Msg)).

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

