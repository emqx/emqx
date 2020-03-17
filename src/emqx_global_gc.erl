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

-module(emqx_global_gc).

-behaviour(gen_server).

-include("types.hrl").

-export([start_link/0, stop/0]).

-export([run/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% 5 minutes
%% -define(DEFAULT_INTERVAL, 300000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(run() -> {ok, timer:time()}).
run() -> gen_server:call(?MODULE, run, infinity).

-spec(stop() -> ok).
stop() -> gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, ensure_timer(#{timer => undefined})}.

handle_call(run, _From, State) ->
    {Time, ok} = timer:tc(fun run_gc/0),
    {reply, {ok, Time div 1000}, State, hibernate};

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, TRef, run}, State = #{timer := TRef}) ->
    ok = run_gc(),
    {noreply, ensure_timer(State), hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internel function
%%--------------------------------------------------------------------

ensure_timer(State) ->
    case emqx:get_env(global_gc_interval) of
        undefined -> State;
        Interval  -> TRef = emqx_misc:start_timer(timer:seconds(Interval), run),
                     State#{timer := TRef}
    end.

run_gc() -> lists:foreach(fun do_gc/1, processes()).

do_gc(Pid) ->
    is_waiting(Pid) andalso garbage_collect(Pid).

-compile({inline, [is_waiting/1]}).
is_waiting(Pid) ->
    {status, waiting} == process_info(Pid, status).

