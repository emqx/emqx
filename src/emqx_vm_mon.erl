%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_vm_mon).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_check_interval/0, set_check_interval/1,
         get_process_high_watermark/0, set_process_high_watermark/1]).

-record(state, {
          check_interval,
          timer,
          process_high_watermark
}).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

get_check_interval() ->
    gen_server:call(?MODULE, get_check_interval, infinity).

set_check_interval(Seconds) ->
    gen_server:call(?MODULE, {set_check_interval, Seconds}, infinity).

get_process_high_watermark() ->
    gen_server:call(?MODULE, get_process_high_watermark, infinity).

set_process_high_watermark(Float) ->
    gen_server:call(?MODULE, {set_process_high_watermark, Float}, infinity).

%%----------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------

init([Opts]) ->
    {ok, start_check_timer(#state{check_interval = proplists:get_value(check_interval, Opts, 30),
                                  process_high_watermark = proplists:get_value(process_high_watermark, Opts, 0.80)})}.

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.check_interval, State};
handle_call({set_check_interval, Seconds}, _From, State) ->
    {reply, ok, State#state{check_interval = Seconds}};

handle_call(get_process_high_watermark, _From, State) ->
    {reply, State#state.process_high_watermark, State};
handle_call({set_process_high_watermark, Float}, _From, State) ->
    {reply, ok, State#state{process_high_watermark = Float}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, Timer, check}, State = #state{timer = Timer}) ->
    ProcessCount = erlang:system_info(process_count),
    case ProcessCount > erlang:system_info(process_limit) * State#state.process_high_watermark of
        true ->
            set_alarm(too_many_processes, ProcessCount);
        false ->
            clear_alarm(too_many_processes)
    end,
    {noreply, start_check_timer(State)}.

terminate(_Reason, #state{timer = Timer}) ->
    emqx_misc:cancel_timer(Timer).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------

start_check_timer(State = #state{check_interval = Interval}) ->
    State#state{timer = emqx_misc:start_timer(timer:seconds(Interval), check)}.

set_alarm(AlarmId, AlarmDescr) ->
    case get(AlarmId) of
        set ->
            ok;
        undefined ->
            alarm_handler:set_alarm({AlarmId, AlarmDescr}),
            put(AlarmId, set), ok
    end.

clear_alarm(AlarmId) ->
    case get(AlarmId) of
        set ->
            alarm_handler:clear_alarm(AlarmId),
            erase(AlarmId), ok;
        undefined ->
            ok
    end.