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

-module(emqx_os_mon).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_cpu_check_interval/0, set_cpu_check_interval/1,
         get_cpu_high_watermark/0, set_cpu_high_watermark/1,
         get_mem_check_interval/0, set_mem_check_interval/1,
         get_sysmem_high_watermark/0, set_sysmem_high_watermark/1,
         get_procmem_high_watermark/0, set_procmem_high_watermark/1]).

-record(state, {
          cpu_check_interval,
          timer,
          cpu_high_watermark
}).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

get_cpu_check_interval() ->
    gen_server:call(?MODULE, get_cpu_check_interval, infinity).

set_cpu_check_interval(Seconds) ->
    gen_server:call(?MODULE, {set_cpu_check_interval, Seconds}, infinity).

get_cpu_high_watermark() ->
    gen_server:call(?MODULE, get_cpu_high_watermark, infinity).

set_cpu_high_watermark(Float) ->
    gen_server:call(?MODULE, {set_cpu_high_watermark, Float}, infinity).

get_mem_check_interval() ->
    memsup:get_check_interval() div 1000.

set_mem_check_interval(Seconds) ->
    memsup:set_check_interval(Seconds div 60).

get_sysmem_high_watermark() ->
    memsup:get_sysmem_high_watermark().

set_sysmem_high_watermark(Float) ->
    memsup:set_sysmem_high_watermark(Float).

get_procmem_high_watermark() ->
    memsup:get_procmem_high_watermark().

set_procmem_high_watermark(Float) ->
    memsup:set_procmem_high_watermark(Float).

%%----------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------

init([Opts]) ->
    cpu_sup:util(),
    set_mem_check_interval(proplists:get_value(mem_check_interval, Opts, 60)),
    set_sysmem_high_watermark(proplists:get_value(sysmem_high_watermark, Opts, 0.80)),
    set_procmem_high_watermark(proplists:get_value(procmem_high_watermark, Opts, 0.05)),
    {ok, start_check_timer(#state{cpu_high_watermark = proplists:get_value(cpu_high_watermark, Opts, 0.80),
                                  cpu_check_interval = proplists:get_value(cpu_check_interval, Opts, 60)})}.

handle_call(get_cpu_check_interval, _From, State) ->
    {reply, State#state.cpu_check_interval, State};
handle_call({set_cpu_check_interval, Seconds}, _From, State) ->
    {reply, ok, State#state{cpu_check_interval = Seconds}};

handle_call(get_cpu_high_watermark, _From, State) ->
    {reply, State#state.cpu_high_watermark, State};
handle_call({set_cpu_high_watermark, Float}, _From, State) ->
    {reply, ok, State#state{cpu_high_watermark = Float}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, Timer, check}, State = #state{timer = Timer, cpu_high_watermark = CPUHighWatermark}) ->
    case cpu_sup:util() of
        0 ->
            {noreply, State#state{timer = undefined}};
        {error, _Reason} ->
            {noreply, start_check_timer(State)};
        Busy ->
            case Busy > 100 * CPUHighWatermark of
                true ->
                    set_alarm(cpu_high_watermark, Busy);
                false ->
                    clear_alarm(cpu_high_watermark)
            end,
            {noreply, start_check_timer(State)}
    end.

terminate(_Reason, #state{timer = Timer}) ->
    emqx_misc:cancel_timer(Timer).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------

start_check_timer(State = #state{cpu_check_interval = Interval}) ->
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