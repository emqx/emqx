%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_os_mon).

-behaviour(gen_server).

-include("logger.hrl").

-logger_header("[OS_MON]").

-export([start_link/0]).

-export([ get_mem_check_interval/0
        , set_mem_check_interval/1
        , get_sysmem_high_watermark/0
        , set_sysmem_high_watermark/1
        , get_procmem_high_watermark/0
        , set_procmem_high_watermark/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-include("emqx.hrl").

-define(OS_MON, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?OS_MON}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

get_mem_check_interval() ->
    memsup:get_check_interval() div 1000.

set_mem_check_interval(Seconds) when Seconds < 60 ->
    memsup:set_check_interval(1);
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

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Opts = emqx_config:get([sysmon, os]),
    set_mem_check_interval(maps:get(mem_check_interval, Opts)),
    set_sysmem_high_watermark(maps:get(sysmem_high_watermark, Opts)),
    set_procmem_high_watermark(maps:get(procmem_high_watermark, Opts)),
    _ = start_check_timer(),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, _Timer, check}, State) ->
    CPUHighWatermark = emqx_config:get([sysmon, os, cpu_high_watermark]) * 100,
    CPULowWatermark = emqx_config:get([sysmon, os, cpu_low_watermark]) * 100,
    _ = case emqx_vm:cpu_util() of %% TODO: should be improved?
        0 -> ok;
        Busy when Busy >= CPUHighWatermark ->
            emqx_alarm:activate(high_cpu_usage, #{usage => io_lib:format("~p%", [Busy]),
                                                  high_watermark => CPUHighWatermark,
                                                  low_watermark => CPULowWatermark}),
            start_check_timer();
        Busy when Busy =< CPULowWatermark ->
            emqx_alarm:deactivate(high_cpu_usage),
            start_check_timer();
        _Busy ->
            start_check_timer()
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_check_timer() ->
    Interval = emqx_config:get([sysmon, os, cpu_check_interval]),
    case erlang:system_info(system_architecture) of
        "x86_64-pc-linux-musl" -> ok;
        _ -> emqx_misc:start_timer(timer:seconds(Interval), check)
    end.
