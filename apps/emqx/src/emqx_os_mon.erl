%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0]).

-export([
    get_mem_check_interval/0,
    set_mem_check_interval/1,
    get_sysmem_high_watermark/0,
    set_sysmem_high_watermark/1,
    get_procmem_high_watermark/0,
    set_procmem_high_watermark/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx.hrl").

-define(OS_MON, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?OS_MON}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

get_mem_check_interval() ->
    memsup:get_check_interval().

set_mem_check_interval(Seconds) when Seconds < 60000 ->
    memsup:set_check_interval(1);
set_mem_check_interval(Seconds) ->
    memsup:set_check_interval(Seconds div 60000).

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
    #{
        sysmem_high_watermark := SysHW,
        procmem_high_watermark := PHW,
        mem_check_interval := MCI
    } = emqx:get_config([sysmon, os]),

    set_sysmem_high_watermark(SysHW),
    set_procmem_high_watermark(PHW),
    set_mem_check_interval(MCI),
    ensure_system_memory_alarm(SysHW),
    _ = start_check_timer(),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    {reply, {error, {unexpected_call, Req}}, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _Timer, check}, State) ->
    CPUHighWatermark = emqx:get_config([sysmon, os, cpu_high_watermark]) * 100,
    CPULowWatermark = emqx:get_config([sysmon, os, cpu_low_watermark]) * 100,
    %% TODO: should be improved?
    _ =
        case emqx_vm:cpu_util() of
            0 ->
                ok;
            Busy when Busy > CPUHighWatermark ->
                Usage = list_to_binary(io_lib:format("~.2f%", [Busy])),
                Message = <<Usage/binary, " cpu usage">>,
                emqx_alarm:activate(
                    high_cpu_usage,
                    #{
                        usage => Usage,
                        high_watermark => CPUHighWatermark,
                        low_watermark => CPULowWatermark
                    },
                    Message
                ),
                start_check_timer();
            Busy when Busy < CPULowWatermark ->
                Usage = list_to_binary(io_lib:format("~.2f%", [Busy])),
                Message = <<Usage/binary, " cpu usage">>,
                emqx_alarm:deactivate(
                    high_cpu_usage,
                    #{
                        usage => Usage,
                        high_watermark => CPUHighWatermark,
                        low_watermark => CPULowWatermark
                    },
                    Message
                ),
                start_check_timer();
            _Busy ->
                start_check_timer()
        end,
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_check_timer() ->
    Interval = emqx:get_config([sysmon, os, cpu_check_interval]),
    case erlang:system_info(system_architecture) of
        "x86_64-pc-linux-musl" -> ok;
        _ -> emqx_misc:start_timer(Interval, check)
    end.

%% At startup, memsup starts first and checks for memory alarms,
%% but emqx_alarm_handler is not yet used instead of alarm_handler,
%% so alarm_handler is used directly for notification (normally emqx_alarm_handler should be used).
%%The internal memsup will no longer trigger events that have been alerted,
%% and there is no exported function to remove the alerted flag,
%% so it can only be checked again at startup.

ensure_system_memory_alarm(HW) ->
    case erlang:whereis(memsup) of
        undefined ->
            ok;
        _Pid ->
            {Total, Allocated, _Worst} = memsup:get_memory_data(),
            case Total =/= 0 andalso Allocated / Total > HW of
                true -> emqx_alarm:activate(high_system_memory_usage, #{high_watermark => HW});
                false -> ok
            end
    end.
