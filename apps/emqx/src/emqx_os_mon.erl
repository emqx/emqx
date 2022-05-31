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

-export([
    current_sysmem_percent/0
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
    gen_server:call(?OS_MON, ?FUNCTION_NAME, infinity).

set_sysmem_high_watermark(Float) ->
    gen_server:call(?OS_MON, {?FUNCTION_NAME, Float}, infinity).

get_procmem_high_watermark() ->
    memsup:get_procmem_high_watermark().

set_procmem_high_watermark(Float) ->
    memsup:set_procmem_high_watermark(Float).

current_sysmem_percent() ->
    Ratio = load_ctl:get_memory_usage(),
    erlang:floor(Ratio * 10000) / 100.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% memsup is not reliable, ignore
    memsup:set_sysmem_high_watermark(1.0),
    #{
        sysmem_high_watermark := SysHW,
        procmem_high_watermark := PHW,
        mem_check_interval := MCI
    } = emqx:get_config([sysmon, os]),

    set_procmem_high_watermark(PHW),
    set_mem_check_interval(MCI),
    ok = update_mem_alarm_status(SysHW),
    _ = start_mem_check_timer(),
    _ = start_cpu_check_timer(),
    {ok, #{sysmem_high_watermark => SysHW}}.

handle_call(get_sysmem_high_watermark, _From, #{sysmem_high_watermark := HWM} = State) ->
    {reply, HWM, State};
handle_call({set_sysmem_high_watermark, New}, _From, #{sysmem_high_watermark := _Old} = State) ->
    ok = update_mem_alarm_status(New),
    {reply, ok, State#{sysmem_high_watermark := New}};
handle_call(Req, _From, State) ->
    {reply, {error, {unexpected_call, Req}}, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _Timer, mem_check}, #{sysmem_high_watermark := HWM} = State) ->
    ok = update_mem_alarm_status(HWM),
    ok = start_mem_check_timer(),
    {noreply, State};
handle_info({timeout, _Timer, cpu_check}, State) ->
    CPUHighWatermark = emqx:get_config([sysmon, os, cpu_high_watermark]) * 100,
    CPULowWatermark = emqx:get_config([sysmon, os, cpu_low_watermark]) * 100,
    case emqx_vm:cpu_util() of
        0 ->
            ok;
        Busy when Busy > CPUHighWatermark ->
            _ = emqx_alarm:activate(
                high_cpu_usage,
                #{
                    usage => Busy,
                    high_watermark => CPUHighWatermark,
                    low_watermark => CPULowWatermark
                },
                usage_msg(Busy, cpu)
            );
        Busy when Busy < CPULowWatermark ->
            ok = emqx_alarm:ensure_deactivated(
                high_cpu_usage,
                #{
                    usage => Busy,
                    high_watermark => CPUHighWatermark,
                    low_watermark => CPULowWatermark
                },
                usage_msg(Busy, cpu)
            );
        _Busy ->
            ok
    end,
    ok = start_cpu_check_timer(),
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

start_cpu_check_timer() ->
    Interval = emqx:get_config([sysmon, os, cpu_check_interval]),
    case erlang:system_info(system_architecture) of
        "x86_64-pc-linux-musl" -> ok;
        _ -> start_timer(Interval, cpu_check)
    end.

is_sysmem_check_supported() ->
    {unix, linux} =:= os:type().

start_mem_check_timer() ->
    Interval = emqx:get_config([sysmon, os, mem_check_interval]),
    case is_integer(Interval) andalso is_sysmem_check_supported() of
        true ->
            start_timer(Interval, mem_check);
        false ->
            ok
    end.

start_timer(Interval, Msg) ->
    _ = emqx_misc:start_timer(Interval, Msg),
    ok.

update_mem_alarm_status(HWM) when HWM > 1.0 orelse HWM < 0.0 ->
    ?SLOG(warning, #{msg => "discarded_out_of_range_mem_alarm_threshold", value => HWM}),
    ok = emqx_alarm:ensure_deactivated(
        high_system_memory_usage,
        #{},
        <<"Deactivated mem usage alarm due to out of range threshold">>
    );
update_mem_alarm_status(HWM) ->
    is_sysmem_check_supported() andalso
        do_update_mem_alarm_status(HWM),
    ok.

do_update_mem_alarm_status(HWM0) ->
    HWM = HWM0 * 100,
    Usage = current_sysmem_percent(),
    case Usage > HWM of
        true ->
            _ = emqx_alarm:activate(
                high_system_memory_usage,
                #{
                    usage => Usage,
                    high_watermark => HWM
                },
                usage_msg(Usage, mem)
            );
        _ ->
            ok = emqx_alarm:ensure_deactivated(
                high_system_memory_usage,
                #{
                    usage => Usage,
                    high_watermark => HWM
                },
                usage_msg(Usage, mem)
            )
    end,
    ok.

usage_msg(Usage, What) ->
    %% devide by 1.0 to ensure float point number
    iolist_to_binary(io_lib:format("~.2f% ~p usage", [Usage / 1.0, What])).
