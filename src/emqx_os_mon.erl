%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/1]).

-export([ get_cpu_check_interval/0
        , set_cpu_check_interval/1
        , get_cpu_high_watermark/0
        , set_cpu_high_watermark/1
        , get_cpu_low_watermark/0
        , set_cpu_low_watermark/1
        , get_mem_check_interval/0
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
-include_lib("lc/include/lc.hrl").

-define(OS_MON, ?MODULE).

start_link(Opts) ->
    gen_server:start_link({local, ?OS_MON}, ?MODULE, [Opts], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

get_cpu_check_interval() ->
    call(get_cpu_check_interval).

set_cpu_check_interval(Seconds) ->
    call({set_cpu_check_interval, Seconds}).

get_cpu_high_watermark() ->
    call(get_cpu_high_watermark).

set_cpu_high_watermark(Float) ->
    call({set_cpu_high_watermark, Float}).

get_cpu_low_watermark() ->
    call(get_cpu_low_watermark).

set_cpu_low_watermark(Float) ->
    call({set_cpu_low_watermark, Float}).

get_mem_check_interval() ->
    call(?FUNCTION_NAME).

set_mem_check_interval(Seconds) ->
    call({?FUNCTION_NAME, Seconds}).

get_sysmem_high_watermark() ->
    call(?FUNCTION_NAME).

set_sysmem_high_watermark(HW) ->
    case load_ctl:get_config() of
        #{ ?MEM_MON_F0 := true } = OldLC ->
            ok = load_ctl:put_config(OldLC#{ ?MEM_MON_F0 => true
                                           , ?MEM_MON_F1 => HW / 100});
        _ ->
            skip
    end,
    gen_server:call(?OS_MON, {?FUNCTION_NAME, HW}, infinity).

get_procmem_high_watermark() ->
    memsup:get_procmem_high_watermark().

set_procmem_high_watermark(HW) ->
    memsup:set_procmem_high_watermark(HW / 100).

call(Req) ->
    gen_server:call(?OS_MON, Req, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    process_flag(trap_exit, true),
    %% make sure memsup will not emit system memory alarms
    memsup:set_sysmem_high_watermark(1),
    set_procmem_high_watermark(proplists:get_value(procmem_high_watermark, Opts)),
    MemCheckInterval = do_resolve_mem_check_interval(proplists:get_value(mem_check_interval, Opts)),
    SysHW = proplists:get_value(sysmem_high_watermark, Opts),
    St = ensure_check_timer(#{cpu_high_watermark => proplists:get_value(cpu_high_watermark, Opts),
                              cpu_low_watermark => proplists:get_value(cpu_low_watermark, Opts),
                              cpu_check_interval => proplists:get_value(cpu_check_interval, Opts),
                              sysmem_high_watermark => SysHW,
                              mem_check_interval => MemCheckInterval,
                              timer => undefined}),
    ok = do_set_mem_check_interval(MemCheckInterval),
    %% update immediately after start/restart
    ok = update_mem_alarm_status(SysHW),
    {ok, ensure_mem_check_timer(St)}.

handle_call(get_sysmem_high_watermark, _From, State) ->
    #{sysmem_high_watermark := SysHW} = State,
    {reply, maybe_round(SysHW), State};
handle_call(get_mem_check_interval, _From, State) ->
    #{mem_check_interval := Interval} = State,
    {reply, Interval, State};
handle_call({set_sysmem_high_watermark, SysHW}, _From, State) ->
    %% update immediately after start/restart
    ok = update_mem_alarm_status(SysHW),
    {reply, ok, State#{sysmem_high_watermark => SysHW}};
handle_call({set_mem_check_interval, Seconds0}, _From, State) ->
    Seconds = do_resolve_mem_check_interval(Seconds0),
    ok = do_set_mem_check_interval(Seconds),
    %% will start taking effect when the current timer expires
    {reply, ok, State#{mem_check_interval => Seconds}};
handle_call(get_cpu_check_interval, _From, State) ->
    {reply, maps:get(cpu_check_interval, State, undefined), State};

handle_call({set_cpu_check_interval, Seconds}, _From, State) ->
    {reply, ok, State#{cpu_check_interval := Seconds}};

handle_call(get_cpu_high_watermark, _From, State) ->
    {reply, maps:get(cpu_high_watermark, State, undefined), State};

handle_call({set_cpu_high_watermark, Float}, _From, State) ->
    {reply, ok, State#{cpu_high_watermark := Float}};

handle_call(get_cpu_low_watermark, _From, State) ->
    {reply, maps:get(cpu_low_watermark, State, undefined), State};

handle_call({set_cpu_low_watermark, Float}, _From, State) ->
    {reply, ok, State#{cpu_low_watermark := Float}};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, Timer, check}, State = #{timer := Timer,
                                               cpu_high_watermark := CPUHighWatermark,
                                               cpu_low_watermark := CPULowWatermark}) ->
    NState =
    case emqx_vm:cpu_util() of %% TODO: should be improved?
        0 ->
            State#{timer := undefined};
        Busy when Busy > CPUHighWatermark ->
            emqx_alarm:activate(high_cpu_usage, #{usage => Busy,
                                                  high_watermark => CPUHighWatermark,
                                                  low_watermark => CPULowWatermark}),
            ensure_check_timer(State);
        Busy when Busy < CPULowWatermark ->
            emqx_alarm:deactivate(high_cpu_usage),
            ensure_check_timer(State);
        _Busy ->
            ensure_check_timer(State)
    end,
    {noreply, NState};
handle_info({timeout, Timer, check_mem}, #{mem_check_timer := Timer,
                                           sysmem_high_watermark := SysHW
                                          } = State) ->
    ok = update_mem_alarm_status(SysHW),
    NState = ensure_mem_check_timer(State#{mem_check_timer := undefined}),
    {noreply, NState};
handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := Timer} = St) ->
    emqx_misc:cancel_timer(maps:get(mem_check_timer, St, undefined)),
    emqx_misc:cancel_timer(Timer).

code_change(_OldVsn, State, _Extra) ->
    %% NOTE: downgrade is not handled as the extra fields added to State
    %% does not affect old version code.
    %% The only thing which may slip through is that a started timer
    %% will result in a "unexpected info" error log for the old version code
    NewState = ensure_mem_check_timer(State),
    SysHW = resolve_sysmem_high_watermark(State),
    {ok, NewState#{sysmem_high_watermark => SysHW}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_check_timer(State = #{cpu_check_interval := Interval}) ->
    case erlang:system_info(system_architecture) of
        "x86_64-pc-linux-musl" -> State;
        _ -> State#{timer := emqx_misc:start_timer(timer:seconds(Interval), check)}
    end.

ensure_mem_check_timer(#{mem_check_timer := Ref} = State) when is_reference(Ref) ->
    %% timer already started
    State;
ensure_mem_check_timer(State) ->
    Interval = resolve_mem_check_interval(State),
    case is_sysmem_check_supported() of
        true ->
            State#{mem_check_timer => emqx_misc:start_timer(timer:seconds(Interval), check_mem),
                   mem_check_interval => Interval
                  };
        false ->
            State#{mem_check_timer => undefined,
                   mem_check_interval => Interval
                  }
    end.

resolve_mem_check_interval(#{mem_check_interval := Seconds}) when is_integer(Seconds) ->
    Seconds;
resolve_mem_check_interval(_) ->
    %% this only happens when hot-upgrade from older version (< 4.3.14, or < 4.4.4)
    try
        %% memsup has interval set API using minutes, but returns in milliseconds from get API
        IntervalMs = memsup:get_check_interval(),
        true = (IntervalMs > 1000),
        IntervalMs div 1000
    catch
        _ : _ ->
            %% this is the memsup default
            60
    end.

is_sysmem_check_supported() ->
    %% sorry Mac and Windows, for now
    {unix, linux} =:= os:type().

%% we still need to set memsup interval for process (not system) memory check
do_set_mem_check_interval(Seconds) ->
    Minutes = Seconds div 60,
    _ = memsup:set_check_interval(Minutes),
    ok.

%% keep the time unit alignment with memsup, minmum interval is 60 seconds.
do_resolve_mem_check_interval(Seconds) ->
    case is_integer(Seconds) andalso Seconds >= 60 of
        true -> Seconds;
        false -> 60
    end.

resolve_sysmem_high_watermark(#{sysmem_high_watermark := SysHW}) -> SysHW;
resolve_sysmem_high_watermark(_) ->
    %% sysmem_high_watermark is not found in state map
    %% get it from memsup
    memsup:get_sysmem_high_watermark().

update_mem_alarm_status(SysHW) ->
    case is_sysmem_check_supported() of
        true ->
            do_update_mem_alarm_status(SysHW);
        false ->
            %% in case the old alarm is activated
            ok = emqx_alarm:ensure_deactivated(high_system_memory_usage, #{reason => disabled})
    end.

do_update_mem_alarm_status(SysHW) ->
    Usage = current_sysmem_percent(),
    case Usage > SysHW of
        true ->
            _ = emqx_alarm:activate(
                high_system_memory_usage,
                #{
                    usage => Usage,
                    high_watermark => SysHW
                }
            );
        _ ->
            ok = emqx_alarm:ensure_deactivated(
                high_system_memory_usage,
                #{
                    usage => Usage,
                    high_watermark => SysHW
                }
            )
    end,
    ok.

current_sysmem_percent() ->
    Ratio = load_ctl:get_memory_usage(),
    erlang:floor(Ratio * 10000) / 100.

maybe_round(X) when is_integer(X) -> X;
maybe_round(X) when is_float(X) ->
    R = erlang:round(X),
    case erlang:abs(X - R) > 1.0e-6 of
        true -> X;
        false -> R
    end.
