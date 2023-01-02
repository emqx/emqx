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

-module(emqx_vm_mon).

-behaviour(gen_server).

-include("logger.hrl").

%% APIs
-export([start_link/1]).

-export([ get_check_interval/0
        , set_check_interval/1
        , get_process_high_watermark/0
        , set_process_high_watermark/1
        , get_process_low_watermark/0
        , set_process_low_watermark/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(VM_MON, ?MODULE).

start_link(Opts) ->
    gen_server:start_link({local, ?VM_MON}, ?MODULE, [Opts], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

get_check_interval() ->
    call(get_check_interval).

set_check_interval(Seconds) ->
    call({set_check_interval, Seconds}).

get_process_high_watermark() ->
    call(get_process_high_watermark).

set_process_high_watermark(Float) ->
    call({set_process_high_watermark, Float}).

get_process_low_watermark() ->
    call(get_process_low_watermark).

set_process_low_watermark(Float) ->
    call({set_process_low_watermark, Float}).

call(Req) ->
    gen_server:call(?VM_MON, Req, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    {ok, ensure_check_timer(#{check_interval => proplists:get_value(check_interval, Opts),
                              process_high_watermark => proplists:get_value(process_high_watermark, Opts),
                              process_low_watermark => proplists:get_value(process_low_watermark, Opts),
                              timer => undefined})}.

handle_call(get_check_interval, _From, State) ->
    {reply, maps:get(check_interval, State, undefined), State};

handle_call({set_check_interval, Seconds}, _From, State) ->
    {reply, ok, State#{check_interval := Seconds}};

handle_call(get_process_high_watermark, _From, State) ->
    {reply, maps:get(process_high_watermark, State, undefined), State};

handle_call({set_process_high_watermark, Float}, _From, State) ->
    {reply, ok, State#{process_high_watermark := Float}};

handle_call(get_process_low_watermark, _From, State) ->
    {reply, maps:get(process_low_watermark, State, undefined), State};

handle_call({set_process_low_watermark, Float}, _From, State) ->
    {reply, ok, State#{process_low_watermark := Float}};

handle_call(Req, _From, State) ->
    ?LOG(error, "[VM_MON] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[VM_MON] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, Timer, check},
            State = #{timer := Timer,
                      process_high_watermark := ProcHighWatermark,
                      process_low_watermark := ProcLowWatermark}) ->
    ProcessCount = erlang:system_info(process_count),
    case ProcessCount / erlang:system_info(process_limit) * 100 of
        Percent when Percent > ProcHighWatermark ->
            emqx_alarm:activate(too_many_processes, #{usage => Percent,
                                                      high_watermark => ProcHighWatermark,
                                                      low_watermark => ProcLowWatermark});
        Percent when Percent < ProcLowWatermark ->
            emqx_alarm:deactivate(too_many_processes);
        _Precent ->
            ok
    end,
    {noreply, ensure_check_timer(State)};

handle_info(Info, State) ->
    ?LOG(error, "[VM_MON] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := Timer}) ->
    emqx_misc:cancel_timer(Timer).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_check_timer(State = #{check_interval := Interval}) ->
    State#{timer := emqx_misc:start_timer(timer:seconds(Interval), check)}.
