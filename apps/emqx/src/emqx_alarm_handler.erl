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

-module(emqx_alarm_handler).

-behaviour(gen_event).

-include("emqx.hrl").
-include("logger.hrl").
-include_lib("lc/include/lc.hrl").

%% gen_event callbacks
-export([
    init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2
]).

-export([
    load/0,
    unload/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load() ->
    gen_event:swap_handler(alarm_handler, {alarm_handler, swap}, {?MODULE, []}).

%% on the way shutting down, give it back to OTP
unload() ->
    gen_event:swap_handler(alarm_handler, {?MODULE, swap}, {alarm_handler, []}).

%%--------------------------------------------------------------------
%% gen_event callbacks
%%--------------------------------------------------------------------

init({_Args, {alarm_handler, _ExistingAlarms}}) ->
    {ok, []};
init(_) ->
    {ok, []}.

handle_event({set_alarm, {process_memory_high_watermark, Pid}}, State) ->
    HighWatermark = emqx_os_mon:get_procmem_high_watermark(),
    Message = to_bin("Process memory usage is higher than ~p%", [HighWatermark]),
    emqx_alarm:activate(
        high_process_memory_usage,
        #{
            pid => list_to_binary(pid_to_list(Pid)),
            high_watermark => HighWatermark
        },
        Message
    ),
    {ok, State};
handle_event({clear_alarm, process_memory_high_watermark}, State) ->
    emqx_alarm:ensure_deactivated(high_process_memory_usage),
    {ok, State};
handle_event({set_alarm, {?LC_ALARM_ID_RUNQ, Info}}, State) ->
    #{node := Node, runq_length := Len} = Info,
    Message = to_bin("VM is overloaded on node: ~p: ~p", [Node, Len]),
    emqx_alarm:activate(runq_overload, Info, Message),
    {ok, State};
handle_event({clear_alarm, ?LC_ALARM_ID_RUNQ}, State) ->
    emqx_alarm:ensure_deactivated(runq_overload),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.

handle_call(_Query, State) ->
    {ok, {error, bad_query}, State}.

terminate(swap, _State) ->
    {emqx_alarm_handler, []};
terminate(_, _) ->
    ok.

to_bin(Format, Args) ->
    io_lib:format(Format, Args).
