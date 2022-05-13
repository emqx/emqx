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

-module(emqx_vm_mon).

-behaviour(gen_server).

-include("logger.hrl").

%% APIs
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(VM_MON, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?VM_MON}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = start_check_timer(),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _Timer, check}, State) ->
    ProcHighWatermark = emqx:get_config([sysmon, vm, process_high_watermark]),
    ProcLowWatermark = emqx:get_config([sysmon, vm, process_low_watermark]),
    ProcessCount = erlang:system_info(process_count),
    case ProcessCount / erlang:system_info(process_limit) of
        Percent when Percent > ProcHighWatermark ->
            Usage = io_lib:format("~p%", [Percent * 100]),
            Message = [Usage, " process usage"],
            emqx_alarm:activate(
                too_many_processes,
                #{
                    usage => Usage,
                    high_watermark => ProcHighWatermark,
                    low_watermark => ProcLowWatermark
                },
                Message
            );
        Percent when Percent < ProcLowWatermark ->
            Usage = io_lib:format("~p%", [Percent * 100]),
            Message = [Usage, " process usage"],
            emqx_alarm:ensure_deactivated(
                too_many_processes,
                #{
                    usage => Usage,
                    high_watermark => ProcHighWatermark,
                    low_watermark => ProcLowWatermark
                },
                Message
            );
        _Precent ->
            ok
    end,
    _ = start_check_timer(),
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
    Interval = emqx:get_config([sysmon, vm, process_check_interval]),
    emqx_misc:start_timer(Interval, check).
