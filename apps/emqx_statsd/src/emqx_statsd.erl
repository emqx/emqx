%%--------------------------------------------------------------------
 %% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

 -module(emqx_statsd).

 -behaviour(gen_server).

 -ifdef(TEST).
 -compile(export_all).
 -compile(nowarn_export_all).
 -endif.

 -include_lib("emqx/include/logger.hrl").

 %% Interface
 -export([start_link/1]).

 %% Internal Exports
 -export([ init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , code_change/3
         , terminate/2
         ]).

 -record(state, {
           timer                :: reference(),
           sample_time_interval :: pos_integer(),
           flush_time_interval  :: pos_integer(),
           estatsd_pid          :: pid()
         }).

 start_link(Opts) ->
     gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

 init([Opts]) ->
     SampleTimeInterval = proplists:get_value(sample_time_interval, Opts),
     FlushTimeInterval = proplists:get_value(flush_time_interval, Opts),
     Ref = erlang:start_timer(SampleTimeInterval, self(), sample_timeout),
     Pid = proplists:get_value(estatsd_pid, Opts),
     {ok, #state{timer = Ref,
                 sample_time_interval = SampleTimeInterval,
                 flush_time_interval = FlushTimeInterval,
                 estatsd_pid = Pid}}.

 handle_call(_Req, _From, State) ->
     {noreply, State}.

 handle_cast(_Msg, State) ->
     {noreply, State}.

 handle_info({timeout, Ref, sample_timeout}, State = #state{sample_time_interval = SampleTimeInterval,
                                                            flush_time_interval = FlushTimeInterval,
                                                            estatsd_pid = Pid,
                                                            timer = Ref}) ->
     ?LOG(debug, "emqx statsd submit"),
     Metrics = emqx_metrics:all() ++ emqx_stats:getstats() ++ emqx_vm_data(),
     SampleRate = SampleTimeInterval / FlushTimeInterval,
     StatsdMetrics = [{gauge, trans_metrics_name(Name), Value, SampleRate, []} || {Name, Value} <- Metrics],
     estatsd:submit(Pid, StatsdMetrics),
     {noreply, State#state{timer = erlang:start_timer(SampleTimeInterval, self(), sample_timeout)}};

 handle_info(_Msg, State) ->
     {noreply, State}.

 code_change(_OldVsn, State, _Extra) ->
     {ok, State}.

 terminate(_Reason, _State) ->
     ok.

 %%------------------------------------------------------------------------------
 %% Internale function
 %%------------------------------------------------------------------------------
 trans_metrics_name(Name) ->
     Name0 = atom_to_binary(Name, utf8),
     binary_to_atom(<<"emqx.", Name0/binary>>, utf8).

 emqx_vm_data() ->
     Idle = case cpu_sup:util([detailed]) of
                {_, 0, 0, _} -> 0; %% Not support for Windows
                {_Num, _Use, IdleList, _} -> proplists:get_value(idle, IdleList, 0)
            end,
     RunQueue = erlang:statistics(run_queue),
     [{run_queue, RunQueue},
      {cpu_idle, Idle},
      {cpu_use, 100 - Idle}] ++ emqx_vm:mem_info().
