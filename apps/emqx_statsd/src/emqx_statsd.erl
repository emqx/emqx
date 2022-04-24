%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_statsd.hrl").

-include_lib("emqx/include/logger.hrl").

-export([
    update/1,
    start/0,
    stop/0,
    restart/0,
    %% for rpc
    do_start/0,
    do_stop/0,
    do_restart/0
]).

%% Interface
-export([start_link/1]).

%% Internal Exports
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {
    timer :: reference() | undefined,
    sample_time_interval :: pos_integer(),
    flush_time_interval :: pos_integer(),
    estatsd_pid :: pid()
}).

update(Config) ->
    case
        emqx_conf:update(
            [statsd],
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            ok = stop(),
            case maps:get(<<"enable">>, Config, true) of
                true ->
                    ok = restart();
                false ->
                    ok = stop()
            end,
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

start() -> check_multicall_result(emqx_statsd_proto_v1:start(mria_mnesia:running_nodes())).
stop() -> check_multicall_result(emqx_statsd_proto_v1:stop(mria_mnesia:running_nodes())).
restart() -> check_multicall_result(emqx_statsd_proto_v1:restart(mria_mnesia:running_nodes())).

do_start() ->
    emqx_statsd_sup:ensure_child_started(?APP, emqx_conf:get([statsd], #{})).

do_stop() ->
    emqx_statsd_sup:ensure_child_stopped(?APP).

do_restart() ->
    ok = do_stop(),
    ok = do_start(),
    ok.

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

init([Opts]) ->
    process_flag(trap_exit, true),
    Tags = tags(maps:get(tags, Opts, #{})),
    {Host, Port} = maps:get(server, Opts, {?DEFAULT_HOST, ?DEFAULT_PORT}),
    Opts1 = maps:without(
        [
            sample_time_interval,
            flush_time_interval
        ],
        Opts#{
            tags => Tags,
            host => Host,
            port => Port,
            prefix => <<"emqx">>
        }
    ),
    {ok, Pid} = estatsd:start_link(maps:to_list(Opts1)),
    SampleTimeInterval = maps:get(sample_time_interval, Opts, ?DEFAULT_FLUSH_TIME_INTERVAL),
    FlushTimeInterval = maps:get(flush_time_interval, Opts, ?DEFAULT_FLUSH_TIME_INTERVAL),
    {ok,
        ensure_timer(#state{
            sample_time_interval = SampleTimeInterval,
            flush_time_interval = FlushTimeInterval,
            estatsd_pid = Pid
        })}.

handle_call(_Req, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {timeout, Ref, sample_timeout},
    State = #state{
        sample_time_interval = SampleTimeInterval,
        flush_time_interval = FlushTimeInterval,
        estatsd_pid = Pid,
        timer = Ref
    }
) ->
    Metrics = emqx_metrics:all() ++ emqx_stats:getstats() ++ emqx_vm_data(),
    SampleRate = SampleTimeInterval / FlushTimeInterval,
    StatsdMetrics = [
        {gauge, trans_metrics_name(Name), Value, SampleRate, []}
     || {Name, Value} <- Metrics
    ],
    estatsd:submit(Pid, StatsdMetrics),
    {noreply, ensure_timer(State)};
handle_info({'EXIT', Pid, Error}, State = #state{estatsd_pid = Pid}) ->
    {stop, {shutdown, Error}, State};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{estatsd_pid = Pid}) ->
    estatsd:stop(Pid),
    ok.

%%------------------------------------------------------------------------------
%% Internal function
%%------------------------------------------------------------------------------
trans_metrics_name(Name) ->
    Name0 = atom_to_binary(Name, utf8),
    binary_to_atom(<<"emqx.", Name0/binary>>, utf8).

emqx_vm_data() ->
    Idle =
        case cpu_sup:util([detailed]) of
            %% Not support for Windows
            {_, 0, 0, _} -> 0;
            {_Num, _Use, IdleList, _} -> proplists:get_value(idle, IdleList, 0)
        end,
    RunQueue = erlang:statistics(run_queue),
    [
        {run_queue, RunQueue},
        {cpu_idle, Idle},
        {cpu_use, 100 - Idle}
    ] ++ emqx_vm:mem_info().

tags(Map) ->
    Tags = maps:to_list(Map),
    [{atom_to_binary(Key, utf8), Value} || {Key, Value} <- Tags].

ensure_timer(State = #state{sample_time_interval = SampleTimeInterval}) ->
    State#state{timer = emqx_misc:start_timer(SampleTimeInterval, sample_timeout)}.

check_multicall_result({Results, []}) ->
    case
        lists:all(
            fun
                (ok) -> true;
                (_) -> false
            end,
            Results
        )
    of
        true -> ok;
        false -> error({bad_result, Results})
    end;
check_multicall_result({_, _}) ->
    error(multicall_failed).
