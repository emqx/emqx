%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    start/0,
    stop/0,
    restart/0,
    %% for rpc: remove after 5.1.x
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

-define(SAMPLE_TIMEOUT, sample_timeout).

%% Remove after 5.1.x
start() -> check_multicall_result(emqx_statsd_proto_v1:start(mria:running_nodes())).
stop() -> check_multicall_result(emqx_statsd_proto_v1:stop(mria:running_nodes())).
restart() -> check_multicall_result(emqx_statsd_proto_v1:restart(mria:running_nodes())).

do_start() ->
    emqx_statsd_sup:ensure_child_started(?APP).

do_stop() ->
    emqx_statsd_sup:ensure_child_stopped(?APP).

do_restart() ->
    ok = do_stop(),
    ok = do_start(),
    ok.

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Conf, []).

init(Conf) ->
    process_flag(trap_exit, true),
    #{
        tags := TagsRaw,
        server := Server,
        sample_time_interval := SampleTimeInterval,
        flush_time_interval := FlushTimeInterval
    } = Conf,
    FlushTimeInterval1 = flush_interval(FlushTimeInterval, SampleTimeInterval),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?SERVER_PARSE_OPTS),
    Tags = maps:fold(fun(K, V, Acc) -> [{to_bin(K), to_bin(V)} | Acc] end, [], TagsRaw),
    Opts = [{tags, Tags}, {host, Host}, {port, Port}, {prefix, <<"emqx">>}],
    {ok, Pid} = estatsd:start_link(Opts),
    {ok,
        ensure_timer(#{
            sample_time_interval => SampleTimeInterval,
            flush_time_interval => FlushTimeInterval1,
            estatsd_pid => Pid
        })}.

handle_call(_Req, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {timeout, Ref, ?SAMPLE_TIMEOUT},
    State = #{
        sample_time_interval := SampleTimeInterval,
        flush_time_interval := FlushTimeInterval,
        estatsd_pid := Pid,
        timer := Ref
    }
) ->
    Metrics = emqx_metrics:all() ++ emqx_stats:getstats() ++ emqx_mgmt:vm_stats(),
    SampleRate = SampleTimeInterval / FlushTimeInterval,
    StatsdMetrics = [
        {gauge, Name, Value, SampleRate, []}
     || {Name, Value} <- Metrics
    ],
    ok = estatsd:submit(Pid, StatsdMetrics),
    {noreply, ensure_timer(State), hibernate};
handle_info({'EXIT', Pid, Error}, State = #{estatsd_pid := Pid}) ->
    {stop, {shutdown, Error}, State};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #{estatsd_pid := Pid}) ->
    estatsd:stop(Pid),
    ok.

%%------------------------------------------------------------------------------
%% Internal function
%%------------------------------------------------------------------------------

flush_interval(FlushInterval, SampleInterval) when FlushInterval >= SampleInterval ->
    FlushInterval;
flush_interval(_FlushInterval, SampleInterval) ->
    ?SLOG(
        warning,
        #{
            msg =>
                "Configured flush_time_interval is lower than sample_time_interval, "
                "setting: flush_time_interval = sample_time_interval."
        }
    ),
    SampleInterval.

ensure_timer(State = #{sample_time_interval := SampleTimeInterval}) ->
    State#{timer => emqx_utils:start_timer(SampleTimeInterval, ?SAMPLE_TIMEOUT)}.

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

to_bin(B) when is_binary(B) -> B;
to_bin(I) when is_integer(I) -> integer_to_binary(I);
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
