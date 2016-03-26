%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc VM System Monitor
-module(emqttd_sysmon).

-behavior(gen_server).

-include("emqttd_internal.hrl").

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tickref, events = [], tracelog}).

-define(LOG_FMT, [{formatter_config, [time, " ", message, "\n"]}]).

-define(LOG(Msg, ProcInfo),
        lager:warning([{sysmon, true}], "~s~n~p", [WarnMsg, ProcInfo])).

-define(LOG(Msg, ProcInfo, PortInfo),
        lager:warning([{sysmon, true}], "~s~n~p~n~p", [WarnMsg, ProcInfo, PortInfo])).

%% @doc Start system monitor
-spec(start_link(Opts :: list(tuple())) ->
      {ok, pid()} | ignore | {error, term()}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    erlang:system_monitor(self(), parse_opt(Opts)),
    {ok, TRef} = timer:send_interval(timer:seconds(1), reset),
    %%TODO: don't trace for performance issue.
    %%{ok, TraceLog} = start_tracelog(proplists:get_value(logfile, Opts)),
    {ok, #state{tickref = TRef}}.

parse_opt(Opts) ->
    parse_opt(Opts, []).
parse_opt([], Acc) ->
    Acc;
parse_opt([{long_gc, false}|Opts], Acc) ->
    parse_opt(Opts, Acc);
parse_opt([{long_gc, Ms}|Opts], Acc) when is_integer(Ms) ->
    parse_opt(Opts, [{long_gc, Ms}|Acc]);
parse_opt([{long_schedule, false}|Opts], Acc) ->
    parse_opt(Opts, Acc);
parse_opt([{long_schedule, Ms}|Opts], Acc) when is_integer(Ms) ->
    parse_opt(Opts, [{long_schedule, Ms}|Acc]);
parse_opt([{large_heap, Size}|Opts], Acc) when is_integer(Size) ->
    parse_opt(Opts, [{large_heap, Size}|Acc]);
parse_opt([{busy_port, true}|Opts], Acc) ->
    parse_opt(Opts, [busy_port|Acc]);
parse_opt([{busy_port, false}|Opts], Acc) ->
    parse_opt(Opts, Acc);
parse_opt([{busy_dist_port, true}|Opts], Acc) ->
    parse_opt(Opts, [busy_dist_port|Acc]);
parse_opt([{busy_dist_port, false}|Opts], Acc) ->
    parse_opt(Opts, Acc);
parse_opt([_Opt|Opts], Acc) ->
    parse_opt(Opts, Acc).

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({monitor, Pid, long_gc, Info}, State) ->
    suppress({long_gc, Pid}, fun() ->
            WarnMsg = io_lib:format("long_gc warning: pid = ~p, info: ~p", [Pid, Info]),
            ?LOG(WarnMsg, procinfo(Pid)),
            publish(long_gc, WarnMsg)
        end, State);

handle_info({monitor, Pid, long_schedule, Info}, State) when is_pid(Pid) ->
    suppress({long_schedule, Pid}, fun() ->
            WarnMsg = io_lib:format("long_schedule warning: pid = ~p, info: ~p", [Pid, Info]),
            ?LOG(WarnMsg, procinfo(Pid)),
            publish(long_schedule, WarnMsg)
        end, State);

handle_info({monitor, Port, long_schedule, Info}, State) when is_port(Port) ->
    suppress({long_schedule, Port}, fun() ->
        WarnMsg  = io_lib:format("long_schedule warning: port = ~p, info: ~p", [Port, Info]),
        ?LOG(WarnMsg, erlang:port_info(Port)),
        publish(long_schedule, WarnMsg)
    end, State);

handle_info({monitor, Pid, large_heap, Info}, State) ->
    suppress({large_heap, Pid}, fun() ->
        WarnMsg = io_lib:format("large_heap warning: pid = ~p, info: ~p", [Pid, Info]),
        ?LOG(WarnMsg, procinfo(Pid)),
        publish(large_heap, WarnMsg)
    end, State);

handle_info({monitor, SusPid, busy_port, Port}, State) ->
    suppress({busy_port, Port}, fun() ->
        WarnMsg = io_lib:format("busy_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
        ?LOG(WarnMsg, procinfo(SusPid), erlang:port_info(Port)),
        publish(busy_port, WarnMsg)
    end, State);

handle_info({monitor, SusPid, busy_dist_port, Port}, State) ->
    suppress({busy_dist_port, Port}, fun() ->
        WarnMsg = io_lib:format("busy_dist_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
        ?LOG(WarnMsg, procinfo(SusPid), erlang:port_info(Port)),
        publish(busy_dist_port, WarnMsg)
    end, State);

handle_info(reset, State) ->
    {noreply, State#state{events = []}, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{tickref = TRef, tracelog = TraceLog}) ->
    timer:cancel(TRef),
    cancel_tracelog(TraceLog).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

suppress(Key, SuccFun, State = #state{events = Events}) ->
    case lists:member(Key, Events) of
        true  ->
            {noreply, State};
        false ->
            SuccFun(),
            {noreply, State#state{events = [Key|Events]}}
    end.

procinfo(Pid) ->
    case {emqttd_vm:get_process_info(Pid), emqttd_vm:get_process_gc(Pid)} of
        {undefined, _} -> undefined;
        {_, undefined} -> undefined;
        {Info, GcInfo} -> Info ++ GcInfo
    end.

publish(Sysmon, WarnMsg) ->
    Msg = emqttd_message:make(sysmon, topic(Sysmon), iolist_to_binary(WarnMsg)),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)).

topic(Sysmon) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['sysmon/', Sysmon]))).

start_tracelog(undefined) ->
    {ok, undefined};
start_tracelog(LogFile) ->
    lager:trace_file(LogFile, [{sysmon, true}], info, ?LOG_FMT).

cancel_tracelog(undefined) ->
    ok;
cancel_tracelog(TraceLog) ->
    lager:stop_trace(TraceLog).

