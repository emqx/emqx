%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc emqttd system monitor
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_sysmon).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tick_tref, events = []}).

%%------------------------------------------------------------------------------
%% @doc Start system monitor
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Opts :: list(tuple())) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Opts]) ->
    erlang:system_monitor(self(), parse_opt(Opts)),
    {ok, TRef} = timer:send_interval(timer:seconds(1), reset),
    {ok, #state{tick_tref = TRef}}.

parse_opt(Opts) ->
    parse_opt(Opts, []).
parse_opt([], Acc) ->
    Acc;
parse_opt([{long_gc, false}|Opts], Acc) ->
    parse_opt(Opts, Acc);
parse_opt([{long_gc, Ms}|Opts], Acc) when is_integer(Ms) ->
    parse_opt(Opts, [{long_gc, Ms}|Acc]);
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
    parse_opt(Opts, Acc).

handle_call(Request, _From, State) ->
    lager:error("Unexpected request: ~p", [Request]),
    {reply, {error, unexpected_request}, State}.

handle_cast(Msg, State) ->
    lager:error("Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({monitor, Pid, long_gc, Info}, State) ->
    suppress({long_gc, Pid}, fun() ->
            WarnMsg = io_lib:format("long_gc: pid = ~p, info: ~p", [Pid, Info]),
            lager:error("~s~n~p", [WarnMsg, procinfo(Pid)]),
            publish(long_gc, WarnMsg)
        end, State);

handle_info({monitor, Pid, long_schedule, Info}, State) when is_pid(Pid) ->
    suppress({long_schedule, Pid}, fun() ->
            WarnMsg = io_lib:format("long_schedule warning: pid = ~p, info: ~p", [Pid, Info]),
            lager:error("~s~n~p", [WarnMsg, procinfo(Pid)]),
            publish(long_schedule, WarnMsg)
        end, State);

handle_info({monitor, Port, long_schedule, Info}, State) when is_port(Port) ->
    suppress({long_schedule, Port}, fun() ->
        WarnMsg  = io_lib:format("long_schedule warning: port = ~p, info: ~p", [Port, Info]),
        lager:error("~s~n~p", [WarnMsg, erlang:port_info(Port)]),
        publish(long_schedule, WarnMsg)
    end, State);

handle_info({monitor, Pid, large_heap, Info}, State) ->
    suppress({large_heap, Pid}, fun() ->
        WarnMsg = io_lib:format("large_heap warning: pid = ~p, info: ~p", [Pid, Info]),
        lager:error("~s~n~p", [WarnMsg, procinfo(Pid)]),
        publish(large_heap, WarnMsg)
    end, State);

handle_info({monitor, SusPid, busy_port, Port}, State) ->
    suppress({busy_port, Port}, fun() ->
        WarnMsg = io_lib:format("busy_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
        lager:error("~s~n~p~n~p", [WarnMsg, procinfo(SusPid), erlang:port_info(Port)]),
        publish(busy_port, WarnMsg)
    end, State);

handle_info({monitor, SusPid, busy_dist_port, Port}, State) ->
    suppress({busy_dist_port, Port}, fun() ->
        WarnMsg = io_lib:format("busy_dist_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
        lager:error("~s~n~p~n~p", [WarnMsg, procinfo(SusPid), erlang:port_info(Port)]),
        publish(busy_dist_port, WarnMsg)
    end, State);

handle_info(reset, State) ->
    {noreply, State#state{events = []}};

handle_info(Info, State) ->
    lager:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{tick_tref = TRef}) ->
    timer:cancel(TRef).

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
    emqttd_pubsub:publish(emqttd_message:set_flag(sys, Msg)).

topic(Sysmon) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['sysmon/', Sysmon]))).

