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
%%% @doc
%%% emqttd system monitor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

%%TODO: this is a demo module....

-module(emqttd_sysmon).

-author("Feng Lee <feng@emqtt.io>").

-behavior(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%%------------------------------------------------------------------------------
%% @doc Start system monitor
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    erlang:system_monitor(self(), [{long_gc, 5000},
                                   {large_heap, 8 * 1024 * 1024},
                                   busy_port]),
    {ok, #state{}}.

handle_call(Request, _From, State) ->
    lager:error("Unexpected request: ~p", [Request]),
    {reply, {error, unexpected_request}, State}.

handle_cast(Msg, State) ->
    lager:error("unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({monitor, GcPid, long_gc, Info}, State) ->
    lager:error("long_gc: gcpid = ~p, ~p ~n ~p", [GcPid, process_info(GcPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), Info]),
    {noreply, State};

handle_info({monitor, GcPid, large_heap, Info}, State) ->
    lager:error("large_heap: gcpid = ~p,~p ~n ~p", [GcPid, process_info(GcPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), Info]),
    {noreply, State};

handle_info({monitor, SusPid, busy_port, Port}, State) ->
    lager:error("busy_port: suspid = ~p, port = ~p", [process_info(SusPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), Port]),
    {noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

