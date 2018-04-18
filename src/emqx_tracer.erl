%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_tracer).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).

-export([start_trace/2, lookup_traces/0, stop_trace/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {level, traces}).

-type(trace_who() :: {client | topic, binary()}).

-define(OPTIONS, [{formatter_config, [time, " [",severity,"] ", message, "\n"]}]).

-define(TRACER, ?MODULE).

%%--------------------------------------------------------------------
%% Start the tracer
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?TRACER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Start/Stop Trace
%%--------------------------------------------------------------------

%% @doc Start to trace client or topic.
-spec(start_trace(trace_who(), string()) -> ok | {error, term()}).
start_trace({client, ClientId}, LogFile) ->
    start_trace({start_trace, {client, ClientId}, LogFile});
start_trace({topic, Topic}, LogFile) ->
    start_trace({start_trace, {topic, Topic}, LogFile}).

start_trace(Req) ->
    gen_server:call(?MODULE, Req, infinity).

%% @doc Stop tracing client or topic.
-spec(stop_trace(trace_who()) -> ok | {error, term()}).
stop_trace({client, ClientId}) ->
    gen_server:call(?MODULE, {stop_trace, {client, ClientId}});
stop_trace({topic, Topic}) ->
    gen_server:call(?MODULE, {stop_trace, {topic, Topic}}).

%% @doc Lookup all traces
-spec(lookup_traces() -> [{Who :: trace_who(), LogFile :: string()}]).
lookup_traces() ->
    gen_server:call(?TRACER, lookup_traces).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Level = emqx_config:get_env(trace_level, debug),
    {ok, #state{level = Level, traces = #{}}}.

handle_call({start_trace, Who, LogFile}, _From,
            State = #state{level = Level, traces = Traces}) ->
    case catch lager:trace_file(LogFile, [Who], Level, ?OPTIONS) of
        {ok, exists} ->
            {reply, {error, alread_existed}, State};
        {ok, Trace} ->
            {reply, ok, State#state{traces = maps:put(Who, {Trace, LogFile}, Traces)}};
        {error, Reason} ->
            emqx_logger:error("[TRACER] trace error: ~p", [Reason]),
            {reply, {error, Reason}, State};
        {'EXIT', Error} ->
            emqx_logger:error("[TRACER] trace exit: ~p", [Error]),
            {reply, {error, Error}, State}
    end;

handle_call({stop_trace, Who}, _From, State = #state{traces = Traces}) ->
    case maps:find(Who, Traces) of
        {ok, {Trace, _LogFile}} ->
            case lager:stop_trace(Trace) of
                ok -> ok;
                {error, Error} -> lager:error("Stop trace ~p error: ~p", [Who, Error])
            end,
            {reply, ok, State#state{traces = maps:remove(Who, Traces)}};
        error ->
            {reply, {error, trance_not_found}, State}
    end;

handle_call(lookup_traces, _From, State = #state{traces = Traces}) ->
    {reply, [{Who, LogFile} || {Who, {_Trace, LogFile}}
                               <- maps:to_list(Traces)], State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[TRACER] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[TRACER] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[TRACER] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

