%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tracer).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).
-export([trace/2]).
-export([start_trace/2, lookup_traces/0, stop_trace/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {level, traces}).

-type(trace_who() :: {client | topic, binary()}).

-define(TRACER, ?MODULE).
-define(OPTIONS, [{formatter_config, [time, " [",severity,"] ", message, "\n"]}]).

-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?TRACER}, ?MODULE, [], []).

trace(publish, #message{topic = <<"$SYS/", _/binary>>}) ->
    %% Dont' trace '$SYS' publish
    ignore;
trace(publish, #message{from = From, topic = Topic, payload = Payload})
    when is_binary(From); is_atom(From) ->
    emqx_logger:info(#{topic => Topic}, "PUBLISH to ~s: ~p", [Topic, Payload]).

%%------------------------------------------------------------------------------
%% Start/Stop trace
%%------------------------------------------------------------------------------

%% @doc Start to trace client or topic.
-spec(start_trace(trace_who(), string()) -> ok | {error, term()}).
start_trace({client, ClientId}, LogFile) ->
    start_trace({start_trace, {client, ClientId}, LogFile});
start_trace({topic, Topic}, LogFile) ->
    start_trace({start_trace, {topic, Topic}, LogFile}).

start_trace(Req) -> gen_server:call(?MODULE, Req, infinity).

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

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, #state{level = emqx_config:get_env(trace_level, debug), traces = #{}}}.

handle_call({start_trace, Who, LogFile}, _From, State = #state{level = Level, traces = Traces}) ->
    case catch logger:trace_file(LogFile, [Who], Level, ?OPTIONS) of
        {ok, exists} ->
            {reply, {error, already_exists}, State};
        {ok, Trace} ->
            {reply, ok, State#state{traces = maps:put(Who, {Trace, LogFile}, Traces)}};
        {error, Reason} ->
            emqx_logger:error("[Tracer] trace error: ~p", [Reason]),
            {reply, {error, Reason}, State};
        {'EXIT', Error} ->
            emqx_logger:error("[Tracer] trace exit: ~p", [Error]),
            {reply, {error, Error}, State}
    end;

handle_call({stop_trace, Who}, _From, State = #state{traces = Traces}) ->
    case maps:find(Who, Traces) of
        {ok, {Trace, _LogFile}} ->
            case logger:stop_trace(Trace) of
                ok -> ok;
                {error, Error} ->
                    emqx_logger:error("[Tracer] stop trace ~p error: ~p", [Who, Error])
            end,
            {reply, ok, State#state{traces = maps:remove(Who, Traces)}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(lookup_traces, _From, State = #state{traces = Traces}) ->
    {reply, [{Who, LogFile} || {Who, {_Trace, LogFile}} <- maps:to_list(Traces)], State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[Tracer] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Tracer] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[Tracer] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

