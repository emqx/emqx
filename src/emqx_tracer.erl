%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_tracer).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).

-export([trace/3]).

-export([start_trace/2, stop_trace/1, all_traces/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {level, traces}).

-type(trace_who() :: {client | topic, binary()}).

-define(OPTIONS, [{formatter_config, [time, " [",severity,"] ", message, "\n"]}]).

%%--------------------------------------------------------------------
%% Start the tracer
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Trace
%%--------------------------------------------------------------------

trace(publish, From, _Msg) when is_atom(From) ->
    %% Dont' trace '$SYS' publish
    ignore;
trace(publish, #client{id = ClientId, username = Username},
      #message{topic = Topic, payload = Payload}) ->
    lager:info([{client, ClientId}, {topic, Topic}],
               "~s/~s PUBLISH to ~s: ~p", [ClientId, Username, Topic, Payload]);
trace(publish, From, #message{topic = Topic, payload = Payload})
    when is_binary(From); is_list(From) ->
    lager:info([{client, From}, {topic, Topic}],
               "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

%%--------------------------------------------------------------------
%% Start/Stop Trace
%%--------------------------------------------------------------------

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

%% @doc Lookup all traces.
-spec(all_traces() -> [{Who :: trace_who(), LogFile :: string()}]).
all_traces() -> gen_server:call(?MODULE, all_traces).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #state{level = emqx:env(trace_level, debug), traces = #{}}}.

handle_call({start_trace, Who, LogFile}, _From, State = #state{level = Level, traces = Traces}) ->
    case lager:trace_file(LogFile, [Who], Level, ?OPTIONS) of
        {ok, exists} ->
            {reply, {error, existed}, State};
        {ok, Trace}  ->
            {reply, ok, State#state{traces = maps:put(Who, {Trace, LogFile}, Traces)}};
        {error, Error} ->
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
            {reply, {error, not_found}, State}
    end;

handle_call(all_traces, _From, State = #state{traces = Traces}) ->
    {reply, [{Who, LogFile} || {Who, {_Trace, LogFile}}
                               <- maps:to_list(Traces)], State};

handle_call(Req, _From, State) ->
    emqx_log:error("[TRACE] Unexpected Call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_log:error("[TRACE] Unexpected Cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_log:error("[TRACE] Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

