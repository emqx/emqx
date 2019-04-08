%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

%% APIs
-export([start_link/0]).

-export([ trace/2
        , start_trace/3
        , lookup_traces/0
        , stop_trace/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {traces}).

-type(trace_who() :: {client_id | topic, binary()}).

-define(TRACER, ?MODULE).
-define(FORMAT, {emqx_logger_formatter,
                  #{template =>
                      [time," [",level,"] ",
                       {client_id,
                          [{peername,
                              [client_id,"@",peername," "],
                              [client_id, " "]}],
                          [{peername,
                              [peername," "],
                              []}]},
                       msg,"\n"]}}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

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

%% @doc Start to trace client_id or topic.
-spec(start_trace(trace_who(), logger:level(), string()) -> ok | {error, term()}).
start_trace({client_id, ClientId}, Level, LogFile) ->
    do_start_trace({client_id, ClientId}, Level, LogFile);
start_trace({topic, Topic}, Level, LogFile) ->
    do_start_trace({topic, Topic}, Level, LogFile).

do_start_trace(Who, Level, LogFile) ->
    #{level := PrimaryLevel} = logger:get_primary_config(),
    try logger:compare_levels(log_level(Level), PrimaryLevel) of
        lt ->
            {error, io_lib:format("Cannot trace at a log level (~s) lower than the primary log level (~s)", [Level, PrimaryLevel])};
        _GtOrEq ->
            gen_server:call(?MODULE, {start_trace, Who, Level, LogFile}, 5000)
    catch
        _:Error ->
           {error, Error}
    end.

%% @doc Stop tracing client_id or topic.
-spec(stop_trace(trace_who()) -> ok | {error, term()}).
stop_trace({client_id, ClientId}) ->
    gen_server:call(?MODULE, {stop_trace, {client_id, ClientId}});
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
    {ok, #state{traces = #{}}}.

handle_call({start_trace, Who, Level, LogFile}, _From, State = #state{traces = Traces}) ->
    case logger:add_handler(handler_id(Who), logger_disk_log_h,
                                #{level => Level,
                                  formatter => ?FORMAT,
                                  filesync_repeat_interval => no_repeat,
                                  config => #{type => halt, file => LogFile},
                                  filter_default => stop,
                                  filters => [{meta_key_filter,
                                              {fun filter_by_meta_key/2, Who} }]}) of
        ok ->
            ?LOG(info, "[Tracer] Start trace for ~p", [Who]),
            {reply, ok, State#state{traces = maps:put(Who, {Level, LogFile}, Traces)}};
        {error, Reason} ->
            ?LOG(error, "[Tracer] Start trace for ~p failed, error: ~p", [Who, Reason]),
            {reply, {error, Reason}, State}
    end;

handle_call({stop_trace, Who}, _From, State = #state{traces = Traces}) ->
    case maps:find(Who, Traces) of
        {ok, _LogFile} ->
            case logger:remove_handler(handler_id(Who)) of
                ok ->
                    ?LOG(info, "[Tracer] Stop trace for ~p", [Who]);
                {error, Reason} ->
                    ?LOG(error, "[Tracer] Stop trace for ~p failed, error: ~p", [Who, Reason])
            end,
            {reply, ok, State#state{traces = maps:remove(Who, Traces)}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(lookup_traces, _From, State = #state{traces = Traces}) ->
    {reply, [{Who, LogFile} || {Who, LogFile} <- maps:to_list(Traces)], State};

handle_call(Req, _From, State) ->
    ?LOG(error, "[Tracer] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Tracer] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "[Tracer] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handler_id({topic, Topic}) ->
    list_to_atom("topic_" ++ binary_to_list(Topic));
handler_id({client_id, ClientId}) ->
    list_to_atom("clientid_" ++ binary_to_list(ClientId)).

filter_by_meta_key(#{meta:=Meta}=LogEvent, {MetaKey, MetaValue}) ->
    case maps:find(MetaKey, Meta) of
        {ok, MetaValue} -> LogEvent;
        {ok, Topic} when MetaKey =:= topic ->
            case emqx_topic:match(Topic, MetaValue) of
                true -> LogEvent;
                false -> ignore
            end;
        _ -> ignore
    end.

log_level(emergency) -> emergency;
log_level(alert) -> alert;
log_level(critical) -> critical;
log_level(error) -> error;
log_level(warning) -> warning;
log_level(notice) -> notice;
log_level(info) -> info;
log_level(debug) -> debug;
log_level(all) -> debug;
log_level(_) -> throw(invalid_log_level).
