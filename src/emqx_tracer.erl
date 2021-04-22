%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Tracer]").

%% APIs
-export([ trace/2
        , start_trace/3
        , lookup_traces/0
        , stop_trace/1
        ]).

-type(trace_who() :: {clientid | topic, binary()}).

-define(TRACER, ?MODULE).
-define(FORMAT, {logger_formatter,
                  #{template =>
                      [time, " [", level, "] ",
                       {clientid,
                          [{peername,
                              [clientid, "@", peername, " "],
                              [clientid, " "]}],
                          [{peername,
                              [peername, " "],
                              []}]},
                       msg, "\n"],
                    single_line => true
                   }}).
-define(TOPIC_TRACE_ID(T), "trace_topic_"++T).
-define(CLIENT_TRACE_ID(C), "trace_clientid_"++C).
-define(TOPIC_TRACE(T), {topic, T}).
-define(CLIENT_TRACE(C), {clientid, C}).

-define(IS_LOG_LEVEL(L),
        L =:= emergency orelse
        L =:= alert orelse
        L =:= critical orelse
        L =:= error orelse
        L =:= warning orelse
        L =:= notice orelse
        L =:= info orelse
        L =:= debug).

-dialyzer({nowarn_function, [install_trace_handler/3]}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
trace(publish, #message{topic = <<"$SYS/", _/binary>>}) ->
    %% Do not trace '$SYS' publish
    ignore;
trace(publish, #message{from = From, topic = Topic, payload = Payload})
        when is_binary(From); is_atom(From) ->
    emqx_logger:info(#{topic => Topic,
                       mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY} },
                     "PUBLISH to ~s: ~0p", [Topic, Payload]).

%% @doc Start to trace clientid or topic.
-spec(start_trace(trace_who(), logger:level() | all, string()) -> ok | {error, term()}).
start_trace(Who, all, LogFile) ->
    start_trace(Who, debug, LogFile);
start_trace(Who, Level, LogFile) ->
    case ?IS_LOG_LEVEL(Level) of
        true ->
            #{level := PrimaryLevel} = logger:get_primary_config(),
            try logger:compare_levels(Level, PrimaryLevel) of
                lt ->
                    {error,
                     io_lib:format("Cannot trace at a log level (~s) "
                                   "lower than the primary log level (~s)",
                                   [Level, PrimaryLevel])};
                _GtOrEq ->
                    install_trace_handler(Who, Level, LogFile)
            catch
                _:Error ->
                {error, Error}
            end;
        false -> {error, {invalid_log_level, Level}}
    end.

%% @doc Stop tracing clientid or topic.
-spec(stop_trace(trace_who()) -> ok | {error, term()}).
stop_trace(Who) ->
    uninstall_trance_handler(Who).

%% @doc Lookup all traces
-spec(lookup_traces() -> [{Who :: trace_who(), LogFile :: string()}]).
lookup_traces() ->
    lists:foldl(fun filter_traces/2, [], emqx_logger:get_log_handlers(started)).

install_trace_handler(Who, Level, LogFile) ->
    case logger:add_handler(handler_id(Who), logger_disk_log_h,
                            #{level => Level,
                              formatter => ?FORMAT,
                              config => #{type => halt, file => LogFile},
                              filter_default => stop,
                              filters => [{meta_key_filter,
                                          {fun filter_by_meta_key/2, Who}}]})
    of
        ok ->
            ?LOG(info, "Start trace for ~p", [Who]);
        {error, Reason} ->
            ?LOG(error, "Start trace for ~p failed, error: ~p", [Who, Reason]),
            {error, Reason}
    end.

uninstall_trance_handler(Who) ->
    case logger:remove_handler(handler_id(Who)) of
        ok ->
            ?LOG(info, "Stop trace for ~p", [Who]);
        {error, Reason} ->
            ?LOG(error, "Stop trace for ~p failed, error: ~p", [Who, Reason]),
            {error, Reason}
    end.

filter_traces(#{id := Id, level := Level, dst := Dst}, Acc) ->
    case atom_to_list(Id) of
        ?TOPIC_TRACE_ID(T)->
            [{?TOPIC_TRACE(T), {Level, Dst}} | Acc];
        ?CLIENT_TRACE_ID(C) ->
            [{?CLIENT_TRACE(C), {Level, Dst}} | Acc];
        _ -> Acc
    end.

handler_id(?TOPIC_TRACE(Topic)) ->
    list_to_atom(?TOPIC_TRACE_ID(handler_name(Topic)));
handler_id(?CLIENT_TRACE(ClientId)) ->
    list_to_atom(?CLIENT_TRACE_ID(handler_name(ClientId))).

filter_by_meta_key(#{meta := Meta} = Log, {Key, Value}) ->
    case is_meta_match(Key, Value, Meta) of
        true -> Log;
        false -> ignore
    end.

is_meta_match(clientid, ClientId, #{clientid := ClientIdStr}) ->
    ClientId =:= iolist_to_binary(ClientIdStr);
is_meta_match(topic, TopicFilter, #{topic := TopicMeta}) ->
    emqx_topic:match(TopicMeta, TopicFilter);
is_meta_match(_, _, _) ->
    false.

handler_name(Bin) ->
    case byte_size(Bin) of
        Size when Size =< 200 -> binary_to_list(Bin);
        _ -> hashstr(Bin)
    end.

hashstr(Bin) ->
    binary_to_list(emqx_misc:bin2hexstr_A_F(Bin)).
