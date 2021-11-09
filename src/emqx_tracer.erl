%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([ trace_publish/1
        , trace_subscribe/3
        , trace_unsubscribe/2
        , start_trace/3
        , start_trace/4
        , lookup_traces/0
        , stop_trace/3
        , stop_trace/2
        ]).

-ifdef(TEST).
-export([is_match/3]).
-endif.

-type(label() :: 'CONNECT' | 'CONNACK' | 'PUBLISH' | 'PUBACK' | 'PUBREC' |
                 'PUBREL' | 'PUBCOMP' | 'SUBSCRIBE' | 'SUBACK' | 'UNSUBSCRIBE' |
                 'UNSUBACK' | 'PINGREQ' | 'PINGRESP' | 'DISCONNECT' | 'AUTH').

-type(tracer() :: #{name := binary(),
                    type := clientid | topic,
                    clientid => emqx_types:clientid(),
                    topic => emqx_types:topic(),
                    labels := [label()]}).

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
                    single_line => false
                   }}).
-define(TOPIC_COMBINATOR, <<"_trace_topic_">>).
-define(CLIENTID_COMBINATOR, <<"_trace_clientid_">>).
-define(TOPIC_TRACE_ID(T, N),
    binary_to_atom(<<(N)/binary, ?TOPIC_COMBINATOR/binary, (T)/binary>>)).
-define(CLIENT_TRACE_ID(C, N),
    binary_to_atom(<<(N)/binary, ?CLIENTID_COMBINATOR/binary, (C)/binary>>)).
-define(TOPIC_TRACE(T, N, M), {topic, T, N, M}).
-define(CLIENT_TRACE(C, N, M), {clientid, C, N, M}).
-define(TOPIC_TRACE(T, N), {topic, T, N}).
-define(CLIENT_TRACE(C, N), {clientid, C, N}).

-define(IS_LOG_LEVEL(L),
        L =:= emergency orelse
        L =:= alert orelse
        L =:= critical orelse
        L =:= error orelse
        L =:= warning orelse
        L =:= notice orelse
        L =:= info orelse
        L =:= debug).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
trace_publish(#message{topic = <<"$SYS/", _/binary>>}) ->
    %% Do not trace '$SYS' publish
    ignore;
trace_publish(#message{from = From, topic = Topic, payload = Payload})
        when is_binary(From); is_atom(From) ->
    emqx_logger:info(#{topic => Topic,
                       mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY} },
                     "PUBLISH to ~s: ~0p", [Topic, Payload]).

trace_subscribe(<<"$SYS/", _/binary>>, _SubId, _SubOpts) -> ignore;
trace_subscribe(Topic, SubId, SubOpts) ->
    emqx_logger:info(#{topic => Topic,
        mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}},
        "~s SUBSCRIBE ~s: Options: ~0p", [SubId, Topic, SubOpts]).

trace_unsubscribe(<<"$SYS/", _/binary>>, _SubOpts) -> ignore;
trace_unsubscribe(Topic, SubOpts) ->
    emqx_logger:info(#{topic => Topic, mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}},
        "~s UNSUBSCRIBE ~s: Options: ~0p",
        [maps:get(subid, SubOpts, ""), Topic, SubOpts]).

-spec(start_trace(clientid | topic, emqx_types:clientid() | emqx_types:topic(),
    logger:level() | all, string()) -> ok | {error, term()}).
start_trace(clientid, ClientId, Level, LogFile) ->
    Who = #{type => clientid, clientid => ClientId, name => ClientId, labels => []},
    start_trace(Who, Level, LogFile);
start_trace(topic, Topic, Level, LogFile) ->
    Who = #{type => topic, topic => Topic, name => Topic, labels => []},
    start_trace(Who, Level, LogFile).

%% @doc Start to trace clientid or topic.
-spec(start_trace(tracer(), logger:level() | all, string()) -> ok | {error, term()}).
start_trace(Who, all, LogFile) ->
    start_trace(Who, debug, LogFile);
start_trace(Who, Level, LogFile) ->
    case ?IS_LOG_LEVEL(Level) of
        true ->
            PrimaryLevel = emqx_logger:get_primary_log_level(),
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

-spec(stop_trace(clientid | topic, emqx_types:clientid() | emqx_types:topic()) ->
    ok | {error, term()}).
stop_trace(Type, ClientIdOrTopic) ->
    stop_trace(Type, ClientIdOrTopic, ClientIdOrTopic).

%% @doc Stop tracing clientid or topic.
-spec(stop_trace(clientid | topic, emqx_types:clientid() | emqx_types:topic(), binary()) ->
    ok | {error, term()}).
stop_trace(clientid, ClientId, Name) ->
    Who = #{type => clientid, clientid => ClientId, name => Name},
    uninstall_trance_handler(Who);
stop_trace(topic, Topic, Name) ->
    Who = #{type => topic, topic => Topic, name => Name},
    uninstall_trance_handler(Who).

%% @doc Lookup all traces
-spec(lookup_traces() -> [#{ type => topic | clientid,
                             name => binary(),
                             topic => emqx_types:topic(),
                             level => logger:level(),
                             dst => file:filename() | console | unknown
                          }]).
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
            ?LOG(info, "Start trace for ~p", [Who]),
            ok;
        {error, Reason} ->
            ?LOG(error, "Start trace for ~p failed, error: ~p", [Who, Reason]),
            {error, Reason}
    end.

uninstall_trance_handler(Who) ->
    case logger:remove_handler(handler_id(Who)) of
        ok ->
            ?LOG(info, "Stop trace for ~p", [Who]),
            ok;
        {error, Reason} ->
            ?LOG(error, "Stop trace for ~p failed, error: ~p", [Who, Reason]),
            {error, Reason}
    end.

filter_traces(#{id := Id, level := Level, dst := Dst}, Acc) ->
    IdStr = atom_to_binary(Id),
    case binary:split(IdStr, [?TOPIC_COMBINATOR]) of
        [Name, Topic] ->
            [#{ type => topic,
                name => Name,
                topic => Topic,
                level => Level,
                dst => Dst} | Acc];
        _ ->
            case binary:split(IdStr, [?CLIENTID_COMBINATOR]) of
                [Name, ClientId] ->
                    [#{ type => clientid,
                        name => Name,
                        clientid => ClientId,
                        level => Level,
                        dst => Dst} | Acc];
                _ -> Acc
            end
    end.

%% Plan to support topic_and_client type, so we have type field.
handler_id(#{type := topic, topic := Topic, name := Name}) ->
    ?TOPIC_TRACE_ID(format(Topic), format(Name));
handler_id(#{type := clientid, clientid := ClientId, name := Name}) ->
    ?CLIENT_TRACE_ID(format(ClientId), format(Name)).

filter_by_meta_key(#{meta := Meta, level := Level} = Log, Context) ->
    case is_match(Context, Meta, Level) of
        true -> Log;
        false -> ignore
    end.

%% When the log level is higher than debug and clientid/topic is match,
%% it will be logged without judging the content inside the labels.
%% When the log level is debug, in addition to the matched clientid/topic,
%% you also need to determine whether the label is in the labels
is_match(#{type := clientid, clientid := ExpectId, labels := Labels},
         #{clientid := RealId} = Meta,
         Level) ->
    is_match(ExpectId =:= iolist_to_binary(RealId), Level, Meta, Labels);
is_match(#{type := topic, topic := TopicFilter, labels := Labels},
         #{topic := Topic} = Meta, Level) ->
    is_match(emqx_topic:match(Topic, TopicFilter), Level, Meta, Labels);
is_match(_, _, _) ->
    false.

is_match(true, debug, Meta, Labels) -> is_match_labels(Meta, Labels);
is_match(Boolean, _, _Meta, _Labels) -> Boolean.

is_match_labels(#{trace_label := 'ALL'}, _Context) -> true;
is_match_labels(_, []) -> true;
is_match_labels(#{trace_label := Packet}, Context) ->
    lists:member(Packet, Context);
is_match_labels(_, _) -> false.

format(List)when is_list(List) ->
    format(list_to_binary(List));
format(Atom)when is_atom(Atom) ->
    format(atom_to_list(Atom));
format(Bin0)when is_binary(Bin0) ->
    case byte_size(Bin0) of
        Size when Size =< 200 -> Bin0;
        _ -> emqx_misc:bin2hexstr_a_f_upper(Bin0)
    end.
