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

-module(emqx_alarm_handler).

-behaviour(gen_event).

-include("emqx.hrl").
-include("logger.hrl").

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2]).

-export([get_alarms/0]).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------

get_alarms() ->
    gen_event:call(alarm_handler, ?MODULE, get_alarms).

%%----------------------------------------------------------------------
%% gen_event callbacks
%%----------------------------------------------------------------------

init({_Args, {alarm_handler, Alarms}}) ->
    {ok, Alarms};
init(_) ->
    {ok, []}.

handle_event({set_alarm, {AlarmId, AlarmDesc = #alarm_desc{timestamp = undefined}}}, Alarms) ->
    handle_event({set_alarm, {AlarmId, AlarmDesc#alarm_desc{timestamp = os:timestamp()}}}, Alarms);
handle_event({set_alarm, Alarm = {AlarmId, _AlarmDesc}}, Alarms) ->
    ?LOG(notice, "Alarm report: set ~p", [Alarm]),
    case encode_alarm(Alarm) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(topic(alert, maybe_to_binary(AlarmId)), Json));
        {error, Reason} ->
            ?LOG(error, "Failed to encode alarm: ~p", [Reason])
    end,
    {ok, [Alarm | Alarms]};
handle_event({clear_alarm, AlarmId}, Alarms) ->
    ?LOG(notice, "Alarm report: clear ~p", [AlarmId]),
    emqx_broker:safe_publish(alarm_msg(topic(clear, maybe_to_binary(AlarmId)), <<"">>)),
    {ok, lists:keydelete(AlarmId, 1, Alarms)};
handle_event(_, Alarms) ->
    {ok, Alarms}.

handle_info(_, Alarms) -> {ok, Alarms}.

handle_call(get_alarms, Alarms) -> {ok, Alarms, Alarms};
handle_call(_Query, Alarms)     -> {ok, {error, bad_query}, Alarms}.

terminate(swap, Alarms) ->
    {emqx_alarm_handler, Alarms};
terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

encode_alarm({AlarmId, #alarm_desc{severity  = Severity, 
                                   title     = Title,
                                   summary   = Summary, 
                                   timestamp = Ts}}) ->
    emqx_json:safe_encode([{id, maybe_to_binary(AlarmId)},
                           {desc, [{severity, Severity},
                                   {title, iolist_to_binary(Title)},
                                   {summary, iolist_to_binary(Summary)},
                                   {ts, emqx_time:now_secs(Ts)}]}]);
encode_alarm({AlarmId, AlarmDesc}) ->
    emqx_json:safe_encode([{id, maybe_to_binary(AlarmId)}, 
                           {desc, maybe_to_binary(AlarmDesc)}]).

alarm_msg(Topic, Payload) ->
    Msg = emqx_message:make(?MODULE, Topic, Payload),
    emqx_message:set_headers(#{'Content-Type' => <<"application/json">>},
                             emqx_message:set_flag(sys, Msg)).

topic(alert, AlarmId) ->
    emqx_topic:systop(<<"alarms/", AlarmId/binary, "/alert">>);
topic(clear, AlarmId) ->
    emqx_topic:systop(<<"alarms/", AlarmId/binary, "/clear">>).

maybe_to_binary(Data) when is_binary(Data) ->
    Data;
maybe_to_binary(Data) ->
    iolist_to_binary(io_lib:format("~p", [Data])).
