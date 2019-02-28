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

-export([load/0, get_alarms/0]).

-record(common_alarm, {id, desc}).
-record(alarm_history, {id, clear_at}).

-define(ALARMS_TAB, emqx_alarms).
-define(ALARM_HISTORY_TAB, emqx_alarm_history).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------

load() ->
    gen_event:swap_handler(alarm_handler, {alarm_handler, swap}, {?MODULE, []}).

get_alarms() ->
    gen_event:call(alarm_handler, ?MODULE, get_alarms).

%%----------------------------------------------------------------------
%% gen_event callbacks
%%----------------------------------------------------------------------

init({_Args, {alarm_handler, Alarms}}) ->
    create_tables(),
    lists:foreach(fun({Id, _Desc}) ->
                      set_alarm_history(Id)
                  end, Alarms),
    {ok, []};
init(_) ->
    create_tables(),
    {ok, []}.

handle_event({set_alarm, {AlarmId, AlarmDesc = #alarm{timestamp = undefined}}}, State) ->
    handle_event({set_alarm, {AlarmId, AlarmDesc#alarm{timestamp = os:timestamp()}}}, State);
handle_event({set_alarm, Alarm = {AlarmId, AlarmDesc}}, State) ->
    ?LOG(notice, "Alarm report: set ~p", [Alarm]),
    case encode_alarm(Alarm) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(topic(alert, maybe_to_binary(AlarmId)), Json));
        {error, Reason} ->
            ?LOG(error, "Failed to encode alarm: ~p", [Reason])
    end,
    set_alarm_(AlarmId, AlarmDesc),
    {ok, State};
handle_event({clear_alarm, AlarmId}, State) ->
    ?LOG(notice, "Alarm report: clear ~p", [AlarmId]),
    emqx_broker:safe_publish(alarm_msg(topic(clear, maybe_to_binary(AlarmId)), <<"">>)),
    clear_alarm_(AlarmId),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_info(_, State) -> {ok, State}.

handle_call(get_alarms, State) ->
    {ok, get_alarms_(), State};
handle_call(_Query, State)     -> {ok, {error, bad_query}, State}.

terminate(swap, _State) ->
    {emqx_alarm_handler, get_alarms_()};
terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_tables() ->
    ok = ekka_mnesia:create_table(?ALARMS_TAB, [
                {type, set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, common_alarm},
                {attributes, record_info(fields, common_alarm)}]),
    ok = ekka_mnesia:create_table(?ALARM_HISTORY_TAB, [
                {type, set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, alarm_history},
                {attributes, record_info(fields, alarm_history)}]).

encode_alarm({AlarmId, #alarm{severity  = Severity, 
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

set_alarm_(Id, Desc) ->
    ok = mnesia:dirty_write(?ALARMS_TAB, #common_alarm{id = Id, desc = Desc}).

clear_alarm_(Id) ->
    ok = mnesia:dirty_delete(?ALARMS_TAB, Id),
    set_alarm_history(Id).

get_alarms_() ->
    Alarms = ets:tab2list(?ALARMS_TAB),
    lists:foldr(fun(#common_alarm{id = Id, desc = Desc}, Acc) -> 
                    Acc ++ [{Id, Desc}];
                   (_, Acc) -> Acc
                end, [], Alarms).

set_alarm_history(Id) ->
    ok = mnesia:dirty_write(?ALARM_HISTORY_TAB, #alarm_history{id = Id,
                                                               clear_at = undefined}).


