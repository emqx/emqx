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

-module(emqx_alarm_mgr).

-behaviour(gen_event).

-include("emqx.hrl").
-include("logger.hrl").

-export([start_link/0]).
-export([alarm_fun/0, get_alarms/0, set_alarm/1, clear_alarm/1]).
-export([add_alarm_handler/1, add_alarm_handler/2, delete_alarm_handler/1]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
         code_change/3]).

-define(ALARM_MGR, ?MODULE).

start_link() ->
    start_with(
      fun(Pid) ->
          gen_event:add_handler(Pid, ?MODULE, [])
      end).

start_with(Fun) ->
    case gen_event:start_link({local, ?ALARM_MGR}) of
        {ok, Pid} -> Fun(Pid), {ok, Pid};
        Error     -> Error
    end.

alarm_fun() -> alarm_fun(false).

alarm_fun(Bool) ->
    fun(alert, _Alarm)   when Bool =:= true  -> alarm_fun(true);
       (alert,  Alarm)   when Bool =:= false -> set_alarm(Alarm), alarm_fun(true);
       (clear,  AlarmId) when Bool =:= true  -> clear_alarm(AlarmId), alarm_fun(false);
       (clear, _AlarmId) when Bool =:= false -> alarm_fun(false)
    end.

-spec(set_alarm(emqx_types:alarm()) -> ok).
set_alarm(Alarm) when is_record(Alarm, alarm) ->
    gen_event:notify(?ALARM_MGR, {set_alarm, Alarm}).

-spec(clear_alarm(any()) -> ok).
clear_alarm(AlarmId) when is_binary(AlarmId) ->
    gen_event:notify(?ALARM_MGR, {clear_alarm, AlarmId}).

-spec(get_alarms() -> list(emqx_types:alarm())).
get_alarms() ->
    gen_event:call(?ALARM_MGR, ?MODULE, get_alarms).

add_alarm_handler(Module) when is_atom(Module) ->
    gen_event:add_handler(?ALARM_MGR, Module, []).

add_alarm_handler(Module, Args) when is_atom(Module) ->
    gen_event:add_handler(?ALARM_MGR, Module, Args).

delete_alarm_handler(Module) when is_atom(Module) ->
    gen_event:delete_handler(?ALARM_MGR, Module, []).

%%------------------------------------------------------------------------------
%% Default Alarm handler
%%------------------------------------------------------------------------------

init(_) -> {ok, #{alarms => []}}.

handle_event({set_alarm, Alarm = #alarm{timestamp = undefined}}, State)->
    handle_event({set_alarm, Alarm#alarm{timestamp = os:timestamp()}}, State);

handle_event({set_alarm, Alarm = #alarm{id = AlarmId}}, State = #{alarms := Alarms}) ->
    case encode_alarm(Alarm) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(alert, AlarmId, Json));
        {error, Reason} ->
            ?ERROR("[AlarmMgr] Failed to encode alarm: ~p", [Reason])
    end,
    {ok, State#{alarms := [Alarm|Alarms]}};

handle_event({clear_alarm, AlarmId}, State = #{alarms := Alarms}) ->
    case emqx_json:safe_encode([{id, AlarmId}, {ts, os:system_time(second)}]) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(clear, AlarmId, Json));
        {error, Reason} ->
            ?ERROR("[AlarmMgr] Failed to encode clear: ~p", [Reason])
    end,
    {ok, State#{alarms := lists:keydelete(AlarmId, 2, Alarms)}, hibernate};

handle_event(Event, State)->
    ?ERROR("[AlarmMgr] unexpected event: ~p", [Event]),
    {ok, State}.

handle_info(Info, State) ->
    ?ERROR("[AlarmMgr] unexpected info: ~p", [Info]),
    {ok, State}.

handle_call(get_alarms, State = #{alarms := Alarms}) ->
    {ok, Alarms, State};

handle_call(Req, State) ->
    ?ERROR("[AlarmMgr] unexpected call: ~p", [Req]),
    {ok, ignored, State}.

terminate(swap, State) ->
    {?MODULE, State};
terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

encode_alarm(#alarm{id = AlarmId, severity = Severity, title = Title,
                    summary = Summary, timestamp = Ts}) ->
    emqx_json:safe_encode([{id, AlarmId}, {severity, Severity},
                           {title, iolist_to_binary(Title)},
                           {summary, iolist_to_binary(Summary)},
                           {ts, emqx_time:now_secs(Ts)}]).

alarm_msg(Type, AlarmId, Json) ->
    Msg = emqx_message:make(?ALARM_MGR, topic(Type, AlarmId), Json),
    emqx_message:set_headers( #{'Content-Type' => <<"application/json">>},
                              emqx_message:set_flag(sys, Msg)).

topic(alert, AlarmId) ->
    emqx_topic:systop(<<"alarms/", AlarmId/binary, "/alert">>);
topic(clear, AlarmId) ->
    emqx_topic:systop(<<"alarms/", AlarmId/binary, "/clear">>).

