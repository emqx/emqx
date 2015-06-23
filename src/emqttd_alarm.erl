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
%%% copy alarm_handler.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_alarm).

-include("emqttd.hrl").

-export([start_link/0, alarm_fun/0, get_alarms/0,
         set_alarm/1, clear_alarm/1,
         add_alarm_handler/1, add_alarm_handler/2,
         delete_alarm_handler/1]).

-export([init/1, handle_event/2, handle_call/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).

start_link() ->
    case gen_event:start_link({local, ?SERVER}) of
	{ok, Pid} ->
	    gen_event:add_handler(?SERVER, ?MODULE, []),
	    {ok, Pid};
	Error ->
        Error
    end.

alarm_fun() ->
    alarm_fun(false).

alarm_fun(Bool) ->
    fun(alert, _Alarm)   when Bool =:= true  -> alarm_fun(true);
       (alert,  Alarm)   when Bool =:= false -> set_alarm(Alarm), alarm_fun(true);
       (clear,  AlarmId) when Bool =:= true  -> clear_alarm(AlarmId), alarm_fun(false);
       (clear, _AlarmId) when Bool =:= false -> alarm_fun(false)
    end.

-spec set_alarm(mqtt_alarm()) -> ok.
set_alarm(Alarm) when is_record(Alarm, mqtt_alarm) ->
    gen_event:notify(?SERVER, {set_alarm, Alarm}).

-spec clear_alarm(any()) -> ok.
clear_alarm(AlarmId) when is_binary(AlarmId) ->
    gen_event:notify(?SERVER, {clear_alarm, AlarmId}).

get_alarms() ->
    gen_event:call(?SERVER, ?MODULE, get_alarms).

add_alarm_handler(Module) when is_atom(Module) ->
    gen_event:add_handler(?SERVER, Module, []).

add_alarm_handler(Module, Args) when is_atom(Module) ->
    gen_event:add_handler(?SERVER, Module, Args).

delete_alarm_handler(Module) when is_atom(Module) ->
    gen_event:delete_handler(?SERVER, Module, []).

%%-----------------------------------------------------------------
%% Default Alarm handler
%%-----------------------------------------------------------------
init(_) ->
    {ok, []}.
    
handle_event({set_alarm, Alarm = #mqtt_alarm{id = AlarmId,
                                             severity = Severity,
                                             title = Title,
                                             summary = Summary}}, Alarms)->
    Timestamp = os:timestamp(),
    Json = mochijson2:encode([{id, AlarmId},
                              {severity, Severity},
                              {title, iolist_to_binary(Title)},
                              {summary, iolist_to_binary(Summary)},
                              {ts, emqttd_util:now_to_secs(Timestamp)}]),
    emqttd_pubsub:publish(alarm_msg(alert, AlarmId, Json)),
    {ok, [Alarm#mqtt_alarm{timestamp = Timestamp} | Alarms]};

handle_event({clear_alarm, AlarmId}, Alarms)->
    Json = mochijson2:encode([{id, AlarmId}, {ts, emqttd_util:now_to_secs()}]),
    emqttd_pubsub:publish(alarm_msg(clear, AlarmId, Json)),
    {ok, lists:keydelete(AlarmId, 2, Alarms)};

handle_event(_, Alarms)->
    {ok, Alarms}.

handle_info(_, Alarms) ->
    {ok, Alarms}.

handle_call(get_alarms, Alarms) ->
    {ok, Alarms, Alarms};

handle_call(_Query, Alarms) -> 
    {ok, {error, bad_query}, Alarms}.

terminate(swap, Alarms) ->
    {?MODULE, Alarms};

terminate(_, _) ->
    ok.

alarm_msg(Type, AlarmId, Json) ->
    #mqtt_message{from    = alarm,
                  qos     = 1,
                  sys     = true,
                  topic   = topic(Type, AlarmId),
                  payload = iolist_to_binary(Json),
                  timestamp = os:timestamp()}.

topic(alert, AlarmId) ->
    emqttd_topic:systop(<<"alarms/", AlarmId/binary, "/alert">>);

topic(clear, AlarmId) ->
    emqttd_topic:systop(<<"alarms/", AlarmId/binary, "/clear">>).


