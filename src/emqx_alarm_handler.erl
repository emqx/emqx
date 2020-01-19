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

-module(emqx_alarm_handler).

-behaviour(gen_event).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Alarm Handler]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% gen_event callbacks
-export([ init/1
        , handle_event/2
        , handle_call/2
        , handle_info/2
        , terminate/2
        ]).

-export([ load/0
        , unload/0
        , get_alarms/0
        , get_alarms/1
        ]).

-record(common_alarm, {id, desc}).
-record(alarm_history, {id, desc, clear_at}).

-define(ALARM_TAB, emqx_alarm).
-define(ALARM_HISTORY_TAB, emqx_alarm_history).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ALARM_TAB, [
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
                {attributes, record_info(fields, alarm_history)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ALARM_TAB),
    ok = ekka_mnesia:copy_table(?ALARM_HISTORY_TAB).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load() ->
    gen_event:swap_handler(alarm_handler, {alarm_handler, swap}, {?MODULE, []}).

%% on the way shutting down, give it back to OTP
unload() ->
    gen_event:swap_handler(alarm_handler, {?MODULE, swap}, {alarm_handler, []}).

get_alarms() ->
    get_alarms(present).

get_alarms(present) ->
    Alarms = ets:tab2list(?ALARM_TAB),
    [{Id, Desc} || #common_alarm{id = Id, desc = Desc} <- Alarms];
get_alarms(history) ->
    Alarms = ets:tab2list(?ALARM_HISTORY_TAB),
    [{Id, Desc, ClearAt} || #alarm_history{id = Id, desc = Desc, clear_at = ClearAt} <- Alarms].

%%--------------------------------------------------------------------
%% gen_event callbacks
%%--------------------------------------------------------------------

init({_Args, {alarm_handler, ExistingAlarms}}) ->
    init_tables(ExistingAlarms),
    {ok, []};

init(_) ->
    init_tables([]),
    {ok, []}.

handle_event({set_alarm, {AlarmId, AlarmDesc = #alarm{timestamp = undefined}}}, State) ->
    handle_event({set_alarm, {AlarmId, AlarmDesc#alarm{timestamp = erlang:system_time(second)}}}, State);
handle_event({set_alarm, Alarm = {AlarmId, AlarmDesc}}, State) ->
    ?LOG(warning, "New Alarm: ~p, Alarm Info: ~p", [AlarmId, AlarmDesc]),
    case encode_alarm(Alarm) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(topic(alert), Json));
        {error, Reason} ->
            ?LOG(error, "Failed to encode alarm: ~p", [Reason])
    end,
    set_alarm_(AlarmId, AlarmDesc),
    {ok, State};
handle_event({clear_alarm, AlarmId}, State) ->
    ?LOG(info, "Clear Alarm: ~p", [AlarmId]),
    case encode_alarm({AlarmId, undefined}) of
        {ok, Json} ->
            emqx_broker:safe_publish(alarm_msg(topic(clear), Json));
        {error, Reason} ->
            ?LOG(error, "Failed to encode alarm: ~p", [Reason])
    end,
    clear_alarm_(AlarmId),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.

handle_call(_Query, State) ->
    {ok, {error, bad_query}, State}.

terminate(swap, _State) ->
    {emqx_alarm_handler, get_alarms()};
terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

init_tables(ExistingAlarms) ->
    mnesia:clear_table(?ALARM_TAB),
    lists:foreach(fun({Id, Desc}) ->
                      set_alarm_history(Id, Desc)
                  end, ExistingAlarms).

encode_alarm({AlarmId, #alarm{severity  = Severity,
                              title     = Title,
                              summary   = Summary,
                              timestamp = Ts}}) ->
    Descr = #{severity => Severity,
              title => iolist_to_binary(Title),
              summary => iolist_to_binary(Summary),
              timestamp => Ts
             },
    emqx_json:safe_encode(#{id => maybe_to_binary(AlarmId),
                            desc => Descr
                           });

encode_alarm({AlarmId, undefined}) ->
    emqx_json:safe_encode(#{id => maybe_to_binary(AlarmId)});
encode_alarm({AlarmId, AlarmDesc}) ->
    emqx_json:safe_encode(#{id => maybe_to_binary(AlarmId),
                            desc => maybe_to_binary(AlarmDesc)
                           }).

alarm_msg(Topic, Payload) ->
    Msg = emqx_message:make(?MODULE, Topic, Payload),
    emqx_message:set_headers(#{'Content-Type' => <<"application/json">>},
                             emqx_message:set_flag(sys, Msg)).

topic(alert) ->
    emqx_topic:systop(<<"alarms/alert">>);
topic(clear) ->
    emqx_topic:systop(<<"alarms/clear">>).

maybe_to_binary(Data) when is_binary(Data) ->
    Data;
maybe_to_binary(Data) ->
    iolist_to_binary(io_lib:format("~p", [Data])).

set_alarm_(Id, Desc) ->
    mnesia:dirty_write(?ALARM_TAB, #common_alarm{id = Id, desc = Desc}).

clear_alarm_(Id) ->
    case mnesia:dirty_read(?ALARM_TAB, Id) of
        [#common_alarm{desc = Desc}] ->
            set_alarm_history(Id, Desc),
            mnesia:dirty_delete(?ALARM_TAB, Id);
        [] -> ok
    end.

set_alarm_history(Id, Desc) ->
    His = #alarm_history{id = Id,
                         desc = Desc,
                         clear_at = erlang:system_time(second)},
    mnesia:dirty_write(?ALARM_HISTORY_TAB, His).
