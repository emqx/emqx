%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_alarm).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Alarm Handler]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([handle_update_config/2]).

-export([ start_link/0
        , stop/0
        ]).

%% API
-export([ activate/1
        , activate/2
        , deactivate/1
        , deactivate/2
        , delete_all_deactivated_alarms/0
        , get_alarms/0
        , get_alarms/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(activated_alarm, {
          name :: binary() | atom(),

          details :: map() | list(),

          message :: binary(),

          activate_at :: integer()
        }).

-record(deactivated_alarm, {
          activate_at :: integer(),

          name :: binary() | atom(),

          details :: map() | list(),

          message :: binary(),

          deactivate_at :: integer() | infinity
        }).

-record(state, {
          timer :: reference()
        }).

-define(ACTIVATED_ALARM, emqx_activated_alarm).

-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).

-rlog_shard({?COMMON_SHARD, ?ACTIVATED_ALARM}).
-rlog_shard({?COMMON_SHARD, ?DEACTIVATED_ALARM}).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ACTIVATED_ALARM,
             [{type, set},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, activated_alarm},
              {attributes, record_info(fields, activated_alarm)}]),
    ok = ekka_mnesia:create_table(?DEACTIVATED_ALARM,
             [{type, ordered_set},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, deactivated_alarm},
              {attributes, record_info(fields, deactivated_alarm)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ACTIVATED_ALARM, disc_copies),
    ok = ekka_mnesia:copy_table(?DEACTIVATED_ALARM, disc_copies).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

activate(Name) ->
    activate(Name, #{}).

activate(Name, Details) ->
    gen_server:call(?MODULE, {activate_alarm, Name, Details}).

deactivate(Name) ->
    gen_server:call(?MODULE, {deactivate_alarm, Name, no_details}).

deactivate(Name, Details) ->
    gen_server:call(?MODULE, {deactivate_alarm, Name, Details}).

delete_all_deactivated_alarms() ->
    gen_server:call(?MODULE, delete_all_deactivated_alarms).

get_alarms() ->
    get_alarms(all).

get_alarms(all) ->
    gen_server:call(?MODULE, {get_alarms, all});

get_alarms(activated) ->
    gen_server:call(?MODULE, {get_alarms, activated});

get_alarms(deactivated) ->
    gen_server:call(?MODULE, {get_alarms, deactivated}).

handle_update_config(#{<<"validity_period">> := Period0} = NewConf, OldConf) ->
    ?MODULE ! {update_timer, hocon_postprocess:duration(Period0)},
    maps:merge(OldConf, NewConf);
handle_update_config(NewConf, OldConf) ->
    maps:merge(OldConf, NewConf).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    deactivate_all_alarms(),
    emqx_config_handler:add_handler([alarm], ?MODULE),
    {ok, #state{timer = ensure_timer(undefined, get_validity_period())}}.

%% suppress dialyzer warning due to dirty read/write race condition.
%% TODO: change from dirty_read/write to transactional.
%% TODO: handle mnesia write errors.
-dialyzer([{nowarn_function, [handle_call/3]}]).
handle_call({activate_alarm, Name, Details}, _From, State) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [#activated_alarm{name = Name}] ->
            {reply, {error, already_existed}, State};
        [] ->
            Alarm = #activated_alarm{name = Name,
                                     details = Details,
                                     message = normalize_message(Name, Details),
                                     activate_at = erlang:system_time(microsecond)},
            ekka_mnesia:dirty_write(?ACTIVATED_ALARM, Alarm),
            do_actions(activate, Alarm, emqx_config:get([alarm, actions])),
            {reply, ok, State}
    end;

handle_call({deactivate_alarm, Name, Details}, _From, State) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [] ->
            {reply, {error, not_found}, State};
        [Alarm] ->
            deactivate_alarm(Details, Alarm),
            {reply, ok, State}
    end;

handle_call(delete_all_deactivated_alarms, _From, State) ->
    clear_table(?DEACTIVATED_ALARM),
    {reply, ok, State};

handle_call({get_alarms, all}, _From, State) ->
    {atomic, Alarms} =
        ekka_mnesia:ro_transaction(
          ?COMMON_SHARD,
          fun() ->
                  [normalize(Alarm) ||
                      Alarm <- ets:tab2list(?ACTIVATED_ALARM)
                          ++ ets:tab2list(?DEACTIVATED_ALARM)]
          end),
    {reply, Alarms, State};

handle_call({get_alarms, activated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?ACTIVATED_ALARM)],
    {reply, Alarms, State};

handle_call({get_alarms, deactivated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?DEACTIVATED_ALARM)],
    {reply, Alarms, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, _TRef, delete_expired_deactivated_alarm},
       #state{timer = TRef} = State) ->
    Period = get_validity_period(),
    delete_expired_deactivated_alarms(erlang:system_time(microsecond) - Period * 1000),
    {noreply, State#state{timer = ensure_timer(TRef, Period)}};

handle_info({update_timer, Period}, #state{timer = TRef} = State) ->
    ?LOG(warning, "update the 'validity_period' timer to ~p", [Period]),
    {noreply, State#state{timer = ensure_timer(TRef, Period)}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_validity_period() ->
    timer:seconds(emqx_config:get([alarm, validity_period])).

deactivate_alarm(Details, #activated_alarm{activate_at = ActivateAt, name = Name,
        details = Details0, message = Msg0}) ->
    SizeLimit = emqx_config:get([alarm, size_limit]),
    case SizeLimit > 0 andalso (mnesia:table_info(?DEACTIVATED_ALARM, size) >= SizeLimit) of
        true ->
            case mnesia:dirty_first(?DEACTIVATED_ALARM) of
                '$end_of_table' -> ok;
                ActivateAt2 ->
                    ekka_mnesia:dirty_delete(?DEACTIVATED_ALARM, ActivateAt2)
            end;
        false -> ok
    end,
    HistoryAlarm = make_deactivated_alarm(ActivateAt, Name, Details0, Msg0,
                        erlang:system_time(microsecond)),
    DeActAlarm = make_deactivated_alarm(ActivateAt, Name, Details,
                    normalize_message(Name, Details),
                    erlang:system_time(microsecond)),
    ekka_mnesia:dirty_write(?DEACTIVATED_ALARM, HistoryAlarm),
    ekka_mnesia:dirty_delete(?ACTIVATED_ALARM, Name),
    do_actions(deactivate, DeActAlarm, emqx_config:get([alarm, actions])).

make_deactivated_alarm(ActivateAt, Name, Details, Message, DeActivateAt) ->
    #deactivated_alarm{
        activate_at = ActivateAt,
        name = Name,
        details = Details,
        message = Message,
        deactivate_at = DeActivateAt}.

deactivate_all_alarms() ->
    lists:foreach(
        fun(#activated_alarm{name = Name,
                             details = Details,
                             message = Message,
                             activate_at = ActivateAt}) ->
            ekka_mnesia:dirty_write(?DEACTIVATED_ALARM,
                #deactivated_alarm{
                    activate_at = ActivateAt,
                    name = Name,
                    details = Details,
                    message = Message,
                    deactivate_at = erlang:system_time(microsecond)})
        end, ets:tab2list(?ACTIVATED_ALARM)),
    clear_table(?ACTIVATED_ALARM).

%% Delete all records from the given table, ignore result.
clear_table(TableName) ->
    case ekka_mnesia:clear_table(TableName) of
        {aborted, Reason} ->
            ?LOG(warning, "Faile to clear table ~p reason: ~p",
                 [TableName, Reason]);
        {atomic, ok} ->
            ok
    end.

ensure_timer(OldTRef, Period) ->
    _ = case is_reference(OldTRef) of
        true -> erlang:cancel_timer(OldTRef);
        false -> ok
    end,
    emqx_misc:start_timer(Period, delete_expired_deactivated_alarm).

delete_expired_deactivated_alarms(Checkpoint) ->
    delete_expired_deactivated_alarms(mnesia:dirty_first(?DEACTIVATED_ALARM), Checkpoint).

delete_expired_deactivated_alarms('$end_of_table', _Checkpoint) ->
    ok;
delete_expired_deactivated_alarms(ActivatedAt, Checkpoint) ->
    case ActivatedAt =< Checkpoint of
        true ->
            ekka_mnesia:dirty_delete(?DEACTIVATED_ALARM, ActivatedAt),
            NActivatedAt = mnesia:dirty_next(?DEACTIVATED_ALARM, ActivatedAt),
            delete_expired_deactivated_alarms(NActivatedAt, Checkpoint);
        false ->
            ok
    end.

do_actions(_, _, []) ->
    ok;
do_actions(activate, Alarm = #activated_alarm{name = Name, message = Message}, [log | More]) ->
    ?LOG(warning, "Alarm ~s is activated, ~s", [Name, Message]),
    do_actions(activate, Alarm, More);
do_actions(deactivate, Alarm = #deactivated_alarm{name = Name}, [log | More]) ->
    ?LOG(warning, "Alarm ~s is deactivated", [Name]),
    do_actions(deactivate, Alarm, More);
do_actions(Operation, Alarm, [publish | More]) ->
    Topic = topic(Operation),
    {ok, Payload} = encode_to_json(Alarm),
    Message = emqx_message:make(?MODULE, 0, Topic, Payload, #{sys => true},
                  #{properties => #{'Content-Type' => <<"application/json">>}}),
    %% TODO log failed publishes
    _ = emqx_broker:safe_publish(Message),
    do_actions(Operation, Alarm, More).

encode_to_json(Alarm) ->
    emqx_json:safe_encode(normalize(Alarm)).

topic(activate) ->
    emqx_topic:systop(<<"alarms/activate">>);
topic(deactivate) ->
    emqx_topic:systop(<<"alarms/deactivate">>).

normalize(#activated_alarm{name = Name,
                           details = Details,
                           message = Message,
                           activate_at = ActivateAt}) ->
    #{name => Name,
      details => Details,
      message => Message,
      activate_at => ActivateAt,
      deactivate_at => infinity,
      activated => true};
normalize(#deactivated_alarm{activate_at = ActivateAt,
                             name = Name,
                             details = Details,
                             message = Message,
                             deactivate_at = DeactivateAt}) ->
    #{name => Name,
      details => Details,
      message => Message,
      activate_at => ActivateAt,
      deactivate_at => DeactivateAt,
      activated => false}.

normalize_message(Name, no_details) ->
    list_to_binary(io_lib:format("~p", [Name]));
normalize_message(high_system_memory_usage, #{high_watermark := HighWatermark}) ->
    list_to_binary(io_lib:format("System memory usage is higher than ~p%", [HighWatermark]));
normalize_message(high_process_memory_usage, #{high_watermark := HighWatermark}) ->
    list_to_binary(io_lib:format("Process memory usage is higher than ~p%", [HighWatermark]));
normalize_message(high_cpu_usage, #{usage := Usage}) ->
    list_to_binary(io_lib:format("~s cpu usage", [Usage]));
normalize_message(too_many_processes, #{usage := Usage}) ->
    list_to_binary(io_lib:format("~s process usage", [Usage]));
normalize_message(partition, #{occurred := Node}) ->
    list_to_binary(io_lib:format("Partition occurs at node ~s", [Node]));
normalize_message(<<"resource", _/binary>>, #{type := Type, id := ID}) ->
    list_to_binary(io_lib:format("Resource ~s(~s) is down", [Type, ID]));
normalize_message(<<"conn_congestion/", Info/binary>>, _) ->
    list_to_binary(io_lib:format("connection congested: ~s", [Info]));
normalize_message(_Name, _UnknownDetails) ->
    <<"Unknown alarm">>.
