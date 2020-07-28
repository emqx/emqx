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

-module(emqx_alarm).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Alarm Handler]").

-export([start_link/0, stop/0]).

%% API
-export([ activate/1
        , activate/2
        , deactivate/1
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

-record(alarm, {
          name :: binary() | atom(),

          details :: map() | list(),
    
          message :: binary(),
    
          activate_at :: integer(),
    
          deactivate_at :: integer() | infinity,
    
          activated :: boolean()
        }).

-record(state, {
          actions :: [action()]
        }).

-type action() :: log | publish | event.

-define(TAB, emqx_alarm).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

activate(Name) ->
    activate(Name, #{}).

activate(Name, Details) ->
    gen_server:call(?MODULE, {activate_alarm, Name, Details}).

deactivate(Name) ->
    gen_server:call(?MODULE, {deactivate_alarm, Name}).

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

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Opts = [{actions, [log, publish]}],
    init([Opts]);
init([Opts]) ->
    ok = ekka_mnesia:create_table(?TAB,
             [{type, bag},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, alarm},
              {attributes, record_info(fields, alarm)}]),
    Actions = proplists:get_value(actions, Opts, [log, publish]),
    deactivate_all_alarms(),
    {ok, #state{actions = Actions}}.

handle_call({activate_alarm, Name, Details}, _From, State = #state{actions = Actions}) ->
    case get(Name) of
        set ->
            {reply, {error, already_existed}, State};
        undefined ->
            Alarm = #alarm{name = Name,
                           details = Details,
                           message = normalize_message(Name, Details),
                           activate_at = erlang:system_time(millisecond),
                           deactivate_at = infinity,
                           activated = true},
            mnesia:dirty_write(?TAB, Alarm),
            put(Name, set),
            do_actions(activate, Alarm, Actions),
            {reply, ok, State}
    end;

handle_call({deactivate_alarm, Name}, _From, State = #state{actions = Actions}) ->
    case get(Name) of
        set ->
            MatchSpec = [{#alarm{name = '$1', activated = '$2', _ = '_'},
                         [{'==', '$1', Name}, {'==', '$2', true}],
                         ['$_']}],
            case mnesia:dirty_select(?TAB, MatchSpec) of
                [] ->
                    erase(Name),
                    {reply, {error, not_found}, State};
                [Alarm | _] ->
                    NAlarm = Alarm#alarm{deactivate_at = erlang:system_time(millisecond),
                                         activated = false},
                    mnesia:dirty_delete_object(?TAB, Alarm),
                    mnesia:dirty_write(?TAB, NAlarm),
                    erase(Name),
                    do_actions(deactivate, NAlarm, Actions),
                    {reply, ok, State}
            end;
        undefined ->
            {reply, {error, not_found}, State}
    end;

handle_call(delete_all_deactivated_alarms, _From, State) ->
    MatchSpec = [{#alarm{activated = '$1', _ = '_'},
                 [{'==', '$1', false}],
                 ['$_']}],
    lists:foreach(fun(Alarm) ->
                      mnesia:dirty_delete_object(?TAB, Alarm)
                  end, mnesia:dirty_select(?TAB, MatchSpec)),
    {reply, ok, State};

handle_call({get_alarms, all}, _From, State) ->
    Alarms = ets:tab2list(?TAB),
    {reply, [normalize(Alarm) || Alarm <- Alarms], State};

handle_call({get_alarms, activated}, _From, State) ->
    MatchSpec = [{#alarm{activated = '$1', _ = '_'},
                 [{'==', '$1', true}],
                 ['$_']}],
    Alarms = [normalize(Alarm) || Alarm <- mnesia:dirty_select(?TAB, MatchSpec)],
    {reply, Alarms, State};

handle_call({get_alarms, deactivated}, _From, State) ->
    MatchSpec = [{#alarm{activated = '$1', _ = '_'},
                 [{'==', '$1', false}],
                 ['$_']}],
    Alarms = [normalize(Alarm) || Alarm <- mnesia:dirty_select(?TAB, MatchSpec)],
    {reply, Alarms, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

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

deactivate_all_alarms() ->
    MatchSpec = [{#alarm{activated = '$1', _ = '_'},
                 [{'==', '$1', true}],
                 ['$_']}],
    case mnesia:dirty_select(?TAB, MatchSpec) of
        [] ->
            ok;
        Alarms ->
            lists:foreach(fun(Alarm) ->
                              NAlarm = Alarm#alarm{deactivate_at = erlang:system_time(millisecond),
                                                   activated = false},
                              mnesia:dirty_delete_object(?TAB, Alarm),
                              mnesia:dirty_write(?TAB, NAlarm)
                          end, Alarms)
    end.

do_actions(_, _, []) ->
    ok;
do_actions(activate, Alarm = #alarm{name = Name, message = Message}, [log | More]) ->
    ?LOG(warning, "Alarm ~p is activated, ~s", [Name, Message]),
    do_actions(activate, Alarm, More);
do_actions(deactivate, Alarm = #alarm{name = Name}, [log | More]) ->
    ?LOG(warning, "Alarm ~p is deactivated", [Name]),
    do_actions(deactivate, Alarm, More);
do_actions(Operation, Alarm, [publish | More]) ->
    Topic = topic(Operation),
    {ok, Payload} = encode_to_json(Alarm),
    Message = emqx_message:make(?MODULE, 0, Topic, Payload, #{sys => true},
                  #{properties => #{'Content-Type' => <<"application/json">>}}),
    emqx_broker:safe_publish(Message),
    do_actions(Operation, Alarm, More).

encode_to_json(Alarm) ->
    emqx_json:safe_encode(normalize(Alarm)).

topic(activate) ->
    emqx_topic:systop(<<"alarms/activate">>);
topic(deactivate) ->
    emqx_topic:systop(<<"alarms/deactivate">>).

normalize(#alarm{name = Name,
                 details = Details,
                 message = Message,
                 activate_at = ActivateAt,
                 deactivate_at = DeactivateAt,
                 activated = Activated}) ->
    #{name => Name,
      details => Details,
      message => Message,
      activate_at => ActivateAt,
      deactivate_at => DeactivateAt,
      activated => Activated}.

normalize_message(high_system_memory_usage, #{high_watermark := HighWatermark}) ->
    list_to_binary(io_lib:format("System memory usage is higher than ~p%", [HighWatermark]));
normalize_message(high_process_memory_usage, #{high_watermark := HighWatermark}) ->
    list_to_binary(io_lib:format("Process memory usage is higher than ~p%", [HighWatermark]));
normalize_message(high_cpu_usage, #{usage := Usage}) ->
    list_to_binary(io_lib:format("~p% cpu usage", [Usage]));
normalize_message(too_many_processes, #{high_watermark := HighWatermark}) ->
    list_to_binary(io_lib:format("High watermark: ~p%", [HighWatermark]));
normalize_message(partition, #{occurred := Node}) ->
    list_to_binary(io_lib:format("Partition occurs at node ~s", [Node]));
normalize_message(_Name, _UnknownDetails) ->
    <<"Unknown alarm">>.

