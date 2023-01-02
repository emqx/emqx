%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs).

-behaviour(gen_server).

-include_lib("include/emqx.hrl").
-include_lib("include/logger.hrl").
-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-logger_header("[SLOW Subs]").

-export([ start_link/1, on_delivery_completed/4, enable/0
        , disable/0, clear_history/0, init_tab/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-compile(nowarn_unused_type).

-type state() :: #{ config := proplist:proplist()
                  , enable := boolean()
                  , last_tick_at := pos_integer()
                  }.

-type message() :: #message{}.

-import(proplists, [get_value/2, get_value/3]).

-type stats_type() :: whole         %% whole = internal + response
                    | internal      %% timespan from message in to deliver
                    | response.     %% timespan from delivery to client response

-type stats_update_args() :: #{session_birth_time := pos_integer()}.

-type stats_update_env() :: #{ threshold := non_neg_integer()
                             , stats_type := stats_type()
                             , max_size := pos_integer()}.

-ifdef(TEST).
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(1)).
-else.
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(10)).
-endif.

-define(NOW, erlang:system_time(millisecond)).
-define(DEF_CALL_TIMEOUT, timer:seconds(10)).

%% erlang term order
%% number < atom < reference < fun < port < pid < tuple < list < bit string

%% ets ordered_set is ascending by term order

%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
%% @doc Start the st_statistics
-spec(start_link(Env :: list()) -> emqx_types:startlink_ret()).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

on_delivery_completed(_ClientInfo, #message{timestamp = Ts}, #{session_birth_time := BirthTime}, _Cfg)
  when Ts =< BirthTime ->
    ok;

on_delivery_completed(ClientInfo, Msg, Env, Cfg) ->
    on_delivery_completed(ClientInfo, Msg, Env, erlang:system_time(millisecond), Cfg).

on_delivery_completed(#{clientid := ClientId},
                      #message{topic = Topic} = Msg,
                      _Env,
                      Now,
                      #{threshold := Threshold,
                        stats_type := StatsType,
                        max_size := MaxSize}) ->
    TimeSpan = calc_timespan(StatsType, Msg, Now),
    case TimeSpan =< Threshold of
        true -> ok;
        _ ->
            Id = ?ID(ClientId, Topic),
            LastUpdateValue = find_last_update_value(Id),
            case TimeSpan =< LastUpdateValue of
                true -> ok;
                _ ->
                    try_insert_to_topk(MaxSize, Now, LastUpdateValue, TimeSpan, Id)
            end
    end.

clear_history() ->
    gen_server:call(?MODULE, ?FUNCTION_NAME, ?DEF_CALL_TIMEOUT).

enable() ->
    gen_server:call(?MODULE, {enable, true}, ?DEF_CALL_TIMEOUT).

disable() ->
    gen_server:call(?MODULE, {enable, false}, ?DEF_CALL_TIMEOUT).

init_tab() ->
    safe_create_tab(?TOPK_TAB, [ ordered_set, public, named_table
                               , {keypos, #top_k.index}, {write_concurrency, true}
                               , {read_concurrency, true}
                               ]),

    safe_create_tab(?INDEX_TAB, [ ordered_set, public, named_table
                                , {keypos, #index_tab.index}, {write_concurrency, true}
                                , {read_concurrency, true}
                                ]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Conf]) ->
    erlang:process_flag(trap_exit, true),
    expire_tick(Conf),
    load(Conf),
    {ok, #{config => Conf,
           last_tick_at => ?NOW,
           enable => true}}.

handle_call({enable, Enable}, _From,
            #{config := Cfg, enable := IsEnable} = State) ->
    State2 = case Enable of
                 IsEnable ->
                     State;
                 true ->
                     load(Cfg),
                     State#{enable := true};
                 _ ->
                     unload(),
                     State#{enable := false}
             end,
    {reply, ok, State2};

handle_call(clear_history, _, State) ->
    do_clear_history(),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(expire_tick, #{config := Cfg} = State) ->
    expire_tick(Cfg),
    Logs = ets:tab2list(?TOPK_TAB),
    do_clear(Cfg, Logs),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _) ->
    unload(),
    ok.

code_change({down, _Vsn}, #{config := Cfg} =  State, ["4.4.0"]) ->
    unload(),

    MaxSize = get_value(top_k_num, Cfg),
    _ = emqx:hook('message.slow_subs_stats',
                  {?MODULE, on_stats_update, [#{max_size => MaxSize}]}),

    erlang:send_after(?EXPIRE_CHECK_INTERVAL, self(), ?FUNCTION_NAME),

    {ok, State};

code_change(_OldVsn, #{config := Conf} = State, ["4.4.0"]) ->
    %% clear old data
    HookPoint = 'message.slow_subs_stats',
    Callbacks = emqx_hooks:lookup(HookPoint),
    _ = [emqx_hooks:del(HookPoint, Action) ||
            {callback, Action, _Filter, _Priority} <- Callbacks],
    try
        ets:delete_all_objects(?TOPK_TAB)
    catch _:_ ->
            ok
    end,

    %% add new table
    init_tab(),
    [_Sup, SupPid] = erlang:get('$ancestors'),
    ets:give_away(?INDEX_TAB, SupPid, undefined),

    %% enable
    expire_tick(Conf),
    load(Conf),
    {ok, State};

code_change(_OldVsn, State, _Extras) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
expire_tick(_) ->
    erlang:send_after(?EXPIRE_CHECK_INTERVAL, self(), ?FUNCTION_NAME).

load(Cfg) ->
    MaxSize = get_value(top_k_num, Cfg),
    StatsType = get_value(stats_type, Cfg, whole),
    Threshold = get_value(threshold, Cfg),
    _ = emqx:hook('delivery.completed',
                  fun ?MODULE:on_delivery_completed/4,
                  [#{max_size => MaxSize,
                     stats_type => StatsType,
                     threshold => Threshold
                    }]),
    ok.

unload() ->
    emqx:unhook('delivery.completed', fun ?MODULE:on_delivery_completed/4),
    do_clear_history().

do_clear(Cfg, Logs) ->
    Now = ?NOW,
    Interval = get_value(expire_interval, Cfg),
    Each = fun(#top_k{index = ?TOPK_INDEX(TimeSpan, Id), last_update_time = Ts}) ->
                   case Now - Ts >= Interval of
                       true ->
                           delete_with_index(TimeSpan, Id);
                       _ ->
                           true
                   end
           end,
    lists:foreach(Each, Logs).

-spec calc_timespan(stats_type(), emqx_types:message(), non_neg_integer()) -> non_neg_integer().
calc_timespan(whole, #message{timestamp = Ts}, Now) ->
    Now - Ts;

calc_timespan(internal, #message{timestamp = Ts} = Msg, Now) ->
    End = emqx_message:get_header(deliver_begin_at, Msg, Now),
    End - Ts;

calc_timespan(response, Msg, Now) ->
    Begin = emqx_message:get_header(deliver_begin_at, Msg, Now),
    Now - Begin.

%% update_topk is safe, because each process has a unique clientid
%% insert or delete are bind to this clientid, so there is no race condition
%%
%% but, the delete_with_index in L249 may have a race condition
%% because the data belong to other clientid will be deleted here (deleted the data written by other processes).%% so it may appear that:
%%   when deleting a record, the other process is performing an update operation on this recrod
%% in order to solve this race condition problem, the index table also uses the ordered_set type,
%% so that even if the above situation occurs, it will only cause the old data to be deleted twice
%% and the correctness of the data will not be affected

try_insert_to_topk(MaxSize, Now, LastUpdateValue, TimeSpan, Id) ->
    case ets:info(?TOPK_TAB, size) of
        Size when Size < MaxSize ->
            update_topk(Now, LastUpdateValue, TimeSpan, Id);
        _Size ->
            case ets:first(?TOPK_TAB) of
                '$end_of_table' ->
                    update_topk(Now, LastUpdateValue, TimeSpan, Id);
                ?TOPK_INDEX(_, Id) ->
                    update_topk(Now, LastUpdateValue, TimeSpan, Id);
                ?TOPK_INDEX(Min, MinId) ->
                    case TimeSpan =< Min of
                        true -> false;
                        _ ->
                            update_topk(Now, LastUpdateValue, TimeSpan, Id),
                            delete_with_index(Min, MinId)
                    end
            end
    end.

-spec find_last_update_value(id()) -> non_neg_integer().
find_last_update_value(Id) ->
    case ets:next(?INDEX_TAB,  ?INDEX(0, Id)) of
        ?INDEX(LastUpdateValue, Id) ->
            LastUpdateValue;
        _ ->
            0
    end.

-spec update_topk(pos_integer(), non_neg_integer(), non_neg_integer(), id()) -> true.
update_topk(Now, LastUpdateValue, TimeSpan, Id) ->
    %% update record
    ets:insert(?TOPK_TAB, #top_k{index = ?TOPK_INDEX(TimeSpan, Id),
                                 last_update_time = Now,
                                 extra = []
                                }),

    %% update index
    ets:insert(?INDEX_TAB, #index_tab{index = ?INDEX(TimeSpan, Id)}),

    %% delete the old record & index
    delete_with_index(LastUpdateValue, Id).

-spec delete_with_index(non_neg_integer(), id()) -> true.
delete_with_index(0, _) ->
    true;

delete_with_index(TimeSpan, Id) ->
    ets:delete(?INDEX_TAB, ?INDEX(TimeSpan, Id)),
    ets:delete(?TOPK_TAB, ?TOPK_INDEX(TimeSpan, Id)).

safe_create_tab(Name, Opts) ->
    case ets:whereis(Name) of
        undefined ->
            Name = ets:new(Name, Opts);
        _ ->
            Name
    end.

do_clear_history() ->
    ets:delete_all_objects(?INDEX_TAB),
    ets:delete_all_objects(?TOPK_TAB).
