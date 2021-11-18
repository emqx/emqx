%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_st_statistics).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_st_statistics/include/emqx_st_statistics.hrl").

-export([ start_link/0, on_publish_done/3, enable/0
        , disable/0, clear_history/0
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

-type state() :: #{period := pos_integer()
                  , last_tick_at := pos_integer()
                  , counter := counters:counter_ref()
                  , enable := boolean()
                  }.

-type log() :: #{ topic := emqx_types:topic()
                , count := pos_integer()
                , average := float()
                }.

-type window_log() :: #{ last_tick_at := pos_integer()
                       , logs := [log()]
                       }.

-record(slow_log, { topic :: emqx_types:topic()
                  , count :: pos_integer()
                  , elapsed :: pos_integer()
                  }).

-type message() :: #message{}.

-define(NOW, erlang:system_time(millisecond)).
-define(QUOTA_IDX, 1).

-type slow_log() :: #slow_log{}.
-type top_k_map() :: #{emqx_types:topic() => top_k()}.

-type publish_done_env() :: #{ ignore_before_create := boolean()
                             , threshold := pos_integer()
                             , counter := counters:counters_ref()
                             }.

-type publish_done_args() :: #{session_rebirth_time => pos_integer()}.

-ifdef(TEST).
-define(TOPK_ACCESS, public).
-else.
-define(TOPK_ACCESS, protected).
-endif.

%% erlang term order
%% number < atom < reference < fun < port < pid < tuple < list < bit string

%% ets ordered_set is ascending by term order

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start the st_statistics
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec on_publish_done(message(), publish_done_args(), publish_done_env()) -> ok.
on_publish_done(#message{timestamp = Timestamp},
                #{session_rebirth_time := Created},
                #{ignore_before_create := IgnoreBeforeCreate})
  when IgnoreBeforeCreate, Timestamp < Created ->
    ok;

on_publish_done(#message{timestamp = Timestamp} = Msg,
                _,
                #{threshold := Threshold, counter := Counter}) ->
    case ?NOW - Timestamp of
        Elapsed when Elapsed > Threshold ->
            case get_log_quota(Counter) of
                true ->
                    update_log(Msg, Elapsed);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

clear_history() ->
    gen_server:call(?MODULE, ?FUNCTION_NAME).

enable() ->
    gen_server:call(?MODULE, {enable, true}).

disable() ->
    gen_server:call(?MODULE, {enable, false}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    init_log_tab(),
    init_topk_tab(),
    notification_tick(),
    Counter = counters:new(1, [write_concurrency]),
    set_log_quota(Counter),
    load(Counter),
    {ok, #{period => 1,
           last_tick_at => ?NOW,
           counter => Counter,
           enable => true}}.

handle_call({enable, Enable}, _From,
            #{counter := Counter, enable := IsEnable} = State) ->
    State2 = case Enable of
                 IsEnable ->
                     State;
                 true ->
                     load(Counter),
                     State#{enable := true};
                 _ ->
                     unload(),
                     State#{enable := false}
             end,
    {reply, ok, State2};

handle_call(clear_history, _, State) ->
    ets:delete_all_objects(?TOPK_TAB),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(notification_tick, #{period := Period} = State) ->
    notification_tick(),
    do_notification(State),
    {noreply, State#{last_tick_at := ?NOW,
                     period := Period + 1}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _) ->
    unload(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
notification_tick() ->
    TimeWindow = emqx:get_config([?MODULE, time_window]),
    erlang:send_after(TimeWindow, self(), ?FUNCTION_NAME).

init_log_tab() ->
    ?LOG_TAB = ets:new(?LOG_TAB, [ set, public, named_table
                                 , {keypos, #slow_log.topic}, {write_concurrency, true}
                                 , {read_concurrency, true}
                                 ]).

init_topk_tab() ->
    ?TOPK_TAB = ets:new(?TOPK_TAB, [ set, ?TOPK_ACCESS, named_table
                                   , {keypos, #top_k.rank}, {write_concurrency, false}
                                   , {read_concurrency, true}
                                   ]).

-spec get_log_quota(counter:counter_ref()) -> boolean().
get_log_quota(Counter) ->
    case counters:get(Counter, ?QUOTA_IDX) of
        Quota when Quota > 0 ->
            counters:sub(Counter, ?QUOTA_IDX, 1),
            true;
        _ ->
            false
    end.

-spec set_log_quota(counter:counter_ref()) -> ok.
set_log_quota(Counter) ->
    MaxLogNum = emqx:get_config([?MODULE, max_log_num]),
    counters:put(Counter, ?QUOTA_IDX, MaxLogNum).

-spec update_log(message(), pos_integer()) -> ok.
update_log(#message{topic = Topic}, Elapsed) ->
    _ = ets:update_counter(?LOG_TAB,
                           Topic,
                           [{#slow_log.count, 1}, {#slow_log.elapsed, Elapsed}],
                           #slow_log{topic = Topic,
                                     count = 0,
                                     elapsed = 0}),
    ok.

-spec do_notification(state()) -> true.
do_notification(#{last_tick_at := TickTime,
                  period := Period,
                  counter := Counter}) ->
    Logs = ets:tab2list(?LOG_TAB),
    ets:delete_all_objects(?LOG_TAB),
    start_publish(Logs, TickTime),
    set_log_quota(Counter),
    update_topk(Logs, Period).

-spec update_topk(list(slow_log()), pos_integer()) -> true.
update_topk(Logs, Period) ->
    TopkMap = get_topk_map(Period),
    TopkMap2 = update_topk_map(Logs, Period, TopkMap),
    SortFun = fun(A, B) ->
                  A#top_k.average_count > B#top_k.average_count
              end,
    MaxRecord = emqx:get_config([?MODULE, top_k_num]),
    TopkL = lists:sort(SortFun, maps:values(TopkMap2)),
    TopkL2 = lists:sublist(TopkL, 1, MaxRecord),
    update_topk_tab(TopkL2).

-spec update_topk_map(list(slow_log()), pos_integer(), top_k_map()) -> top_k_map().
update_topk_map([#slow_log{topic = Topic,
                           count = LogTimes,
                           elapsed = LogElapsed} | T], Period, TopkMap) ->
    case maps:get(Topic, TopkMap, undefined) of
        undefined ->
            Record = #top_k{rank = 1,
                            topic = Topic,
                            average_count = LogTimes,
                            average_elapsed = LogElapsed},
            TopkMap2 = TopkMap#{Topic => Record},
            update_topk_map(T, Period, TopkMap2);
        #top_k{average_count = AvgCount,
               average_elapsed = AvgElapsed} = Record ->
            NewPeriod = Period + 1,
            %% (a + b) / c = a / c + b / c
            %% average_count(elapsed) dived NewPeriod in function get_topk_maps
            AvgCount2 = AvgCount + LogTimes / NewPeriod,
            AvgElapsed2 = AvgElapsed + LogElapsed / NewPeriod,
            Record2 = Record#top_k{average_count = AvgCount2,
                                   average_elapsed = AvgElapsed2},
            update_topk_map(T, Period, TopkMap#{Topic := Record2})
    end;

update_topk_map([], _, TopkMap) ->
    TopkMap.

-spec update_topk_tab(list(top_k())) -> true.
update_topk_tab(Records) ->
    Zip = fun(Rank, Item) -> Item#top_k{rank = Rank} end,
    Len = erlang:length(Records),
    RankedTopics = lists:zipwith(Zip, lists:seq(1, Len), Records),
    ets:insert(?TOPK_TAB, RankedTopics).

start_publish(Logs, TickTime) ->
    emqx_pool:async_submit({fun do_publish/2, [Logs, TickTime]}).

do_publish([], _) ->
    ok;

do_publish(Logs, TickTime) ->
    BatchSize = emqx:get_config([?MODULE, notice_batch_size]),
    do_publish(Logs, BatchSize, TickTime, []).

do_publish([Log | T], Size, TickTime, Cache) when Size > 0 ->
    Cache2 = [convert_to_notice(Log) | Cache],
    do_publish(T, Size - 1, TickTime, Cache2);

do_publish(Logs, Size, TickTime, Cache) when Size =:= 0 ->
    publish(TickTime, Cache),
    do_publish(Logs, TickTime);

do_publish([], _, TickTime, Cache) ->
    publish(TickTime, Cache),
    ok.

convert_to_notice(#slow_log{topic = Topic,
                            count = Count,
                            elapsed = Elapsed}) ->
    #{topic => Topic,
      count => Count,
      average => Elapsed / Count}.

publish(TickTime, Notices) ->
    WindowLog = #{last_tick_at => TickTime,
                  logs => Notices},
    Payload = emqx_json:encode(WindowLog),
    #{notice_qos := QoS, notice_topic := Topic} = emqx:get_config([?MODULE]),
    _ = emqx:publish(#message{ id = emqx_guid:gen()
                             , qos = QoS
                             , from = ?MODULE
                             , topic = check_topic(Topic)
                             , payload = Payload
                             , timestamp = ?NOW
                             }),
    ok.

load(Counter) ->
    #{ ignore_before_create := IgnoreBeforeCreate
     , threshold_time := Threshold
     } = emqx:get_config([?MODULE]),
    _ = emqx:hook('message.publish_done',
                  {?MODULE,
                   on_publish_done,
                   [#{ignore_before_create => IgnoreBeforeCreate,
                      threshold => Threshold,
                      counter => Counter}
                   ]}),
    ok.

unload() ->
    emqx:unhook('message.publish_done', {?MODULE, on_publish_done}).

-spec check_topic(string() | binary()) -> binary().
check_topic(Topic) when is_binary(Topic) ->
    Topic;

check_topic(Topic) ->
    erlang:list_to_binary(Topic).

-spec get_topk_map(pos_integer()) -> top_k_map().
get_topk_map(Period) ->
    Size = ets:info(?TOPK_TAB, size),
    get_topk_map(1, Size, Period, #{}).

-spec get_topk_map(pos_integer(),
                   non_neg_integer(), pos_integer(), top_k_map()) -> top_k_map().
get_topk_map(Index, Size, _, TopkMap) when Index > Size ->
    TopkMap;
get_topk_map(Index, Size, Period, TopkMap) ->
    [#top_k{topic = Topic,
            average_count = AvgCount,
            average_elapsed = AvgElapsed} = R] = ets:lookup(?TOPK_TAB, Index),
    NewPeriod = Period + 1,
    TotalTimes = AvgCount * Period,
    AvgCount2 = TotalTimes / NewPeriod,
    AvgElapsed2 = TotalTimes * AvgElapsed / NewPeriod,
    TopkMap2 = TopkMap#{Topic => R#top_k{average_count = AvgCount2,
                                         average_elapsed = AvgElapsed2}},
    get_topk_map(Index + 1, Size, Period, TopkMap2).
