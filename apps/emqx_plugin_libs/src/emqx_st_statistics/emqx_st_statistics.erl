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

-module(emqx_st_statistics).

-behaviour(gen_server).

-include_lib("include/emqx.hrl").
-include_lib("include/logger.hrl").
-include("include/emqx_st_statistics.hrl").

-logger_header("[SLOW TOPICS]").

-export([ start_link/1, on_publish_done/3, enable/0
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

-type state() :: #{ config := proplist:proplist()
                  , period := pos_integer()
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

-import(proplists, [get_value/2]).

-define(NOW, erlang:system_time(millisecond)).
-define(QUOTA_IDX, 1).

-type slow_log() :: #slow_log{}.
-type top_k_map() :: #{emqx_types:topic() => top_k()}.

-ifdef(TEST).
-define(TOPK_ACCESS, public).
-else.
-define(TOPK_ACCESS, protected).
-endif.

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

-spec on_publish_done(message(), pos_integer(), counters:counters_ref()) -> ok.
on_publish_done(#message{timestamp = Timestamp} = Msg, Threshold, Counter) ->
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

init([Env]) ->
    erlang:process_flag(trap_exit, true),
    init_log_tab(Env),
    init_topk_tab(Env),
    notification_tick(Env),
    Counter = counters:new(1, [write_concurrency]),
    set_log_quota(Env, Counter),
    Threshold = get_value(threshold_time, Env),
    load(Threshold, Counter),
    {ok, #{config => Env,
           period => 1,
           last_tick_at => ?NOW,
           counter => Counter,
           enable => true}}.

handle_call({enable, Enable}, _From,
            #{config := Cfg, counter := Counter, enable := IsEnable} = State) ->
    State2 = case Enable of
                 IsEnable ->
                     State;
                 true ->
                     Threshold = get_value(threshold_time, Cfg),
                     load(Threshold, Counter),
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

handle_info(notification_tick, #{config := Cfg, period := Period} = State) ->
    notification_tick(Cfg),
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
notification_tick(Env) ->
    TimeWindow = get_value(time_window, Env),
    erlang:send_after(TimeWindow, self(), ?FUNCTION_NAME).

init_log_tab(_) ->
    ?LOG_TAB = ets:new(?LOG_TAB, [ set, public, named_table
                                 , {keypos, #slow_log.topic}, {write_concurrency, true}
                                 , {read_concurrency, true}
                                 ]).

init_topk_tab(_) ->
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

-spec set_log_quota(proplists:proplist(), counter:counter_ref()) -> ok.
set_log_quota(Cfg, Counter) ->
    MaxLogNum = get_value(max_log_num, Cfg),
    counters:put(Counter, ?QUOTA_IDX, MaxLogNum).

-spec update_log(message(), pos_integer()) -> ok.
update_log(#message{topic = Topic}, Elapsed) ->
    _ = ets:update_counter(?LOG_TAB,
                           Topic,
                           [{#slow_log.count, 1}, {#slow_log.elapsed, Elapsed}],
                           #slow_log{topic = Topic,
                                     count = 1,
                                     elapsed = Elapsed}),
    ok.

-spec do_notification(state()) -> true.
do_notification(#{last_tick_at := TickTime,
                  config := Cfg,
                  period := Period,
                  counter := Counter}) ->
    Logs = ets:tab2list(?LOG_TAB),
    ets:delete_all_objects(?LOG_TAB),
    start_publish(Logs, TickTime, Cfg),
    set_log_quota(Cfg, Counter),
    MaxRecord = get_value(top_k_num, Cfg),
    update_topk(Logs, MaxRecord, Period).

-spec update_topk(list(slow_log()), pos_integer(), pos_integer()) -> true.
update_topk(Logs, MaxRecord, Period) ->
    TopkMap = get_topk_map(Period),
    TopkMap2 = update_topk_map(Logs, Period, TopkMap),
    SortFun = fun(A, B) ->
                      A#top_k.average_count > B#top_k.average_count
              end,
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

start_publish(Logs, TickTime, Cfg) ->
    emqx_pool:async_submit({fun do_publish/3, [Logs, TickTime, Cfg]}).

do_publish([], _, _) ->
    ok;

do_publish(Logs, TickTime, Cfg) ->
    BatchSize = get_value(notice_batch_size, Cfg),
    do_publish(Logs, BatchSize, TickTime, Cfg, []).

do_publish([Log | T], Size, TickTime, Cfg, Cache) when Size > 0 ->
    Cache2 = [convert_to_notice(Log) | Cache],
    do_publish(T, Size - 1, TickTime, Cfg, Cache2);

do_publish(Logs, Size, TickTime, Cfg, Cache) when Size =:= 0 ->
    publish(TickTime, Cfg, Cache),
    do_publish(Logs, TickTime, Cfg);

do_publish([], _, TickTime, Cfg, Cache) ->
    publish(TickTime, Cfg, Cache),
    ok.

convert_to_notice(#slow_log{topic = Topic,
                            count = Count,
                            elapsed = Elapsed}) ->
    #{topic => Topic,
      count => Count,
      average => Elapsed / Count}.

publish(TickTime, Cfg, Notices) ->
    WindowLog = #{last_tick_at => TickTime,
                  logs => Notices},
    Payload = emqx_json:encode(WindowLog),
    _ = emqx:publish(#message{ id = emqx_guid:gen()
                             , qos = get_value(notice_qos, Cfg)
                             , from = ?MODULE
                             , topic = get_topic(Cfg)
                             , payload = Payload
                             , timestamp = ?NOW
                             }),
    ok.

load(Threshold, Counter) ->
    _ = emqx:hook('message.publish_done', fun ?MODULE:on_publish_done/3, [Threshold, Counter]),
    ok.

unload() ->
    emqx:unhook('message.publish_done', fun ?MODULE:on_publish_done/3).

-spec get_topic(proplists:proplist()) -> binary().
get_topic(Cfg) ->
    case get_value(notice_topic, Cfg) of
        Topic when is_binary(Topic) ->
            Topic;
        Topic ->
            erlang:list_to_binary(Topic)
    end.

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
