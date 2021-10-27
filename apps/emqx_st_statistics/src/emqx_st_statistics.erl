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

-include_lib("include/emqx.hrl").
-include_lib("include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-logger_header("[SLOW TOPICS]").

-export([ start_link/1, on_publish_done/3, enable/0
        , disable/0
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
                  , index := index_map()
                  , begin_time := pos_integer()
                  , counter := counters:counter_ref()
                  , enable := boolean()
                  }.

-type log() :: #{ topic := emqx_types:topic()
                , times := pos_integer()
                , average := float()
                }.

-type window_log() :: #{ begin_time := pos_integer()
                       , logs := [log()]
                       }.

-record(slow_log, { topic :: emqx_types:topic()
                  , times :: non_neg_integer()
                  , elapsed :: non_neg_integer()
                  }).

-record(top_k, { key :: any()
               , average :: float()}).

-type message() :: #message{}.

-import(proplists, [get_value/2]).

-define(LOG_TAB, emqx_st_statistics_log).
-define(TOPK_TAB, emqx_st_statistics_topk).
-define(NOW, erlang:system_time(millisecond)).
-define(TOP_KEY(Times, Topic), {Times, Topic}).
-define(QUOTA_IDX, 1).

-type top_key() :: ?TOP_KEY(pos_integer(), emqx_types:topic()).
-type index_map() :: #{emqx_types:topic() => pos_integer()}.

%% erlang term order
%% number < atom < reference < fun < port < pid < tuple < list < bit string

%% ets ordered_set is ascending by term order

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
           index => #{},
           begin_time => ?NOW,
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

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(notification_tick, #{config := Cfg} = State) ->
    notification_tick(Cfg),
    Index2 = do_notification(State),
    {noreply, State#{index := Index2,
                     begin_time := ?NOW}};

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
    ?TOPK_TAB = ets:new(?TOPK_TAB, [ ordered_set, protected, named_table
                                   , {keypos, #top_k.key}, {write_concurrency, true}
                                   , {read_concurrency, false}
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

-spec update_log(message(), non_neg_integer()) -> ok.
update_log(#message{topic = Topic}, Elapsed) ->
    _ = ets:update_counter(?LOG_TAB,
                           Topic,
                           [{#slow_log.times, 1}, {#slow_log.elapsed, Elapsed}],
                           #slow_log{topic = Topic,
                                 times = 1,
                                 elapsed = Elapsed}),
    ok.

-spec do_notification(state()) -> index_map().
do_notification(#{begin_time := BeginTime,
                  config := Cfg,
                  index := IndexMap,
                  counter := Counter}) ->
    Logs = ets:tab2list(?LOG_TAB),
    ets:delete_all_objects(?LOG_TAB),
    start_publish(Logs, BeginTime, Cfg),
    set_log_quota(Cfg, Counter),
    MaxRecord = get_value(top_k_num, Cfg),
    Size = ets:info(?TOPK_TAB, size),
    update_top_k(Logs, erlang:max(0, MaxRecord - Size), IndexMap).

-spec update_top_k(list(#slow_log{}), non_neg_integer(), index_map()) -> index_map().
update_top_k([#slow_log{topic = Topic,
                        times = NewTimes,
                        elapsed = Elapsed} = Log | T],
             Left,
             IndexMap) ->
    case maps:get(Topic, IndexMap, 0) of
        0 ->
            try_insert_new(Log, Left, T, IndexMap);
        Times ->
            [#top_k{key = Key, average = Average}] = ets:lookup(?TOPK_TAB, ?TOP_KEY(Times, Topic)),
            Times2 = Times + NewTimes,
            Total = Times * Average + Elapsed,
            Average2 = Total / Times2,
            ets:delete(?TOPK_TAB, Key),
            ets:insert(?TOPK_TAB, #top_k{key = ?TOP_KEY(Times2, Topic), average = Average2}),
            update_top_k(T, Left, IndexMap#{Topic := Times2})
    end;

update_top_k([], _, IndexMap) ->
    IndexMap.

-spec try_insert_new(#slow_log{},
                     non_neg_integer(), list(#slow_log{}), index_map()) -> index_map().
try_insert_new(#slow_log{topic = Topic,
                         times = Times,
                         elapsed = Elapsed}, Left, Logs, IndexMap) when Left > 0 ->
    Average = Elapsed / Times,
    ets:insert_new(?TOPK_TAB, #top_k{key = ?TOP_KEY(Times, Topic), average = Average}),
    update_top_k(Logs, Left - 1, IndexMap#{Topic => Times});

try_insert_new(#slow_log{topic = Topic,
                         times = Times,
                         elapsed = Elapsed}, Left, Logs, IndexMap) ->
    ?TOP_KEY(MinTimes, MinTopic) = MinKey = ets:first(?TOPK_TAB),
    case MinTimes > Times of
        true ->
            update_top_k(Logs, Left, IndexMap);
        _ ->
            Average = Elapsed / Times,
            ets:delete(?TOPK_TAB, MinKey),
            ets:insert_new(?TOPK_TAB, #top_k{key = ?TOP_KEY(Times, Topic), average = Average}),
            update_top_k(Logs,
                         Left - 1,
                         maps:put(Topic, Times, maps:remove(MinTopic, IndexMap)))
    end.

start_publish(Logs, BeginTime, Cfg) ->
    emqx_pool:async_submit({fun do_publish/3, [Logs, BeginTime, Cfg]}).

do_publish([], _, _) ->
    ok;

do_publish(Logs, BeginTime, Cfg) ->
    BatchSize = get_value(notice_batch_size, Cfg),
    do_publish(Logs, BatchSize, BeginTime, Cfg, []).

do_publish([Log | T], Size, BeginTime, Cfg, Cache) when Size > 0 ->
    Cache2 = [convert_to_notice(Log) | Cache],
    do_publish(T, Size - 1, BeginTime, Cfg, Cache2);

do_publish(Logs, Size, BeginTime, Cfg, Cache) when Size =:= 0 ->
    publish(BeginTime, Cfg, Cache),
    do_publish(Logs, BeginTime, Cfg);

do_publish([], _, BeginTime, Cfg, Cache) ->
    publish(BeginTime, Cfg, Cache),
    ok.

convert_to_notice(#slow_log{topic = Topic,
                            times = Times,
                            elapsed = Elapsed}) ->
    #{topic => Topic,
      times => Times,
      average => Elapsed / Times}.

publish(BeginTime, Cfg, Notices) ->
    WindowLog = #{begin_time => BeginTime,
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

get_topic(Cfg) ->
    case get_value(notice_topic, Cfg) of
        Topic when is_binary(Topic) ->
            Topic;
        Topic ->
            erlang:list_to_binary(Topic)
    end.
