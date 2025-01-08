%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_topic_metrics).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([
    on_message_publish/1,
    on_message_delivered/2,
    on_message_dropped/3
]).

%% API functions
-export([
    start_link/0,
    stop/0
]).

-export([
    enable/0,
    disable/0
]).

-export([max_limit/0]).

-export([
    metrics/0,
    metrics/1,
    register/1,
    deregister/1,
    deregister_all/0,
    is_registered/1,
    all_registered_topics/0,
    reset/0,
    reset/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_info/2,
    handle_cast/2,
    terminate/2
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(MAX_TOPICS, 512).
-define(TAB, ?MODULE).

-define(TOPIC_METRICS, [
    'messages.in',
    'messages.out',
    'messages.qos0.in',
    'messages.qos0.out',
    'messages.qos1.in',
    'messages.qos1.out',
    'messages.qos2.in',
    'messages.qos2.out',
    'messages.dropped'
]).

-define(TICKING_INTERVAL, 1).
-define(SPEED_AVERAGE_WINDOW_SIZE, 5).
-define(SPEED_MEDIUM_WINDOW_SIZE, 60).
-define(SPEED_LONG_WINDOW_SIZE, 300).

-record(speed, {
    last = 0 :: number(),
    last_v = 0 :: number(),
    last_medium = 0 :: number(),
    last_long = 0 :: number()
}).

-record(state, {
    speeds :: #{{binary(), atom()} => #speed{}}
}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
max_limit() ->
    ?MAX_TOPICS.

enable() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_TOPIC_METRICS),
    emqx_hooks:put('message.dropped', {?MODULE, on_message_dropped, []}, ?HP_TOPIC_METRICS),
    emqx_hooks:put('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_TOPIC_METRICS).

disable() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('message.dropped', {?MODULE, on_message_dropped}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    deregister_all().

on_message_publish(#message{topic = Topic, qos = QoS}) ->
    case is_registered(Topic) of
        true ->
            try_inc(Topic, 'messages.in'),
            case QoS of
                ?QOS_0 -> inc(Topic, 'messages.qos0.in');
                ?QOS_1 -> inc(Topic, 'messages.qos1.in');
                ?QOS_2 -> inc(Topic, 'messages.qos2.in')
            end;
        false ->
            ok
    end.

on_message_delivered(_, #message{topic = Topic, qos = QoS}) ->
    case is_registered(Topic) of
        true ->
            try_inc(Topic, 'messages.out'),
            case QoS of
                ?QOS_0 -> inc(Topic, 'messages.qos0.out');
                ?QOS_1 -> inc(Topic, 'messages.qos1.out');
                ?QOS_2 -> inc(Topic, 'messages.qos2.out')
            end;
        false ->
            ok
    end.

on_message_dropped(#message{topic = Topic}, _, _) ->
    case is_registered(Topic) of
        true ->
            inc(Topic, 'messages.dropped');
        false ->
            ok
    end.

start_link() ->
    Opts = emqx_conf:get([topic_metrics], []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

metrics() ->
    [format(TopicMetrics) || TopicMetrics <- ets:tab2list(?TAB)].

metrics(Topic) ->
    case ets:lookup(?TAB, Topic) of
        [] ->
            {error, topic_not_found};
        [TopicMetrics] ->
            format(TopicMetrics)
    end.

register(Topic) when is_binary(Topic) ->
    gen_server:call(?MODULE, {register, Topic}).

deregister(Topic) when is_binary(Topic) ->
    gen_server:call(?MODULE, {deregister, Topic}).

deregister_all() ->
    gen_server:call(?MODULE, {deregister, all}).

is_registered(Topic) ->
    try
        ets:member(?TAB, Topic)
    catch
        error:badarg ->
            false
    end.

all_registered_topics() ->
    [Topic || {Topic, _} <- ets:tab2list(?TAB)].

reset(Topic) ->
    case is_registered(Topic) of
        true ->
            gen_server:call(?MODULE, {reset, Topic});
        false ->
            {error, topic_not_found}
    end.

reset() ->
    gen_server:call(?MODULE, {reset, all}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    erlang:process_flag(trap_exit, true),
    ok = emqx_utils_ets:new(?TAB, [{read_concurrency, true}]),
    erlang:send_after(timer:seconds(?TICKING_INTERVAL), self(), ticking),
    Fun =
        fun(#{topic := Topic}, CurrentSpeeds) ->
            case do_register(Topic, CurrentSpeeds) of
                {ok, NSpeeds} ->
                    NSpeeds;
                {error, already_existed} ->
                    CurrentSpeeds;
                {error, quota_exceeded} ->
                    error("max topic metrics quota exceeded")
            end
        end,
    ?tp(debug, emqx_topic_metrics_started, #{}),
    {ok, #state{speeds = lists:foldl(Fun, #{}, Opts)}, hibernate}.

handle_call({register, Topic}, _From, State = #state{speeds = Speeds}) ->
    case do_register(Topic, Speeds) of
        {ok, NSpeeds} ->
            {reply, ok, State#state{speeds = NSpeeds}};
        Error ->
            {reply, Error, State}
    end;
handle_call({deregister, all}, _From, State) ->
    true = ets:delete_all_objects(?TAB),
    {reply, ok, State#state{speeds = #{}}};
handle_call({deregister, Topic}, _From, State = #state{speeds = Speeds}) ->
    case is_registered(Topic) of
        false ->
            {reply, {error, topic_not_found}, State};
        true ->
            true = ets:delete(?TAB, Topic),
            NSpeeds = lists:foldl(
                fun(Metric, Acc) ->
                    maps:remove({Topic, Metric}, Acc)
                end,
                Speeds,
                ?TOPIC_METRICS
            ),
            {reply, ok, State#state{speeds = NSpeeds}}
    end;
handle_call({reset, all}, _From, State = #state{speeds = Speeds}) ->
    Fun =
        fun(T, NSpeeds) ->
            reset_topic(T, NSpeeds)
        end,
    {reply, ok, State#state{speeds = lists:foldl(Fun, Speeds, ets:tab2list(?TAB))}};
handle_call({reset, Topic}, _From, State = #state{speeds = Speeds}) ->
    NSpeeds = reset_topic(Topic, Speeds),
    {reply, ok, State#state{speeds = NSpeeds}};
handle_call({get_rates, Topic, Metric}, _From, State = #state{speeds = Speeds}) ->
    case is_registered(Topic) of
        false ->
            {reply, {error, topic_not_found}, State};
        true ->
            case maps:get({Topic, Metric}, Speeds, undefined) of
                undefined ->
                    {reply, {error, invalid_metric}, State};
                #speed{last = Short, last_medium = Medium, last_long = Long} ->
                    {reply, #{short => Short, medium => Medium, long => Long}, State}
            end
    end.

handle_cast(Msg, State) ->
    ?tp(error, emqx_topic_metrics_unexpected_cast, #{cast => Msg}),
    {noreply, State}.

handle_info(ticking, State = #state{speeds = Speeds}) ->
    NSpeeds = maps:map(
        fun({Topic, Metric}, Speed) ->
            case val(Topic, Metric) of
                {error, topic_not_found} -> maps:remove({Topic, Metric}, Speeds);
                Val -> calculate_speed(Val, Speed)
            end
        end,
        Speeds
    ),
    erlang:send_after(timer:seconds(?TICKING_INTERVAL), self(), ticking),
    {noreply, State#state{speeds = NSpeeds}};
handle_info(Info, State) ->
    ?tp(error, emqx_topic_metrics_unexpected_info, #{info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

reset_topic({Topic, Data}, Speeds) ->
    CRef = maps:get(counter_ref, Data),
    ok = reset_counter(CRef),
    ResetTime = emqx_utils_calendar:now_to_rfc3339(),
    true = ets:insert(?TAB, {Topic, Data#{reset_time => ResetTime}}),
    Fun =
        fun(Metric, CurrentSpeeds) ->
            maps:put({Topic, Metric}, #speed{}, CurrentSpeeds)
        end,
    lists:foldl(Fun, Speeds, ?TOPIC_METRICS);
reset_topic(Topic, Speeds) ->
    T = hd(ets:lookup(?TAB, Topic)),
    reset_topic(T, Speeds).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
do_register(Topic, Speeds) ->
    case is_registered(Topic) of
        true ->
            {error, already_existed};
        false ->
            case {number_of_registered_topics() < ?MAX_TOPICS, emqx_topic:wildcard(Topic)} of
                {_, true} ->
                    {error, bad_topic};
                {false, _} ->
                    {error, quota_exceeded};
                {true, false} ->
                    CreateTime = emqx_rule_funcs:now_rfc3339(),
                    CRef = counters:new(counters_size(), [write_concurrency]),
                    ok = reset_counter(CRef),
                    Data = #{
                        counter_ref => CRef,
                        create_time => CreateTime
                    },
                    true = ets:insert(?TAB, {Topic, Data}),
                    NSpeeds = lists:foldl(
                        fun(Metric, Acc) ->
                            maps:put({Topic, Metric}, #speed{}, Acc)
                        end,
                        Speeds,
                        ?TOPIC_METRICS
                    ),
                    {ok, NSpeeds}
            end
    end.

format({Topic, Data}) ->
    CRef = maps:get(counter_ref, Data),
    Fun =
        fun(Key, Metrics) ->
            CounterKey = to_count(Key),
            Counter = counters:get(CRef, metric_idx(Key)),
            RateKey = to_rate(Key),
            Rate = emqx_rule_funcs:float(rate(Topic, Key), 4),
            maps:put(RateKey, Rate, maps:put(CounterKey, Counter, Metrics))
        end,
    Metrics = lists:foldl(Fun, #{}, ?TOPIC_METRICS),
    CreateTime = maps:get(create_time, Data),
    TopicMetrics = #{
        topic => Topic,
        metrics => Metrics,
        create_time => CreateTime
    },
    case maps:get(reset_time, Data, undefined) of
        undefined ->
            TopicMetrics;
        ResetTime ->
            TopicMetrics#{reset_time => ResetTime}
    end.

try_inc(Topic, Metric) ->
    _ = inc(Topic, Metric),
    ok.

inc(Topic, Metric) ->
    inc(Topic, Metric, 1).

inc(Topic, Metric, Val) ->
    case get_counters(Topic) of
        {error, topic_not_found} ->
            {error, topic_not_found};
        CRef ->
            case metric_idx(Metric) of
                {error, invalid_metric} ->
                    {error, invalid_metric};
                Idx ->
                    counters:add(CRef, Idx, Val)
            end
    end.

val(Topic, Metric) ->
    case ets:lookup(?TAB, Topic) of
        [] ->
            {error, topic_not_found};
        [{Topic, Data}] ->
            CRef = maps:get(counter_ref, Data),
            case metric_idx(Metric) of
                {error, invalid_metric} ->
                    {error, invalid_metric};
                Idx ->
                    counters:get(CRef, Idx)
            end
    end.

rate(Topic, Metric) ->
    case gen_server:call(?MODULE, {get_rates, Topic, Metric}) of
        #{short := Last} ->
            Last;
        {error, Reason} ->
            {error, Reason}
    end.

metric_idx('messages.in') -> 01;
metric_idx('messages.out') -> 02;
metric_idx('messages.qos0.in') -> 03;
metric_idx('messages.qos0.out') -> 04;
metric_idx('messages.qos1.in') -> 05;
metric_idx('messages.qos1.out') -> 06;
metric_idx('messages.qos2.in') -> 07;
metric_idx('messages.qos2.out') -> 08;
metric_idx('messages.dropped') -> 09;
metric_idx(_) -> {error, invalid_metric}.

to_count('messages.in') ->
    'messages.in.count';
to_count('messages.out') ->
    'messages.out.count';
to_count('messages.qos0.in') ->
    'messages.qos0.in.count';
to_count('messages.qos0.out') ->
    'messages.qos0.out.count';
to_count('messages.qos1.in') ->
    'messages.qos1.in.count';
to_count('messages.qos1.out') ->
    'messages.qos1.out.count';
to_count('messages.qos2.in') ->
    'messages.qos2.in.count';
to_count('messages.qos2.out') ->
    'messages.qos2.out.count';
to_count('messages.dropped') ->
    'messages.dropped.count'.

to_rate('messages.in') ->
    'messages.in.rate';
to_rate('messages.out') ->
    'messages.out.rate';
to_rate('messages.qos0.in') ->
    'messages.qos0.in.rate';
to_rate('messages.qos0.out') ->
    'messages.qos0.out.rate';
to_rate('messages.qos1.in') ->
    'messages.qos1.in.rate';
to_rate('messages.qos1.out') ->
    'messages.qos1.out.rate';
to_rate('messages.qos2.in') ->
    'messages.qos2.in.rate';
to_rate('messages.qos2.out') ->
    'messages.qos2.out.rate';
to_rate('messages.dropped') ->
    'messages.dropped.rate'.

reset_counter(CRef) ->
    [counters:put(CRef, Idx, 0) || Idx <- lists:seq(1, counters_size())],
    ok.

get_counters(Topic) ->
    case ets:lookup(?TAB, Topic) of
        [] -> {error, topic_not_found};
        [{Topic, Data}] -> maps:get(counter_ref, Data)
    end.

counters_size() ->
    length(?TOPIC_METRICS).

number_of_registered_topics() ->
    ets:info(?TAB, size).

calculate_speed(CurVal, #speed{
    last = Last,
    last_v = LastVal,
    last_medium = LastMedium,
    last_long = LastLong
}) ->
    %% calculate the current speed based on the last value of the counter
    CurSpeed = (CurVal - LastVal) / ?TICKING_INTERVAL,
    #speed{
        last_v = CurVal,
        last = short_mma(Last, CurSpeed),
        last_medium = medium_mma(LastMedium, CurSpeed),
        last_long = long_mma(LastLong, CurSpeed)
    }.

%% Modified Moving Average ref: https://en.wikipedia.org/wiki/Moving_average
mma(WindowSize, LastSpeed, CurSpeed) ->
    (LastSpeed * (WindowSize - 1) + CurSpeed) / WindowSize.

short_mma(LastSpeed, CurSpeed) ->
    mma(?SPEED_AVERAGE_WINDOW_SIZE, LastSpeed, CurSpeed).

medium_mma(LastSpeed, CurSpeed) ->
    mma(?SPEED_MEDIUM_WINDOW_SIZE, LastSpeed, CurSpeed).

long_mma(LastSpeed, CurSpeed) ->
    mma(?SPEED_LONG_WINDOW_SIZE, LastSpeed, CurSpeed).
