%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_monitor).

-include("emqx_dashboard.hrl").

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(gen_server).

-export([create_tables/0]).
-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    samplers/0,
    samplers/2,
    current_rate/1,
    granularity_adapter/1
]).

-ifdef(TEST).
-export([current_rate_cluster/0]).
-endif.

%% for rpc
-export([do_sample/2]).

-define(TAB, ?MODULE).

%% 1 hour = 60 * 60 * 1000 milliseconds
-define(CLEAN_EXPIRED_INTERVAL, 60 * 60 * 1000).
%% 7 days = 7 * 24 * 60 * 60 * 1000 milliseconds
-define(RETENTION_TIME, 7 * 24 * 60 * 60 * 1000).

-record(state, {
    last
}).

-record(emqx_monit, {
    time :: integer(),
    data :: map()
}).

create_tables() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {local_content, true},
        {storage, disc_copies},
        {record_name, emqx_monit},
        {attributes, record_info(fields, emqx_monit)}
    ]),
    [?TAB].

%% -------------------------------------------------------------------------------------------------
%% API

samplers() ->
    format(do_sample(all, infinity)).

samplers(NodeOrCluster, Latest) ->
    Time = latest2time(Latest),
    case format(do_sample(NodeOrCluster, Time)) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        List when is_list(List) ->
            granularity_adapter(List)
    end.

latest2time(infinity) -> infinity;
latest2time(Latest) -> erlang:system_time(millisecond) - (Latest * 1000).

%% When the number of samples exceeds 1000, it affects the rendering speed of dashboard UI.
%% granularity_adapter is an oversampling of the samples.
%% Use more granular data and reduce data density.
%%
%% [
%%   Data1 = #{time => T1, k1 => 1, k2 => 2},
%%   Data2 = #{time => T2, k1 => 3, k2 => 4},
%%   ...
%% ]
%% After granularity_adapter, Merge Data1 Data2
%%
%% [
%%   #{time => T2, k1 => 1 + 3, k2 =>  2 + 6},
%%   ...
%% ]
%%
granularity_adapter(List) when length(List) > 1000 ->
    granularity_adapter(List, []);
granularity_adapter(List) ->
    List.

current_rate(all) ->
    current_rate_cluster();
current_rate(Node) when Node == node() ->
    try
        {ok, Rate} = do_call(current_rate),
        {ok, Rate}
    catch
        _E:R ->
            ?SLOG(warning, #{msg => "dashboard_monitor_error", reason => R}),
            %% Rate map 0, ensure api will not crash.
            %% When joining cluster, dashboard monitor restart.
            Rate0 = [
                {Key, 0}
             || Key <- ?GAUGE_SAMPLER_LIST ++ maps:values(?DELTA_SAMPLER_RATE_MAP)
            ],
            {ok, maps:merge(maps:from_list(Rate0), non_rate_value())}
    end;
current_rate(Node) ->
    case emqx_dashboard_proto_v1:current_rate(Node) of
        {badrpc, Reason} ->
            {badrpc, {Node, Reason}};
        {ok, Rate} ->
            {ok, Rate}
    end.

%% Get the current rate. Not the current sampler data.
current_rate_cluster() ->
    Fun =
        fun
            (Node, Cluster) when is_map(Cluster) ->
                case current_rate(Node) of
                    {ok, CurrentRate} ->
                        merge_cluster_rate(CurrentRate, Cluster);
                    {badrpc, Reason} ->
                        {badrpc, {Node, Reason}}
                end;
            (_Node, Error) ->
                Error
        end,
    case lists:foldl(Fun, #{}, mria:cluster_nodes(running)) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        Rate ->
            {ok, Rate}
    end.

%% -------------------------------------------------------------------------------------------------
%% gen_server functions

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    sample_timer(),
    clean_timer(),
    {ok, #state{last = undefined}}.

handle_call(current_rate, _From, State = #state{last = Last}) ->
    NowTime = erlang:system_time(millisecond),
    NowSamplers = sample(NowTime),
    Rate = cal_rate(NowSamplers, Last),
    NonRateValue = non_rate_value(),
    Samples = maps:merge(Rate, NonRateValue),
    {reply, {ok, Samples}, State};
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({sample, Time}, State = #state{last = Last}) ->
    Now = sample(Time),
    {atomic, ok} = flush(Last, Now),
    ?tp(dashboard_monitor_flushed, #{}),
    sample_timer(),
    {noreply, State#state{last = Now}};
handle_info(clean_expired, State) ->
    clean(),
    clean_timer(),
    {noreply, State};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% Internal functions

do_call(Request) ->
    gen_server:call(?MODULE, Request, 5000).

do_sample(all, Time) ->
    do_sample(mria:cluster_nodes(running), Time, #{});
do_sample(Node, Time) when Node == node() ->
    MS = match_spec(Time),
    internal_format(ets:select(?TAB, MS));
do_sample(Node, Time) ->
    case emqx_dashboard_proto_v1:do_sample(Node, Time) of
        {badrpc, Reason} ->
            {badrpc, {Node, Reason}};
        Res ->
            Res
    end.

do_sample([], _Time, Res) ->
    Res;
do_sample([Node | Nodes], Time, Res) ->
    case do_sample(Node, Time) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        Samplers ->
            do_sample(Nodes, Time, merge_cluster_samplers(Samplers, Res))
    end.

match_spec(infinity) ->
    [{'$1', [], ['$1']}];
match_spec(Time) ->
    [{{'_', '$1', '_'}, [{'>=', '$1', Time}], ['$_']}].

merge_cluster_samplers(Node, Cluster) ->
    maps:fold(fun merge_cluster_samplers/3, Cluster, Node).

merge_cluster_samplers(TS, NodeData, Cluster) ->
    case maps:get(TS, Cluster, undefined) of
        undefined ->
            Cluster#{TS => NodeData};
        ClusterData ->
            Cluster#{TS => merge_cluster_sampler_map(NodeData, ClusterData)}
    end.

merge_cluster_sampler_map(M1, M2) ->
    Fun =
        fun
            (topics, Map) ->
                Map#{topics => maps:get(topics, M1)};
            (Key, Map) ->
                Map#{Key => maps:get(Key, M1, 0) + maps:get(Key, M2, 0)}
        end,
    lists:foldl(Fun, #{}, ?SAMPLER_LIST).

merge_cluster_rate(Node, Cluster) ->
    Fun =
        fun
            %% cluster-synced values
            (durable_subscriptions, V, NCluster) ->
                NCluster#{durable_subscriptions => V};
            (topics, V, NCluster) ->
                NCluster#{topics => V};
            (retained_msg_count, V, NCluster) ->
                NCluster#{retained_msg_count => V};
            (shared_subscriptions, V, NCluster) ->
                NCluster#{shared_subscriptions => V};
            (license_quota, V, NCluster) ->
                NCluster#{license_quota => V};
            %% for cluster sample, ignore node_uptime
            (node_uptime, _V, NCluster) ->
                NCluster;
            (Key, Value, NCluster) ->
                ClusterValue = maps:get(Key, NCluster, 0),
                NCluster#{Key => Value + ClusterValue}
        end,
    maps:fold(Fun, Cluster, Node).

format({badrpc, Reason}) ->
    {badrpc, Reason};
format(Data) ->
    All = maps:fold(fun format/3, [], Data),
    Compare = fun(#{time_stamp := T1}, #{time_stamp := T2}) -> T1 =< T2 end,
    lists:sort(Compare, All).

format(TimeStamp, Data, All) ->
    [Data#{time_stamp => TimeStamp} | All].

cal_rate(_Now, undefined) ->
    AllSamples = ?GAUGE_SAMPLER_LIST ++ maps:values(?DELTA_SAMPLER_RATE_MAP),
    lists:foldl(fun(Key, Acc) -> Acc#{Key => 0} end, #{}, AllSamples);
cal_rate(
    #emqx_monit{data = NowData, time = NowTime},
    #emqx_monit{data = LastData, time = LastTime} = Last
) ->
    case NowTime - LastTime of
        0 ->
            %% make sure: not divide by zero
            timer:sleep(5),
            NewSamplers = sample(erlang:system_time(millisecond)),
            cal_rate(NewSamplers, Last);
        TimeDelta ->
            Filter = fun(Key, _) -> lists:member(Key, ?GAUGE_SAMPLER_LIST) end,
            Gauge = maps:filter(Filter, NowData),
            {_, _, _, Rate} =
                lists:foldl(
                    fun cal_rate_/2,
                    {NowData, LastData, TimeDelta, Gauge},
                    ?DELTA_SAMPLER_LIST
                ),
            Rate
    end.

cal_rate_(Key, {Now, Last, TDelta, Res}) ->
    NewValue = maps:get(Key, Now),
    LastValue = maps:get(Key, Last),
    Rate = ((NewValue - LastValue) * 1000) div TDelta,
    RateKey = maps:get(Key, ?DELTA_SAMPLER_RATE_MAP),
    {Now, Last, TDelta, Res#{RateKey => Rate}}.

granularity_adapter([], Res) ->
    lists:reverse(Res);
granularity_adapter([Sampler], Res) ->
    granularity_adapter([], [Sampler | Res]);
granularity_adapter([Sampler1, Sampler2 | Rest], Res) ->
    Fun =
        fun(Key, M) ->
            Value1 = maps:get(Key, Sampler1),
            Value2 = maps:get(Key, Sampler2),
            M#{Key => Value1 + Value2}
        end,
    granularity_adapter(Rest, [lists:foldl(Fun, Sampler2, ?DELTA_SAMPLER_LIST) | Res]).

%% -------------------------------------------------------------------------------------------------
%% timer

sample_timer() ->
    {NextTime, Remaining} = next_interval(),
    erlang:send_after(Remaining, self(), {sample, NextTime}).

clean_timer() ->
    erlang:send_after(?CLEAN_EXPIRED_INTERVAL, self(), clean_expired).

%% Per interval seconds.
%% As an example:
%%  Interval = 10
%%  The monitor will start working at full seconds, as like 00:00:00, 00:00:10, 00:00:20 ...
%% Ensure that the monitor data of all nodes in the cluster are aligned in time
next_interval() ->
    Interval = emqx_conf:get([dashboard, sample_interval], ?DEFAULT_SAMPLE_INTERVAL) * 1000,
    Now = erlang:system_time(millisecond),
    NextTime = ((Now div Interval) + 1) * Interval,
    Remaining = NextTime - Now,
    {NextTime, Remaining}.

%% -------------------------------------------------------------------------------------------------
%% data

sample(Time) ->
    Fun =
        fun(Key, Acc) ->
            Acc#{Key => getstats(Key)}
        end,
    Data = lists:foldl(Fun, #{}, ?SAMPLER_LIST),
    #emqx_monit{time = Time, data = Data}.

flush(_Last = undefined, Now) ->
    store(Now);
flush(_Last = #emqx_monit{data = LastData}, Now = #emqx_monit{data = NowData}) ->
    Store = Now#emqx_monit{data = delta(LastData, NowData)},
    store(Store).

delta(LastData, NowData) ->
    Fun =
        fun(Key, Data) ->
            Value = maps:get(Key, NowData) - maps:get(Key, LastData),
            Data#{Key => Value}
        end,
    lists:foldl(Fun, NowData, ?DELTA_SAMPLER_LIST).

store(MonitData) ->
    {atomic, ok} =
        mria:transaction(mria:local_content_shard(), fun mnesia:write/3, [?TAB, MonitData, write]).

clean() ->
    Now = erlang:system_time(millisecond),
    ExpiredMS = [{{'_', '$1', '_'}, [{'>', {'-', Now, '$1'}, ?RETENTION_TIME}], ['$_']}],
    Expired = ets:select(?TAB, ExpiredMS),
    lists:foreach(
        fun(Data) ->
            true = ets:delete_object(?TAB, Data)
        end,
        Expired
    ),
    ok.

%% To make it easier to do data aggregation
internal_format(List) when is_list(List) ->
    Fun =
        fun(Data, All) ->
            maps:merge(internal_format(Data), All)
        end,
    lists:foldl(Fun, #{}, List);
internal_format(#emqx_monit{time = Time, data = Data}) ->
    #{Time => Data}.

getstats(Key) ->
    %% Stats ets maybe not exist when ekka join.
    try
        stats(Key)
    catch
        _:_ -> 0
    end.

stats(connections) -> emqx_stats:getstat('connections.count');
stats(durable_subscriptions) -> emqx_stats:getstat('durable_subscriptions.count');
stats(live_connections) -> emqx_stats:getstat('live_connections.count');
stats(cluster_sessions) -> emqx_stats:getstat('cluster_sessions.count');
stats(topics) -> emqx_stats:getstat('topics.count');
stats(subscriptions) -> emqx_stats:getstat('subscriptions.count');
stats(shared_subscriptions) -> emqx_stats:getstat('subscriptions.shared.count');
stats(retained_msg_count) -> emqx_stats:getstat('retained.count');
stats(received) -> emqx_metrics:val('messages.received');
stats(received_bytes) -> emqx_metrics:val('bytes.received');
stats(sent) -> emqx_metrics:val('messages.sent');
stats(sent_bytes) -> emqx_metrics:val('bytes.sent');
stats(validation_succeeded) -> emqx_metrics:val('messages.validation_succeeded');
stats(validation_failed) -> emqx_metrics:val('messages.validation_failed');
stats(dropped) -> emqx_metrics:val('messages.dropped');
stats(persisted) -> emqx_metrics:val('messages.persisted').

%% -------------------------------------------------------------------------------------------------
%% Retained && License Quota

%% the non rate values should be same on all nodes
non_rate_value() ->
    (license_quota())#{
        retained_msg_count => stats(retained_msg_count),
        shared_subscriptions => stats(shared_subscriptions),
        node_uptime => emqx_sys:uptime()
    }.

-if(?EMQX_RELEASE_EDITION == ee).
license_quota() ->
    case emqx_license_checker:limits() of
        {ok, #{max_connections := Quota}} ->
            #{license_quota => Quota};
        {error, no_license} ->
            #{license_quota => 0}
    end.
-else.
license_quota() ->
    #{}.
-endif.
