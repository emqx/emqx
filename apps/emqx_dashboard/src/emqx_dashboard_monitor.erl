%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(gen_server).

-boot_mnesia({mnesia, [boot]}).

-export([ start_link/0]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ mnesia/1]).

-export([ samplers/0
        , samplers/2
        , current_rate/0
        , current_rate/1
        , granularity_adapter/1
        ]).

%% for rpc
-export([ do_sample/2]).

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

mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {local_content, true},
        {storage, disc_copies},
        {record_name, emqx_monit},
        {attributes, record_info(fields, emqx_monit)}]).

%% -------------------------------------------------------------------------------------------------
%% API

samplers() ->
    format(do_sample(all, infinity)).

samplers(NodeOrCluster, Latest) ->
    Now = erlang:system_time(millisecond),
    MatchTime = Now - (Latest * 1000),
    case format(do_sample(NodeOrCluster, MatchTime)) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        List when is_list(List) ->
            granularity_adapter(List)
    end.

granularity_adapter(List) when length(List) > 100 ->
    granularity_adapter(List, []);
granularity_adapter(List) ->
    List.

current_rate() ->
    Fun =
        fun(Node, Cluster) ->
            case current_rate(Node) of
                {ok, CurrentRate} ->
                    merge_cluster_rate(CurrentRate, Cluster);
                {badrpc, Reason} ->
                    {badrpc, {Node, Reason}}
            end
        end,
    lists:foldl(Fun, #{}, mria_mnesia:cluster_nodes(running)).

current_rate(all) ->
    current_rate();
current_rate(Node) when Node == node() ->
    do_call(current_rate);
current_rate(Node) ->
    case rpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Node], 5000) of
        {badrpc, Reason} ->
            {badrpc, {Node, Reason}};
        {ok, Rate} ->
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
    {reply, {ok, Rate}, State};
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({sample, Time}, State = #state{last = Last}) ->
    Now = sample(Time),
    {atomic, ok} = flush(Last, Now),
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

do_sample(all, MatchTime) ->
    Fun =
        fun(Node, All) ->
            case do_sample(Node, MatchTime) of
                {badrpc, Reason} ->
                    {badrpc, {Node, Reason}};
                NodeSamplers ->
                    merge_cluster_samplers(NodeSamplers, All)
            end
        end,
    lists:foldl(Fun, #{}, mria_mnesia:cluster_nodes(running));
do_sample(Node, MatchTime) when Node == node() ->
    MS = match_spec(MatchTime),
    internal_format(ets:select(?TAB, MS));
do_sample(Node, MatchTime) ->
    rpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Node, MatchTime], 5000).

match_spec(infinity) ->
    [{'$1',[],['$1']}];
match_spec(MatchTime) ->
    [{{'_', '$1', '_'}, [{'>=', '$1', MatchTime}], ['$_']}].

merge_cluster_samplers(Node, Cluster) ->
    maps:fold(fun merge_cluster_samplers/3, Cluster, Node).

merge_cluster_samplers(TS, NodeData, Cluster) ->
    case maps:get(TS, Cluster, undefined) of
        undefined ->
            Cluster#{TS => NodeData};
        ClusterData ->
            Cluster#{TS => count_map(NodeData, ClusterData)}
    end.

merge_cluster_rate(Node, Cluster) ->
    Fun =
        fun(Key, Value, NCluster) ->
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

cal_rate( #emqx_monit{data = NowData, time = NowTime}
        , #emqx_monit{data = LastData, time = LastTime}) ->
    TimeDelta = NowTime - LastTime,
    Filter = fun(Key, _) -> lists:member(Key, ?GAUGE_SAMPLER_LIST) end,
    Gauge = maps:filter(Filter, NowData),
    {_, _, _, Rate} =
        lists:foldl(fun cal_rate_/2, {NowData, LastData, TimeDelta, Gauge}, ?DELTA_SAMPLER_LIST),
    Rate.

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
        fun(Key, Res) ->
            maps:put(Key, value(Key), Res)
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
    lists:foreach(fun(Data) ->
        true = ets:delete_object(?TAB, Data)
    end, Expired),
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

count_map(M1, M2) ->
    Fun =
        fun(Key, Map) ->
            Map#{key => maps:get(Key, M1) + maps:get(Key, M2)}
        end,
    lists:foldl(Fun, #{}, ?SAMPLER_LIST).

value(connections) -> emqx_stats:getstat('connections.count');
value(routes) -> emqx_stats:getstat('routes.count');
value(subscriptions) -> emqx_stats:getstat('subscriptions.count');
value(received) -> emqx_metrics:val('messages.received');
value(received_bytes) -> emqx_metrics:val('bytes.received');
value(sent) -> emqx_metrics:val('messages.sent');
value(sent_bytes) -> emqx_metrics:val('bytes.sent');
value(dropped) -> emqx_metrics:val('messages.dropped').
