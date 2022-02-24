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
        , samplers/1
        , samplers/2
        ]).

%% for rpc
-export([ do_sample/1]).

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

samplers() ->
    samplers(all).

samplers(NodeOrCluster) ->
    format(do_sample(NodeOrCluster)).

samplers(NodeOrCluster, 0) ->
    samplers(NodeOrCluster);
samplers(NodeOrCluster, Latest) ->
    case samplers(NodeOrCluster) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        List when is_list(List) ->
            case erlang:length(List) - Latest of
                Start when Start > 0 ->
                    lists:sublist(List, Start, Latest);
                _ ->
                    List
            end
    end.

%%%===================================================================
%%% gen_server functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    sample_timer(),
    clean_timer(),
    {ok, #state{last = undefined}}.

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_sample(all) ->
    Fun =
        fun(Node, All) ->
            case do_sample(Node) of
                {badrpc, Reason} ->
                    {badrpc, {Node, Reason}};
                NodeSamplers ->
                    merge_cluster_samplers(NodeSamplers, All)
            end
        end,
    lists:foldl(Fun, #{}, mria_mnesia:cluster_nodes(running));
do_sample(Node) when Node == node() ->
    ExpiredMS = [{'$1',[],['$1']}],
    internal_format(ets:select(?TAB, ExpiredMS));
do_sample(Node) ->
    rpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Node], 5000).

merge_cluster_samplers(Node, Cluster) ->
    maps:fold(fun merge_cluster_samplers/3, Cluster, Node).

merge_cluster_samplers(TS, NodeData, Cluster) ->
    case maps:get(TS, Cluster, undefined) of
        undefined ->
            Cluster#{TS => NodeData};
        ClusterData ->
            Cluster#{TS => count_map(NodeData, ClusterData)}
    end.

format({badrpc, Reason}) ->
    {badrpc, Reason};
format(Data) ->
    All = maps:fold(fun format/3, [], Data),
    Compare = fun(#{time_stamp := T1}, #{time_stamp := T2}) -> T1 =< T2 end,
    lists:sort(Compare, All).

format(TimeStamp, Data, All) ->
    [Data#{time_stamp => TimeStamp} | All].

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
