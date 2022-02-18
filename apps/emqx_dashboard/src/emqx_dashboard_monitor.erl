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

-include_lib("stdlib/include/ms_transform.hrl").

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

-export([ samples/0
        , samples/1
        , aggregate_samplers/0
        ]).

-define(TAB, ?MODULE).

%% 10 seconds
-define(DEFAULT_INTERVAL, 10).

-ifdef(TEST).
%% for test
-define(CLEAN_EXPIRED_INTERVAL, 2 * 1000).
-define(RETENTION_TIME, 3 * 1000).
-define(DEFAULT_GET_DATA_TIME, 5* 1000).

-else.

%% 1 hour = 60 * 60 * 1000 milliseconds
-define(CLEAN_EXPIRED_INTERVAL, 60 * 60 * 1000).
%% 7 days = 7 * 24 * 60 * 60 * 1000 milliseconds
-define(RETENTION_TIME, 7 * 24 * 60 * 60 * 1000).
%% 1 day = 60 * 60 * 1000 milliseconds
-define(DEFAULT_GET_DATA_TIME, 60 * 60 * 1000).

-endif.

-record(state, {
    last
    }).

-record(emqx_monit, {
    time :: integer(),
    data :: map()
    }).


-define(DELTA_LIST,
    [ received
    , received_bytes
    , sent
    , sent_bytes
    , dropped
    ]).

-define(SAMPLER_LIST,
    [ subscriptions
    , routes
    , connections
    ] ++ ?DELTA_LIST).

mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {local_content, true},
        {storage, disc_copies},
        {record_name, emqx_monit},
        {attributes, record_info(fields, emqx_monit)}]).

aggregate_samplers() ->
    [#{node => Node, data => samples(Node)} || Node <- mria_mnesia:cluster_nodes(running)].

samples() ->
    All = [samples(Node) || Node <- mria_mnesia:cluster_nodes(running)],
    lists:foldl(fun merge_cluster_samplers/2, #{}, All).

samples(Node) when Node == node() ->
    get_data(?DEFAULT_GET_DATA_TIME);
samples(Node) ->
    rpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Node]).

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
    Interval = emqx_conf:get([dashboard, monitor, interval], ?DEFAULT_INTERVAL) * 1000,
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
    lists:foldl(Fun, NowData, ?DELTA_LIST).

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

get_data(PastTime) ->
    Now = erlang:system_time(millisecond),
    ExpiredMS = [{{'_', '$1', '_'}, [{'<', {'-', Now, '$1'}, PastTime}], ['$_']}],
    format(ets:select(?TAB, ExpiredMS)).

format(List) when is_list(List) ->
    Fun =
        fun(Data, All) ->
            maps:merge(format(Data), All)
        end,
    lists:foldl(Fun, #{}, List);
format(#emqx_monit{time = Time, data = Data}) ->
    #{Time => Data}.

merge_cluster_samplers(Node, Cluster) ->
    maps:fold(fun merge_cluster_samplers/3, Cluster, Node).

merge_cluster_samplers(TS, NodeData, Cluster) ->
    case maps:get(TS, Cluster, undefined) of
        undefined ->
            Cluster#{TS => NodeData};
        ClusterData ->
            Cluster#{TS => count_map(NodeData, ClusterData)}
    end.

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
