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

-module(emqx_mgmt).

-include("emqx_mgmt.hrl").
-elvis([{elvis_style, invalid_dynamic_call, disable}]).
-elvis([{elvis_style, god_modules, disable}]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% Nodes and Brokers API
-export([
    list_nodes/0,
    lookup_node/1,
    list_brokers/0,
    lookup_broker/1,
    node_info/0,
    node_info/1,
    broker_info/0,
    broker_info/1
]).

%% Metrics and Stats
-export([
    get_metrics/0,
    get_metrics/1,
    get_stats/0,
    get_stats/1
]).

%% Clients, Sessions
-export([
    lookup_client/2,
    lookup_client/3,
    kickout_client/1,
    list_authz_cache/1,
    list_client_subscriptions/1,
    client_subscriptions/2,
    clean_authz_cache/1,
    clean_authz_cache/2,
    clean_authz_cache_all/0,
    clean_authz_cache_all/1,
    clean_pem_cache_all/0,
    clean_pem_cache_all/1,
    set_ratelimit_policy/2,
    set_quota_policy/2,
    set_keepalive/2
]).

%% Internal functions
-export([do_call_client/2]).

%% Subscriptions
-export([
    list_subscriptions/1,
    list_subscriptions_via_topic/2,
    list_subscriptions_via_topic/3,
    lookup_subscriptions/1,
    lookup_subscriptions/2,

    do_list_subscriptions/0
]).

%% PubSub
-export([
    subscribe/2,
    do_subscribe/2,
    publish/1,
    unsubscribe/2,
    do_unsubscribe/2,
    unsubscribe_batch/2,
    do_unsubscribe_batch/2
]).

%% Alarms
-export([
    get_alarms/1,
    get_alarms/2,
    deactivate/2,
    delete_all_deactivated_alarms/0,
    delete_all_deactivated_alarms/1
]).

%% Banned
-export([
    create_banned/1,
    delete_banned/1
]).

%% Common Table API
-export([max_row_limit/0]).

-define(APP, emqx_management).

-elvis([{elvis_style, god_modules, disable}]).

%%--------------------------------------------------------------------
%% Node Info
%%--------------------------------------------------------------------

list_nodes() ->
    Running = mria_mnesia:cluster_nodes(running),
    Stopped = mria_mnesia:cluster_nodes(stopped),
    DownNodes = lists:map(fun stopped_node_info/1, Stopped),
    [{Node, node_info(Node)} || Node <- Running] ++ DownNodes.

lookup_node(Node) -> node_info(Node).

node_info() ->
    {UsedRatio, Total} = get_sys_memory(),
    Info = maps:from_list([{K, list_to_binary(V)} || {K, V} <- emqx_vm:loads()]),
    BrokerInfo = emqx_sys:info(),
    Info#{
        node => node(),
        otp_release => otp_rel(),
        memory_total => Total,
        memory_used => erlang:round(Total * UsedRatio),
        process_available => erlang:system_info(process_limit),
        process_used => erlang:system_info(process_count),

        max_fds => proplists:get_value(
            max_fds, lists:usort(lists:flatten(erlang:system_info(check_io)))
        ),
        connections => ets:info(emqx_channel, size),
        node_status => 'Running',
        uptime => proplists:get_value(uptime, BrokerInfo),
        version => iolist_to_binary(proplists:get_value(version, BrokerInfo)),
        role => mria_rlog:role()
    }.

get_sys_memory() ->
    case os:type() of
        {unix, linux} ->
            load_ctl:get_sys_memory();
        _ ->
            {0, 0}
    end.

node_info(Node) ->
    wrap_rpc(emqx_management_proto_v2:node_info(Node)).

stopped_node_info(Node) ->
    #{name => Node, node_status => 'Stopped'}.

%%--------------------------------------------------------------------
%% Brokers
%%--------------------------------------------------------------------

list_brokers() ->
    [{Node, broker_info(Node)} || Node <- mria_mnesia:running_nodes()].

lookup_broker(Node) ->
    broker_info(Node).

broker_info() ->
    Info = maps:from_list([{K, iolist_to_binary(V)} || {K, V} <- emqx_sys:info()]),
    Info#{node => node(), otp_release => otp_rel(), node_status => 'Running'}.

broker_info(Node) ->
    wrap_rpc(emqx_management_proto_v2:broker_info(Node)).

%%--------------------------------------------------------------------
%% Metrics and Stats
%%--------------------------------------------------------------------

get_metrics() ->
    nodes_info_count([get_metrics(Node) || Node <- mria_mnesia:running_nodes()]).

get_metrics(Node) ->
    wrap_rpc(emqx_proto_v1:get_metrics(Node)).

get_stats() ->
    GlobalStatsKeys =
        [
            'retained.count',
            'retained.max',
            'topics.count',
            'topics.max',
            'subscriptions.shared.count',
            'subscriptions.shared.max'
        ],
    CountStats = nodes_info_count([
        begin
            Stats = get_stats(Node),
            delete_keys(Stats, GlobalStatsKeys)
        end
     || Node <- mria_mnesia:running_nodes()
    ]),
    GlobalStats = maps:with(GlobalStatsKeys, maps:from_list(get_stats(node()))),
    maps:merge(CountStats, GlobalStats).

delete_keys(List, []) ->
    List;
delete_keys(List, [Key | Keys]) ->
    delete_keys(proplists:delete(Key, List), Keys).

get_stats(Node) ->
    wrap_rpc(emqx_proto_v1:get_stats(Node)).

nodes_info_count(PropList) ->
    NodeCount =
        fun({Key, Value}, Result) ->
            Count = maps:get(Key, Result, 0),
            Result#{Key => Count + Value}
        end,
    AllCount =
        fun(StatsMap, Result) ->
            lists:foldl(NodeCount, Result, StatsMap)
        end,
    lists:foldl(AllCount, #{}, PropList).

%%--------------------------------------------------------------------
%% Clients
%%--------------------------------------------------------------------

lookup_client({clientid, ClientId}, FormatFun) ->
    lists:append([
        lookup_client(Node, {clientid, ClientId}, FormatFun)
     || Node <- mria_mnesia:running_nodes()
    ]);
lookup_client({username, Username}, FormatFun) ->
    lists:append([
        lookup_client(Node, {username, Username}, FormatFun)
     || Node <- mria_mnesia:running_nodes()
    ]).

lookup_client(Node, Key, {M, F}) ->
    case wrap_rpc(emqx_cm_proto_v1:lookup_client(Node, Key)) of
        {error, Err} ->
            {error, Err};
        L ->
            lists:map(
                fun({Chan, Info0, Stats}) ->
                    Info = Info0#{node => Node},
                    M:F({Chan, Info, Stats})
                end,
                L
            )
    end.

kickout_client({ClientID, FormatFun}) ->
    case lookup_client({clientid, ClientID}, FormatFun) of
        [] ->
            {error, not_found};
        _ ->
            Results = [kickout_client(Node, ClientID) || Node <- mria_mnesia:running_nodes()],
            check_results(Results)
    end.

kickout_client(Node, ClientId) ->
    wrap_rpc(emqx_cm_proto_v1:kickout_client(Node, ClientId)).

list_authz_cache(ClientId) ->
    call_client(ClientId, list_authz_cache).

list_client_subscriptions(ClientId) ->
    Results = [client_subscriptions(Node, ClientId) || Node <- mria_mnesia:running_nodes()],
    Filter =
        fun
            ({error, _}) ->
                false;
            ({_Node, List}) ->
                erlang:is_list(List) andalso 0 < erlang:length(List)
        end,
    case lists:filter(Filter, Results) of
        [] -> [];
        [Result | _] -> Result
    end.

client_subscriptions(Node, ClientId) ->
    {Node, wrap_rpc(emqx_broker_proto_v1:list_client_subscriptions(Node, ClientId))}.

clean_authz_cache(ClientId) ->
    Results = [clean_authz_cache(Node, ClientId) || Node <- mria_mnesia:running_nodes()],
    check_results(Results).

clean_authz_cache(Node, ClientId) ->
    wrap_rpc(emqx_proto_v1:clean_authz_cache(Node, ClientId)).

clean_authz_cache_all() ->
    Results = [{Node, clean_authz_cache_all(Node)} || Node <- mria_mnesia:running_nodes()],
    wrap_results(Results).

clean_pem_cache_all() ->
    Results = [{Node, clean_pem_cache_all(Node)} || Node <- mria_mnesia:running_nodes()],
    wrap_results(Results).

wrap_results(Results) ->
    case lists:filter(fun({_Node, Item}) -> Item =/= ok end, Results) of
        [] -> ok;
        BadNodes -> {error, BadNodes}
    end.

clean_authz_cache_all(Node) ->
    wrap_rpc(emqx_proto_v1:clean_authz_cache(Node)).

clean_pem_cache_all(Node) ->
    wrap_rpc(emqx_proto_v1:clean_pem_cache(Node)).

set_ratelimit_policy(ClientId, Policy) ->
    call_client(ClientId, {ratelimit, Policy}).

set_quota_policy(ClientId, Policy) ->
    call_client(ClientId, {quota, Policy}).

set_keepalive(ClientId, Interval) when Interval >= 0 andalso Interval =< 65535 ->
    call_client(ClientId, {keepalive, Interval});
set_keepalive(_ClientId, _Interval) ->
    {error, <<"mqtt3.1.1 specification: keepalive must between 0~65535">>}.

%% @private
call_client(ClientId, Req) ->
    Results = [call_client(Node, ClientId, Req) || Node <- mria_mnesia:running_nodes()],
    Expected = lists:filter(
        fun
            ({error, _}) -> false;
            (_) -> true
        end,
        Results
    ),
    case Expected of
        [] -> {error, not_found};
        [Result | _] -> Result
    end.

%% @private
-spec do_call_client(emqx_types:clientid(), term()) -> term().
do_call_client(ClientId, Req) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            {error, not_found};
        Pids when is_list(Pids) ->
            Pid = lists:last(Pids),
            case emqx_cm:get_chan_info(ClientId, Pid) of
                #{conninfo := #{conn_mod := ConnMod}} ->
                    erlang:apply(ConnMod, call, [Pid, Req]);
                undefined ->
                    {error, not_found}
            end
    end.

%% @private
call_client(Node, ClientId, Req) ->
    wrap_rpc(emqx_management_proto_v2:call_client(Node, ClientId, Req)).

%%--------------------------------------------------------------------
%% Subscriptions
%%--------------------------------------------------------------------

-spec do_list_subscriptions() -> [map()].
do_list_subscriptions() ->
    case check_row_limit([mqtt_subproperty]) of
        false ->
            throw(max_row_limit);
        ok ->
            [
                #{topic => Topic, clientid => ClientId, options => Options}
             || {{Topic, ClientId}, Options} <- ets:tab2list(mqtt_subproperty)
            ]
    end.

list_subscriptions(Node) ->
    wrap_rpc(emqx_management_proto_v2:list_subscriptions(Node)).

list_subscriptions_via_topic(Topic, FormatFun) ->
    lists:append([
        list_subscriptions_via_topic(Node, Topic, FormatFun)
     || Node <- mria_mnesia:running_nodes()
    ]).

list_subscriptions_via_topic(Node, Topic, _FormatFun = {M, F}) ->
    case wrap_rpc(emqx_broker_proto_v1:list_subscriptions_via_topic(Node, Topic)) of
        {error, Reason} -> {error, Reason};
        Result -> M:F(Result)
    end.

lookup_subscriptions(ClientId) ->
    lists:append([lookup_subscriptions(Node, ClientId) || Node <- mria_mnesia:running_nodes()]).

lookup_subscriptions(Node, ClientId) ->
    wrap_rpc(emqx_broker_proto_v1:list_client_subscriptions(Node, ClientId)).

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

subscribe(ClientId, TopicTables) ->
    subscribe(mria_mnesia:running_nodes(), ClientId, TopicTables).

subscribe([Node | Nodes], ClientId, TopicTables) ->
    case wrap_rpc(emqx_management_proto_v2:subscribe(Node, ClientId, TopicTables)) of
        {error, _} -> subscribe(Nodes, ClientId, TopicTables);
        {subscribe, Res} -> {subscribe, Res, Node}
    end;
subscribe([], _ClientId, _TopicTables) ->
    {error, channel_not_found}.

-spec do_subscribe(emqx_types:clientid(), emqx_types:topic_filters()) ->
    {subscribe, _} | {error, atom()}.
do_subscribe(ClientId, TopicTables) ->
    case ets:lookup(emqx_channel, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {subscribe, TopicTables}
    end.

publish(Msg) ->
    emqx_metrics:inc_msg(Msg),
    emqx:publish(Msg).

-spec unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe(ClientId, Topic) ->
    unsubscribe(mria_mnesia:running_nodes(), ClientId, Topic).

-spec unsubscribe([node()], emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe([Node | Nodes], ClientId, Topic) ->
    case wrap_rpc(emqx_management_proto_v2:unsubscribe(Node, ClientId, Topic)) of
        {error, _} -> unsubscribe(Nodes, ClientId, Topic);
        Re -> Re
    end;
unsubscribe([], _ClientId, _Topic) ->
    {error, channel_not_found}.

-spec do_unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, _}.
do_unsubscribe(ClientId, Topic) ->
    case ets:lookup(emqx_channel, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {unsubscribe, [emqx_topic:parse(Topic)]}
    end.

-spec unsubscribe_batch(emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe_batch(ClientId, Topics) ->
    unsubscribe_batch(mria_mnesia:running_nodes(), ClientId, Topics).

-spec unsubscribe_batch([node()], emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe_batch, _} | {error, channel_not_found}.
unsubscribe_batch([Node | Nodes], ClientId, Topics) ->
    case wrap_rpc(emqx_management_proto_v2:unsubscribe_batch(Node, ClientId, Topics)) of
        {error, _} -> unsubscribe_batch(Nodes, ClientId, Topics);
        Re -> Re
    end;
unsubscribe_batch([], _ClientId, _Topics) ->
    {error, channel_not_found}.

-spec do_unsubscribe_batch(emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe_batch, _} | {error, _}.
do_unsubscribe_batch(ClientId, Topics) ->
    case ets:lookup(emqx_channel, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {unsubscribe, [emqx_topic:parse(Topic) || Topic <- Topics]}
    end.

%%--------------------------------------------------------------------
%% Get Alarms
%%--------------------------------------------------------------------

get_alarms(Type) ->
    [{Node, get_alarms(Node, Type)} || Node <- mria_mnesia:running_nodes()].

get_alarms(Node, Type) ->
    add_duration_field(wrap_rpc(emqx_proto_v1:get_alarms(Node, Type))).

deactivate(Node, Name) ->
    wrap_rpc(emqx_proto_v1:deactivate_alarm(Node, Name)).

delete_all_deactivated_alarms() ->
    [delete_all_deactivated_alarms(Node) || Node <- mria_mnesia:running_nodes()].

delete_all_deactivated_alarms(Node) ->
    wrap_rpc(emqx_proto_v1:delete_all_deactivated_alarms(Node)).

add_duration_field(Alarms) ->
    Now = erlang:system_time(microsecond),
    add_duration_field(Alarms, Now, []).

add_duration_field([], _Now, Acc) ->
    Acc;
add_duration_field([Alarm = #{activated := true, activate_at := ActivateAt} | Rest], Now, Acc) ->
    add_duration_field(Rest, Now, [Alarm#{duration => Now - ActivateAt} | Acc]);
add_duration_field(
    [
        Alarm = #{
            activated := false,
            activate_at := ActivateAt,
            deactivate_at := DeactivateAt
        }
        | Rest
    ],
    Now,
    Acc
) ->
    add_duration_field(Rest, Now, [Alarm#{duration => DeactivateAt - ActivateAt} | Acc]).

%%--------------------------------------------------------------------
%% Banned API
%%--------------------------------------------------------------------

create_banned(Banned) ->
    emqx_banned:create(Banned).

delete_banned(Who) ->
    emqx_banned:delete(Who).

%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------

wrap_rpc({badrpc, Reason}) ->
    {error, Reason};
wrap_rpc(Res) ->
    Res.

otp_rel() ->
    iolist_to_binary([emqx_vm:get_otp_version(), "/", erlang:system_info(version)]).

check_row_limit(Tables) ->
    check_row_limit(Tables, max_row_limit()).

check_row_limit([], _Limit) ->
    ok;
check_row_limit([Tab | Tables], Limit) ->
    case table_size(Tab) > Limit of
        true -> false;
        false -> check_row_limit(Tables, Limit)
    end.

check_results(Results) ->
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true -> ok;
        false -> wrap_rpc(lists:last(Results))
    end.

max_row_limit() ->
    ?MAX_ROW_LIMIT.

table_size(Tab) -> ets:info(Tab, size).
