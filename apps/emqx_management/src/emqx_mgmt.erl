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

-module(emqx_mgmt).

-include("emqx_mgmt.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

-elvis([{elvis_style, invalid_dynamic_call, disable}]).
-elvis([{elvis_style, god_modules, disable}]).

-include_lib("stdlib/include/qlc.hrl").

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
    kickout_clients/1,
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
    set_keepalive/2,

    do_kickout_clients/1
]).

%% Internal functions
-export([do_call_client/2]).

%% Subscriptions
-export([
    list_subscriptions/1,
    list_subscriptions_via_topic/2,
    list_subscriptions_via_topic/3,

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
-export([
    default_row_limit/0,
    vm_stats/0
]).

-elvis([{elvis_style, god_modules, disable}]).

%%--------------------------------------------------------------------
%% Node Info
%%--------------------------------------------------------------------

list_nodes() ->
    Running = emqx:cluster_nodes(running),
    Stopped = emqx:cluster_nodes(stopped),
    DownNodes = lists:map(fun stopped_node_info/1, Stopped),
    [{Node, Info} || #{node := Node} = Info <- node_info(Running)] ++ DownNodes.

lookup_node(Node) ->
    [Info] = node_info([Node]),
    Info.

node_info() ->
    {UsedRatio, Total} = get_sys_memory(),
    Info = maps:from_list(emqx_vm:loads()),
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
        connections => ets:info(?CHAN_TAB, size),
        node_status => 'running',
        uptime => proplists:get_value(uptime, BrokerInfo),
        version => iolist_to_binary(proplists:get_value(version, BrokerInfo)),
        edition => emqx_release:edition_longstr(),
        role => mria_rlog:role(),
        log_path => log_path(),
        sys_path => iolist_to_binary(code:root_dir())
    }.

log_path() ->
    RootDir = code:root_dir(),
    Configs = logger:get_handler_config(),
    case get_log_path(Configs) of
        undefined ->
            <<"log.file.enable is false, not logging to file.">>;
        Path ->
            iolist_to_binary(filename:join(RootDir, Path))
    end.

get_log_path([#{config := #{file := Path}} | _LoggerConfigs]) ->
    filename:dirname(Path);
get_log_path([_LoggerConfig | LoggerConfigs]) ->
    get_log_path(LoggerConfigs);
get_log_path([]) ->
    undefined.

get_sys_memory() ->
    case os:type() of
        {unix, linux} ->
            emqx_mgmt_cache:get_sys_memory();
        _ ->
            {0, 0}
    end.

node_info(Nodes) ->
    emqx_rpc:unwrap_erpc(emqx_management_proto_v3:node_info(Nodes)).

stopped_node_info(Node) ->
    {Node, #{node => Node, node_status => 'stopped'}}.

vm_stats() ->
    Idle =
        case cpu_sup:util([detailed]) of
            %% Not support for Windows
            {_, 0, 0, _} -> 0;
            {_Num, _Use, IdleList, _} -> proplists:get_value(idle, IdleList, 0)
        end,
    RunQueue = erlang:statistics(run_queue),
    {MemUsedRatio, MemTotal} = get_sys_memory(),
    [
        {run_queue, RunQueue},
        {cpu_idle, Idle},
        {cpu_use, 100 - Idle},
        {total_memory, MemTotal},
        {used_memory, erlang:round(MemTotal * MemUsedRatio)}
    ].

%%--------------------------------------------------------------------
%% Brokers
%%--------------------------------------------------------------------

list_brokers() ->
    Running = emqx:running_nodes(),
    [{Node, Broker} || #{node := Node} = Broker <- broker_info(Running)].

lookup_broker(Node) ->
    [Broker] = broker_info([Node]),
    Broker.

broker_info() ->
    Info = lists:foldl(fun convert_broker_info/2, #{}, emqx_sys:info()),
    Info#{node => node(), otp_release => otp_rel(), node_status => 'running'}.

convert_broker_info({uptime, Uptime}, M) ->
    M#{uptime => emqx_datetime:human_readable_duration_string(Uptime)};
convert_broker_info({K, V}, M) ->
    M#{K => iolist_to_binary(V)}.

broker_info(Nodes) ->
    emqx_rpc:unwrap_erpc(emqx_management_proto_v3:broker_info(Nodes)).

%%--------------------------------------------------------------------
%% Metrics and Stats
%%--------------------------------------------------------------------

get_metrics() ->
    nodes_info_count([get_metrics(Node) || Node <- emqx:running_nodes()]).

get_metrics(Node) ->
    unwrap_rpc(emqx_proto_v1:get_metrics(Node)).

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
    CountStats = nodes_info_count(
        lists:foldl(
            fun(Node, Acc) ->
                case get_stats(Node) of
                    {error, _} ->
                        Acc;
                    Stats ->
                        [delete_keys(Stats, GlobalStatsKeys) | Acc]
                end
            end,
            [],
            emqx:running_nodes()
        )
    ),
    GlobalStats = maps:with(GlobalStatsKeys, maps:from_list(get_stats(node()))),
    maps:merge(CountStats, GlobalStats).

delete_keys(List, []) ->
    List;
delete_keys(List, [Key | Keys]) ->
    delete_keys(proplists:delete(Key, List), Keys).

get_stats(Node) ->
    unwrap_rpc(emqx_proto_v1:get_stats(Node)).

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
     || Node <- emqx:running_nodes()
    ]);
lookup_client({username, Username}, FormatFun) ->
    lists:append([
        lookup_client(Node, {username, Username}, FormatFun)
     || Node <- emqx:running_nodes()
    ]).

lookup_client(Node, Key, FormatFun) ->
    case unwrap_rpc(emqx_cm_proto_v1:lookup_client(Node, Key)) of
        {error, Err} ->
            {error, Err};
        L ->
            lists:map(
                fun({Chan, Info0, Stats}) ->
                    Info = Info0#{node => Node},
                    maybe_format(FormatFun, {Chan, Info, Stats})
                end,
                L
            )
    end.

maybe_format(undefined, A) ->
    A;
maybe_format({M, F}, A) ->
    M:F(A).

kickout_client(ClientId) ->
    case lookup_client({clientid, ClientId}, undefined) of
        [] ->
            {error, not_found};
        _ ->
            Results = [kickout_client(Node, ClientId) || Node <- emqx:running_nodes()],
            check_results(Results)
    end.

kickout_client(Node, ClientId) ->
    unwrap_rpc(emqx_cm_proto_v1:kickout_client(Node, ClientId)).

kickout_clients(ClientIds) when is_list(ClientIds) ->
    F = fun(Node) ->
        emqx_management_proto_v4:kickout_clients(Node, ClientIds)
    end,
    Results = lists:map(F, emqx:running_nodes()),
    case lists:filter(fun(Res) -> Res =/= ok end, Results) of
        [] ->
            ok;
        [Result | _] ->
            unwrap_rpc(Result)
    end.

do_kickout_clients(ClientIds) when is_list(ClientIds) ->
    F = fun(ClientId) ->
        ChanPids = emqx_cm:lookup_channels(local, ClientId),
        lists:foreach(
            fun(ChanPid) -> emqx_cm:kick_session(ClientId, ChanPid) end,
            ChanPids
        )
    end,
    lists:foreach(F, ClientIds).

list_authz_cache(ClientId) ->
    call_client(ClientId, list_authz_cache).

list_client_subscriptions(ClientId) ->
    case lookup_client({clientid, ClientId}, undefined) of
        [] ->
            {error, not_found};
        _ ->
            Results = [client_subscriptions(Node, ClientId) || Node <- emqx:running_nodes()],
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
            end
    end.

client_subscriptions(Node, ClientId) ->
    {Node, unwrap_rpc(emqx_broker_proto_v1:list_client_subscriptions(Node, ClientId))}.

clean_authz_cache(ClientId) ->
    Results = [clean_authz_cache(Node, ClientId) || Node <- emqx:running_nodes()],
    check_results(Results).

clean_authz_cache(Node, ClientId) ->
    unwrap_rpc(emqx_proto_v1:clean_authz_cache(Node, ClientId)).

clean_authz_cache_all() ->
    Results = [{Node, clean_authz_cache_all(Node)} || Node <- emqx:running_nodes()],
    wrap_results(Results).

clean_pem_cache_all() ->
    Results = [{Node, clean_pem_cache_all(Node)} || Node <- emqx:running_nodes()],
    wrap_results(Results).

wrap_results(Results) ->
    case lists:filter(fun({_Node, Item}) -> Item =/= ok end, Results) of
        [] -> ok;
        BadNodes -> {error, BadNodes}
    end.

clean_authz_cache_all(Node) ->
    unwrap_rpc(emqx_proto_v1:clean_authz_cache(Node)).

clean_pem_cache_all(Node) ->
    unwrap_rpc(emqx_proto_v1:clean_pem_cache(Node)).

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
    Results = [call_client(Node, ClientId, Req) || Node <- emqx:running_nodes()],
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
    unwrap_rpc(emqx_management_proto_v3:call_client(Node, ClientId, Req)).

%%--------------------------------------------------------------------
%% Subscriptions
%%--------------------------------------------------------------------

-spec do_list_subscriptions() -> no_return().
do_list_subscriptions() ->
    %% [FIXME] Add function to `emqx_broker` that returns list of subscriptions
    %% and either redirect from here or bpapi directly (EMQX-8993).
    throw(not_implemented).

list_subscriptions(Node) ->
    unwrap_rpc(emqx_management_proto_v3:list_subscriptions(Node)).

list_subscriptions_via_topic(Topic, FormatFun) ->
    lists:append([
        list_subscriptions_via_topic(Node, Topic, FormatFun)
     || Node <- emqx:running_nodes()
    ]).

list_subscriptions_via_topic(Node, Topic, _FormatFun = {M, F}) ->
    case unwrap_rpc(emqx_broker_proto_v1:list_subscriptions_via_topic(Node, Topic)) of
        {error, Reason} -> {error, Reason};
        Result -> M:F(Result)
    end.

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

subscribe(ClientId, TopicTables) ->
    subscribe(emqx:running_nodes(), ClientId, TopicTables).

subscribe([Node | Nodes], ClientId, TopicTables) ->
    case unwrap_rpc(emqx_management_proto_v3:subscribe(Node, ClientId, TopicTables)) of
        {error, _} -> subscribe(Nodes, ClientId, TopicTables);
        {subscribe, Res} -> {subscribe, Res, Node}
    end;
subscribe([], _ClientId, _TopicTables) ->
    {error, channel_not_found}.

-spec do_subscribe(emqx_types:clientid(), emqx_types:topic_filters()) ->
    {subscribe, _} | {error, atom()}.
do_subscribe(ClientId, TopicTables) ->
    case ets:lookup(?CHAN_TAB, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {subscribe, TopicTables}
    end.

publish(Msg) ->
    emqx_metrics:inc_msg(Msg),
    emqx:publish(Msg).

-spec unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe(ClientId, Topic) ->
    unsubscribe(emqx:running_nodes(), ClientId, Topic).

-spec unsubscribe([node()], emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe([Node | Nodes], ClientId, Topic) ->
    case unwrap_rpc(emqx_management_proto_v3:unsubscribe(Node, ClientId, Topic)) of
        {error, _} -> unsubscribe(Nodes, ClientId, Topic);
        Re -> Re
    end;
unsubscribe([], _ClientId, _Topic) ->
    {error, channel_not_found}.

-spec do_unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, _}.
do_unsubscribe(ClientId, Topic) ->
    case ets:lookup(?CHAN_TAB, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {unsubscribe, [emqx_topic:parse(Topic)]}
    end.

-spec unsubscribe_batch(emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe, _} | {error, channel_not_found}.
unsubscribe_batch(ClientId, Topics) ->
    unsubscribe_batch(emqx:running_nodes(), ClientId, Topics).

-spec unsubscribe_batch([node()], emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe_batch, _} | {error, channel_not_found}.
unsubscribe_batch([Node | Nodes], ClientId, Topics) ->
    case unwrap_rpc(emqx_management_proto_v3:unsubscribe_batch(Node, ClientId, Topics)) of
        {error, _} -> unsubscribe_batch(Nodes, ClientId, Topics);
        Re -> Re
    end;
unsubscribe_batch([], _ClientId, _Topics) ->
    {error, channel_not_found}.

-spec do_unsubscribe_batch(emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe_batch, _} | {error, _}.
do_unsubscribe_batch(ClientId, Topics) ->
    case ets:lookup(?CHAN_TAB, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] -> Pid ! {unsubscribe, [emqx_topic:parse(Topic) || Topic <- Topics]}
    end.

%%--------------------------------------------------------------------
%% Get Alarms
%%--------------------------------------------------------------------

get_alarms(Type) ->
    [{Node, get_alarms(Node, Type)} || Node <- emqx:running_nodes()].

get_alarms(Node, Type) ->
    add_duration_field(unwrap_rpc(emqx_proto_v1:get_alarms(Node, Type))).

deactivate(Node, Name) ->
    unwrap_rpc(emqx_proto_v1:deactivate_alarm(Node, Name)).

delete_all_deactivated_alarms() ->
    [delete_all_deactivated_alarms(Node) || Node <- emqx:running_nodes()].

delete_all_deactivated_alarms(Node) ->
    unwrap_rpc(emqx_proto_v1:delete_all_deactivated_alarms(Node)).

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
unwrap_rpc({badrpc, Reason}) ->
    {error, Reason};
unwrap_rpc(Res) ->
    Res.

otp_rel() ->
    iolist_to_binary([emqx_vm:get_otp_version(), "/", erlang:system_info(version)]).

check_results(Results) ->
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true -> ok;
        false -> unwrap_rpc(lists:last(Results))
    end.

default_row_limit() ->
    ?DEFAULT_ROW_LIMIT.
