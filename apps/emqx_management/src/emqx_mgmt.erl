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

-include_lib("stdlib/include/qlc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-elvis([{elvis_style, invalid_dynamic_call, #{ignore => [emqx_mgmt]}}]).
-elvis([{elvis_style, god_modules, #{ignore => [emqx_mgmt]}}]).

%% Nodes and Brokers API
-export([ list_nodes/0
        , lookup_node/1
        , list_brokers/0
        , lookup_broker/1
        , node_info/1
        , broker_info/1
        ]).

%% Metrics and Stats
-export([ get_metrics/0
        , get_metrics/1
        , get_stats/0
        , get_stats/1
        ]).

%% Clients, Sessions
-export([ lookup_client/2
        , lookup_client/3
        , kickout_client/1
        , list_acl_cache/1
        , clean_acl_cache/1
        , clean_acl_cache/2
        , clean_acl_cache_all/0
        , clean_acl_cache_all/1
        , set_ratelimit_policy/2
        , set_quota_policy/2
        , set_keepalive/2
        ]).

-export([ clean_pem_cache/0
        , clean_pem_cache/1
        ]).

%% Internal funcs
-export([call_client/3]).

%% Subscriptions
-export([ list_subscriptions/1
        , list_subscriptions_via_topic/2
        , list_subscriptions_via_topic/3
        , lookup_subscriptions/2
        , lookup_subscriptions/3
        ]).

%% Routes
-export([ lookup_routes/1
        ]).

%% PubSub
-export([ subscribe/2
        , do_subscribe/2
        , publish/1
        , unsubscribe/2
        , do_unsubscribe/2
        ]).

%% Plugins
-export([ list_plugins/0
        , list_plugins/1
        , load_plugin/2
        , unload_plugin/2
        , reload_plugin/2
        ]).

%% Listeners
-export([ list_listeners/0
        , list_listeners/1
        , restart_listener/2
        ]).

%% Alarms
-export([ get_alarms/1
        , get_alarms/2
        , deactivate/2
        , delete_all_deactivated_alarms/0
        , delete_all_deactivated_alarms/1
        ]).

%% Banned
-export([ create_banned/1
        , delete_banned/1
        ]).

-ifndef(EMQX_ENTERPRISE).

-export([ enable_telemetry/0
        , disable_telemetry/0
        , get_telemetry_status/0
        , get_telemetry_data/0
        ]).

-endif.

%% Common Table API
-export([ item/2
        , max_row_limit/0
        ]).

-define(MAX_ROW_LIMIT, 10000).

-define(APP, emqx_management).
%%--------------------------------------------------------------------
%% Node Info
%%--------------------------------------------------------------------

list_nodes() ->
    Running = mnesia:system_info(running_db_nodes),
    Stopped = mnesia:system_info(db_nodes) -- Running,
    DownNodes = lists:map(fun stopped_node_info/1, Stopped),
    [{Node, node_info(Node)} || Node <- Running] ++ DownNodes.

lookup_node(Node) -> node_info(Node).

node_info(Node) when Node =:= node() ->
    {UsedRatio, Total} = get_sys_memory(),
    Info = maps:from_list([{K, list_to_binary(V)} || {K, V} <- emqx_vm:loads()]),
    BrokerInfo = emqx_sys:info(),
    Info#{node              => node(),
          otp_release       => iolist_to_binary(otp_rel()),
          memory_total      => Total,
          memory_used       => erlang:round(Total * UsedRatio),
          process_available => erlang:system_info(process_limit),
          process_used      => erlang:system_info(process_count),
          max_fds           => proplists:get_value(max_fds,
              lists:usort(lists:flatten(erlang:system_info(check_io)))),
          connections       => ets:info(emqx_channel, size),
          live_connections  => emqx_cm:get_connected_client_count(),
          node_status       => 'Running',
          uptime            => iolist_to_binary(proplists:get_value(uptime, BrokerInfo)),
          version           => iolist_to_binary(proplists:get_value(version, BrokerInfo))
          };
node_info(Node) ->
    rpc_call(Node, node_info, [Node]).

get_sys_memory() ->
    case os:type() of
        {unix, linux} ->
            load_ctl:get_sys_memory();
        _ ->
            {0, 0}
    end.

stopped_node_info(Node) ->
    #{name => Node, node_status => 'Stopped'}.

%%--------------------------------------------------------------------
%% Brokers
%%--------------------------------------------------------------------

list_brokers() ->
    [{Node, broker_info(Node)} || Node <- ekka_mnesia:running_nodes()].

lookup_broker(Node) ->
    broker_info(Node).

broker_info(Node) when Node =:= node() ->
    Info = maps:from_list([{K, iolist_to_binary(V)} || {K, V} <- emqx_sys:info()]),
    Info#{node => Node, otp_release => iolist_to_binary(otp_rel()), node_status => 'Running'};

broker_info(Node) ->
    rpc_call(Node, broker_info, [Node]).

%%--------------------------------------------------------------------
%% Metrics and Stats
%%--------------------------------------------------------------------

get_metrics() ->
    [{Node, get_metrics(Node)} || Node <- ekka_mnesia:running_nodes()].

get_metrics(Node) when Node =:= node() ->
    emqx_metrics:all();
get_metrics(Node) ->
    rpc_call(Node, get_metrics, [Node]).

get_stats() ->
    [{Node, get_stats(Node)} || Node <- ekka_mnesia:running_nodes()].

get_stats(Node) when Node =:= node() ->
    emqx_stats:getstats();
get_stats(Node) ->
    rpc_call(Node, get_stats, [Node]).

%%--------------------------------------------------------------------
%% Clients
%%--------------------------------------------------------------------

lookup_client({clientid, ClientId}, FormatFun) ->
    lists:append([lookup_client(Node, {clientid, ClientId}, FormatFun)
        || Node <- ekka_mnesia:running_nodes()]);

lookup_client({username, Username}, FormatFun) ->
    lists:append([lookup_client(Node, {username, Username}, FormatFun)
        || Node <- ekka_mnesia:running_nodes()]).

lookup_client(Node, {clientid, ClientId}, {M,F}) when Node =:= node() ->
    lists:append(lists:map(
      fun(Key) ->
        lists:map(fun M:F/1, ets:lookup(emqx_channel_info, Key))
      end, ets:lookup(emqx_channel, ClientId)));

lookup_client(Node, {clientid, ClientId}, FormatFun) ->
    rpc_call(Node, lookup_client, [Node, {clientid, ClientId}, FormatFun]);

lookup_client(Node, {username, Username}, {M,F}) when Node =:= node() ->
    MatchSpec = [{ {'_', #{clientinfo => #{username => '$1'}}, '_'}
                 , [{'=:=','$1', Username}]
                 , ['$_']
                 }],
    lists:map(fun M:F/1, ets:select(emqx_channel_info, MatchSpec));

lookup_client(Node, {username, Username}, FormatFun) ->
    rpc_call(Node, lookup_client, [Node, {username, Username}, FormatFun]).

kickout_client(ClientId) ->
    Results = [kickout_client(Node, ClientId) || Node <- ekka_mnesia:running_nodes()],
    has_any_ok(Results).

kickout_client(Node, ClientId) when Node =:= node() ->
    emqx_cm:kick_session(ClientId);

kickout_client(Node, ClientId) ->
    rpc_call(Node, kickout_client, [Node, ClientId]).

list_acl_cache(ClientId) ->
    call_client(ClientId, list_acl_cache).

clean_acl_cache(ClientId) ->
    Results = [clean_acl_cache(Node, ClientId) || Node <- ekka_mnesia:running_nodes()],
    has_any_ok(Results).

clean_acl_cache(Node, ClientId) when Node =:= node() ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            {error, not_found};
        Pids when is_list(Pids) ->
            erlang:send(lists:last(Pids), clean_acl_cache),
            ok
    end;
clean_acl_cache(Node, ClientId) ->
    rpc_call(Node, clean_acl_cache, [Node, ClientId]).

clean_acl_cache_all() ->
    for_nodes(fun clean_acl_cache_all/1).

for_nodes(F) ->
    Results = [{Node, F(Node)} || Node <- ekka_mnesia:running_nodes()],
    case lists:filter(fun({_Node, Res}) -> Res =/= ok end, Results) of
        []  -> ok;
        BadNodes -> {error, BadNodes}
    end.

clean_acl_cache_all(Node) when Node =:= node() ->
    emqx_acl_cache:drain_cache();
clean_acl_cache_all(Node) ->
    rpc_call(Node, clean_acl_cache_all, [Node]).

set_ratelimit_policy(ClientId, Policy) ->
    call_client(ClientId, {ratelimit, Policy}).

set_quota_policy(ClientId, Policy) ->
    call_client(ClientId, {quota, Policy}).

set_keepalive(ClientId, Interval)when Interval >= 0 andalso Interval =< 65535 ->
    call_client(ClientId, {keepalive, Interval});
set_keepalive(_ClientId, _Interval) ->
    {error, ?ERROR2, <<"mqtt3.1.1 specification: keepalive must between 0~65535">>}.

clean_pem_cache() ->
    for_nodes(fun clean_pem_cache/1).

clean_pem_cache(Node) when Node =:= node() ->
    _ = ssl_pem_cache:clear(),
    ok;
clean_pem_cache(Node) ->
    rpc_call(Node, ?FUNCTION_NAME, [Node]).

%% @private
call_client(ClientId, Req) ->
    Results = [call_client(Node, ClientId, Req) || Node <- ekka_mnesia:running_nodes()],
    Expected = lists:filter(fun({error, _}) -> false;
                               (_) -> true
                            end, Results),
    case Expected of
        [] -> {error, not_found};
        [Result | _] -> Result
    end.

%% @private
call_client(Node, ClientId, Req) when Node =:= node() ->
    case emqx_cm:lookup_channels(ClientId) of
        [] -> {error, not_found};
        Pids when is_list(Pids) ->
            Pid = lists:last(Pids),
            case emqx_cm:get_chan_info(ClientId, Pid) of
                #{conninfo := #{conn_mod := ConnMod}} ->
                    erlang:apply(ConnMod, call, [Pid, Req]);
                undefined -> {error, not_found}
            end
    end;
call_client(Node, ClientId, Req) ->
    rpc_call(Node, call_client, [Node, ClientId, Req]).

%%--------------------------------------------------------------------
%% Subscriptions
%%--------------------------------------------------------------------

list_subscriptions(Node) when Node =:= node() ->
    case check_row_limit([mqtt_subproperty]) of
        false -> throw(max_row_limit);
        ok    -> [item(subscription, Sub) || Sub <- ets:tab2list(mqtt_subproperty)]
    end;

list_subscriptions(Node) ->
    rpc_call(Node, list_subscriptions, [Node]).

list_subscriptions_via_topic(Topic, FormatFun) ->
    lists:append([list_subscriptions_via_topic(Node, Topic, FormatFun)
        || Node <- ekka_mnesia:running_nodes()]).


list_subscriptions_via_topic(Node, Topic, {M,F}) when Node =:= node() ->
    MatchSpec = [{{{'_', '$1'}, '_'}, [{'=:=','$1', Topic}], ['$_']}],
    erlang:apply(M, F, [ets:select(emqx_suboption, MatchSpec)]);

list_subscriptions_via_topic(Node, Topic, FormatFun) ->
    rpc_call(Node, list_subscriptions_via_topic, [Node, Topic, FormatFun]).

lookup_subscriptions(ClientId, FormatFun) ->
    lists:append([lookup_subscriptions(Node, ClientId, FormatFun) || Node <- ekka_mnesia:running_nodes()]).

lookup_subscriptions(Node, ClientId, {M, F}) when Node =:= node() ->
    Result = case ets:lookup(emqx_subid, ClientId) of
                 [] -> [];
                 [{_, Pid}] ->
                     ets:match_object(emqx_suboption, {{Pid, '_'}, '_'})
             end,
    %% format at the called node
    erlang:apply(M, F, [Result]);

lookup_subscriptions(Node, ClientId, FormatFun) ->
    rpc_call(Node, lookup_subscriptions, [Node, ClientId, FormatFun]).

%%--------------------------------------------------------------------
%% Routes
%%--------------------------------------------------------------------

lookup_routes(Topic) ->
    emqx_router:lookup_routes(Topic).

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

subscribe(ClientId, TopicTables) ->
    subscribe(ekka_mnesia:running_nodes(), ClientId, TopicTables).

subscribe([Node | Nodes], ClientId, TopicTables) ->
    case rpc_call(Node, do_subscribe, [ClientId, TopicTables]) of
        {error, _} -> subscribe(Nodes, ClientId, TopicTables);
        Re -> Re
    end;

subscribe([], _ClientId, _TopicTables) ->
    {error, channel_not_found}.

do_subscribe(ClientId, TopicTables) ->
    case ets:lookup(emqx_channel, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] ->
            Pid ! {subscribe, TopicTables}
    end.

%%TODO: ???
publish(Msg) ->
    emqx_metrics:inc_msg(Msg),
    emqx:publish(Msg).

unsubscribe(ClientId, Topic) ->
    unsubscribe(ekka_mnesia:running_nodes(), ClientId, Topic).

unsubscribe([Node | Nodes], ClientId, Topic) ->
    case rpc_call(Node, do_unsubscribe, [ClientId, Topic]) of
        {error, _} -> unsubscribe(Nodes, ClientId, Topic);
        Re -> Re
    end;

unsubscribe([], _ClientId, _Topic) ->
    {error, channel_not_found}.

do_unsubscribe(ClientId, Topic) ->
    case ets:lookup(emqx_channel, ClientId) of
        [] -> {error, channel_not_found};
        [{_, Pid}] ->
            Pid ! {unsubscribe, [emqx_topic:parse(Topic)]}
    end.

%%--------------------------------------------------------------------
%% Plugins
%%--------------------------------------------------------------------

list_plugins() ->
    [{Node, list_plugins(Node)} || Node <- ekka_mnesia:running_nodes()].

list_plugins(Node) when Node =:= node() ->
    emqx_plugins:list();
list_plugins(Node) ->
    rpc_call(Node, list_plugins, [Node]).

load_plugin(Node, Plugin) when Node =:= node() ->
    emqx_plugins:load(Plugin);
load_plugin(Node, Plugin) ->
    rpc_call(Node, load_plugin, [Node, Plugin]).

unload_plugin(Node, Plugin) when Node =:= node() ->
    emqx_plugins:unload(Plugin);
unload_plugin(Node, Plugin) ->
    rpc_call(Node, unload_plugin, [Node, Plugin]).

reload_plugin(Node, Plugin) when Node =:= node() ->
    emqx_plugins:reload(Plugin);
reload_plugin(Node, Plugin) ->
    rpc_call(Node, reload_plugin, [Node, Plugin]).

%%--------------------------------------------------------------------
%% Listeners
%%--------------------------------------------------------------------

list_listeners() ->
    [{Node, list_listeners(Node)} || Node <- ekka_mnesia:running_nodes()].

list_listeners(Node) when Node =:= node() ->
    Tcp = lists:map(fun({{Protocol, ListenOn}, _Pid}) ->
        #{protocol        => Protocol,
          listen_on       => ListenOn,
          identifier      => emqx_listeners:find_id_by_listen_on(ListenOn),
          acceptors       => esockd:get_acceptors({Protocol, ListenOn}),
          max_conns       => esockd:get_max_connections({Protocol, ListenOn}),
          current_conns   => esockd:get_current_connections({Protocol, ListenOn}),
          shutdown_count  => esockd:get_shutdown_count({Protocol, ListenOn})}
    end, esockd:listeners()),
    Http = lists:map(fun({Protocol, Opts}) ->
        #{protocol        => Protocol,
          listen_on       => format_http_bind(Opts),
          acceptors       => maps:get( num_acceptors
                                     , proplists:get_value(transport_options, Opts, #{}), 0),
          max_conns       => proplists:get_value(max_connections, Opts),
          current_conns   => proplists:get_value(all_connections, Opts),
          shutdown_count  => []}
    end, ranch:info()),
    Tcp ++ Http;

list_listeners(Node) ->
    rpc_call(Node, list_listeners, [Node]).

restart_listener(Node, Identifier) when Node =:= node() ->
    emqx_listeners:restart_listener(Identifier);

restart_listener(Node, Identifier) ->
    rpc_call(Node, restart_listener, [Node, Identifier]).

%%--------------------------------------------------------------------
%% Get Alarms
%%--------------------------------------------------------------------

get_alarms(Type) ->
    [{Node, get_alarms(Node, Type)} || Node <- ekka_mnesia:running_nodes()].

get_alarms(Node, Type) when Node =:= node() ->
    add_duration_field(emqx_alarm:get_alarms(Type));
get_alarms(Node, Type) ->
    rpc_call(Node, get_alarms, [Node, Type]).

deactivate(Node, Name) when Node =:= node() ->
    emqx_alarm:deactivate(Name);
deactivate(Node, Name) ->
    rpc_call(Node, deactivate, [Node, Name]).

delete_all_deactivated_alarms() ->
    [delete_all_deactivated_alarms(Node) || Node <- ekka_mnesia:running_nodes()].

delete_all_deactivated_alarms(Node) when Node =:= node() ->
    emqx_alarm:delete_all_deactivated_alarms();
delete_all_deactivated_alarms(Node) ->
    rpc_call(Node, emqx_alarm, delete_all_deactivated_alarms, []).

add_duration_field(Alarms) ->
    Now = erlang:system_time(microsecond),
    add_duration_field(Alarms, Now, []).

add_duration_field([], _Now, Acc) ->
    Acc;
add_duration_field([Alarm = #{activated := true, activate_at := ActivateAt} | Rest], Now, Acc) ->
    add_duration_field(Rest, Now, [Alarm#{duration => Now - ActivateAt} | Acc]);
add_duration_field([Alarm = #{activated := false,
    activate_at := ActivateAt, deactivate_at := DeactivateAt} | Rest], Now, Acc) ->
    add_duration_field(Rest, Now, [Alarm#{duration => DeactivateAt - ActivateAt} | Acc]).

%%--------------------------------------------------------------------
%% Banned API
%%--------------------------------------------------------------------

create_banned(Banned) ->
    emqx_banned:create(Banned).

delete_banned(Who) ->
    emqx_banned:delete(Who).



%%--------------------------------------------------------------------
%% Telemetry API
%%--------------------------------------------------------------------

-ifndef(EMQX_ENTERPRISE).

enable_telemetry() ->
    lists:foreach(fun enable_telemetry/1,ekka_mnesia:running_nodes()).

enable_telemetry(Node) when Node =:= node() ->
    emqx_telemetry:enable();
enable_telemetry(Node) ->
    rpc_call(Node, enable_telemetry, [Node]).

disable_telemetry() ->
    lists:foreach(fun disable_telemetry/1,ekka_mnesia:running_nodes()).

disable_telemetry(Node) when Node =:= node() ->
    emqx_telemetry:disable();
disable_telemetry(Node) ->
    rpc_call(Node, disable_telemetry, [Node]).

get_telemetry_status() ->
    [{enabled, emqx_telemetry:is_enabled()}].

get_telemetry_data() ->
    emqx_telemetry:get_telemetry().

-endif.

%%--------------------------------------------------------------------
%% Common Table API
%%--------------------------------------------------------------------

item(subscription, {{Topic, ClientId}, Options}) ->
    #{topic => Topic, clientid => ClientId, options => Options};

item(route, #route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node};
item(route, {Topic, Node}) ->
    #{topic => Topic, node => Node}.

%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------

rpc_call(Node, Fun, Args) ->
    rpc_call(Node, ?MODULE, Fun, Args).

rpc_call(Node, Mod, Fun, Args) ->
    case rpc:call(Node, Mod, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.

otp_rel() ->
    lists:concat([emqx_vm:get_otp_version(), "/", erlang:system_info(version)]).

check_row_limit(Tables) ->
    check_row_limit(Tables, max_row_limit()).

check_row_limit([], _Limit) ->
    ok;
check_row_limit([Tab | Tables], Limit) ->
    case table_size(Tab) > Limit of
        true  -> false;
        false -> check_row_limit(Tables, Limit)
    end.

max_row_limit() ->
    application:get_env(?APP, max_row_limit, ?MAX_ROW_LIMIT).

table_size(Tab) -> ets:info(Tab, size).

has_any_ok(Results) ->
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true -> ok;
        false -> lists:last(Results)
    end.

format_http_bind(Opts) ->
    Port = proplists:get_value(port, Opts),
    case proplists:get_value(ip, Opts) of
        undefined -> Port;
        IP -> {IP, Port}
    end.
