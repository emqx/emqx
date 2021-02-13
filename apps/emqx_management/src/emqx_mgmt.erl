%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(proplists, [get_value/2]).
-import(lists, [foldl/3]).

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
        , get_all_topic_metrics/0
        , get_topic_metrics/1
        , get_topic_metrics/2
        , register_topic_metrics/1
        , register_topic_metrics/2
        , unregister_topic_metrics/1
        , unregister_topic_metrics/2
        , unregister_all_topic_metrics/0
        , unregister_all_topic_metrics/1
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
        , set_ratelimit_policy/2
        , set_quota_policy/2
        ]).

%% Internal funcs
-export([call_client/3]).

%% Subscriptions
-export([ list_subscriptions/1
        , list_subscriptions_via_topic/2
        , list_subscriptions_via_topic/3
        , lookup_subscriptions/1
        , lookup_subscriptions/2
        ]).

%% Routes
-export([ list_routes/0
        , lookup_routes/1
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

%% Modules
-export([ list_modules/0
        , list_modules/1
        , load_module/2
        , unload_module/2
        , reload_module/2
        ]).

%% Listeners
-export([ list_listeners/0
        , list_listeners/1
        , restart_listener/2
        , restart_listener/3
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

%% Export/Import
-export([ export_rules/0
        , export_resources/0
        , export_blacklist/0
        , export_applications/0
        , export_users/0
        , export_auth_mnesia/0
        , export_acl_mnesia/0
        , import_rules/1
        , import_resources/1
        , import_blacklist/1
        , import_applications/1
        , import_users/1
        , import_auth_clientid/1 %% BACKW: 4.1.x
        , import_auth_username/1 %% BACKW: 4.1.x
        , import_auth_mnesia/2
        , import_acl_mnesia/2
        , to_version/1
        ]).

-export([ enable_telemetry/0
        , disable_telemetry/0
        , get_telemetry_status/0
        , get_telemetry_data/0
        ]).

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
    Memory  = emqx_vm:get_memory(),
    Info = maps:from_list([{K, list_to_binary(V)} || {K, V} <- emqx_vm:loads()]),
    BrokerInfo = emqx_sys:info(),
    Info#{node              => node(),
          otp_release       => iolist_to_binary(otp_rel()),
          memory_total      => get_value(allocated, Memory),
          memory_used       => get_value(used, Memory),
          process_available => erlang:system_info(process_limit),
          process_used      => erlang:system_info(process_count),
          max_fds           => get_value(max_fds, lists:usort(lists:flatten(erlang:system_info(check_io)))),
          connections       => ets:info(emqx_channel, size),
          node_status       => 'Running',
          uptime            => iolist_to_binary(proplists:get_value(uptime, BrokerInfo)),
          version           => iolist_to_binary(proplists:get_value(version, BrokerInfo))
          };
node_info(Node) ->
    rpc_call(Node, node_info, [Node]).

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

get_all_topic_metrics() ->
    lists:foldl(fun(Topic, Acc) ->
                    case get_topic_metrics(Topic) of
                        {error, _Reason} ->
                            Acc;
                        Metrics ->
                            [#{topic => Topic, metrics => Metrics} | Acc]
                    end
                end, [], emqx_mod_topic_metrics:all_registered_topics()).

get_topic_metrics(Topic) ->
    lists:foldl(fun(Node, Acc) ->
                    case get_topic_metrics(Node, Topic) of
                        {error, _Reason} ->
                            Acc;
                        Metrics ->
                            case Acc of
                                [] -> Metrics;
                                _ ->
                                    lists:foldl(fun({K, V}, Acc0) ->
                                                    [{K, V + proplists:get_value(K, Metrics, 0)} | Acc0]
                                                end, [], Acc)
                            end
                    end
                end, [], ekka_mnesia:running_nodes()).

get_topic_metrics(Node, Topic) when Node =:= node() ->
    emqx_mod_topic_metrics:metrics(Topic);
get_topic_metrics(Node, Topic) ->
    rpc_call(Node, get_topic_metrics, [Node, Topic]).

register_topic_metrics(Topic) ->
    Results = [register_topic_metrics(Node, Topic) || Node <- ekka_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

register_topic_metrics(Node, Topic) when Node =:= node() ->
    emqx_mod_topic_metrics:register(Topic);
register_topic_metrics(Node, Topic) ->
    rpc_call(Node, register_topic_metrics, [Node, Topic]).

unregister_topic_metrics(Topic) ->
    Results = [unregister_topic_metrics(Node, Topic) || Node <- ekka_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

unregister_topic_metrics(Node, Topic) when Node =:= node() ->
    emqx_mod_topic_metrics:unregister(Topic);
unregister_topic_metrics(Node, Topic) ->
    rpc_call(Node, unregister_topic_metrics, [Node, Topic]).

unregister_all_topic_metrics() ->
    Results = [unregister_all_topic_metrics(Node) || Node <- ekka_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

unregister_all_topic_metrics(Node) when Node =:= node() ->
    emqx_mod_topic_metrics:unregister_all();
unregister_all_topic_metrics(Node) ->
    rpc_call(Node, unregister_topic_metrics, [Node]).

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
    lists:append([lookup_client(Node, {clientid, ClientId}, FormatFun) || Node <- ekka_mnesia:running_nodes()]);

lookup_client({username, Username}, FormatFun) ->
    lists:append([lookup_client(Node, {username, Username}, FormatFun) || Node <- ekka_mnesia:running_nodes()]).

lookup_client(Node, {clientid, ClientId}, {M,F}) when Node =:= node() ->
    M:F(ets:lookup(emqx_channel, ClientId));

lookup_client(Node, {clientid, ClientId}, FormatFun) ->
    rpc_call(Node, lookup_client, [Node, {clientid, ClientId}, FormatFun]);

lookup_client(Node, {username, Username}, {M,F}) when Node =:= node() ->
    MatchSpec = [{{'$1', #{clientinfo => #{username => '$2'}}, '_'}, [{'=:=','$2', Username}], ['$1']}],
    M:F(ets:select(emqx_channel_info, MatchSpec));

lookup_client(Node, {username, Username}, FormatFun) ->
    rpc_call(Node, lookup_client, [Node, {username, Username}, FormatFun]).

kickout_client(ClientId) ->
    Results = [kickout_client(Node, ClientId) || Node <- ekka_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

kickout_client(Node, ClientId) when Node =:= node() ->
    emqx_cm:kick_session(ClientId);

kickout_client(Node, ClientId) ->
    rpc_call(Node, kickout_client, [Node, ClientId]).

list_acl_cache(ClientId) ->
    call_client(ClientId, list_acl_cache).

clean_acl_cache(ClientId) ->
    Results = [clean_acl_cache(Node, ClientId) || Node <- ekka_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

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

set_ratelimit_policy(ClientId, Policy) ->
    call_client(ClientId, {ratelimit, Policy}).

set_quota_policy(ClientId, Policy) ->
    call_client(ClientId, {quota, Policy}).

%% @private
call_client(ClientId, Req) ->
    Results = [call_client(Node, ClientId, Req) || Node <- ekka_mnesia:running_nodes()],
    Expected = lists:filter(fun({error, _}) -> false;
                               (_) -> true
                            end, Results),
    case Expected of
        [] -> {error, not_found};
        [Result|_] -> Result
    end.

%% @private
call_client(Node, ClientId, Req) when Node =:= node() ->
    case emqx_cm:lookup_channels(ClientId) of
        [] -> {error, not_found};
        Pids when is_list(Pids) ->
            Pid = lists:last(Pids),
            case emqx_cm:get_chan_info(ClientId, Pid) of
                #{conninfo := #{conn_mod := ConnMod}} ->
                    ConnMod:call(Pid, Req);
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
    lists:append([list_subscriptions_via_topic(Node, Topic, FormatFun) || Node <- ekka_mnesia:running_nodes()]).

list_subscriptions_via_topic(Node, Topic, {M,F}) when Node =:= node() ->
    MatchSpec = [{{{'_', '$1'}, '_'}, [{'=:=','$1', Topic}], ['$_']}],
    M:F(ets:select(emqx_suboption, MatchSpec));

list_subscriptions_via_topic(Node, {topic, Topic}, FormatFun) ->
    rpc_call(Node, list_subscriptions_via_topic, [Node, {topic, Topic}, FormatFun]).

lookup_subscriptions(ClientId) ->
    lists:append([lookup_subscriptions(Node, ClientId) || Node <- ekka_mnesia:running_nodes()]).

lookup_subscriptions(Node, ClientId) when Node =:= node() ->
    case ets:lookup(emqx_subid, ClientId) of
        [] -> [];
        [{_, Pid}] ->
            ets:match_object(emqx_suboption, {{Pid, '_'}, '_'})
    end;

lookup_subscriptions(Node, ClientId) ->
    rpc_call(Node, lookup_subscriptions, [Node, ClientId]).

%%--------------------------------------------------------------------
%% Routes
%%--------------------------------------------------------------------

list_routes() ->
    case check_row_limit([emqx_route]) of
        false -> throw(max_row_limit);
        ok    -> ets:tab2list(emqx_route)
    end.

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
%% Modules
%%--------------------------------------------------------------------

list_modules() ->
    [{Node, list_modules(Node)} || Node <- ekka_mnesia:running_nodes()].

list_modules(Node) when Node =:= node() ->
    emqx_modules:list();
list_modules(Node) ->
    rpc_call(Node, list_modules, [Node]).

load_module(Node, Module) when Node =:= node() ->
    emqx_modules:load(Module);
load_module(Node, Module) ->
    rpc_call(Node, load_module, [Node, Module]).

unload_module(Node, Module) when Node =:= node() ->
    emqx_modules:unload(Module);
unload_module(Node, Module) ->
    rpc_call(Node, unload_module, [Node, Module]).

reload_module(Node, Module) when Node =:= node() ->
    emqx_modules:reload(Module);
reload_module(Node, Module) ->
    rpc_call(Node, reload_module, [Node, Module]).
%%--------------------------------------------------------------------
%% Listeners
%%--------------------------------------------------------------------

list_listeners() ->
    [{Node, list_listeners(Node)} || Node <- ekka_mnesia:running_nodes()].

list_listeners(Node) when Node =:= node() ->
    Tcp = lists:map(fun({{Protocol, ListenOn}, _Pid}) ->
        #{protocol        => Protocol,
          listen_on       => ListenOn,
          acceptors       => esockd:get_acceptors({Protocol, ListenOn}),
          max_conns       => esockd:get_max_connections({Protocol, ListenOn}),
          current_conns   => esockd:get_current_connections({Protocol, ListenOn}),
          shutdown_count  => esockd:get_shutdown_count({Protocol, ListenOn})}
    end, esockd:listeners()),
    Http = lists:map(fun({Protocol, Opts}) ->
        #{protocol        => Protocol,
          listen_on       => proplists:get_value(port, Opts),
          acceptors       => maps:get(num_acceptors, proplists:get_value(transport_options, Opts, #{}), 0),
          max_conns       => proplists:get_value(max_connections, Opts),
          current_conns   => proplists:get_value(all_connections, Opts),
          shutdown_count  => []}
    end, ranch:info()),
    Tcp ++ Http;

list_listeners(Node) ->
    rpc_call(Node, list_listeners, [Node]).

restart_listener(Proto, ListenOn) ->
    case find_listener_opts(Proto, ListenOn) of
        {ok, Listener} ->
            emqx_listeners:restart_listener(Listener);
        {error, Error} ->
            {error, Error}
    end.

restart_listener(Node, Proto, ListenOn) when Node =:= node() ->
    case find_listener_opts(Proto, ListenOn) of
        {ok, Listener} ->
            emqx_listeners:restart_listener(Listener);
        {error, Error} ->
            {error, Error}
    end;

restart_listener(Node, Proto, ListenOn) ->
    rpc_call(Node, restart_listener, [Node, Proto, ListenOn]).

find_listener_opts(Proto, ListenOn) ->
    ProtoAtom = list_to_atom(Proto),
    case parse_listenon(ListenOn) of
        {ok, ListenOnParsed} ->
            foldl(fun({LisProtocol, LisListenOn, LisOpts}, Acc) ->
                if
                LisProtocol == ProtoAtom andalso LisListenOn == ListenOnParsed ->
                    {ok, {LisProtocol, LisListenOn, LisOpts}};
                true ->
                    Acc
                end
            end, {error, "Listener/options not found"}, emqx:get_env(listeners, []));
        {error, Error} ->
            {error, Error}
    end.

parse_listenon(ListenOn) ->
    case string:tokens(ListenOn, ":") of
        [] ->
            {error, "missing port"};
        [Port] ->
            {ok, list_to_integer(Port)};
        [IP, Port] ->
            case inet:parse_address(IP) of
                {ok, IP1}      -> {ok, {IP1, list_to_integer(Port)}};
                {error, Error} -> {error, Error}
            end
    end.

%%--------------------------------------------------------------------
%% Get Alarms
%%--------------------------------------------------------------------

get_alarms(Type) ->
    [{Node, get_alarms(Node, Type)} || Node <- ekka_mnesia:running_nodes()].

get_alarms(Node, Type) when Node =:= node() ->
    emqx_alarm:get_alarms(Type);
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
    rpc_call(Node, delete_deactivated_alarms, [Node]).

%%--------------------------------------------------------------------
%% Banned API
%%--------------------------------------------------------------------

create_banned(Banned) ->
    emqx_banned:create(Banned).

delete_banned(Who) ->
    emqx_banned:delete(Who).

%%--------------------------------------------------------------------
%% Data Export and Import
%%--------------------------------------------------------------------

export_rules() ->
    lists:map(fun({_, RuleId, _, RawSQL, _, _, _, _, _, _, Actions, Enabled, Desc}) ->
                   [{id, RuleId},
                     {rawsql, RawSQL},
                     {actions, actions_to_prop_list(Actions)},
                     {enabled, Enabled},
                     {description, Desc}]
              end, emqx_rule_registry:get_rules()).

export_resources() ->
    lists:map(fun({_, Id, Type, Config, CreatedAt, Desc}) ->
                   NCreatedAt = case CreatedAt of
                                    undefined -> null;
                                    _ -> CreatedAt
                                end,
                   [{id, Id},
                    {type, Type},
                    {config, maps:to_list(Config)},
                    {created_at, NCreatedAt},
                    {description, Desc}]
              end, emqx_rule_registry:get_resources()).

export_blacklist() ->
    lists:map(fun(#banned{who = Who, by = By, reason = Reason, at = At, until = Until}) ->
                  NWho = case Who of
                             {peerhost, Peerhost} -> {peerhost, inet:ntoa(Peerhost)};
                             _ -> Who
                         end,
                  [{who, [NWho]}, {by, By}, {reason, Reason}, {at, At}, {until, Until}]
              end, ets:tab2list(emqx_banned)).

export_applications() ->
    lists:map(fun({_, AppID, AppSecret, Name, Desc, Status, Expired}) ->
                  [{id, AppID}, {secret, AppSecret}, {name, Name}, {desc, Desc}, {status, Status}, {expired, Expired}]
              end, ets:tab2list(mqtt_app)).

export_users() ->
    lists:map(fun({_, Username, Password, Tags}) ->
                  [{username, Username}, {password, base64:encode(Password)}, {tags, Tags}]
              end, ets:tab2list(mqtt_admin)).

export_auth_mnesia() ->
    case ets:info(emqx_user) of
        undefined -> [];
        _ ->
            lists:map(fun({_, {Type, Login}, Password, CreatedAt}) ->
                          [{login, Login}, {type, Type}, {password, base64:encode(Password)}, {created_at, CreatedAt}]
                      end, ets:tab2list(emqx_user))
    end.

export_acl_mnesia() ->
    case ets:info(emqx_acl) of
        undefined -> [];
        _ ->
            lists:map(fun({_, Filter, Action, Access, CreatedAt}) ->
                          Filter1 = case Filter of
                              {{Type, TypeValue}, Topic} ->
                                  [{type, Type}, {type_value, TypeValue}, {topic, Topic}];
                              {Type, Topic} ->
                                  [{type, Type}, {topic, Topic}]
                          end,
                          Filter1 ++ [{action, Action}, {access, Access}, {created_at, CreatedAt}]
                      end, ets:tab2list(emqx_acl))
    end.

import_rules(Rules) ->
    lists:foreach(fun(#{<<"id">> := RuleId,
                        <<"rawsql">> := RawSQL,
                        <<"actions">> := Actions,
                        <<"enabled">> := Enabled,
                        <<"description">> := Desc}) ->
                      Rule = #{
                        id => RuleId,
                        rawsql => RawSQL,
                        actions => map_to_actions(Actions),
                        enabled => Enabled,
                        description => Desc
                      },
                      try emqx_rule_engine:create_rule(Rule)
                      catch throw:{resource_not_initialized, _ResId} ->
                          emqx_rule_engine:create_rule(Rule#{enabled => false})
                      end
                  end, Rules).

import_resources(Reources) ->
    lists:foreach(fun(#{<<"id">> := Id,
                        <<"type">> := Type,
                        <<"config">> := Config,
                        <<"created_at">> := CreatedAt,
                        <<"description">> := Desc}) ->
                      NCreatedAt = case CreatedAt of
                                       null -> undefined;
                                       _ -> CreatedAt
                                   end,
                      emqx_rule_engine:create_resource(#{id => Id,
                                                         type => any_to_atom(Type),
                                                         config => Config,
                                                         created_at => NCreatedAt,
                                                         description => Desc})
                  end, Reources).

import_blacklist(Blacklist) ->
    lists:foreach(fun(#{<<"who">> := Who,
                        <<"by">> := By,
                        <<"reason">> := Reason,
                        <<"at">> := At,
                        <<"until">> := Until}) ->
                      NWho = case Who of
                                 #{<<"peerhost">> := Peerhost} ->
                                     {ok, NPeerhost} = inet:parse_address(Peerhost),
                                     {peerhost, NPeerhost};
                                 #{<<"clientid">> := ClientId} -> {clientid, ClientId};
                                 #{<<"username">> := Username} -> {username, Username}
                             end,
                     emqx_banned:create(#banned{who = NWho, by = By, reason = Reason, at = At, until = Until})
                  end, Blacklist).

import_applications(Apps) ->
    lists:foreach(fun(#{<<"id">> := AppID,
                        <<"secret">> := AppSecret,
                        <<"name">> := Name,
                        <<"desc">> := Desc,
                        <<"status">> := Status,
                        <<"expired">> := Expired}) ->
                      NExpired = case is_integer(Expired) of
                                     true -> Expired;
                                     false -> undefined
                                 end,
                      emqx_mgmt_auth:force_add_app(AppID, Name, AppSecret, Desc, Status, NExpired)
                  end, Apps).

import_users(Users) ->
    lists:foreach(fun(#{<<"username">> := Username,
                        <<"password">> := Password,
                        <<"tags">> := Tags}) ->
                      NPassword = base64:decode(Password),
                      emqx_dashboard_admin:force_add_user(Username, NPassword, Tags)
                  end, Users).

import_auth_clientid(Lists) ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            [ mnesia:dirty_write({emqx_user, {clientid, Clientid}, base64:decode(Password), erlang:system_time(millisecond)})
              || #{<<"clientid">> := Clientid, <<"password">> := Password} <- Lists ]
    end.

import_auth_username(Lists) ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            [ mnesia:dirty_write({emqx_user, {username, Username}, base64:decode(Password), erlang:system_time(millisecond)})
              || #{<<"username">> := Username, <<"password">> := Password} <- Lists ]
    end.

import_auth_mnesia(Auths, FromVersion) when FromVersion =:= "4.0" orelse
                                            FromVersion =:= "4.1" ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            CreatedAt = erlang:system_time(millisecond),
            [ begin
                mnesia:dirty_write({emqx_user, {username, Login}, base64:decode(Password), CreatedAt})
              end
              || #{<<"login">> := Login,
                   <<"password">> := Password} <- Auths ]

    end;

import_auth_mnesia(Auths, _) ->
    case ets:info(emqx_user) of
        undefined -> ok;
        _ ->
            [ mnesia:dirty_write({emqx_user, {any_to_atom(Type), Login}, base64:decode(Password), CreatedAt})
              || #{<<"login">> := Login,
                   <<"type">> := Type,
                   <<"password">> := Password,
                   <<"created_at">> := CreatedAt } <- Auths ]
    end.

import_acl_mnesia(Acls, FromVersion) when FromVersion =:= "4.0" orelse
                                          FromVersion =:= "4.1" ->
    case ets:info(emqx_acl) of
        undefined -> ok;
        _ ->
            CreatedAt = erlang:system_time(millisecond),
            [begin
                 Allow1 = case any_to_atom(Allow) of
                              true -> allow;
                              false -> deny
                          end,
                 mnesia:dirty_write({emqx_acl, {{username, Login}, Topic}, any_to_atom(Action), Allow1, CreatedAt})
             end || #{<<"login">> := Login,
                      <<"topic">> := Topic,
                      <<"allow">> := Allow,
                      <<"action">> := Action} <- Acls]
    end;

import_acl_mnesia(Acls, _) ->
    case ets:info(emqx_acl) of
        undefined -> ok;
        _ ->
            [ begin
              Filter = case maps:get(<<"type_value">>, Map, undefined) of
                  undefined ->
                      {any_to_atom(maps:get(<<"type">>, Map)), maps:get(<<"topic">>, Map)};
                  Value ->
                      {{any_to_atom(maps:get(<<"type">>, Map)), Value}, maps:get(<<"topic">>, Map)}
              end,
              mnesia:dirty_write({emqx_acl ,Filter, any_to_atom(Action), any_to_atom(Access), CreatedAt})
              end
              || Map = #{<<"action">> := Action,
                         <<"access">> := Access,
                         <<"created_at">> := CreatedAt} <- Acls ]
    end.

any_to_atom(L) when is_list(L) -> list_to_atom(L);
any_to_atom(B) when is_binary(B) -> binary_to_atom(B, utf8);
any_to_atom(A) when is_atom(A) -> A.

to_version(Version) when is_integer(Version) ->
    integer_to_list(Version);
to_version(Version) when is_binary(Version) ->
    binary_to_list(Version);
to_version(Version) when is_list(Version) ->
    Version.

%%--------------------------------------------------------------------
%% Telemtry API
%%--------------------------------------------------------------------

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

%%--------------------------------------------------------------------
%% Common Table API
%%--------------------------------------------------------------------

item(client, {ClientId, ChanPid}) ->
    Attrs = case emqx_cm:get_chan_info(ClientId, ChanPid) of
                undefined -> #{};
                Attrs0 -> Attrs0
            end,
    Stats = case emqx_cm:get_chan_stats(ClientId, ChanPid) of
                undefined -> #{};
                Stats0 -> maps:from_list(Stats0)
            end,
    ClientInfo = maps:get(clientinfo, Attrs, #{}),
    ConnInfo = maps:get(conninfo, Attrs, #{}),
    Session = case maps:get(session, Attrs, #{}) of
                  undefined -> #{};
                  _Sess -> _Sess
              end,
    SessCreated = maps:get(created_at, Session, maps:get(connected_at, ConnInfo)),
    Connected = case maps:get(conn_state, Attrs) of
                    connected -> true;
                    _ -> false
                end,
    NStats = Stats#{max_subscriptions => maps:get(subscriptions_max, Stats, 0),
                    max_inflight => maps:get(inflight_max, Stats, 0),
                    max_awaiting_rel => maps:get(awaiting_rel_max, Stats, 0),
                    max_mqueue => maps:get(mqueue_max, Stats, 0),
                    inflight => maps:get(inflight_cnt, Stats, 0),
                    awaiting_rel => maps:get(awaiting_rel_cnt, Stats, 0)},
    lists:foldl(fun(Items, Acc) ->
                    maps:merge(Items, Acc)
                end, #{connected => Connected},
                [maps:with([ subscriptions_cnt, max_subscriptions,
                             inflight, max_inflight, awaiting_rel,
                             max_awaiting_rel, mqueue_len, mqueue_dropped,
                             max_mqueue, heap_size, reductions, mailbox_len,
                             recv_cnt, recv_msg, recv_oct, recv_pkt, send_cnt,
                             send_msg, send_oct, send_pkt], NStats),
                 maps:with([clientid, username, mountpoint, is_bridge, zone], ClientInfo),
                 maps:with([clean_start, keepalive, expiry_interval, proto_name,
                            proto_ver, peername, connected_at, disconnected_at], ConnInfo),
                 #{created_at => SessCreated}]);

item(subscription, {{Topic, ClientId}, Options}) ->
    #{topic => Topic, clientid => ClientId, options => Options};

item(route, #route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node};
item(route, {Topic, Node}) ->
    #{topic => Topic, node => Node}.

%%--------------------------------------------------------------------
%% Internel Functions.
%%--------------------------------------------------------------------

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.

otp_rel() ->
    lists:concat(["R", erlang:system_info(otp_release), "/", erlang:system_info(version)]).

check_row_limit(Tables) ->
    check_row_limit(Tables, max_row_limit()).

check_row_limit([], _Limit) ->
    ok;
check_row_limit([Tab|Tables], Limit) ->
    case table_size(Tab) > Limit of
        true  -> false;
        false -> check_row_limit(Tables, Limit)
    end.

max_row_limit() ->
    application:get_env(?APP, max_row_limit, ?MAX_ROW_LIMIT).

table_size(Tab) -> ets:info(Tab, size).

map_to_actions(Maps) ->
    [map_to_action(M) || M <- Maps].

map_to_action(Map = #{<<"id">> := ActionInstId, <<"name">> := Name, <<"args">> := Args}) ->
    #{id => ActionInstId,
      name => any_to_atom(Name),
      args => Args,
      fallbacks => map_to_actions(maps:get(<<"fallbacks">>, Map, []))}.

actions_to_prop_list(Actions) ->
    [action_to_prop_list(Act) || Act <- Actions].

action_to_prop_list({action_instance, ActionInstId, Name, FallbackActions, Args}) ->
    [{id, ActionInstId},
     {name, Name},
     {fallbacks, actions_to_prop_list(FallbackActions)},
     {args, Args}].

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_listenon_test() ->
    ?assertEqual({ok, 8883}, parse_listenon("8883")),
    ?assertEqual({ok, 8883}, parse_listenon(":8883")),
    ?assertEqual({ok, {{127,0,0,1},8883}}, parse_listenon("127.0.0.1:8883")),
    ?assertEqual({error, "missing port"}, parse_listenon("")),
    ?assertEqual({error, einval}, parse_listenon("localhost:8883")).

-endif.
