%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_mgmt).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-include_lib("stdlib/include/qlc.hrl").

-import(proplists, [get_value/2]).

-export([brokers/0, broker/1, metrics/0, metrics/1, stats/1, stats/0,
         plugins/0, plugins/1, listeners/0, listener/1, nodes_info/0, node_info/1]).

-export([plugin_list/1, plugin_unload/2, plugin_load/2]).

-export([client_list/4, session_list/4, route_list/3, subscription_list/4, alarm_list/0]).

-export([client/1, session/1, route/1, subscription/1]).

-export([query_table/4, lookup_table/3]).

-export([publish/1, subscribe/1, unsubscribe/1]).

-export([kick_client/1, clean_acl_cache/2]).

-export([modify_config/3, modify_config/4, get_configs/0, get_config/1]).

-define(KB, 1024).
-define(MB, (1024*1024)).
-define(GB, (1024*1024*1024)).

-define(EMPTY_KEY(Key), ((Key == undefined) orelse (Key == <<>>))).

brokers() ->
    [{Node, broker(Node)} || Node <- ekka_mnesia:running_nodes()].

broker(Node) when Node =:= node() ->
    emqttd_broker:info();
broker(Node) ->
    rpc_call(Node, broker, [Node]).

metrics() ->
    [{Node, metrics(Node)} || Node <- ekka_mnesia:running_nodes()].

metrics(Node) when Node =:= node() ->
    emqttd_metrics:all();
metrics(Node)  ->
    rpc_call(Node, metrics, [Node]).

stats() ->
    [{Node, stats(Node)} || Node <- ekka_mnesia:running_nodes()].

stats(Node) when Node =:= node() ->
    emqttd_stats:getstats();
stats(Node) ->
    rpc_call(Node, stats, [Node]).

plugins() ->
    [{Node, plugins(Node)} || Node <- ekka_mnesia:running_nodes()].

plugins(Node) when Node =:= node() ->
    emqttd_plugins:list(Node);
plugins(Node) ->
    rpc_call(Node, plugins, [Node]).

listeners() ->
    [{Node, listener(Node)} || Node <- ekka_mnesia:running_nodes()].

listener(Node) when Node =:= node() ->
    lists:map(fun({{Protocol, ListenOn}, Pid}) ->
                Info = [{acceptors,      esockd:get_acceptors(Pid)},
                        {max_clients,    esockd:get_max_clients(Pid)},
                        {current_clients,esockd:get_current_clients(Pid)},
                        {shutdown_count, esockd:get_shutdown_count(Pid)}],
                {Protocol, ListenOn, Info}
            end, esockd:listeners());

listener(Node) ->
    rpc_call(Node, listener, [Node]).

nodes_info() ->
    Running = mnesia:system_info(running_db_nodes),
    Stopped = mnesia:system_info(db_nodes) -- Running,
    DownNodes = lists:map(fun stop_node/1, Stopped),
    [node_info(Node) || Node <- Running] ++ DownNodes.

node_info(Node) when Node =:= node() ->
    CpuInfo = [{K, list_to_binary(V)} || {K, V} <- emqttd_vm:loads()],
    Memory  = emqttd_vm:get_memory(),
    OtpRel  = "R" ++ erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version),
    [{name, node()},
     {otp_release, list_to_binary(OtpRel)},
     {memory_total, kmg(get_value(allocated, Memory))},
     {memory_used,  kmg(get_value(used, Memory))},
     {process_available, erlang:system_info(process_limit)},
     {process_used, erlang:system_info(process_count)},
     {max_fds, get_value(max_fds, erlang:system_info(check_io))},
     {clients, ets:info(mqtt_client, size)},
     {node_status, 'Running'} | CpuInfo];

node_info(Node) ->
    rpc_call(Node, node_info, [Node]).

stop_node(Node) ->
    [{name, Node}, {node_status, 'Stopped'}].
%%--------------------------------------------------------
%% plugins
%%--------------------------------------------------------
plugin_list(Node) when Node =:= node() ->
    emqttd_plugins:list();
plugin_list(Node) ->
    rpc_call(Node, plugin_list, [Node]).

plugin_load(Node, PluginName) when Node =:= node() ->
    emqttd_plugins:load(PluginName);
plugin_load(Node, PluginName) ->
    rpc_call(Node, plugin_load, [Node, PluginName]).

plugin_unload(Node, PluginName) when Node =:= node() ->
    emqttd_plugins:unload(PluginName);
plugin_unload(Node, PluginName) ->
    rpc_call(Node, plugin_unload, [Node, PluginName]).

%%--------------------------------------------------------
%% client
%%--------------------------------------------------------
client_list(Node, Key, PageNo, PageSize) when Node =:= node() ->
    client_list(Key, PageNo, PageSize);
client_list(Node, Key, PageNo, PageSize) ->
    rpc_call(Node, client_list, [Node, Key, PageNo, PageSize]).

client(ClientId) ->
    lists:flatten([client_list(Node, ClientId, 1, 20) || Node <- ekka_mnesia:running_nodes()]).

%%--------------------------------------------------------
%% session
%%--------------------------------------------------------
session_list(Node, Key, PageNo, PageSize) when Node =:= node() ->
    session_list(Key, PageNo, PageSize);
session_list(Node, Key, PageNo, PageSize) ->
    rpc_call(Node, session_list, [Node, Key, PageNo, PageSize]).

session(ClientId) ->
    lists:flatten([session_list(Node, ClientId, 1, 20) || Node <- ekka_mnesia:running_nodes()]).

%%--------------------------------------------------------
%% subscription
%%--------------------------------------------------------
subscription_list(Node, Key, PageNo, PageSize) when Node =:= node() ->
    subscription_list(Key, PageNo, PageSize);
subscription_list(Node, Key, PageNo, PageSize) ->
    rpc_call(Node, subscription_list, [Node, Key, PageNo, PageSize]).

subscription(Key) ->
    lists:flatten([subscription_list(Node, Key, 1, 20) || Node <- ekka_mnesia:running_nodes()]).

%%--------------------------------------------------------
%% Routes
%%--------------------------------------------------------
route(Key) -> route_list(Key, 1, 20).

%%--------------------------------------------------------
%% alarm
%%--------------------------------------------------------
alarm_list() ->
    emqttd_alarm:get_alarms().

query_table(Qh, PageNo, PageSize, TotalNum) ->
    Cursor = qlc:cursor(Qh),
    case PageNo > 1 of
        true  -> qlc:next_answers(Cursor, (PageNo - 1) * PageSize);
        false -> ok
    end,
    Rows = qlc:next_answers(Cursor, PageSize),
    qlc:delete_cursor(Cursor),
    [{totalNum, TotalNum},
     {totalPage, total_page(TotalNum, PageSize)},
     {result, Rows}].

total_page(TotalNum, PageSize) ->
    case TotalNum rem PageSize of
        0 -> TotalNum div PageSize;
        _ -> (TotalNum div PageSize) + 1
    end.

%%TODO: refactor later...
lookup_table(LookupFun, _PageNo, _PageSize) ->
    Rows = LookupFun(),
    Rows.

%%--------------------------------------------------------------------
%% mqtt 
%%--------------------------------------------------------------------
publish({ClientId, Topic, Payload, Qos, Retain}) ->
    case validate(topic, Topic) of
        true ->
            Msg = emqttd_message:make(ClientId, Qos, Topic, Payload),
            emqttd:publish(Msg#mqtt_message{retain  = Retain}),
            ok;
        false ->
            {error, format_error(Topic, "validate topic: ${0} fail")}
    end.

subscribe({ClientId, Topic, Qos}) ->
    case validate(topic, Topic) of
        true ->
            case emqttd_sm:lookup_session(ClientId) of
                undefined ->
                    {error, format_error(ClientId, "Clientid: ${0} not found")};
                #mqtt_session{sess_pid = SessPid} ->  
                    emqttd_session:subscribe(SessPid, [{Topic, [{qos, Qos}]}]),
                    ok
            end;
        false ->
            {error, format_error(Topic, "validate topic: ${0} fail")}
    end.

unsubscribe({ClientId, Topic})-> 
    case validate(topic, Topic) of
        true ->
            case emqttd_sm:lookup_session(ClientId) of
                undefined ->
                    {error, format_error(ClientId, "Clientid: ${0} not found")};
                #mqtt_session{sess_pid = SessPid} ->   
                    emqttd_session:unsubscribe(SessPid, [{Topic, []}]),
                    ok
            end;
        false ->
            {error, format_error(Topic, "validate topic: ${0} fail")}
    end.
    
% publish(Messages) ->
%     lists:foldl(
%         fun({ClientId, Topic, Payload, Qos, Retain}, {Success, Failed}) -> 
%             case validate(topic, Topic) of
%                 true ->
%                     Msg = emqttd_message:make(ClientId, Qos, Topic, Payload),
%                     emqttd:publish(Msg#mqtt_message{retain  = Retain}),
%                     {[[{topic, Topic}]| Success], Failed};
%                 false ->
%                     {Success, [[{topic, Topic}]| Failed]}
%             end
%         end, {[], []}, Messages).

% subscribers(Subscribers) ->
%     lists:foldl(
%         fun({ClientId, Topic, Qos}, {Success, Failed}) ->
%             case emqttd_sm:lookup_session(ClientId) of
%                 undefined ->
%                     {Success, [[{client_id, ClientId}]|Failed]};
%                 #mqtt_session{sess_pid = SessPid} ->  
%                     emqttd_session:subscribe(SessPid, [{Topic, [{qos, Qos}]}]),
%                     {[[{client_id, ClientId}]| Success], Failed}
%             end
%         end,{[], []}, Subscribers).

% unsubscribers(UnSubscribers)-> 
%     lists:foldl(
%         fun({ClientId, Topic}, {Success, Failed}) ->
%             case emqttd_sm:lookup_session(ClientId) of
%                 undefined ->
%                     {Success, [[{client_id, ClientId}]|Failed]};
%                 #mqtt_session{sess_pid = SessPid} ->   
%                     emqttd_session:unsubscriber(SessPid, [{Topic, []}]),
%                     {[[{client_id, ClientId}]| Success], Failed}
%             end
%         end, {[], []}, UnSubscribers).

%%--------------------------------------------------------------------
%% manager API
%%--------------------------------------------------------------------
kick_client(ClientId) ->
    Result = [kick_client(Node, ClientId) || Node <- ekka_mnesia:running_nodes()],
    lists:any(fun(Item) -> Item =:= ok end, Result).

kick_client(Node, ClientId) when Node =:= node() ->
    case emqttd_cm:lookup(ClientId) of
        undefined -> error;
        #mqtt_client{client_pid = Pid}-> emqttd_client:kick(Pid)
    end;
kick_client(Node, ClientId) ->
    rpc_call(Node, kick_client, [Node, ClientId]).


clean_acl_cache(ClientId, Topic) ->
    Result = [clean_acl_cache(Node, ClientId, Topic) || Node <- ekka_mnesia:running_nodes()],
    lists:any(fun(Item) -> Item =:= ok end, Result).

clean_acl_cache(Node, ClientId, Topic) when Node =:= node() ->
    case emqttd_cm:lookup(ClientId) of
        undefined -> error;
        #mqtt_client{client_pid = Pid}-> emqttd_client:clean_acl_cache(Pid, Topic)
    end;
clean_acl_cache(Node, ClientId, Topic) ->
    rpc_call(Node, clean_acl_cache, [Node, ClientId, Topic]).

%%--------------------------------------------------------------------
%% Config ENV
%%--------------------------------------------------------------------
modify_config(App, Key, Value) ->
    Result = [modify_config(Node, App, Key, Value) || Node <- ekka_mnesia:running_nodes()],
    lists:any(fun(Item) -> Item =:= ok end, Result).

modify_config(Node, App, Key, Value) when Node =:= node() ->
    emqttd_config:set(App, Key, Value);
modify_config(Node, App, Key, Value) ->
    rpc_call(Node, modify_config, [Node, App, Key, Value]).

get_configs() ->
    [{Node, get_config(Node)} || Node <- ekka_mnesia:running_nodes()].

get_config(Node) when Node =:= node()->
    emqttd_cli_config:all_cfgs();
get_config(Node) ->
    rpc_call(Node, get_config, [Node]).

%%--------------------------------------------------------------------
%% Internel Functions.
%%--------------------------------------------------------------------

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.

kmg(Byte) when Byte > ?GB ->
    float(Byte / ?GB, "G");
kmg(Byte) when Byte > ?MB ->
    float(Byte / ?MB, "M");
kmg(Byte) when Byte > ?KB ->
    float(Byte / ?MB, "K");
kmg(Byte) ->
    Byte.
float(F, S) ->
    iolist_to_binary(io_lib:format("~.2f~s", [F, S])).

validate(qos, Qos) ->
    (Qos >= ?QOS_0) and (Qos =< ?QOS_2);

validate(topic, Topic) ->
    emqttd_topic:validate({name, Topic}).

client_list(ClientId, PageNo, PageSize) when ?EMPTY_KEY(ClientId) ->
    TotalNum = ets:info(mqtt_client, size),
    Qh = qlc:q([R || R <- ets:table(mqtt_client)]),
    query_table(Qh, PageNo, PageSize, TotalNum);

client_list(ClientId, PageNo, PageSize) ->
    Fun = fun() -> ets:lookup(mqtt_client, ClientId) end,
    lookup_table(Fun, PageNo, PageSize).

session_list(ClientId, PageNo, PageSize) when ?EMPTY_KEY(ClientId) ->
    TotalNum = lists:sum([ets:info(Tab, size) || Tab <- [mqtt_local_session]]),
    Qh = qlc:append([qlc:q([E || E <- ets:table(Tab)]) || Tab <- [mqtt_local_session]]),
    query_table(Qh, PageNo, PageSize, TotalNum);

session_list(ClientId, PageNo, PageSize) ->
    MP = {ClientId, '_', '_', '_'},
    Fun = fun() -> lists:append([ets:match_object(Tab, MP) || Tab <- [mqtt_local_session]]) end,
    lookup_table(Fun, PageNo, PageSize).

subscription_list(Key, PageNo, PageSize) when ?EMPTY_KEY(Key) ->
    TotalNum = ets:info(mqtt_subproperty, size),
    Qh = qlc:q([E || E <- ets:table(mqtt_subproperty)]),
    query_table(Qh, PageNo, PageSize, TotalNum);

subscription_list(Key, PageNo, PageSize) ->
    Keys = ets:lookup(mqtt_subscription, Key),
    Fun = case length(Keys) == 0 of
        true ->
            MP = {{Key, '_'}, '_'},
            fun() -> ets:match_object(mqtt_subproperty, MP) end;
        false ->
            fun() ->
                lists:map(fun({S, T}) ->[R] = ets:lookup(mqtt_subproperty, {T, S}), R end, Keys)
            end
    end,
    lookup_table(Fun, PageNo, PageSize).

route_list(Topic, PageNo, PageSize) when ?EMPTY_KEY(Topic) ->
    TotalNum = lists:sum([ets:info(Tab, size) || Tab <- tables()]),
    Qh = qlc:append([qlc:q([E || E <- ets:table(Tab)]) || Tab <- tables()]),
    query_table(Qh, PageNo, PageSize, TotalNum);

route_list(Topic, PageNo, PageSize) ->
    Fun = fun() -> lists:append([ets:lookup(Tab, Topic) || Tab <- tables()]) end,
    lookup_table(Fun, PageNo, PageSize).

tables() ->
    [mqtt_route, mqtt_local_route].

format_error(Val, Msg) ->
    re:replace(Msg, <<"\\$\\{[^}]+\\}">>, Val, [global, {return, binary}]).

