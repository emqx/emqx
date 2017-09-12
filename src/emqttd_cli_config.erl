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

-module (emqttd_cli_config).

-export ([register_config_cli/0,
          register_config/0,
          run/1,
          set_usage/0,
          all_cfgs/0,
          get_cfg/2,
          get_cfg/3,
          read_config/1,
          write_config/2]).

-define(APP, emqttd).
-define(TAB, emqttd_config).

register_config() ->
    application:start(clique),
    F = fun() -> ekka_mnesia:running_nodes() end,
    clique:register_node_finder(F),
    register_config_cli(),
    create_config_tab().

create_config_tab() ->
    case ets:info(?TAB, name) of
        undefined ->
            ets:new(?TAB, [named_table, public]),
            {ok, PluginsEtcDir} = emqttd:env(plugins_etc_dir),
            Files = filelib:wildcard("*.conf", PluginsEtcDir),
            lists:foreach(fun(File) ->
                [FileName | _] = string:tokens(File, "."),
                Configs = cuttlefish_conf:file(lists:concat([PluginsEtcDir, File])),
                ets:insert(?TAB, {list_to_atom(FileName), Configs})
            end, Files);
        _ ->
            ok
    end.

read_config(App) ->
    case ets:lookup(?TAB, App) of
        [] -> [];
        [{_, Value}] -> Value
    end.

write_config(App, Terms) ->
    ets:insert(?TAB, {App, Terms}).

run(Cmd) ->
    clique:run(Cmd).

register_config_cli() ->
    ok = clique_config:load_schema([code:priv_dir(?APP)], ?APP),
    register_protocol_formatter(),
    register_client_formatter(),
    register_session_formatter(),
    register_queue_formatter(),
    register_lager_formatter(),

    register_auth_config(),
    register_protocol_config(),
    register_connection_config(),
    register_client_config(),
    register_session_config(),
    register_queue_config(),
    register_broker_config(),
    register_lager_config().

set_usage() ->
    io:format("~-40s# ~-20s# ~-20s ~p~n", ["key", "value", "datatype", "app"]),
    io:format("------------------------------------------------------------------------------------------------~n"),
    lists:foreach(fun({Key, Val, Datatype, App}) ->
        io:format("~-40s# ~-20s# ~-20s ~p~n", [Key, Val, Datatype, App])
    end, all_cfgs()),
    io:format("------------------------------------------------------------------------------------------------~n"),
    io:format("Usage: set key=value --app=appname~n").

all_cfgs() ->
    {Mappings, Mappings1} = lists:foldl(
        fun({Key, {_, Map, _}}, {Acc, Acc1}) ->
            Map1 = lists:map(fun(M) -> {cuttlefish_mapping:variable(M), Key} end, Map),
            {Acc ++ Map, Acc1 ++ Map1}
        end, {[], []}, ets:tab2list(clique_schema)),
    lists:foldl(fun({Key, _}, Acc) ->
        case lists:keyfind(cuttlefish_variable:tokenize(Key), 2, Mappings) of
            false -> Acc;
            Map ->
                Datatype = format_datatype(cuttlefish_mapping:datatype(Map)),
                App = proplists:get_value(cuttlefish_variable:tokenize(Key), Mappings1),
                [{_, [Val0]}] = clique_config:show([Key], [{app, App}]),
                Val = any_to_string(proplists:get_value(Key, Val0)),
                [{Key, Val, Datatype, App} | Acc]
        end
    end, [],lists:sort(ets:tab2list(clique_config))).

get_cfg(App, Key) ->
    get_cfg(App, Key, undefined).

get_cfg(App, Key, Def) ->
    [{_, [Val0]}] = clique_config:show([Key], [{app, App}]),
    proplists:get_value(Key, Val0, Def).

format_datatype(Value) ->
    format_datatype(Value, "").

format_datatype([Head], Acc) when is_tuple(Head) ->
    [Head1 | _] = erlang:tuple_to_list(Head),
    lists:concat([Acc, Head1]);
format_datatype([Head], Acc) ->
    lists:concat([Acc, Head]);
format_datatype([Head | Tail], Acc) when is_tuple(Head)->
    [Head1 | _] = erlang:tuple_to_list(Head),
    format_datatype(Tail, Acc ++ lists:concat([Head1, ", "]));
format_datatype([Head | Tail], Acc) ->
    format_datatype(Tail, Acc ++ lists:concat([Head, ", "])).

%%--------------------------------------------------------------------
%% Auth/Acl
%%--------------------------------------------------------------------

register_auth_config() ->
    ConfigKeys = ["mqtt.allow_anonymous",
                  "mqtt.acl_nomatch",
                  "mqtt.acl_file",
                  "mqtt.cache_acl"],
    [clique:register_config(Key , fun auth_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

auth_config_callback([_, KeyStr], Value) ->
    application:set_env(?APP, l2a(KeyStr), Value), " successfully\n".
    
%%--------------------------------------------------------------------
%% MQTT Protocol
%%--------------------------------------------------------------------

register_protocol_formatter() ->
    ConfigKeys = ["max_clientid_len", 
                  "max_packet_size",
                  "websocket_protocol_header",
                  "keepalive_backoff"],
    [clique:register_formatter(["mqtt", Key], fun protocol_formatter_callback/2) || Key <- ConfigKeys].

protocol_formatter_callback([_, "websocket_protocol_header"], Params) ->
    Params;
protocol_formatter_callback([_, Key], Params) ->
    proplists:get_value(l2a(Key), Params).

register_protocol_config() ->
    ConfigKeys = ["mqtt.max_clientid_len",
                  "mqtt.max_packet_size",
                  "mqtt.websocket_protocol_header",
                  "mqtt.keepalive_backoff"],
    [clique:register_config(Key , fun protocol_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

protocol_config_callback([_AppStr, KeyStr], Value) ->
    protocol_config_callback(protocol, l2a(KeyStr), Value).
protocol_config_callback(_App, websocket_protocol_header, Value) ->
    application:set_env(?APP, websocket_protocol_header, Value),
    " successfully\n";
protocol_config_callback(App, Key, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(Key, 1, Env, {Key, Value})),
    " successfully\n".

%%--------------------------------------------------------------------
%% MQTT Connection
%%--------------------------------------------------------------------

register_connection_config() ->
    ConfigKeys = ["mqtt.conn.force_gc_count"],
    [clique:register_config(Key , fun connection_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

connection_config_callback([_, KeyStr0, KeyStr1], Value) ->
    KeyStr = lists:concat([KeyStr0, "_", KeyStr1]),
    application:set_env(?APP, l2a(KeyStr), Value),
    " successfully\n".

%%--------------------------------------------------------------------
%% MQTT Client
%%--------------------------------------------------------------------

register_client_formatter() ->
    ConfigKeys = ["max_publish_rate", 
                  "idle_timeout",
                  "enable_stats"],
    [clique:register_formatter(["mqtt", "client", Key], fun client_formatter_callback/2) || Key <- ConfigKeys].

client_formatter_callback([_, _, Key], Params) ->
    proplists:get_value(list_to_atom(Key), Params).

register_client_config() ->
    ConfigKeys = ["mqtt.client.max_publish_rate",
                  "mqtt.client.idle_timeout",
                  "mqtt.client.enable_stats"],
    [clique:register_config(Key , fun client_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

client_config_callback([_, AppStr, KeyStr], Value) ->
    client_config_callback(l2a(AppStr), l2a(KeyStr), Value).

client_config_callback(App, idle_timeout, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(client_idle_timeout, 1, Env, {client_idle_timeout, Value})),
    " successfully\n";
client_config_callback(App, enable_stats, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(client_enable_stats, 1, Env, {client_enable_stats, Value})),
    " successfully\n";
client_config_callback(App, Key, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(Key, 1, Env, {Key, Value})),
    " successfully\n".

%%--------------------------------------------------------------------
%% session
%%--------------------------------------------------------------------

register_session_formatter() ->
    ConfigKeys = ["max_subscriptions", 
                  "upgrade_qos",
                  "max_inflight",
                  "retry_interval",
                  "max_awaiting_rel",
                  "await_rel_timeout",
                  "enable_stats",
                  "expiry_interval",
                  "ignore_loop_deliver"],
    [clique:register_formatter(["mqtt", "session", Key], fun session_formatter_callback/2) || Key <- ConfigKeys].

session_formatter_callback([_, _, Key], Params) ->
    proplists:get_value(list_to_atom(Key), Params).

register_session_config() ->
    ConfigKeys = ["mqtt.session.max_subscriptions",
                  "mqtt.session.upgrade_qos",
                  "mqtt.session.max_inflight",
                  "mqtt.session.retry_interval",
                  "mqtt.session.max_awaiting_rel",
                  "mqtt.session.await_rel_timeout",
                  "mqtt.session.enable_stats",
                  "mqtt.session.expiry_interval",
                  "mqtt.session.ignore_loop_deliver"],
    [clique:register_config(Key , fun session_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

session_config_callback([_, AppStr, KeyStr], Value) ->
    session_config_callback(l2a(AppStr), l2a(KeyStr), Value).
session_config_callback(App, Key, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(Key, 1, Env, {Key, Value})),
    " successfully\n".

l2a(List) -> list_to_atom(List).

%%--------------------------------------------------------------------
%% MQTT MQueue
%%--------------------------------------------------------------------

register_queue_formatter() ->
    ConfigKeys = ["type", 
                  "priority",
                  "max_length",
                  "low_watermark",
                  "high_watermark",
                  "store_qos0"],
    [clique:register_formatter(["mqtt", "mqueue", Key], fun queue_formatter_callback/2) || Key <- ConfigKeys].

queue_formatter_callback([_, _, Key], Params) ->
    proplists:get_value(list_to_atom(Key), Params).

register_queue_config() ->
    ConfigKeys = ["mqtt.mqueue.type",
                  "mqtt.mqueue.priority",
                  "mqtt.mqueue.max_length",
                  "mqtt.mqueue.low_watermark",
                  "mqtt.mqueue.high_watermark",
                  "mqtt.mqueue.store_qos0"],
    [clique:register_config(Key , fun queue_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

queue_config_callback([_, AppStr, KeyStr], Value) ->
    queue_config_callback(l2a(AppStr), l2a(KeyStr), Value).

queue_config_callback(App, low_watermark, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(low_watermark, 1, Env, {low_watermark, Value})),
    " successfully\n";
queue_config_callback(App, high_watermark, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(high_watermark, 1, Env, {high_watermark, Value})),
    " successfully\n";
queue_config_callback(App, Key, Value) ->
    {ok, Env} = emqttd:env(App),
    application:set_env(?APP, App, lists:keyreplace(Key, 1, Env, {Key, Value})),
    " successfully\n".

%%--------------------------------------------------------------------
%% MQTT Broker
%%--------------------------------------------------------------------

register_broker_config() ->
    ConfigKeys = ["mqtt.broker.sys_interval"],
    [clique:register_config(Key , fun broker_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

broker_config_callback([_, KeyStr0, KeyStr1], Value) ->
    KeyStr = lists:concat([KeyStr0, "_", KeyStr1]),
    application:set_env(?APP, l2a(KeyStr), Value),
    " successfully\n".

%%--------------------------------------------------------------------
%% MQTT Lager
%%--------------------------------------------------------------------

register_lager_formatter() ->
    ConfigKeys = ["level"],
    [clique:register_formatter(["log", "console", Key], fun lager_formatter_callback/2) || Key <- ConfigKeys].

lager_formatter_callback(_, Params) ->
    proplists:get_value(lager_console_backend, Params).

register_lager_config() ->
    ConfigKeys = ["log.console.level"],
    [clique:register_config(Key , fun lager_config_callback/2) || Key <- ConfigKeys],
    ok = register_config_whitelist(ConfigKeys).

lager_config_callback(_, Value) ->
    lager:set_loglevel(lager_console_backend, Value),
    " successfully\n".

register_config_whitelist(ConfigKeys) ->
  clique:register_config_whitelist(ConfigKeys, ?APP).

%%--------------------------------------------------------------------
%% Inner Function
%%--------------------------------------------------------------------
any_to_string(I) when is_integer(I) ->
    integer_to_list(I);
any_to_string(F) when is_float(F)->
    float_to_list(F,[{decimals, 4}]);
any_to_string(A) when is_atom(A) ->
    atom_to_list(A);
any_to_string(B) when is_binary(B) ->
    binary_to_list(B);
any_to_string(L)  when is_list(L) ->
    L.
