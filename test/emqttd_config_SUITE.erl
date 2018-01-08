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

-module(emqttd_config_SUITE).

-compile(export_all).

-include("emqttd.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, emq_config}].

groups() ->
    [{emq_config, [sequence],
     [run_protocol_cmd,
      run_client_cmd,
      run_session_cmd,
      run_queue_cmd,
      run_auth_cmd,
      run_lager_cmd,
      run_connection_cmd,
      run_broker_config]
     }].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

run_protocol_cmd(_Config) ->
    SetConfigKeys = [{"max_clientid_len=2048", int},
                     {"max_packet_size=1024", int},
           %          {"websocket_protocol_header=off", atom},
                     {"keepalive_backoff=0.5", float}],
    lists:foreach(fun set_cmd/1, SetConfigKeys),
    R = lists:sort(lists:map(fun env_value/1, SetConfigKeys)),
    {ok, E} =  application:get_env(emqttd, protocol),
    ?assertEqual(R, lists:sort(E)),
    emqttd_cli_config:run(["config", "set", "mqtt.websocket_protocol_header=off", "--app=emqttd"]),
    {ok, E1} =  application:get_env(emqttd, websocket_protocol_header),
    ?assertEqual(false, E1).

run_client_cmd(_Config) ->
    SetConfigKeys = [{"max_publish_rate=100", int},
                     {"idle_timeout=60s", date},
                     {"enable_stats=on", atom}],
    lists:foreach(fun(Key) -> set_cmd("client", Key) end, SetConfigKeys),
    R = lists:sort(lists:map(fun(Key) -> env_value("client", Key) end, SetConfigKeys)),
    {ok, E} =  application:get_env(emqttd, client),
    ?assertEqual(R, lists:sort(E)).

run_session_cmd(_Config) ->
    SetConfigKeys = [{"max_subscriptions=5", int},
                     {"upgrade_qos=on", atom},
                     {"max_inflight=64", int},
                     {"retry_interval=60s", date},
                     {"max_awaiting_rel=200", int},
                     {"await_rel_timeout=60s",date},
                     {"enable_stats=on", atom},
                     {"expiry_interval=60s", date},
                     {"ignore_loop_deliver=true", atom}],
    lists:foreach(fun(Key) -> set_cmd("session", Key) end, SetConfigKeys),
    R = lists:sort(lists:map(fun env_value/1, SetConfigKeys)),
    {ok, E} =  application:get_env(emqttd, session),
    ?assertEqual(R, lists:sort(E)).

run_queue_cmd(_Config) ->
    SetConfigKeys = [{"type=priority", atom},
                     {"priority=hah", string},
                     {"max_length=2000", int},
                     {"low_watermark=40%",percent},
                     {"high_watermark=80%", percent},
                     {"store_qos0=false", atom}],
    lists:foreach(fun(Key) -> set_cmd("mqueue", Key) end, SetConfigKeys),
    R = lists:sort(lists:map(fun env_value/1, SetConfigKeys)),
    {ok, E} =  application:get_env(emqttd, mqueue),
    ?assertEqual(R, lists:sort(E)).

run_auth_cmd(_Config) ->
    SetConfigKeys = [{"allow_anonymous=true", atom},
                     {"acl_nomatch=deny", atom},
                     {"acl_file=etc/test.acl", string},
                     {"cache_acl=false", atom}],
    lists:foreach(fun set_cmd/1, SetConfigKeys),
    {ok, true} =  application:get_env(emqttd, allow_anonymous),
    {ok, deny} =  application:get_env(emqttd, acl_nomatch),
    {ok, "etc/test.acl"} =  application:get_env(emqttd, acl_file),
    {ok, false} =  application:get_env(emqttd, cache_acl).

run_lager_cmd(_Config) ->
    emqttd_cli_config:run(["config", "set", "log.console.level=info", "--app=emqttd"]),
    ok.

run_connection_cmd(_Config) ->
    emqttd_cli_config:run(["config", "set", "mqtt.conn.force_gc_count=1000", "--app=emqttd"]),
    {ok, E} =  application:get_env(emqttd, conn_force_gc_count),
    ?assertEqual(1000, E).

run_broker_config(_Config) ->
    emqttd_cli_config:run(["config", "set", "mqtt.broker.sys_interval=6000ms", "--app=emqttd"]),
    {ok, E} =  application:get_env(emqttd, broker_sys_interval),
    ?assertEqual(6000, E).

env_value("client", {Key, Type}) ->
    case string:split(Key, "=") of
    ["max_publish_rate", V] ->
        {list_to_atom("max_publish_rate"), format(Type, V)};
    [K, V] ->
        {list_to_atom(string:join(["client", K], "_")), format(Type, V)}
    end.

env_value({Key, Type}) ->
    [K, V] = string:split(Key, "="),
    {list_to_atom(K), format(Type, V)}.

format(string, S) -> S;
format(atom, "on") -> true;
format(atom, "off") -> false;
format(atom, A) -> list_to_atom(A);
format(float, F) -> list_to_float(F);
format(percent, P) ->
    {match, [N]} = re:run(P, "^([0-9]+)%$", [{capture, all_but_first, list}]),
    list_to_integer(N) / 100;
format(int, I)    -> list_to_integer(I);
format(date, _I)    -> 60000.

set_cmd({Key, _Type}) ->
    emqttd_cli_config:run(["config", "set", string:join(["mqtt", Key], "."), "--app=emqttd"]).

set_cmd(Pre, {Key, _Type}) ->
    emqttd_cli_config:run(["config", "set",  string:join(["mqtt", Pre, Key], "."), "--app=emqttd"]).
