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

-module(emqx_web_hook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(HOOK_LOOKUP(H), emqx_hooks:lookup(list_to_atom(H))).
-define(ACTION(Name), #{<<"action">> := Name}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [{group, http},
     {group, https},
     {group, ipv6http},
     {group, ipv6https}].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{http, [sequence], Cases},
     {https, [sequence], Cases},
     {ipv6http, [sequence], Cases},
     {ipv6https, [sequence], Cases}].

init_per_group(Name, Config) ->
    application:ensure_all_started(emqx_management),
    set_special_cfgs(),
    case Name of
        http ->
            emqx_ct_helpers:start_apps(apps(), fun set_special_configs_http/1);
        https ->
            emqx_ct_helpers:start_apps(apps(), fun set_special_configs_https/1);
        ipv6http ->
            emqx_ct_helpers:start_apps(apps(), fun set_special_configs_ipv6_http/1);
        ipv6https ->
            emqx_ct_helpers:start_apps(apps(), fun set_special_configs_ipv6_https/1)
    end,
    Config.

end_per_group(_Name, Config) ->
    emqx_ct_helpers:stop_apps(apps()),
    Config.

set_special_configs_http(_) ->
    application:set_env(emqx_web_hook, url, "http://127.0.0.1:9999").

set_special_configs_https(_) ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    SslOpts = [{keyfile, Path ++ "/client-key.pem"},
               {certfile, Path ++ "/client-cert.pem"},
               {cacertfile, Path ++ "/ca.pem"}],
    application:set_env(emqx_web_hook, ssl, true),
    application:set_env(emqx_web_hook, ssloptions, SslOpts),
    application:set_env(emqx_web_hook, url, "https://127.0.0.1:8888").

set_special_configs_ipv6_http(_) ->
    application:set_env(emqx_web_hook, url, "http://[::1]:9999").

set_special_configs_ipv6_https(N) ->
    set_special_configs_https(N),
    application:set_env(emqx_web_hook, url, "https://[::1]:8888").

set_special_cfgs() ->
    AllRules = [{"message.acked",        "{\"action\": \"on_message_acked\"}"},
                {"message.delivered",    "{\"action\": \"on_message_delivered\"}"},
                {"message.publish",      "{\"action\": \"on_message_publish\"}"},
                {"session.terminated",   "{\"action\": \"on_session_terminated\"}"},
                {"session.unsubscribed", "{\"action\": \"on_session_unsubscribed\"}"},
                {"session.subscribed",   "{\"action\": \"on_session_subscribed\"}"},
                {"client.unsubscribe",   "{\"action\": \"on_client_unsubscribe\"}"},
                {"client.subscribe",     "{\"action\": \"on_client_subscribe\"}"},
                {"client.disconnected",  "{\"action\": \"on_client_disconnected\"}"},
                {"client.connected",     "{\"action\": \"on_client_connected\"}"},
                {"client.connack",       "{\"action\": \"on_client_connack\"}"},
                {"client.connect",       "{\"action\": \"on_client_connect\"}"}],
    application:set_env(emqx_web_hook, rules, AllRules).
%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_valid(Config) ->
    {ok, ServerPid} = http_server:start_link(),
    application:set_env(emqx_web_hook, headers, [{"k1","K1"}, {"k2", "K2"}]),
    {ok, C} = emqtt:start_link([ {clientid, <<"simpleClient">>}
                               , {proto_ver, v5}
                               , {keepalive, 60}
                               ]),
    try 
        {ok, _} = emqtt:connect(C),
        emqtt:subscribe(C, <<"TopicA">>, qos2),
        emqtt:publish(C, <<"TopicA">>, <<"Payload...">>, qos2),
        emqtt:unsubscribe(C, <<"TopicA">>),
        emqtt:disconnect(C),
        timer:sleep(100),
        [begin
            Maps = emqx_json:decode(P, [return_maps]),
            validate_hook_resp(Maps),
            validate_hook_headers(Headers)
        end
    || {{P, _Bool}, Headers} <- http_server:get_received_data()]
    after
        http_server:stop(ServerPid)
    end,
    Config.

t_check_hooked(_) ->
    {ok, Rules} = application:get_env(emqx_web_hook, rules),
    lists:foreach(fun({HookName, _Action}) ->
                          Hooks = ?HOOK_LOOKUP(HookName),
                          ?assertEqual(true, length(Hooks) > 0)
                  end, Rules).

t_change_config(_) ->
    {ok, Rules} = application:get_env(emqx_web_hook, rules),
    emqx_web_hook:unload(),
    HookRules = lists:keydelete("message.delivered", 1, Rules),
    application:set_env(emqx_web_hook, rules, HookRules),
    emqx_web_hook:load(),
    ?assertEqual([], ?HOOK_LOOKUP("message.delivered")),
    emqx_web_hook:unload(),
    application:set_env(emqx_web_hook, rules, Rules),
    emqx_web_hook:load().

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

validate_hook_headers(Headers) ->
    ?assertEqual(<<"K1">>, maps:get(<<"k1">>, Headers)),
    ?assertEqual(<<"K2">>, maps:get(<<"k2">>, Headers)).

validate_hook_resp(Body = ?ACTION(<<"client_connect">>)) ->
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"client_connack">>)) ->
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"success">>, maps:get(<<"conn_ack">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"client_connected">>)) ->
    _ = maps:get(<<"connected_at">>, Body),
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"client_disconnected">>)) ->
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"client_subscribe">>)) ->
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"client_unsubscribe">>)) ->
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"session_subscribed">>)) ->
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"session_unsubscribed">>)) ->
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"session_terminated">>)) ->
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_username_clientid(Body);
validate_hook_resp(Body = ?ACTION(<<"message_publish">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body);
validate_hook_resp(Body = ?ACTION(<<"message_delivered">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body);
validate_hook_resp(Body = ?ACTION(<<"message_acked">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body).

assert_username_clientid(#{<<"clientid">> := ClientId, <<"username">> := Username}) ->
    ?assertEqual(<<"simpleClient">>, ClientId),
    ?assertEqual(null, Username).

assert_messages_attrs(#{ <<"ts">> := _
                       , <<"qos">> := _
                       , <<"topic">> := _
                       , <<"retain">> := _
                       , <<"payload">> := _
                       , <<"from_username">> := _
                       , <<"from_client_id">> := _
                       }) ->
    ok.

apps() ->
    [emqx_web_hook, emqx_modules, emqx_management, emqx_rule_engine].
