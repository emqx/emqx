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

-module(emqx_web_hook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").

-define(HOOK_LOOKUP(H), emqx_hooks:lookup(list_to_atom(H))).
-define(ACTION(Name), #{<<"action">> := Name}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [ {group, http}
    , {group, https}
    , {group, ipv6http}
    , {group, ipv6https}
    , test_rule_webhook
    , test_preproc_headers
    ].

groups() ->
    Cases = [test_full_flow],
    [ {http, [sequence], Cases}
    , {https, [sequence], Cases}
    , {ipv6http, [sequence], Cases}
    , {ipv6https, [sequence], Cases}
    ].

start_apps() ->
    [application:load(App) || App <- apps()],
    emqx_ct_helpers:start_apps(apps()).
start_apps(F) ->
    [application:load(App) || App <- apps()],
    emqx_ct_helpers:start_apps(apps(), F).

init_per_group(rules, Config) -> Config;
init_per_group(Name, Config) ->
    net_kernel:start(['test@127.0.0.1', longnames]),
    application:ensure_all_started(emqx_management),
    set_special_cfgs(),
    BasePort =
        case Name of
            all -> 8801;
            http -> 8811;
            https -> 8821;
            ipv6http -> 8831;
            ipv6https -> 8841
        end,
    CF = case Name of
             all ->  fun set_special_configs_http/1;
             http -> fun set_special_configs_http/1;
             https -> fun set_special_configs_https/1;
             ipv6http -> fun set_special_configs_ipv6_http/1;
             ipv6https -> fun set_special_configs_ipv6_https/1
         end,
    start_apps(fun(_) -> CF(BasePort) end),
    Opts = case atom_to_list(Name) of
               "ipv6" ++ _ -> [{ip, {0,0,0,0,0,0,0,1}}, inet6];
               _ -> [inet]
           end,
    [{base_port, BasePort}, {transport_opts, Opts} | Config].

init_per_testcase(test_rule_webhook, Config) ->
    net_kernel:start(['test@127.0.0.1', longnames]),
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    Handler = fun(_) ->
        application:set_env(emqx_web_hook, rules, []),
        application:set_env(emqx_web_hook, url, "http://127.0.0.1:9999/"),
        application:set_env(emqx_web_hook, ssl, false),
        application:set_env(emqx_web_hook, ssloptions, [])
    end,
    ok = start_apps(Handler),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_group(_Name, Config) ->
    emqx_ct_helpers:stop_apps(apps()),
    Config.

set_special_configs_http(Port) ->
    application:set_env(emqx_web_hook, url, "http://127.0.0.1:" ++ integer_to_list(Port)).

set_special_configs_https(Port) ->
    set_ssl_configs(),
    application:set_env(emqx_web_hook, url, "https://127.0.0.1:" ++ integer_to_list(Port+1)).

set_special_configs_ipv6_http(Port) ->
    application:set_env(emqx_web_hook, url, "http://[::1]:" ++ integer_to_list(Port)).

set_special_configs_ipv6_https(Port) ->
    set_ssl_configs(),
    application:set_env(emqx_web_hook, url, "https://[::1]:" ++ integer_to_list(Port+1)).

set_ssl_configs() ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    SslOpts = [{keyfile, Path ++ "/client-key.pem"},
               {certfile, Path ++ "/client-cert.pem"},
               {cacertfile, Path ++ "/ca.pem"}],
    application:set_env(emqx_web_hook, ssl, true),
    application:set_env(emqx_web_hook, ssloptions, SslOpts).

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

test_preproc_headers(_) ->
    TestTable = [
     {#{<<"Content_TYPE">> => <<"application/JSON">>, <<"Key">> => <<"Val">>},
      #{<<"content-type">> => <<"application/JSON">>, <<"key">> => <<"Val">>}
     },
     {#{<<"${ContentTypeKey}">> => <<"application/JSON">>},
      #{<<"content-type">> => <<"application/JSON">>}
     },
     {#{<<"content-type">> => <<"${ContentTypeVal}">>},
      #{<<"content-type">> => <<"application/JSON">>}
     },
     {#{<<"Content_type">> => <<"${ContentTypeVal}">>},
      #{<<"content-type">> => <<"application/JSON">>}
     },
     {#{<<"${ContentTypeKey}">> => <<"${ContentTypeVal}">>, <<"Key">> => <<"Val">>},
      #{<<"content-type">> => <<"application/JSON">>, <<"key">> => <<"Val">>}
     },
     {#{<<"${ContentTypeKey}">> => <<"${ContentTypeVal}">>, <<"Key">> => <<"Val">>},
      #{<<"content-type">> => <<"application/JSON">>, <<"key">> => <<"Val">>}
     },
     {#{<<"Content_${TypeKey}">> => <<"application/${TypeVal}">>, <<"Key">> => <<"Val">>},
      #{<<"content-type">> => <<"application/JSON">>, <<"key">> => <<"Val">>}
     }
    ],
    SelectedData1 = #{
        <<"ContentTypeKey">> => <<"content-type">>,
        <<"ContentTypeVal">> => <<"application/JSON">>,
        <<"TypeKey">> => <<"type">>,
        <<"TypeVal">> => <<"JSON">>
    },
    SelectedData2 = #{
        <<"ContentTypeKey">> => <<"ConTent_Type">>,
        <<"ContentTypeVal">> => <<"application/JSON">>,
        <<"TypeKey">> => <<"TYPe">>,
        <<"TypeVal">> => <<"JSON">>
    },
    [begin
        ct:pal("test_preproc_headers, input: ~p, method: ~p, selected: ~p", [Input, Method, Selected]),
        Headers0 = emqx_web_hook_actions:preproc_and_normalise_headers(Input),
        Headers1 = emqx_web_hook_actions:maybe_remove_content_type_header(Headers0, Method),
        Result0 = emqx_web_hook_actions:maybe_proc_headers(Headers1, Method, Selected),
        Expected1 = case Method =/= post andalso Method =/= put of
            true -> maps:remove(<<"content-type">>, Expected);
            false -> Expected
        end,
        ?assertEqual(Expected1, maps:from_list(Result0))
     end ||
     {Input, Expected} <- TestTable,
     Selected <- [SelectedData1, SelectedData2],
     Method <- [post, put, get, delete]
    ].

test_rule_webhook(_) ->
    {ok, ServerPid} = http_server:start_link(self(), 9999, []),
    receive {ServerPid, ready} -> ok
    after 1000 -> error(timeout)
    end,

    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = ResId}} = emqx_rule_engine:create_resource(
            #{type => web_hook,
              config => #{<<"url">> => "http://127.0.0.1:9999/"},
              description => <<"For testing">>}),
    {ok, #rule{id = Id}} = emqx_rule_engine:create_rule(
                #{rawsql => "select * from \"t1\"",
                  actions => [#{name => 'data_to_webserver',
                                args => #{<<"$resource">> => ResId}}],
                  type => web_hook,
                  description => <<"For testing">>
                  }),

    Properties = #{'User-Property' => [{<<"user_property_key">>, <<"user_property_value">>}]},
    ClientId = iolist_to_binary(["client-", integer_to_list(erlang:unique_integer([positive]))]),

    {ok, Client} = emqtt:start_link([ {clientid, ClientId}
                                    , {proto_ver, v5}
                                    , {keepalive, 60}
                                    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _} = emqtt:publish(Client, <<"t1">>, Properties, <<"Payload...">>, [{qos, 2}]),

    Res = receive {http_server, {Any, _Bool}, _Header} -> {ok, Any}
          after 100 -> error
          end,

    ?assertMatch({ok, _}, Res),
    {ok, Body} = Res,
    ?assertNotEqual([], binary:matches(Body, <<"User-Property">>)),
    ?assertNotEqual([], binary:matches(Body, <<"user_property_key">>)),
    ?assertNotEqual([], binary:matches(Body, <<"user_property_value">>)),

    emqtt:stop(Client),
    http_server:stop(ServerPid),
    emqx_rule_registry:remove_rule(Id),
    emqx_rule_registry:remove_resource(ResId),
    ok.

test_full_flow(Config) ->
    [_|_] = Opts = proplists:get_value(transport_opts, Config),
    BasePort = proplists:get_value(base_port, Config),
    Tester = self(),
    {ok, ServerPid} = http_server:start_link(Tester, BasePort, Opts),
    receive {ServerPid, ready} -> ok
    after 1000 -> error(timeout)
    end,
    application:set_env(emqx_web_hook, headers, [{"k1","K1"}, {"k2", "K2"}]),
    ClientId = iolist_to_binary(["client-", integer_to_list(erlang:unique_integer([positive]))]),
    {ok, C} = emqtt:start_link([ {clientid, ClientId}
                               , {proto_ver, v5}
                               , {keepalive, 60}
                               ]),
    try
        do_test_full_flow(C, ClientId)
    after
        Ref = erlang:monitor(process, ServerPid),
        http_server:stop(ServerPid),
        receive {'DOWN', Ref, _, _, _} -> ok
        after 5000 -> error(timeout)
        end
    end.

do_test_full_flow(C, ClientId) ->
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos2),
    {ok, _} = emqtt:publish(C, <<"TopicA">>, <<"Payload...">>, qos2),
    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicA">>),
    emqtt:disconnect(C),
    validate_params_and_headers(undefined, ClientId).

validate_params_and_headers(ClientState, ClientId) ->
    receive
        {http_server, {Params0, _Bool}, Headers} ->
            Params = emqx_json:decode(Params0, [return_maps]),
            try
                validate_hook_resp(ClientId, Params),
                validate_hook_headers(Headers),
                case maps:get(<<"action">>, Params) of
                    <<"session_terminated">> ->
                        ok;
                    <<"client_connect">> ->
                        validate_params_and_headers(connected, ClientId);
                    _ ->
                        validate_params_and_headers(ClientState, ClientId) %% continue looping
                end
            catch
                throw : {unknown_client, Other} ->
                    ct:pal("ignored_event_from_other_client ~p~nexpecting:~p~n~p~n~p",
                           [Other, ClientId, Params, Headers]),
                    validate_params_and_headers(ClientState, ClientId) %% continue looping
            end
    after
        5000 ->
            case ClientState =:= undefined of
              true  -> error("client_was_never_connected");
              false -> error("terminate_action_is_not_received_in_time")
            end
    end.

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

validate_hook_resp(ClientId, Body = ?ACTION(<<"client_connect">>)) ->
    assert_username_clientid(ClientId, Body),
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    ok;
validate_hook_resp(ClientId, Body = ?ACTION(<<"client_connack">>)) ->
    assert_username_clientid(ClientId, Body),
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"success">>, maps:get(<<"conn_ack">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    ok;
validate_hook_resp(ClientId, Body = ?ACTION(<<"client_connected">>)) ->
    assert_username_clientid(ClientId, Body),
    _ = maps:get(<<"connected_at">>, Body),
    ?assertEqual(5,  maps:get(<<"proto_ver">>, Body)),
    ?assertEqual(60, maps:get(<<"keepalive">>, Body)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"client_disconnected">>)) ->
    assert_username_clientid(ClientId, Body),
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"client_subscribe">>)) ->
    assert_username_clientid(ClientId, Body),
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"client_unsubscribe">>)) ->
    assert_username_clientid(ClientId, Body),
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"session_subscribed">>)) ->
    assert_username_clientid(ClientId, Body),
    _ = maps:get(<<"opts">>, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"session_unsubscribed">>)) ->
    assert_username_clientid(ClientId, Body),
    ?assertEqual(<<"TopicA">>, maps:get(<<"topic">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(ClientId, Body = ?ACTION(<<"session_terminated">>)) ->
    assert_username_clientid(ClientId, Body),
    ?assertEqual(<<"normal">>, maps:get(<<"reason">>, Body)),
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body));
validate_hook_resp(_ClientId, Body = ?ACTION(<<"message_publish">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body);
validate_hook_resp(_ClientId, Body = ?ACTION(<<"message_delivered">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body);
validate_hook_resp(_ClientId, Body = ?ACTION(<<"message_acked">>)) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"node">>, Body)),
    assert_messages_attrs(Body).

assert_username_clientid(ClientId, #{<<"clientid">> := ClientId, <<"username">> := Username}) ->
    ?assertEqual(null, Username);
assert_username_clientid(_ClientId, #{<<"clientid">> := Other}) ->
    throw({unknown_client, Other}).

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
