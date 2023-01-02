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

-module(emqx_lua_hook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [case01, case02, case03, case04,
     case11, case12, case13,
     case21, case22,
     case31, case32,
     case41, case42, case43,
     case51, case52, case53,
     case61, case62,
     case71, case72, case73,
     case81, case82, case83,
     case101,
     case110, case111, case112, case113, case114, case115,
     case201, case202, case203, case204, case205,
     case301, case302,
     t_stop_sub
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_lua_hook], fun set_special_configs/1),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_lua_hook]),
    Config.

set_special_configs(emqx) ->
    application:set_env(emqx, modules, []);
set_special_configs(_App) ->
    ok.

init_per_testcase(_, Config) ->
    ok = filelib:ensure_dir(filename:join([emqx_lua_hook:lua_dir(), "a"])),
    emqx_lua_hook:start_link(),
    Config.

end_per_testcase(_, _Config) ->
    emqx_lua_hook:stop(),
    AllScripts = filelib:wildcard(filename:join([emqx_lua_hook:lua_dir(), "*"])),
    [file:delete(Filename) || Filename <- AllScripts].

case01(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(ClientId, Username, topic, payload, qos, retain)"
            "\n    return topic, \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), from = <<"myclient">>, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{username => <<"tester">>}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{payload = <<"hello">>}, Ret).

case02(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return false"     % return false to stop hook
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), from = <<"myclient">>, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{username => <<"tester">>}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{headers = #{username => <<"tester">>, allow_publish => false}}, Ret).

case03(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =  "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{from = <<"myclient">>, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{username => <<"tester">>}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg, Ret).

case04(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    if clientid == \"broker\" then"
            "\n        return topic, \"hello broker\", qos, retain"
            "\n    else"
            "\n        return false"     % return false to stop hook
            "\n    end"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), from = <<"broker">>, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{username => <<"tester">>}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{payload = <<"hello broker">>}, Ret).

case11(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_delivered(clientid, username, topic, payload, qos, retain)"
            "\n    return false"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_delivered\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.delivered', [#{clientid => <<"myclient">>, username => <<"myuser">>}], Msg),
    ?assertEqual(Msg, Ret),
    ok = file:delete(ScriptName).

case12(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_delivered(clientid, username, topic, payload, qos, retain)"
            "\n    return topic, \"hello broker\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_delivered\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.delivered', [#{clientid => <<"myclient">>, username => <<"myuser">>}], Msg),
    ?assertEqual(Msg#message{payload = <<"hello broker">>}, Ret).

case13(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_delivered(clientid, username, topic, payload, qos, retain)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_delivered\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.delivered', [#{clientid => <<"myclient">>, username => <<"myuser">>}], Msg),
    ?assertEqual(Msg, Ret).

case21(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_acked(clientid, username, topic, payload, qos, retain)"
            "\n    return true"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_acked\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run('message.acked', [#{clientid => <<"myclient">>, username => <<"myuser">>}, Msg]),
    ?assertEqual(ok, Ret).

case22(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_acked(clientid, username, topic, payload, qos, retain)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_acked\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run('message.acked', [#{clientid => <<"myclient">>, username => <<"myuser">>}, Msg]),
    ?assertEqual(ok, Ret),
    ok = file:delete(ScriptName).

case31(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_connected(clientid, username)"
            "\n    return 0"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_connected\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    ?assertEqual(ok,
                 emqx_hooks:run('client.connected',
                                [#{clientid => <<"myclient">>, username => <<"tester">>}, #{}])).

case32(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_connected(clientid, username)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_connected\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    ?assertEqual(ok,
                 emqx_hooks:run('client.connected',
                                [#{clientid => <<"myclient">>, username => <<"tester">>}, #{}])).

case41(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_subscribe(clientid, username, topic)"
            "\n    if topic == \"a/b/c\" then"
            "\n        topic = \"a1/b1/c1\";"
            "\n    end"
            "\n    return topic"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_subscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.subscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual([{<<"a1/b1/c1">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}], Ret).

case42(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_subscribe(clientid, username, topic)"
            "\n    return false"     % return false to stop hook
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_subscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.subscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual(TopicTable, Ret).

case43(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_subscribe(clientid, username, topic)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_subscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.subscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual(TopicTable, Ret).

case51(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_unsubscribe(clientid, username, topic)"
            "\n    if topic == \"a/b/c\" then"
            "\n        topic = \"a1/b1/c1\";"
            "\n    end"
            "\n    return topic"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_unsubscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.unsubscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual([{<<"a1/b1/c1">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}], Ret).

case52(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_unsubscribe(clientid, username, topic)"
            "\n    return false"     % return false to stop hook
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_unsubscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.unsubscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual(TopicTable, Ret).

case53(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_unsubscribe(clientid, username, topic)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_unsubscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret = emqx_hooks:run_fold('client.unsubscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual(TopicTable, Ret).

case61(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_disconnected(clientid, username, reasoncode)"
            "\n    return 0"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_disconnected\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    ?assertEqual(ok,
                 emqx_hooks:run('client.disconnected',
                                [#{clientid => <<"myclient">>, username => <<"tester">>}, 0])).

case62(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_client_disconnected(clientid, username, reasoncode)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_disconnected\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    ?assertEqual(ok,
                 emqx_hooks:run('client.disconnected',
                                [#{clientid => <<"myclient">>, username => <<"tester">>}, 0])).

case71(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_subscribed(clientid, username, topic)"
            "\n    return 0"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_subscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case72(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_subscribed(clientid, username, topic)"
            "\n    return false"     % return false to stop hook
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_subscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case73(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_subscribed(clientid, username, topic)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_subscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case81(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_unsubscribed(clientid, username, topic)"
            "\n    return 0"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_unsubscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.unsubscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case82(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_unsubscribed(clientid, username, topic)"
            "\n    return false"     % return false to stop hook
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_unsubscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.unsubscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case83(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_unsubscribed(clientid, username, topic)"
            "\n    return 9/0"     % this code has fatal error
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_session_unsubscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.unsubscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case101(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    ScriptName2 = filename:join([emqx_lua_hook:lua_dir(), "mn.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return topic, \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code),

    Code2 =    "function on_client_subscribe(clientid, username, topic)"
            "\n    if topic == \"a/b/c\" then"
            "\n        topic = \"a1/b1/c1\";"
            "\n    end"
            "\n    return topic"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_client_subscribe\""
            "\nend",
    ok = file:write_file(ScriptName2, Code2), ok = emqx_lua_hook:load_scripts(),

    ID = emqx_guid:gen(),
    Ret = emqx_hooks:run_fold('message.publish',[], #message{id = ID, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}}),
    ?assertEqual(#message{id = ID, qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"hello">>, headers = #{}}, Ret),

    TopicTable = [{<<"a/b/c">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}],
    Ret2 = emqx_hooks:run_fold('client.subscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual([{<<"a1/b1/c1">>, [qos, 1]}, {<<"d/+/e">>, [{qos, 2}]}], Ret2).

case110(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return \"changed/topic\", \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{topic = <<"changed/topic">>, payload = <<"hello">>}, Ret).

case111(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =  " function on_message_publish(topic, payload, qos, retain)"
            "\n    return \"changed/topic\", \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    emqx_ctl:run_command(["luahook", "unload", ScriptName]),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg, Ret).

case112(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =  " function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return \"changed/topic\", \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    emqx_ctl:run_command(["luahook", "unload", "abc.lua"]),
    timer:sleep(100),
    emqx_ctl:run_command(["luahook", "load", "abc.lua"]),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{topic = <<"changed/topic">>, payload = <<"hello">>}, Ret).

case113(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    ScriptDisabled = ScriptName ++ ".x",
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
                "\n    return \"changed/topic\", \"hello\", qos, retain"
                "\nend"
                "\n"
                "\nfunction register_hook()"
                "\n    return \"on_message_publish\""
                "\nend",
    ok = file:write_file(ScriptName, Code),
    file:delete(ScriptDisabled),
    emqx_ctl:run_command(["luahook", "disable", "abc.lua"]),   % this command will rename "abc.lua" to "abc.lua.x"

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg, Ret),
    true = filelib:is_file(ScriptDisabled).

case114(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua.x"]),     % disabled script
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return \"changed/topic\", \"hello\", qos, retain"
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    emqx_ctl:run_command(["luahook", "enable", "abc.lua"]),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{topic = <<"changed/topic">>, payload = <<"hello">>}, Ret).

case115(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return \"changed/topic\", \"hello\", qos, retain"
            "\nend"
            "\n"
            "function on_client_subscribe(ClientId, Username, Topic)"
            "\n    return \"play/football\""
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\", \"on_client_subscribe\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    emqx_ctl:run_command(["luahook", "reload", "abc.lua"]),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{topic = <<"changed/topic">>, payload = <<"hello">>}, Ret),

    TopicTable = [{<<"d/+/e">>, [{qos, 2}]}],
    Ret2 = emqx_hooks:run_fold('client.subscribe',[#{clientid => <<"myclient">>, username => <<"myuser">>}, #{}], TopicTable),
    ?assertEqual([{<<"play/football">>, [{qos, 2}]}], Ret2).

case201(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_session_subscribed(clientid, username, topic)"
            "\n    return 0"
            "\nend"
            "\n"
            "\nfunction on_session_subscribed1()"  % register_hook() is missing
            "\n    return \"on_session_subscribed\""
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case202(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function abc(clientid, username, topic)"
            "\n    return 0"
            "\nend"
            "\n"
            "\n9/0",   % error code
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case203(_Config) ->
    file:del_dir(emqx_lua_hook:lua_dir()),  % if this dir is not exist, what will happen?
    emqx_lua_hook:load_scripts(),

    Topic = <<"a/b/c">>,
    Ret = emqx_hooks:run('session.subscribed',[#{clientid => <<"myclient">>, username => <<"myuser">>}, Topic, #{first => false}]),
    ?assertEqual(ok, Ret).

case204(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return topic, payload .. \"_Z\", qos, retain"
            "\nend"
            "\n"
            "function on_client_subscribe(ClientId, Username, Topic)"
            "\n    return \"play/football\""
            "\nend"
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\", \"on_client_subscribe\", \"on_message_publish\""  % if 2 on_message_publish() are registered, what will happend?
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg#message{payload = <<"123_Z">>}, Ret).

case205(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =    "function on_message_publish(clientid, username, topic, payload, qos, retain)"
            "\n    return topic, \"hello\", qos, retain"
            "\nend_with_error"  %% syntax error
            "\n"
            "\nfunction register_hook()"
            "\n    return \"on_message_publish\", \"on_client_subscribe\", \"on_message_publish\""  % if 2 on_message_publish() are registered, what will happend?
            "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    Msg = #message{id = emqx_guid:gen(), qos = 2, flags = #{retain => true}, topic = <<"a/b/c">>, payload = <<"123">>, headers = #{}},
    Ret = emqx_hooks:run_fold('message.publish',[], Msg),
    ?assertEqual(Msg, Ret).

case301(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =   "function on_client_authenticate(clientid, username, peerhost, password)"
           "\n    return \"ok\""
           "\nend"
           "\n"
           "\nfunction register_hook()"
           "\n    return \"on_client_authenticate\""
           "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),

    ClientInfo = #{clientid => undefined,
                   username => <<"test">>,
                   peerhost => {127, 0, 0, 1},
                   password => <<"mqtt">>
                  },
    Result = #{auth_result => success, anonymous => true},
    ?assertEqual(Result#{auth_result => success},
                 emqx_hooks:run_fold('client.authenticate', [ClientInfo], Result)).

case302(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code =   "function on_client_check_acl(clientid, username, peerhost, password, topic, pubsub)"
           "\n    return \"allow\""
           "\nend"
           "\n"
           "\nfunction register_hook()"
           "\n    return \"on_client_check_acl\""
           "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    ClientInfo = #{clientid => undefined,
                   username => <<"test">>,
                   peerhost => {127, 0, 0, 1},
                   password => <<"mqtt">>
                  },
    ?assertEqual(allow, emqx_hooks:run_fold('client.check_acl',
                                            [ClientInfo, publish, <<"mytopic">>], deny)).

t_stop_sub(_Config) ->
    ScriptName = filename:join([emqx_lua_hook:lua_dir(), "abc.lua"]),
    Code = "function on_client_subscribe(clientid, username, topic)"
           "\n  return false"
           "\nend"
           "\n"
           "\nfunction register_hook()"
           "\n    return \"on_client_subscribe\""
           "\nend",
    ok = file:write_file(ScriptName, Code), ok = emqx_lua_hook:load_scripts(),
    ClientInfo = #{clientid => undefined,
                   username => <<"test">>,
                   peerhost => {127, 0, 0, 1},
                   password => <<"mqtt">>
                  },
    OriginalTopicFilters = [{Topic = <<"u">>,
                             Opts = #{nl => 0,qos => 0,rap => 0,rh => 0}}],
    Props = #{},
    Expected = [{Topic, Opts#{deny_subscription => true}}],
    ?assertEqual(Expected, emqx_hooks:run_fold('client.subscribe',
                                         [ClientInfo, Props],
                                         OriginalTopicFilters)).
