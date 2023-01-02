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

-module(emqx_mgmt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(LOG_LEVELS, ["debug", "error", "info"]).
-define(LOG_HANDLER_ID, [file, default]).

all() ->
    OtherTCs = (emqx_ct:all(?MODULE) -- manage_apps_tests()) -- check_cli_tests(),
    [{group, manage_apps},
     {group, check_cli}] ++ OtherTCs.

manage_apps_tests() ->
    [t_app, t_alarms].

check_cli_tests() ->
    [t_cli,
     t_log_cmd,
     t_mgmt_cmd,
     t_status_cmd,
     t_clients_cmd,
     t_vm_cmd,
     t_plugins_cmd,
     t_trace_cmd,
     t_traces_cmd,
     t_broker_cmd,
     t_router_cmd,
     t_subscriptions_cmd,
     t_listeners_cmd_old,
     t_listeners_cmd_new
    ].

groups() ->
    [{manage_apps, [sequence],
      manage_apps_tests()},
     {check_cli, [sequence],
      check_cli_tests()}].

apps() ->
    [emqx_management, emqx_auth_mnesia, emqx_modules].

init_per_suite(Config) ->
    ekka_mnesia:start(),
    emqx_mgmt_auth:mnesia(boot),
    emqx_ct_helpers:start_apps(apps()),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps(apps()).

init_per_testcase(t_plugins_cmd, Config) ->
    meck:new(emqx_plugins, [non_strict, passthrough]),
    meck:expect(emqx_plugins, load, fun(_) -> ok end),
    meck:expect(emqx_plugins, unload, fun(_) -> ok end),
    meck:expect(emqx_plugins, reload, fun(_) -> ok end),
    mock_print(),
    Config;
init_per_testcase(t_import_outside_backup_dir, Config) ->
    BackupDir = emqx_mgmt_data_backup:backup_dir(),
    {ok, Files} = file:list_dir(BackupDir),
    lists:foreach(
      fun(F) ->
              file:delete(filename:join(BackupDir, F))
      end, Files),
    RandomName = emqx_guid:to_hexstr(emqx_guid:gen()),
    Filepath = "/tmp/" ++ binary_to_list(RandomName) ++ ".json",
    FakeData = #{version => "4.4"},
    ok = file:write_file(Filepath, emqx_json:encode(FakeData)),
    [ {tmp_file, Filepath}
    | Config];
init_per_testcase(t_backup_file, Config) ->
    BackupDir = emqx_mgmt_data_backup:backup_dir(),
    {ok, Files} = file:list_dir(BackupDir),
    lists:foreach(
      fun(F) ->
              file:delete(filename:join(BackupDir, F))
      end, Files),
    Config;
init_per_testcase(Case, Config) when Case =:= t_alarms ->
    try
        ?MODULE:Case({'init', Config})
    catch
        error : function_clause ->
            Config
    end;
init_per_testcase(_Case, Config) ->
    mock_print(),
    Config.

end_per_testcase(t_plugins_cmd, _Config) ->
    meck:unload(emqx_plugins),
    unmock_print();
end_per_testcase(t_import_outside_backup_dir, Config) ->
    Filepath = ?config(tmp_file, Config),
    file:delete(Filepath),
    BackupDir = emqx_mgmt_data_backup:backup_dir(),
    {ok, Files} = file:list_dir(BackupDir),
    lists:foreach(
      fun(F) ->
              file:delete(filename:join(BackupDir, F))
      end, Files),
    ok;
end_per_testcase(t_backup_file, _Config) ->
    BackupDir = emqx_mgmt_data_backup:backup_dir(),
    {ok, Files} = file:list_dir(BackupDir),
    lists:foreach(
      fun(F) ->
              file:delete(filename:join(BackupDir, F))
      end, Files),
    ok;
end_per_testcase(Case, Config) when Case =:= t_alarms ->
    try
        ?MODULE:Case({'end', Config})
    catch
        error : function_clause ->
            ok
    end;
end_per_testcase(_Case, _Config) ->
    unmock_print().

t_app(_Config) ->
    {ok, AppSecret} = emqx_mgmt_auth:add_app(<<"app_id">>, <<"app_name">>),
    ?assert(emqx_mgmt_auth:is_authorized(<<"app_id">>, AppSecret)),
    ?assertEqual(AppSecret, emqx_mgmt_auth:get_appsecret(<<"app_id">>)),
    ?assertEqual({<<"app_id">>, AppSecret,
                  <<"app_name">>, <<"Application user">>,
                  true, undefined},
                 lists:keyfind(<<"app_id">>, 1, emqx_mgmt_auth:list_apps())),
    emqx_mgmt_auth:del_app(<<"app_id">>),
    %% Use the default application secret
    application:set_env(emqx_management, application, [{default_secret, <<"public">>}]),
    {ok, AppSecret1} = emqx_mgmt_auth:add_app(
                         <<"app_id">>, <<"app_name">>, <<"app_desc">>, true, undefined),
    ?assert(emqx_mgmt_auth:is_authorized(<<"app_id">>, AppSecret1)),
    ?assertEqual(AppSecret1, emqx_mgmt_auth:get_appsecret(<<"app_id">>)),
    ?assertEqual(AppSecret1, <<"public">>),
    ?assertEqual({<<"app_id">>, AppSecret1, <<"app_name">>, <<"app_desc">>, true, undefined},
                 lists:keyfind(<<"app_id">>, 1, emqx_mgmt_auth:list_apps())),
    emqx_mgmt_auth:del_app(<<"app_id">>),
    application:set_env(emqx_management, application, []),
    %% Specify the application secret
    {ok, AppSecret2} = emqx_mgmt_auth:add_app(
                         <<"app_id">>, <<"app_name">>, <<"secret">>,
                         <<"app_desc">>, true, undefined),
    ?assert(emqx_mgmt_auth:is_authorized(<<"app_id">>, AppSecret2)),
    ?assertEqual(AppSecret2, emqx_mgmt_auth:get_appsecret(<<"app_id">>)),
    ?assertEqual({<<"app_id">>, AppSecret2, <<"app_name">>, <<"app_desc">>, true, undefined},
                 lists:keyfind(<<"app_id">>, 1, emqx_mgmt_auth:list_apps())),
    emqx_mgmt_auth:del_app(<<"app_id">>),
    ok.

t_alarms({'init', Config}) ->
    meck:new(rpc, [unstick]),
    meck:expect(rpc, call, 4, ok),
    Config;
t_alarms({'end', _}) ->
    meck:unload(rpc);
t_alarms(_) ->
    ok = emqx_mgmt:delete_all_deactivated_alarms(remote_node),
    ?assert(meck:called(rpc, call, [remote_node, emqx_alarm, delete_all_deactivated_alarms, []])).

t_log_cmd(_) ->
    lists:foreach(fun(Level) ->
                      emqx_mgmt_cli:log(["primary-level", Level]),
                      ?assertEqual(Level ++ "\n", emqx_mgmt_cli:log(["primary-level"]))
                  end, ?LOG_LEVELS),
    lists:foreach(fun(Level) ->
                     emqx_mgmt_cli:log(["set-level", Level]),
                     ?assertEqual(Level ++ "\n", emqx_mgmt_cli:log(["primary-level"]))
                  end, ?LOG_LEVELS),
    [lists:foreach(fun(Level) ->
                         ?assertEqual(Level ++ "\n", emqx_mgmt_cli:log(["handlers", "set-level",
                                                                      atom_to_list(Id), Level]))
                      end, ?LOG_LEVELS)
        || #{id := Id} <- emqx_logger:get_log_handlers()].

t_mgmt_cmd(_) ->
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["lookup", "emqx_appid"]), "Not Found.")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["insert", "emqx_appid", "emqx_name"]), "AppSecret:")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["insert", "emqx_appid", "emqx_name"]), "Error:")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["lookup", "emqx_appid"]), "app_id:")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["update", "emqx_appid", "ts"]), "update successfully")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:mgmt(
                                      ["delete", "emqx_appid"]), "ok")),
    ok = emqx_mgmt_cli:mgmt(["list"]).

t_status_cmd(_) ->
    %% init internal status seem to be always 'starting' when running ct tests
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:status([]), "Node\s.*@.*\sis\sstart(ed|ing)")).

t_broker_cmd(_) ->
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:broker([]), "sysdescr")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:broker(["stats"]), "subscriptions.shared")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:broker(["metrics"]), "bytes.sent")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:broker([undefined]), "broker")).

t_clients_cmd(_) ->
    process_flag(trap_exit, true),
    {ok, T} = emqtt:start_link([{clientid, <<"client12">>},
                                {username, <<"testuser1">>},
                                {password, <<"pass1">>}
                               ]),
    {ok, _} = emqtt:connect(T),
    timer:sleep(300),
    emqx_mgmt_cli:clients(["list"]),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:clients(["show", "client12"]), "client12")),
    ?assertEqual("ok\n", emqx_mgmt_cli:clients(["kick", "client12"])),
    timer:sleep(500),
    ?assertEqual("ok\n", emqx_mgmt_cli:clients(["kick", "client12"])),
    receive
        {'EXIT', T, _} ->
            ok
    after
        500 ->
            erlang:error("Client is not kick")
    end,
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),
    Packet = raw_send_serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                                                   clientid = <<"client13">>})),
    ok = rfc6455_client:send_binary(WS, Packet),
    Connack = ?CONNACK_PACKET(?CONNACK_ACCEPT),
    {binary, Bin} = rfc6455_client:recv(WS),
    {ok, Connack, <<>>, _} = raw_recv_pase(Bin),
    timer:sleep(300),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:clients(["show", "client13"]), "client13")),

    % emqx_mgmt_cli:clients(["kick", "client13"]),
    % timer:sleep(500),
    % ?assertMatch({match, _}, re:run(emqx_mgmt_cli:clients(["show", "client13"]), "Not Found")).
    ok.

raw_recv_pase(Packet) ->
    emqx_frame:parse(Packet).

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

t_vm_cmd(_) ->
    [[?assertMatch({match, _}, re:run(Result, Name))
      || Result <- emqx_mgmt_cli:vm([Name])]
     || Name <- ["load", "memory", "process", "io", "ports"]],
    [?assertMatch({match, _}, re:run(Result, "load"))
     || Result <- emqx_mgmt_cli:vm(["load"])],
    [?assertMatch({match, _}, re:run(Result, "memory"))
     || Result <- emqx_mgmt_cli:vm(["memory"])],
    [?assertMatch({match, _}, re:run(Result, "process"))
     || Result <- emqx_mgmt_cli:vm(["process"])],
    [?assertMatch({match, _}, re:run(Result, "io"))
     || Result <- emqx_mgmt_cli:vm(["io"])],
    [?assertMatch({match, _}, re:run(Result, "ports"))
     || Result <- emqx_mgmt_cli:vm(["ports"])].

t_trace_cmd(_) ->
    logger:set_primary_config(level, debug),
    {ok, T} = emqtt:start_link([{clientid, <<"client">>},
                                 {username, <<"testuser">>},
                                 {password, <<"pass">>}
                                ]),
    emqtt:connect(T),
    emqtt:subscribe(T, <<"a/b/c">>),
    Trace1 = emqx_mgmt_cli:trace(["start", "client", "client",
                                  "log/clientid_trace.log"]),
    ?assertMatch({match, _}, re:run(Trace1, "successfully")),
    Trace2 = emqx_mgmt_cli:trace(["stop", "client", "client"]),
    ?assertMatch({match, _}, re:run(Trace2, "successfully")),
    Trace3 = emqx_mgmt_cli:trace(["start", "client", "client",
                                  "log/clientid_trace.log",
                                  "error"]),
    ?assertMatch({match, _}, re:run(Trace3, "successfully")),
    Trace4 = emqx_mgmt_cli:trace(["stop", "client", "client"]),
    ?assertMatch({match, _}, re:run(Trace4, "successfully")),
    Trace5 = emqx_mgmt_cli:trace(["start", "topic", "a/b/c",
                                  "log/clientid_trace.log"]),
    ?assertMatch({match, _}, re:run(Trace5, "successfully")),
    Trace6 = emqx_mgmt_cli:trace(["stop", "topic", "a/b/c"]),
    ?assertMatch({match, _}, re:run(Trace6, "successfully")),
    Trace7 = emqx_mgmt_cli:trace(["start", "topic", "a/b/c",
                                  "log/clientid_trace.log", "error"]),
    ?assertMatch({match, _}, re:run(Trace7, "successfully")),
    logger:set_primary_config(level, error).

t_traces_cmd(_) ->
    emqx_trace:create_table(),
    Count1 = emqx_mgmt_cli:traces(["list"]),
    ?assertEqual(0, Count1),
    Error1 = emqx_mgmt_cli:traces(["start", "test-name", "client", "clientid-dev"]),
    ?assertMatch({match, _}, re:run(Error1, "Tracer module not started")),
    emqx_trace:start_link(),
    Trace1 = emqx_mgmt_cli:traces(["start", "test-name", "client", "clientid-dev"]),
    ?assertMatch({match, _}, re:run(Trace1, "successfully")),
    Count2 = emqx_mgmt_cli:traces(["list"]),
    ?assertEqual(1, Count2),
    Error2 = emqx_mgmt_cli:traces(["start", "test-name", "client", "clientid-dev"]),
    ?assertMatch({match, _}, re:run(Error2, "already_existed")),
    Trace2 = emqx_mgmt_cli:traces(["stop", "test-name"]),
    ?assertMatch({match, _}, re:run(Trace2, "successfully")),
    Count3 = emqx_mgmt_cli:traces(["list"]),
    ?assertEqual(1, Count3),
    Trace3 = emqx_mgmt_cli:traces(["delete", "test-name"]),
    ?assertMatch({match, _}, re:run(Trace3, "successfully")),
    Count4 = emqx_mgmt_cli:traces(["list"]),
    ?assertEqual(0, Count4),
    Error3 = emqx_mgmt_cli:traces(["delete", "test-name"]),
    ?assertMatch({match, _}, re:run(Error3, "not_found")),
    ok.

t_router_cmd(_) ->
    {ok, T} = emqtt:start_link([{clientid, <<"client1">>},
                                 {username, <<"testuser1">>},
                                 {password, <<"pass1">>}
                                ]),
    emqtt:connect(T),
    emqtt:subscribe(T, <<"a/b/c">>),
    {ok, T1} = emqtt:start_link([{clientid, <<"client2">>},
                                  {username, <<"testuser2">>},
                                  {password, <<"pass2">>}
                                 ]),

    emqtt:connect(T1),
    emqtt:subscribe(T1, <<"a/b/c/d">>),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:routes(["list"]), "a/b/c | a/b/c")),
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:routes(["show", "a/b/c"]), "a/b/c")).

t_subscriptions_cmd(_) ->
    {ok, T3} = emqtt:start_link([{clientid, <<"client">>},
                                 {username, <<"testuser">>},
                                 {password, <<"pass">>}
                                ]),
    {ok, _} = emqtt:connect(T3),
    {ok, _, _} = emqtt:subscribe(T3, <<"b/b/c">>),
    timer:sleep(300),
    [?assertMatch({match, _} , re:run(Result, "b/b/c"))
     || Result <- emqx_mgmt_cli:subscriptions(["show", <<"client">>])],
    ?assertEqual(emqx_mgmt_cli:subscriptions(["add", "client", "b/b/c", "0"]), "ok\n"),
    ?assertEqual(emqx_mgmt_cli:subscriptions(["del", "client", "b/b/c"]), "ok\n").

t_listeners_cmd_old(_) ->
    ok = emqx_listeners:ensure_all_started(),
    ?assertEqual(emqx_mgmt_cli:listeners([]), ok),
    ?assertEqual(
       "Stop mqtt:wss:external listener on 0.0.0.0:8084 successfully.\n",
       emqx_mgmt_cli:listeners(["stop", "wss", "8084"])
      ).

t_listeners_cmd_new(_) ->
    ok = emqx_listeners:ensure_all_started(),
    ?assertEqual(emqx_mgmt_cli:listeners([]), ok),
    ?assertEqual(
       "Stop mqtt:wss:external listener on 0.0.0.0:8084 successfully.\n",
       emqx_mgmt_cli:listeners(["stop", "mqtt:wss:external"])
      ),
    ?assertEqual(
       emqx_mgmt_cli:listeners(["restart", "mqtt:tcp:external"]),
       "Restarted mqtt:tcp:external listener successfully.\n"
      ),
    ?assertEqual(
       emqx_mgmt_cli:listeners(["restart", "mqtt:ssl:external"]),
       "Restarted mqtt:ssl:external listener successfully.\n"
      ),
    ?assertEqual(
       emqx_mgmt_cli:listeners(["restart", "bad:listener:identifier"]),
       "Failed to restart bad:listener:identifier listener:"
       " {no_such_listener,\"bad:listener:identifier\"}\n"
      ).

t_plugins_cmd(_) ->
    ?assertEqual(emqx_mgmt_cli:plugins(["list"]), ok),
    ?assertEqual(
       emqx_mgmt_cli:plugins(["unload", "emqx_auth_mnesia"]),
       "Plugin emqx_auth_mnesia unloaded successfully.\n"
      ),
    ?assertEqual(
       emqx_mgmt_cli:plugins(["load", "emqx_auth_mnesia"]),
       "Plugin emqx_auth_mnesia loaded successfully.\n"
      ),
    ?assertEqual(
       emqx_mgmt_cli:plugins(["unload", "emqx_management"]),
       "Plugin emqx_management can not be unloaded.\n"
      ).

t_cli(_) ->
    ?assertMatch({match, _}, re:run(emqx_mgmt_cli:status([""]), "status")),
    [?assertMatch({match, _}, re:run(Value, "broker"))
     || Value <- emqx_mgmt_cli:broker([""])],
    [?assertMatch({match, _}, re:run(Value, "cluster"))
     || Value <- emqx_mgmt_cli:cluster([""])],
    [?assertMatch({match, _}, re:run(Value, "clients"))
     || Value <- emqx_mgmt_cli:clients([""])],
    [?assertMatch({match, _}, re:run(Value, "routes"))
     || Value <- emqx_mgmt_cli:routes([""])],
    [?assertMatch({match, _}, re:run(Value, "subscriptions"))
     || Value <- emqx_mgmt_cli:subscriptions([""])],
    [?assertMatch({match, _}, re:run(Value, "plugins"))
     || Value <- emqx_mgmt_cli:plugins([""])],
    [?assertMatch({match, _}, re:run(Value, "listeners"))
     || Value <- emqx_mgmt_cli:listeners([""])],
    [?assertMatch({match, _}, re:run(Value, "vm"))
     || Value <- emqx_mgmt_cli:vm([""])],
    [?assertMatch({match, _}, re:run(Value, "mnesia"))
     || Value <- emqx_mgmt_cli:mnesia([""])],
    [?assertMatch({match, _}, re:run(Value, "trace"))
     || Value <- emqx_mgmt_cli:trace([""])],
    [?assertMatch({match, _}, re:run(Value, "traces"))
        || Value <- emqx_mgmt_cli:traces([""])],
    [?assertMatch({match, _}, re:run(Value, "mgmt"))
     || Value <- emqx_mgmt_cli:mgmt([""])].

t_backup_file(_)->
    Filename = <<"test.json">>,
    BadFilename = <<"bad.notjson">>,
    Bin = emqx_json:encode(#{a => b}),
    BadBin = <<"[bad json]">>,

    {error, bad_filename} = emqx_mgmt_data_backup:upload_backup_file(BadFilename, Bin),
    {error, bad_json} = emqx_mgmt_data_backup:upload_backup_file(Filename, BadBin),

    ok = emqx_mgmt_data_backup:upload_backup_file(Filename, Bin),
    {ok, #{file := <<"{\"a\":\"b\"}">>, filename := <<"test.json">>}} =
        emqx_mgmt_data_backup:read_backup_file(Filename),
    [{_, FileInfoList}] = emqx_mgmt_data_backup:list_backup_file(),
    Filename = proplists:get_value(filename, FileInfoList),
    ok = emqx_mgmt_data_backup:delete_backup_file(Filename),

    {error, not_found} = emqx_mgmt_data_backup:delete_backup_file(BadFilename),
    ok.

t_import_outside_backup_dir(Config) ->
    Filepath = ?config(tmp_file, Config),
    Env = "{}",
    ?assertEqual(ok, emqx_mgmt_data_backup:import(Filepath, Env)),
    ok.

mock_print() ->
    ok = safe_unmeck(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end).

unmock_print() ->
    meck:unload(emqx_ctl).

safe_unmeck(Module) ->
    try
        meck:unload(Module),
        ok
    catch
        _ : _ ->
            ok
    end.
