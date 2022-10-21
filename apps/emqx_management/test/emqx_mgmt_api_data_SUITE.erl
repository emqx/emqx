%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Basci testing the `/api/v4/data/*` APIs on local/cluster
-module(emqx_mgmt_api_data_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-import(emqx_mgmt_api_test_helpers,
        [request_api/3,
         request_api/4,
         request_api/5,
         auth_header_/0]).

-define(DEPS_APPS, [emqx, emqx_rule_engine, emqx_management, emqx_dashboard]).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    %% start master node
    net_kernel:start(['master@127.0.0.1', longnames]),
    emqx_ct_helpers:boot_modules(all),
    PortDiscovery = application:get_env(gen_rpc, port_discovery),
    application:set_env(gen_rpc, port_discovery, stateless),
    application:ensure_all_started(gen_rpc),

    application:load(emqx_modules),
    emqx_ct_helpers:start_apps(?DEPS_APPS),

    %% start slave node with emqx_management listen on 8082,
    %% emqx_dashboard listen on 18084
    Node = emqx_node_helpers:start_slave(
             api_data_test_slave12,
             #{start_apps => ?DEPS_APPS,
               env_handler => fun set_slave_apps_cfgs/1
              }),
    [{port_discovery, PortDiscovery}, {slave_node, Node} | Cfg].

end_per_suite(Cfg) ->
    Node = proplists:get_value(slave_node, Cfg),
    emqx_node_helpers:stop_slave(Node),
    emqx_ct_helpers:stop_apps(lists:reverse(?DEPS_APPS)),
    case proplists:get_value(port_discovery, Cfg) of
      {ok, OldValue} -> application:set_env(gen_rpc, port_discovery, OldValue);
      _ -> ok
    end.

set_slave_apps_cfgs(emqx) ->
    SlaveDataDir = ensure_slave_data_dir(),
    application:set_env(emqx, data_dir, SlaveDataDir),
    application:set_env(gen_rpc, port_discovery, stateless),
    application:set_env(emqx, listeners, []);
set_slave_apps_cfgs(App = emqx_management) ->
    application:set_env(App, default_application_id, "admin"),
    application:set_env(App, default_application_secret, "public"),
    application:set_env(App, listeners,
                        [{http, 8082,
                          [{num_acceptors, 2},
                           {max_connections, 512}]}
                        ]);
set_slave_apps_cfgs(App = emqx_dashboard) ->
    application:set_env(App, default_user_username, "admin"),
    application:set_env(App, default_user_passwd, "public"),
    application:set_env(App, listeners,
                        [{http, 18084,
                          [{num_acceptors, 4},
                           {max_connections, 512}]}]);
set_slave_apps_cfgs(_) ->
    ok.

ensure_slave_data_dir() ->
    SlaveDataDir =  slave_data_dir(),
    case file:make_dir(SlaveDataDir) of
        ok -> ok;
        {error, eexist} -> ok
    end,
    SlaveDataDir.

slave_data_dir() ->
    emqx_ct_helpers:app_path(emqx, filename:join(["data", "slave_data"])).

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_data(_) ->
    %% backup
    {ok, Data} = request_api(post, master_uri(["data","export"]), [], auth_header_(), [#{}]),
    #{<<"filename">> := Filename, <<"node">> := Node} = emqx_ct_http:get_http_data(Data),
    {ok, DataList} = request_api(get, master_uri(["data","export"]), auth_header_()),
    ?assertEqual(true, lists:member(emqx_ct_http:get_http_data(Data), emqx_ct_http:get_http_data(DataList))),
    %% restore for special node
    ?assertMatch({ok, _}, request_api(post, master_uri(["data","import"]), [], auth_header_(), #{<<"filename">> => Filename, <<"node">> => Node})),
    %% restore for local node
    ?assertMatch({ok, _}, request_api(post, master_uri(["data","import"]), [], auth_header_(), #{<<"filename">> => Filename})),
    %% delete backup file
    ?assertMatch({ok, _}, request_api(delete, master_uri(["data", "file", Filename]), [], auth_header_())),
    ok.

t_data_import_content(_) ->
    %% backup & read the file content
    {ok, Data} = request_api(post, master_uri(["data","export"]), [], auth_header_(), [#{}]),
    #{<<"filename">> := Filename} = emqx_ct_http:get_http_data(Data),
    Dir = emqx:get_env(data_dir),
    {ok, Bin} = file:read_file(filename:join(Dir, Filename)),
    Content = emqx_json:decode(Bin),
    %% restore by file connect
    ?assertMatch(
       {ok, "{\"code\":0}"},
       request_api(post, master_uri(["data","import"]), [], auth_header_(), Content)
      ),
    %% delelte all stored files
    {ok, Files0} = request_api(get, master_uri(["data","export"]), [], auth_header_()),
    #{<<"code">> := 0, <<"data">> := Files} = emqx_json:decode(Files0, [return_maps]),
    lists:foreach(
      fun(#{<<"filename">> := Name}) ->
              {ok, _} = request_api(delete, master_uri(["data", "file", Name]), [], auth_header_())
      end, Files).

t_backup_on_slave_download_from_master(Cfg) ->
    %% assert data dir is different
    Node = proplists:get_value(slave_node, Cfg),
    MasterDataDir = emqx:get_env(data_dir),
    SlaveDataDir = rpc:call(Node, emqx, get_env, [data_dir]),
    ?assertNotEqual(MasterDataDir, SlaveDataDir),
    %% backup on slave node
    {ok, Data} = request_api(post, slave_uri(["data","export"]), [], auth_header_(), [#{}]),
    #{<<"filename">> := Filename} = emqx_ct_http:get_http_data(Data),
    {ok, Bin} = file:read_file(filename:join(slave_data_dir(), Filename)),
    %% download from master node
    {ok, File0}= request_api(get, master_uri(["data", "file", Filename]), [], auth_header_()),
    #{<<"filename">> := Filename, <<"file">> := Bin} = emqx_json:decode(File0, [return_maps]),
    ok.

%%--------------------------------------------------------------------
%% helpers

master_uri(Parts) ->
    "http://127.0.0.1:8081/api/v4/" ++ filename:join(Parts).

slave_uri(Parts) ->
    "http://127.0.0.1:8082/api/v4/" ++ filename:join(Parts).
