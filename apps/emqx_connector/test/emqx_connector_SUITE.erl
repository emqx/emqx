%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BRIDGE_CONF_DEFAULT, <<"bridges: {}">>).
-define(MQTT_CONNECTOR(Username), #{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => Username,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).
-define(CONNECTOR_TYPE, <<"mqtt">>).
-define(CONNECTOR_NAME, <<"test_connector_42">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
    [].

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    %% some testcases (may from other app) already get emqx_connector started
    _ = application:stop(emqx_resource),
    _ = application:stop(emqx_connector),
    ok = emqx_common_test_helpers:start_apps(
        [
            emqx_connector,
            emqx_bridge
        ]
    ),
    ok = emqx_common_test_helpers:load_config(emqx_connector_schema, <<"connectors: {}">>),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx_connector,
        emqx_bridge
    ]),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    Config.
end_per_testcase(_, _Config) ->
    ok.

t_list_raw_empty(_) ->
    ok = emqx_config:erase(hd(emqx_connector:config_key_path())),
    Result = emqx_connector:list_raw(),
    ?assertEqual([], Result).

t_lookup_raw_error(_) ->
    Result = emqx_connector:lookup_raw(<<"foo:bar">>),
    ?assertEqual({error, not_found}, Result).

t_parse_connector_id_error(_) ->
    ?assertError(
        {invalid_connector_id, <<"foobar">>}, emqx_connector:parse_connector_id(<<"foobar">>)
    ).

t_update_connector_does_not_exist(_) ->
    Config = ?MQTT_CONNECTOR(<<"user1">>),
    ?assertMatch({ok, _Config}, emqx_connector:update(?CONNECTOR_TYPE, ?CONNECTOR_NAME, Config)).

t_delete_connector_does_not_exist(_) ->
    ?assertEqual({ok, #{post_config_update => #{}}}, emqx_connector:delete(<<"foo:bar">>)).

t_connector_id_using_list(_) ->
    <<"foo:bar">> = emqx_connector:connector_id("foo", "bar").
