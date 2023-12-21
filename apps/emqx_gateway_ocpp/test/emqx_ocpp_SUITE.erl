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

-module(emqx_ocpp_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_fields_exist/2,
        request/2,
        request/3
    ]
).

-define(HEARTBEAT, <<$\n>>).

%% erlfmt-ignore
-define(CONF_DEFAULT, <<"
    gateway.ocpp {
      mountpoint = \"ocpp/\"
      default_heartbeat_interval = \"60s\"
      heartbeat_checking_times_backoff = 1
      message_format_checking = disable
      upstream {
        topic = \"cp/${clientid}\"
        reply_topic = \"cp/${clientid}/Reply\"
        error_topic = \"cp/${clientid}/Reply\"
      }
      dnstream {
        topic = \"cs/${clientid}\"
      }
      listeners.ws.default {
          bind = \"0.0.0.0:33033\"
          websocket.path = \"/ocpp\"
      }
    }
">>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    application:load(emqx_gateway_ocpp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

default_config() ->
    ?CONF_DEFAULT.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_update_listeners(_Config) ->
    {200, [DefaultListener]} = request(get, "/gateways/ocpp/listeners"),

    ListenerConfKeys =
        [
            id,
            type,
            name,
            enable,
            enable_authn,
            bind,
            acceptors,
            max_connections,
            max_conn_rate,
            proxy_protocol,
            proxy_protocol_timeout,
            websocket,
            tcp_options
        ],
    StatusKeys = [status, node_status],

    assert_fields_exist(ListenerConfKeys ++ StatusKeys, DefaultListener),
    ?assertMatch(
        #{
            id := <<"ocpp:ws:default">>,
            type := <<"ws">>,
            name := <<"default">>,
            enable := true,
            enable_authn := true,
            bind := <<"0.0.0.0:33033">>,
            websocket := #{path := <<"/ocpp">>}
        },
        DefaultListener
    ),

    UpdateBody = emqx_utils_maps:deep_put(
        [websocket, path],
        maps:with(ListenerConfKeys, DefaultListener),
        <<"/ocpp2">>
    ),
    {200, _} = request(put, "/gateways/ocpp/listeners/ocpp:ws:default", UpdateBody),

    {200, [UpdatedListener]} = request(get, "/gateways/ocpp/listeners"),
    ?assertMatch(#{websocket := #{path := <<"/ocpp2">>}}, UpdatedListener).

t_enable_disable_gw_ocpp(_Config) ->
    AssertEnabled = fun(Enabled) ->
        {200, R} = request(get, "/gateways/ocpp"),
        E = maps:get(enable, R),
        ?assertEqual(E, Enabled),
        timer:sleep(500),
        ?assertEqual(E, emqx:get_config([gateway, ocpp, enable]))
    end,
    ?assertEqual({204, #{}}, request(put, "/gateways/ocpp/enable/false", <<>>)),
    AssertEnabled(false),
    ?assertEqual({204, #{}}, request(put, "/gateways/ocpp/enable/true", <<>>)),
    AssertEnabled(true).
