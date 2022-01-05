%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(GP(S), begin S, receive {fmt, P} -> P; O -> O end end).

%% this parses to #{}, will not cause config cleanup
%% so we will need call emqx_config:erase
-define(CONF_DEFAULT, <<"
gateway {}
">>).

%% The config with json format for mqtt-sn gateway
-define(CONF_MQTTSN, "
{\"idle_timeout\": \"30s\",
 \"enable_stats\": true,
 \"mountpoint\": \"mqttsn/\",
 \"gateway_id\": 1,
 \"broadcast\": true,
 \"enable_qos3\": true,
 \"predefined\": [{\"id\": 1001, \"topic\": \"pred/a\"}],
 \"listeners\":
    [{\"type\": \"udp\",
      \"name\": \"ct\",
      \"bind\": \"2885\"
    }]
}
").

%%--------------------------------------------------------------------
%% Setup
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_config:erase(gateway),
    emqx_config:init_load(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_authn, emqx_gateway]),
    Conf.

end_per_suite(Conf) ->
    emqx_mgmt_api_test_util:end_suite([emqx_gateway, emqx_authn, emqx_conf]),
    Conf.

init_per_testcase(_, Conf) ->
    Self = self(),
    ok = meck:new(emqx_ctl, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_ctl, usage,
                     fun(L) -> emqx_ctl:format_usage(L) end),
    ok = meck:expect(emqx_ctl, print,
                     fun(Fmt) ->
                        Self ! {fmt, emqx_ctl:format(Fmt)}
                     end),
    ok = meck:expect(emqx_ctl, print,
                     fun(Fmt, Args) ->
                        Self ! {fmt, emqx_ctl:format(Fmt, Args)}
                     end),
    Conf.

end_per_testcase(_, _) ->
    meck:unload([emqx_ctl]),
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

%% TODO:

t_load_unload(_) ->
    ok.

t_gateway_registry_usage(_) ->
    ?assertEqual(
       ["gateway-registry list # List all registered gateways\n"],
       emqx_gateway_cli:'gateway-registry'(usage)).

t_gateway_registry_list(_) ->
    emqx_gateway_cli:'gateway-registry'(["list"]),
    ?assertEqual(
       "Registered Name: coap, Callback Module: emqx_coap_impl\n"
       "Registered Name: exproto, Callback Module: emqx_exproto_impl\n"
       "Registered Name: lwm2m, Callback Module: emqx_lwm2m_impl\n"
       "Registered Name: mqttsn, Callback Module: emqx_sn_impl\n"
       "Registered Name: stomp, Callback Module: emqx_stomp_impl\n"
       , acc_print()).

t_gateway_usage(_) ->
    ?assertEqual(
       ["gateway list                     # List all gateway\n",
        "gateway lookup <Name>            # Lookup a gateway detailed informations\n",
        "gateway load   <Name> <JsonConf> # Load a gateway with config\n",
        "gateway unload <Name>            # Unload the gateway\n",
        "gateway stop   <Name>            # Stop the gateway\n",
        "gateway start  <Name>            # Start the gateway\n"],
       emqx_gateway_cli:gateway(usage)
     ).

t_gateway_list(_) ->
    emqx_gateway_cli:gateway(["list"]),
    ?assertEqual(
      "Gateway(name=coap, status=unloaded)\n"
      "Gateway(name=exproto, status=unloaded)\n"
      "Gateway(name=lwm2m, status=unloaded)\n"
      "Gateway(name=mqttsn, status=unloaded)\n"
      "Gateway(name=stomp, status=unloaded)\n"
      , acc_print()).

t_gateway_load_unload_lookup(_) ->
    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    ?assertEqual("undefined\n", acc_print()),

    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    %% TODO: bad config name, format???

    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    %% TODO: assert it. for example:
    %% name: mqttsn
    %% status: running
    %% created_at: 2022-01-05T14:40:20.039+08:00
    %% started_at: 2022-01-05T14:42:37.894+08:00
    %% config: #{broadcast => false,enable => true,enable_qos3 => true,
    %%           enable_stats => true,gateway_id => 1,idle_timeout => 30000,
    %%           mountpoint => <<>>,predefined => []}
    _ = acc_print(),

    emqx_gateway_cli:gateway(["load", "mqttsn", "{}"]),
    ?assertEqual(
        "Error: The mqttsn gateway already loaded\n"
        , acc_print()),

    emqx_gateway_cli:gateway(["load", "bad-gw-name", "{}"]),
    %% TODO: assert it. for example:
    %% Error: Illegal gateway name
    _ = acc_print(),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% Always return ok, even the gateway has unloaded
    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    ?assertEqual("undefined\n", acc_print()),
    ok.

t_gateway_start_stop(_) ->
    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["stop", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% dupliacted stop gateway, return ok
    emqx_gateway_cli:gateway(["stop", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["start", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% dupliacted start gateway, return ok
    emqx_gateway_cli:gateway(["start", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    ok.

t_gateway_clients_usage(_) ->
    ok.

t_gateway_clients_list(_) ->
    ok.

t_gateway_clients_lookup(_) ->
    ok.

t_gateway_clients_kick(_) ->
    ok.

t_gateway_metrcis_usage(_) ->
    ok.

t_gateway_metrcis(_) ->
    ok.

acc_print() ->
    lists:concat(lists:reverse(acc_print([]))).

acc_print(Acc) ->
    receive
        {fmt, S} -> acc_print([S|Acc])
    after 200 ->
        Acc
    end.
