%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

t_gateway_load(_) ->
    ok.

t_gateway_unload(_) ->
    ok.

t_gateway_start(_) ->
    ok.

t_gateway_stop(_) ->
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
