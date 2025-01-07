%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_cm_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(GWNAME, mqttsn).
-define(CLIENTID, <<"client1">>).

-define(CONF_DEFAULT, <<"gateway {}">>).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_config:erase(gateway),
    emqx_gateway_test_utils:load_all_gateway_apps(),
    emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([]),
    Conf.

end_per_suite(_Conf) ->
    emqx_common_test_helpers:stop_apps([]),
    emqx_config:delete_override_conf_files().

init_per_testcase(_TestCase, Conf) ->
    {ok, Pid} = emqx_gateway_cm_registry:start_link(?GWNAME),
    [{registry, Pid} | Conf].

end_per_testcase(_TestCase, Conf) ->
    Pid = proplists:get_value(registry, Conf),
    gen_server:stop(Pid),
    Conf.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_tabname(_) ->
    ?assertEqual(
        emqx_gateway_gw_name_channel_registry,
        emqx_gateway_cm_registry:tabname(gw_name)
    ).

t_register_unregister_channel(_) ->
    ok = emqx_gateway_cm_registry:register_channel(?GWNAME, ?CLIENTID),
    ?assertEqual(
        [{channel, ?CLIENTID, self()}],
        ets:tab2list(emqx_gateway_cm_registry:tabname(?GWNAME))
    ),

    ?assertEqual(
        [self()],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ),

    ok = emqx_gateway_cm_registry:unregister_channel(?GWNAME, ?CLIENTID),

    ?assertEqual(
        [],
        ets:tab2list(emqx_gateway_cm_registry:tabname(?GWNAME))
    ),
    ?assertEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ).

t_cleanup_channels_mnesia_down(Conf) ->
    Pid = proplists:get_value(registry, Conf),
    emqx_gateway_cm_registry:register_channel(?GWNAME, ?CLIENTID),
    ?assertEqual(
        [self()],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ),
    Pid ! {membership, {mnesia, down, node()}},
    ct:sleep(100),
    ?assertEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ).

t_cleanup_channels_node_down(Conf) ->
    Pid = proplists:get_value(registry, Conf),
    emqx_gateway_cm_registry:register_channel(?GWNAME, ?CLIENTID),
    ?assertEqual(
        [self()],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ),
    Pid ! {membership, {node, down, node()}},
    ct:sleep(100),
    ?assertEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(?GWNAME, ?CLIENTID)
    ).

t_handle_unexpected_msg(Conf) ->
    Pid = proplists:get_value(registry, Conf),
    _ = Pid ! unexpected_info,
    ok = gen_server:cast(Pid, unexpected_cast),
    ignored = gen_server:call(Pid, unexpected_call).
