%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cm_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_gateway, ?CONF_DEFAULT}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Conf)}
    ),
    [{apps, Apps} | Conf].

end_per_suite(Conf) ->
    ok = emqx_cth_suite:stop(?config(apps, Conf)),
    ok.

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
