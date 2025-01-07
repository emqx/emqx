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
-module(emqx_mgmt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([ident/1]).

-define(FORMATFUN, {?MODULE, ident}).

all() ->
    [
        {group, persistence_disabled},
        {group, persistence_enabled},
        {group, cm_registry_enabled},
        {group, cm_registry_disabled}
    ].

groups() ->
    CMRegistryTCs = [t_call_client_cluster],
    TCs = emqx_common_test_helpers:all(?MODULE) -- CMRegistryTCs,
    [
        {persistence_disabled, [], TCs},
        {persistence_enabled, [], [t_persist_list_subs]},
        {cm_registry_enabled, CMRegistryTCs},
        {cm_registry_disabled, CMRegistryTCs}
    ].

init_per_group(persistence_disabled, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, "durable_sessions { enable = false }"},
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {apps, Apps}
        | Config
    ];
init_per_group(persistence_enabled, Config) ->
    case emqx_ds_test_helpers:skip_if_norepl() of
        false ->
            Apps = emqx_cth_suite:start(
                [
                    {emqx,
                        "durable_sessions {\n"
                        "  enable = true\n"
                        "  heartbeat_interval = 100ms\n"
                        "  renew_streams_interval = 100ms\n"
                        "}"},
                    emqx_management
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps}
                | Config
            ];
        Yes ->
            Yes
    end;
init_per_group(cm_registry_enabled, Config) ->
    [{emqx_config, "broker.enable_session_registry = true"} | Config];
init_per_group(cm_registry_disabled, Config) ->
    [{emqx_config, "broker.enable_session_registry = false"} | Config].

end_per_group(_Grp, Config) ->
    case ?config(apps, Config) of
        undefined -> ok;
        Apps -> emqx_cth_suite:stop(Apps)
    end.

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

init_per_testcase(TestCase, Config) ->
    meck:expect(emqx, running_nodes, 0, [node()]),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TestCase, Config).

end_per_testcase(TestCase, Config) ->
    meck:unload(emqx),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TestCase, Config).

t_list_nodes(init, Config) ->
    meck:expect(
        emqx,
        cluster_nodes,
        fun
            (running) -> [node()];
            (stopped) -> ['stopped@node']
        end
    ),
    Config;
t_list_nodes('end', _Config) ->
    ok.

t_list_nodes(_) ->
    NodeInfos = emqx_mgmt:list_nodes(),
    Node = node(),
    ?assertMatch(
        [
            {Node, #{node := Node, node_status := 'running'}},
            {'stopped@node', #{node := 'stopped@node', node_status := 'stopped'}}
        ],
        NodeInfos
    ).

t_lookup_node(init, Config) ->
    meck:new(os, [passthrough, unstick, no_link]),
    OsType = os:type(),
    meck:expect(os, type, 0, {win32, winME}),
    [{os_type, OsType} | Config];
t_lookup_node('end', Config) ->
    %% We need to restore the original behavior so that rebar3 doesn't crash. If
    %% we'd `meck:unload(os)` or not set `no_link` then `ct` crashes calling
    %% `os` with "The code server called the unloaded module `os'".
    OsType = ?config(os_type, Config),
    meck:expect(os, type, 0, OsType),
    ok.

t_lookup_node(_) ->
    Node = node(),
    ?assertMatch(
        #{node := Node, node_status := 'running', memory_total := 0},
        emqx_mgmt:lookup_node(node())
    ),
    ?assertMatch(
        {error, _},
        emqx_mgmt:lookup_node('fake@nohost')
    ),
    ok.

t_list_brokers(_) ->
    Node = node(),
    ?assertMatch(
        [{Node, #{node := Node, node_status := running, uptime := _}}],
        emqx_mgmt:list_brokers()
    ).

t_lookup_broker(_) ->
    Node = node(),
    ?assertMatch(
        #{node := Node, node_status := running, uptime := _},
        emqx_mgmt:lookup_broker(Node)
    ).

t_get_metrics(_) ->
    Metrics = emqx_mgmt:get_metrics(),
    ?assert(maps:size(Metrics) > 0),
    ?assertMatch(
        Metrics, maps:from_list(emqx_mgmt:get_metrics(node()))
    ).

t_lookup_client(init, Config) ->
    setup_clients(Config);
t_lookup_client('end', Config) ->
    disconnect_clients(Config).

t_lookup_client(_Config) ->
    [{Chan, Info, Stats}] = emqx_mgmt:lookup_client({clientid, <<"client1">>}, ?FORMATFUN),
    ?assertEqual(
        [{Chan, Info, Stats}],
        emqx_mgmt:lookup_client({username, <<"user1">>}, ?FORMATFUN)
    ),
    ?assertEqual([], emqx_mgmt:lookup_client({clientid, <<"notfound">>}, ?FORMATFUN)),
    meck:expect(emqx, running_nodes, 0, [node(), 'fake@nonode']),
    try
        emqx:update_config([broker, enable_session_registry], false),
        ?assertMatch(
            [_ | {error, nodedown}], emqx_mgmt:lookup_client({clientid, <<"client1">>}, ?FORMATFUN)
        )
    after
        emqx:update_config([broker, enable_session_registry], true)
    end.

t_kickout_client(init, Config) ->
    process_flag(trap_exit, true),
    setup_clients(Config);
t_kickout_client('end', _Config) ->
    ok.

t_kickout_client(Config) ->
    [C | _] = ?config(clients, Config),
    ok = emqx_mgmt:kickout_client(<<"client1">>),
    receive
        {'EXIT', C, Reason} ->
            ?assertEqual({shutdown, tcp_closed}, Reason);
        Foo ->
            error({unexpected, Foo})
    after 1000 ->
        error(timeout)
    end,
    ?assertEqual({error, not_found}, emqx_mgmt:kickout_client(<<"notfound">>)).

t_list_authz_cache(init, Config) ->
    setup_clients(Config);
t_list_authz_cache('end', Config) ->
    disconnect_clients(Config).

t_list_authz_cache(_) ->
    ?assertNotMatch({error, _}, emqx_mgmt:list_authz_cache(<<"client1">>)),
    ?assertMatch({error, not_found}, emqx_mgmt:list_authz_cache(<<"notfound">>)).

t_list_client_subscriptions(init, Config) ->
    setup_clients(Config);
t_list_client_subscriptions('end', Config) ->
    disconnect_clients(Config).

t_list_client_subscriptions(Config) ->
    [Client | _] = ?config(clients, Config),
    ?assertEqual([], emqx_mgmt:list_client_subscriptions(<<"client1">>)),
    emqtt:subscribe(Client, <<"t/#">>),
    ?assertMatch({_, [{<<"t/#">>, _Opts}]}, emqx_mgmt:list_client_subscriptions(<<"client1">>)),
    ?assertEqual({error, not_found}, emqx_mgmt:list_client_subscriptions(<<"notfound">>)).

t_clean_cache(init, Config) ->
    setup_clients(Config);
t_clean_cache('end', Config) ->
    disconnect_clients(Config).

t_clean_cache(_Config) ->
    ?assertNotMatch(
        {error, _},
        emqx_mgmt:clean_authz_cache(<<"client1">>)
    ),
    ?assertNotMatch(
        {error, _},
        emqx_mgmt:clean_authz_cache_all()
    ),
    ?assertNotMatch(
        {error, _},
        emqx_mgmt:clean_pem_cache_all()
    ),
    meck:expect(emqx, running_nodes, 0, [node(), 'fake@nonode']),
    ?assertMatch(
        {error, [{'fake@nonode', {error, _}}]},
        emqx_mgmt:clean_authz_cache_all()
    ),
    ?assertMatch(
        {error, [{'fake@nonode', {error, _}}]},
        emqx_mgmt:clean_pem_cache_all()
    ).

t_set_client_props(init, Config) ->
    setup_clients(Config);
t_set_client_props('end', Config) ->
    disconnect_clients(Config).

t_set_client_props(_Config) ->
    ?assertEqual(
        % [FIXME] not implemented at this point?
        ignored,
        emqx_mgmt:set_ratelimit_policy(<<"client1">>, foo)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_mgmt:set_ratelimit_policy(<<"notfound">>, foo)
    ),
    ?assertEqual(
        % [FIXME] not implemented at this point?
        ignored,
        emqx_mgmt:set_quota_policy(<<"client1">>, foo)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_mgmt:set_quota_policy(<<"notfound">>, foo)
    ),
    ?assertEqual(
        ok,
        emqx_mgmt:set_keepalive(<<"client1">>, 3600)
    ),
    ?assertMatch(
        {error, _},
        emqx_mgmt:set_keepalive(<<"client1">>, true)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_mgmt:set_keepalive(<<"notfound">>, 3600)
    ),
    ok.

t_list_subscriptions_via_topic(init, Config) ->
    setup_clients(Config);
t_list_subscriptions_via_topic('end', Config) ->
    disconnect_clients(Config).

t_list_subscriptions_via_topic(Config) ->
    [Client | _] = ?config(clients, Config),
    ?assertEqual([], emqx_mgmt:list_subscriptions_via_topic(<<"t/#">>, ?FORMATFUN)),
    emqtt:subscribe(Client, <<"t/#">>),
    ?assertMatch(
        [{{<<"t/#">>, _SubPid}, _Opts}],
        emqx_mgmt:list_subscriptions_via_topic(<<"t/#">>, ?FORMATFUN)
    ).

t_pubsub_api(init, Config) ->
    setup_clients(Config);
t_pubsub_api('end', Config) ->
    disconnect_clients(Config).

-define(TT(Topic), {Topic, #{qos => 0}}).

t_pubsub_api(Config) ->
    [Client | _] = ?config(clients, Config),
    ?assertEqual([], emqx_mgmt:list_subscriptions_via_topic(<<"t/#">>, ?FORMATFUN)),
    ?assertMatch(
        {subscribe, _, _},
        emqx_mgmt:subscribe(<<"client1">>, [?TT(<<"t/#">>), ?TT(<<"t1/#">>), ?TT(<<"t2/#">>)])
    ),
    timer:sleep(100),
    ?assertMatch(
        [{{<<"t/#">>, _SubPid}, _Opts}],
        emqx_mgmt:list_subscriptions_via_topic(<<"t/#">>, ?FORMATFUN)
    ),
    Message = emqx_message:make(?MODULE, 0, <<"t/foo">>, <<"helloworld">>, #{}, #{}),
    emqx_mgmt:publish(Message),
    Recv =
        receive
            {publish, #{client_pid := Client, payload := <<"helloworld">>}} ->
                ok
        after 100 ->
            timeout
        end,
    ?assertEqual(ok, Recv),
    ?assertEqual({error, channel_not_found}, emqx_mgmt:subscribe(<<"notfound">>, [?TT(<<"t/#">>)])),
    ?assertNotMatch({error, _}, emqx_mgmt:unsubscribe(<<"client1">>, <<"t/#">>)),
    ?assertEqual({error, channel_not_found}, emqx_mgmt:unsubscribe(<<"notfound">>, <<"t/#">>)),
    Node = node(),
    ?assertMatch(
        {Node, [{<<"t1/#">>, _}, {<<"t2/#">>, _}]},
        emqx_mgmt:list_client_subscriptions(<<"client1">>)
    ),
    ?assertMatch(
        {unsubscribe, [{<<"t1/#">>, _}, {<<"t2/#">>, _}]},
        emqx_mgmt:unsubscribe_batch(<<"client1">>, [<<"t1/#">>, <<"t2/#">>])
    ),
    timer:sleep(100),
    ?assertMatch([], emqx_mgmt:list_client_subscriptions(<<"client1">>)),
    ?assertEqual(
        {error, channel_not_found},
        emqx_mgmt:unsubscribe_batch(<<"notfound">>, [<<"t1/#">>, <<"t2/#">>])
    ).

t_alarms(init, Config) ->
    [
        emqx_mgmt:deactivate(Node, Name)
     || {Node, ActiveAlarms} <- emqx_mgmt:get_alarms(activated), #{name := Name} <- ActiveAlarms
    ],
    emqx_mgmt:delete_all_deactivated_alarms(),
    Config;
t_alarms('end', Config) ->
    Config.

t_alarms(_) ->
    Node = node(),
    ?assertEqual(
        [{node(), []}],
        emqx_mgmt:get_alarms(all)
    ),
    emqx_alarm:activate(foo),
    ?assertMatch(
        [{Node, [#{name := foo, activated := true, duration := _}]}],
        emqx_mgmt:get_alarms(all)
    ),
    emqx_alarm:activate(bar),
    ?assertMatch(
        [{Node, [#{name := foo, activated := true}, #{name := bar, activated := true}]}],
        sort_alarms(emqx_mgmt:get_alarms(all))
    ),
    ?assertEqual(
        ok,
        emqx_mgmt:deactivate(node(), bar)
    ),
    ?assertMatch(
        [{Node, [#{name := foo, activated := true}, #{name := bar, activated := false}]}],
        sort_alarms(emqx_mgmt:get_alarms(all))
    ),
    ?assertMatch(
        [{Node, [#{name := foo, activated := true}]}],
        emqx_mgmt:get_alarms(activated)
    ),
    ?assertMatch(
        [{Node, [#{name := bar, activated := false}]}],
        emqx_mgmt:get_alarms(deactivated)
    ),
    ?assertEqual(
        [ok],
        emqx_mgmt:delete_all_deactivated_alarms()
    ),
    ?assertMatch(
        [{Node, [#{name := foo, activated := true}]}],
        emqx_mgmt:get_alarms(all)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_mgmt:deactivate(node(), bar)
    ).

t_banned(_) ->
    Banned = #{
        who => {clientid, <<"TestClient">>},
        by => <<"banned suite">>,
        reason => <<"test">>,
        at => erlang:system_time(second),
        until => erlang:system_time(second) + 1
    },
    ?assertMatch(
        {ok, _},
        emqx_mgmt:create_banned(Banned)
    ),
    ?assertEqual(
        ok,
        emqx_mgmt:delete_banned({clientid, <<"TestClient">>})
    ).

%% This testcase verifies the behavior of various read-only functions
%% used by REST API via `emqx_mgmt' module:
t_persist_list_subs(_) ->
    ClientId = <<"persistent_client">>,
    Topics = lists:sort([<<"foo/bar">>, <<"/a/+//+/#">>, <<"foo">>]),
    VerifySubs =
        fun() ->
            {Node, Ret} = emqx_mgmt:list_client_subscriptions(ClientId),
            ?assert(Node =:= node() orelse Node =:= undefined, Node),
            {TopicsL, SubProps} = lists:unzip(Ret),
            ?assertEqual(Topics, lists:sort(TopicsL)),
            [?assertMatch(#{rh := _, rap := _, nl := _, qos := _}, I) || I <- SubProps]
        end,
    %% 0. Verify that management functions work for missing clients:
    ?assertMatch(
        {error, not_found},
        emqx_mgmt:list_client_subscriptions(ClientId)
    ),
    %% 1. Connect the client and subscribe to topics:
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
    ]),
    {ok, _} = emqtt:connect(Client),
    [{ok, _, _} = emqtt:subscribe(Client, I, qos2) || I <- Topics],
    %% 2. Verify that management functions work for the connected
    %% clients:
    VerifySubs(),
    %% 3. Disconnect the client:
    emqtt:disconnect(Client),
    %% 4. Verify that management functions work for the offline
    %% clients:
    VerifySubs().

t_call_client_cluster(Config) ->
    [Node1, Node2] = ?config(cluster, Config),
    [Node1ClientId, Node2ClientId] = ?config(client_ids, Config),
    ?assertMatch(
        {[], #{}}, rpc:call(Node1, emqx_mgmt, list_client_msgs, client_msgs_args(Node1ClientId))
    ),
    ?assertMatch(
        {[], #{}}, rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_args(Node2ClientId))
    ),
    ?assertMatch(
        {[], #{}}, rpc:call(Node1, emqx_mgmt, list_client_msgs, client_msgs_args(Node2ClientId))
    ),
    ?assertMatch(
        {[], #{}}, rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_args(Node1ClientId))
    ),

    case proplists:get_value(name, ?config(tc_group_properties, Config)) of
        cm_registry_disabled ->
            %% Simulating crashes that must be handled by erpc multicall
            ?assertMatch(
                {error, _},
                rpc:call(Node1, emqx_mgmt, list_client_msgs, client_msgs_bad_args(Node2ClientId))
            ),
            ?assertMatch(
                {error, _},
                rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_bad_args(Node1ClientId))
            );
        cm_registry_enabled ->
            %% Direct call to remote pid is expected to crash
            ?assertMatch(
                {badrpc, {'EXIT', _}},
                rpc:call(Node1, emqx_mgmt, list_client_msgs, client_msgs_bad_args(Node1ClientId))
            ),
            ?assertMatch(
                {badrpc, {'EXIT', _}},
                rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_bad_args(Node2ClientId))
            );
        _ ->
            ok
    end,

    NotFoundClientId = <<"no_such_client_id">>,
    ?assertEqual(
        {error, not_found},
        rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_args(NotFoundClientId))
    ),
    ?assertEqual(
        {error, not_found},
        rpc:call(Node2, emqx_mgmt, list_client_msgs, client_msgs_args(NotFoundClientId))
    ).

t_call_client_cluster(init, Config) ->
    Apps = [{emqx, ?config(emqx_config, Config)}, emqx_management],
    [Node1, Node2] =
        Cluster = emqx_cth_cluster:start(
            [
                {list_to_atom(atom_to_list(?MODULE) ++ "1"), #{role => core, apps => Apps}},
                {list_to_atom(atom_to_list(?MODULE) ++ "2"), #{role => core, apps => Apps}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    {ok, Node1Client, Node1ClientId} = connect_client(Node1),
    {ok, Node2Client, Node2ClientId} = connect_client(Node2),
    %% They may exit during the test due to simulated crashes
    unlink(Node1Client),
    unlink(Node2Client),
    [
        {cluster, Cluster},
        {client_ids, [Node1ClientId, Node2ClientId]},
        {client_pids, [Node1Client, Node2Client]}
        | Config
    ];
t_call_client_cluster('end', Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    [exit(ClientPid, kill) || ClientPid <- ?config(client_pids, Config)],
    ok.

%%% helpers
ident(Arg) ->
    Arg.

sort_alarms([{Node, Alarms}]) ->
    [{Node, lists:sort(fun(#{activate_at := A}, #{activate_at := B}) -> A < B end, Alarms)}].

setup_clients(Config) ->
    {ok, C} = emqtt:start_link([{clientid, <<"client1">>}, {username, <<"user1">>}]),
    {ok, _} = emqtt:connect(C),
    [{clients, [C]} | Config].

disconnect_clients(Config) ->
    Clients = ?config(clients, Config),
    lists:foreach(fun emqtt:disconnect/1, Clients).

get_mqtt_port(Node) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

connect_client(Node) ->
    Port = get_mqtt_port(Node),
    ClientId = <<(atom_to_binary(Node))/binary, "_client">>,
    {ok, Client} = emqtt:start_link([
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, Client, ClientId}.

client_msgs_args(ClientId) ->
    [mqueue_msgs, ClientId, #{limit => 10, continuation => none}].

client_msgs_bad_args(ClientId) ->
    [mqueue_msgs, ClientId, "bad_page_params"].
