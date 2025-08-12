%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("typerefl/include/types.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(NEW_CLIENTID(),
    iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(?LINE))
).
-define(NEW_USERNAME(), iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

-define(WAIT_FOR_DOWN(Pid, Timeout),
    (fun() ->
        receive
            {'DOWN', _, process, P, Reason} when Pid =:= P ->
                Reason
        after Timeout ->
            erlang:error(timeout)
        end
    end)()
).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(ALARM, <<"invalid_namespaced_configs">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(app_specs(), #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ?MODULE:Case({'end', Config}),
    ok.

app_specs() ->
    [
        emqx,
        {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
        emqx_mt,
        emqx_management
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

injected_fields() ->
    #{
        'roots.high' => [{foo, hoconsc:mk(hoconsc:ref(?MODULE, foo), #{})}]
    }.

fields(foo) ->
    [{bar, hoconsc:mk(persistent_term:get({?MODULE, bar_type}, binary()), #{})}].

connect(ClientId, Username) ->
    connect(#{clientid => ClientId, username => Username}).

connect(Opts0) ->
    DefaultOpts = #{proto_ver => v5},
    Opts = maps:merge(DefaultOpts, Opts0),
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, _Reason} = E ->
            catch emqtt:stop(Pid),
            receive
                {'DOWN', _, process, Pid, _, _} -> ok
            after 3000 ->
                exit(Pid, kill)
            end,
            erlang:error(E)
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_connect_disconnect({init, Config}) ->
    Config;
t_connect_disconnect({'end', _Config}) ->
    ok;
t_connect_disconnect(_Config) ->
    ClientId = ?NEW_CLIENTID(),
    Username = ?NEW_USERNAME(),
    Pid = connect(ClientId, Username),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Username)),
    ?assertEqual({error, not_found}, emqx_mt:count_clients(<<"unknown">>)),
    ?assertEqual({ok, [ClientId]}, emqx_mt:list_clients(Username)),
    ?assertEqual({error, not_found}, emqx_mt:list_clients(<<"unknown">>)),
    ?assertEqual([Username], emqx_mt:list_ns()),
    ok = emqtt:stop(Pid),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_proc_deleted},
            3000
        )
    ),
    ok.

t_session_limit_exceeded({init, Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1),
    Config;
t_session_limit_exceeded({'end', _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(infinity);
t_session_limit_exceeded(_Config) ->
    Ns = ?NEW_USERNAME(),
    C1 = ?NEW_CLIENTID(),
    C2 = ?NEW_CLIENTID(),
    Pid1 = connect(C1, Ns),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns)),
    %% two reasons may race
    try
        {ok, _} = connect(C2, Ns)
    catch
        error:{error, {quota_exceeded, _}} ->
            ok;
        exit:{shutdown, quota_exceeded} ->
            ok
    end,
    ok = emqtt:stop(Pid1).

%% if a client reconnects, it should not consume the session quota
t_session_reconnect({init, Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1),
    Config;
t_session_reconnect({'end', _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(infinity);
t_session_reconnect(_Config) ->
    Ns = ?NEW_USERNAME(),
    C1 = ?NEW_CLIENTID(),
    Pid1 = connect(C1, Ns),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns)),
    Pid2 = connect(C1, Ns),
    {ok, #{tns := Ns, clientid := C1, proc := CPid2}} = ?block_until(
        #{?snk_kind := multi_tenant_client_added},
        3000
    ),
    R = ?WAIT_FOR_DOWN(Pid1, 3000),
    ?assertMatch({shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}, R),
    ok = emqtt:stop(Pid2),
    _ = ?WAIT_FOR_DOWN(Pid2, 3000),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid2},
            3000
        )
    ),
    ok = emqx_mt_state:evict_ccache(Ns),
    ?assertEqual({ok, 0}, emqx_mt:count_clients(Ns)),
    ok.

%% Verifies that we initialize existing limiter groups when booting up the node.
t_initialize_limiter_groups({init, Config}) ->
    ClusterSpec = [{mt_initialize1, #{apps => app_specs()}}],
    ClusterOpts = #{
        work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
        shutdown => 5_000
    },
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(ClusterSpec, ClusterOpts),
    Cluster = emqx_cth_cluster:start(NodeSpecs),
    [{cluster, Cluster}, {node_specs, NodeSpecs} | Config];
t_initialize_limiter_groups({'end', Config}) ->
    Cluster = ?config(cluster, Config),
    ok = emqx_cth_cluster:stop(Cluster),
    ok;
t_initialize_limiter_groups(Config) when is_list(Config) ->
    [N] = ?config(cluster, Config),
    NodeSpecs = ?config(node_specs, Config),
    %% Setup namespace with limiters
    Params1 = emqx_mt_api_SUITE:tenant_limiter_params(),
    Params2 = emqx_mt_api_SUITE:client_limiter_params(),
    Params = emqx_utils_maps:deep_merge(Params1, Params2),
    Ns = atom_to_binary(?FUNCTION_NAME),
    ?ON(N, begin
        ok = emqx_mt_config:create_managed_ns(Ns),
        {ok, _} = emqx_mt_config:update_managed_ns_config(Ns, Params)
    end),
    %% Restart node
    %% N.B. For some reason, even using `shutdown => 5_000`, mnesia does not seem to
    %% correctly sync/flush data to disk when restarting the peer node.  We call
    %% `mnesia:sync_log` here to force it to sync data so that it's correctly loaded when
    %% the peer restarts.  Without this, the table is empty after the restart...
    ?ON(N, ok = mnesia:sync_log()),
    [N] = emqx_cth_cluster:restart(NodeSpecs),
    %% Client should connect fine
    ?check_trace(
        begin
            C1 = ?NEW_CLIENTID(),
            {ok, Pid1} = emqtt:start_link(#{
                username => Ns,
                clientid => C1,
                proto_ver => v5,
                port => emqx_mt_api_SUITE:get_mqtt_tcp_port(N)
            }),
            {ok, _} = emqtt:connect(Pid1),
            emqtt:stop(Pid1)
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(["hook_callback_exception"], Trace)),
            ok
        end
    ),
    ok.

-doc """
When (re)starting a node, it's possible that due to a bug or to intentionally
non-backwards compatible schema changes, we have persisted namespaced configs that are
invalid in mnesia.

This test attempts to emulate that situation, and verify that we _can_ start the node
normally, but such namespace will be "unavailable" in the sense that some operations such
as MQTT clients attempting to connect or data integrations will not work until the
configuration is manually fixed.

Note that this is just an emulation of such situation, because here in tests we don't
start the peer node exactly how it happens in a release.  For a real test, see the boot
tests that exist outside CT in this repo.
""".
t_namespaced_bad_config_during_start({init, Config}) ->
    Config;
t_namespaced_bad_config_during_start({'end', _Config}) ->
    ok;
t_namespaced_bad_config_during_start(Config) when is_list(Config) ->
    Ns = <<"some_namespace">>,
    {ok, Agent} = emqx_utils_agent:start_link(_BarType0 = binary()),
    SetType = fun() ->
        BarType = emqx_utils_agent:get(Agent),
        persistent_term:put({?MODULE, bar_type}, BarType)
    end,
    AppSpecs = [
        {emqx_conf, #{
            before_start =>
                fun(App, AppCfg) ->
                    SetType(),
                    ok = emqx_config:add_allowed_namespaced_config_root(<<"foo">>),
                    ok = emqx_schema_hooks:inject_from_modules([?MODULE]),
                    emqx_cth_suite:inhibit_config_loader(App, AppCfg)
                end
        }},
        emqx,
        {emqx_mt, #{
            after_start =>
                fun() ->
                    {ok, _} = emqx:update_config(
                        [mqtt, client_attrs_init],
                        [
                            #{
                                <<"expression">> => <<"username">>,
                                <<"set_as_attr">> => <<"tns">>
                            }
                        ]
                    )
                end
        }}
    ],
    NodeSpecs =
        [N1Spec] = emqx_cth_cluster:mk_nodespecs(
            [
                {corrupt_namespace_cfg1, #{apps => AppSpecs}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    ct:pal("starting cluster"),
    Nodes = [N1] = emqx_cth_cluster:start(NodeSpecs),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    ct:timetrap({seconds, 15}),
    ct:pal("cluster started"),
    ?assertMatch([_], ?ON(N1, emqx:get_config([mqtt, client_attrs_init]))),
    ct:pal("seeding namespace"),
    ?ON(
        N1,
        ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(
            emqx_schema, Ns
        )
    ),
    ct:pal("seeded namespace"),
    ct:pal("updating config that will become \"corrupt\" later on"),
    {ok, #{
        config := #{bar := <<"hello">>},
        namespace := Ns,
        raw_config := #{<<"bar">> := <<"hello">>}
    }} =
        ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => <<"hello">>}, #{namespace => Ns})),
    ct:pal("injecting failure"),
    _ = emqx_utils_agent:get_and_update(Agent, fun(_Old) -> {unused, _BarType1 = integer()} end),
    ct:pal("restarting node; should not fail to start"),
    {[N1], {ok, _}} =
        ?wait_async_action(
            emqx_cth_cluster:restart([N1Spec]),
            #{?snk_kind := "corrupt_ns_checker_started"}
        ),
    ?assertMatch(#{<<"foo">> := _}, ?ON(N1, emqx_config:get_namespace_config_errors(Ns))),
    Alarms1 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    ?assertMatch(
        [
            #{
                name := ?ALARM,
                message := <<"Namespaces with invalid configurations">>,
                details := #{
                    problems :=
                        #{
                            Ns :=
                                #{<<"foo">> := #{<<"kind">> := <<"validation_error">>}}
                        }
                }
            }
        ],
        [A || A = #{name := ?ALARM} <- Alarms1]
    ),
    %% Using the same value (now invalid)
    ?assertMatch(
        {error, #{reason := "Unable to parse integer value"}},
        ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => <<"hello">>}, #{namespace => Ns}))
    ),
    ?assertMatch([_], ?ON(N1, emqx:get_config([mqtt, client_attrs_init]))),
    ClientId = ?NEW_CLIENTID(),
    Port1 = emqx_mt_api_SUITE:get_mqtt_tcp_port(N1),
    ?assertError(
        {error, {server_unavailable, _}},
        connect(#{
            clientid => ClientId,
            username => Ns,
            port => Port1
        })
    ),
    %% With valid values of new type; should succeed and heal the node
    ?assertMatch(
        {{ok, #{namespace := Ns, config := #{bar := 1}, raw_config := #{<<"bar">> := 1}}}, {ok, _}},
        ?wait_async_action(
            ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => 1}, #{namespace => Ns})),
            #{?snk_kind := "corrupt_ns_checker_checked", ns := Ns}
        )
    ),
    ?assertMatch(undefined, ?ON(N1, emqx_config:get_namespace_config_errors(Ns))),
    KeyPath = [foo, bar],
    ?assertMatch(1, ?ON(N1, emqx:get_namespaced_config(Ns, KeyPath))),
    ?assertMatch(1, ?ON(N1, emqx:get_raw_namespaced_config(Ns, KeyPath))),
    Alarms2 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    ?assertMatch([], [A || A = #{name := ?ALARM} <- Alarms2]),
    ok.
