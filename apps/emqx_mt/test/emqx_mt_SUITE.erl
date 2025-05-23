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
    ?MODULE:Case({'end', Config}),
    ok.

app_specs() ->
    [
        emqx,
        {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
        emqx_mt,
        emqx_management
    ].

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

connect(ClientId, Username) ->
    Opts = [
        {clientid, ClientId},
        {username, Username},
        {proto_ver, v5}
    ],
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
