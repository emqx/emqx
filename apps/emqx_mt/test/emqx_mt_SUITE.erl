%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            emqx_mt,
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    ?MODULE:Case({init, Config}),
    Config.

end_per_testcase(Case, Config) ->
    snabbkaffe:stop(),
    ?MODULE:Case({'end', Config}),
    ok.

t_connect_disconnect({init, _Config}) ->
    ok;
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
    connect(#{clientid => ClientId, username => Username}).

connect(Opts) when is_map(Opts) ->
    {ok, Pid} = emqtt:start_link(maps:merge(#{proto_ver => v5}, Opts)),
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

t_session_limit_exceeded({init, _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1);
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
t_session_reconnect({init, _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1);
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

%% A persistent session that disconnects and then reconnects with
%% clean_start=false under a different namespace moves to the new namespace in
%% the client index.  Regression test for emqx/emqx#18533: the resume path
%% fires 'session.resumed' instead of 'session.created', which used to leave
%% the client listed under the old namespace.
t_session_resume_namespace_change({init, _Config}) ->
    ok;
t_session_resume_namespace_change({'end', _Config}) ->
    ok;
t_session_resume_namespace_change(_Config) ->
    Ns1 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns1"]),
    Ns2 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns2"]),
    C = ?NEW_CLIENTID(),
    Pid1 = connect(#{
        clientid => C,
        username => Ns1,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    }),
    {ok, #{proc := CPid1}} =
        ?block_until(#{?snk_kind := multi_tenant_client_added, tns := Ns1}, 3000),
    ?assertEqual({ok, [C]}, emqx_mt:list_clients(Ns1)),
    ok = emqtt:disconnect(Pid1),
    _ = ?WAIT_FOR_DOWN(Pid1, 3000),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(#{
                clientid => C,
                username => Ns2,
                clean_start => false,
                properties => #{'Session-Expiry-Interval' => 300}
            }),
            #{?snk_kind := multi_tenant_client_added, tns := Ns2},
            3000
        ),
    %% The resumed session took over the old channel process; its 'DOWN'
    %% removes the stale entry from the old namespace.
    {ok, _} =
        ?block_until(#{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid1}, 3000),
    ?assertEqual({ok, [C]}, emqx_mt:list_clients(Ns2)),
    ?assertEqual({ok, []}, emqx_mt:list_clients(Ns1)),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns2)),
    ?assertEqual({ok, 0}, emqx_mt:count_clients(Ns1)),
    ok = emqtt:stop(Pid2),
    ok.

%% A persistent session taken over while the previous connection is still
%% live, under a different namespace, moves to the new namespace in the
%% client index.
t_session_takeover_namespace_change({init, _Config}) ->
    ok;
t_session_takeover_namespace_change({'end', _Config}) ->
    ok;
t_session_takeover_namespace_change(_Config) ->
    Ns1 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns1"]),
    Ns2 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns2"]),
    C = ?NEW_CLIENTID(),
    Pid1 = connect(#{
        clientid => C,
        username => Ns1,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    }),
    {ok, #{proc := CPid1}} =
        ?block_until(#{?snk_kind := multi_tenant_client_added, tns := Ns1}, 3000),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(#{
                clientid => C,
                username => Ns2,
                clean_start => false,
                properties => #{'Session-Expiry-Interval' => 300}
            }),
            #{?snk_kind := multi_tenant_client_added, tns := Ns2},
            3000
        ),
    ?assertMatch(
        {shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}},
        ?WAIT_FOR_DOWN(Pid1, 3000)
    ),
    {ok, _} =
        ?block_until(#{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid1}, 3000),
    ?assertEqual({ok, [C]}, emqx_mt:list_clients(Ns2)),
    ?assertEqual({ok, []}, emqx_mt:list_clients(Ns1)),
    ok = emqtt:stop(Pid2),
    ok.

%% A persistent session that reconnects with clean_start=false under the same
%% namespace stays listed once, with no duplicate entry.
t_session_resume_same_namespace({init, _Config}) ->
    ok;
t_session_resume_same_namespace({'end', _Config}) ->
    ok;
t_session_resume_same_namespace(_Config) ->
    Ns = ?NEW_USERNAME(),
    C = ?NEW_CLIENTID(),
    Pid1 = connect(#{
        clientid => C,
        username => Ns,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    }),
    {ok, #{proc := CPid1}} =
        ?block_until(#{?snk_kind := multi_tenant_client_added, tns := Ns}, 3000),
    ok = emqtt:disconnect(Pid1),
    _ = ?WAIT_FOR_DOWN(Pid1, 3000),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(#{
                clientid => C,
                username => Ns,
                clean_start => false,
                properties => #{'Session-Expiry-Interval' => 300}
            }),
            #{?snk_kind := multi_tenant_client_added, tns := Ns},
            3000
        ),
    {ok, _} =
        ?block_until(#{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid1}, 3000),
    ?assertEqual({ok, [C]}, emqx_mt:list_clients(Ns)),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns)),
    ok = emqtt:stop(Pid2),
    ok.

%% A client that reconnects with clean_start=true under a different namespace
%% moves to the new namespace in the client index (the 'session.created' path;
%% guards against regressing the already-working behavior).
t_session_clean_start_namespace_change({init, _Config}) ->
    ok;
t_session_clean_start_namespace_change({'end', _Config}) ->
    ok;
t_session_clean_start_namespace_change(_Config) ->
    Ns1 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns1"]),
    Ns2 = iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-ns2"]),
    C = ?NEW_CLIENTID(),
    Pid1 = connect(#{
        clientid => C,
        username => Ns1,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    }),
    {ok, #{proc := CPid1}} =
        ?block_until(#{?snk_kind := multi_tenant_client_added, tns := Ns1}, 3000),
    ok = emqtt:disconnect(Pid1),
    _ = ?WAIT_FOR_DOWN(Pid1, 3000),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(#{
                clientid => C,
                username => Ns2,
                clean_start => true
            }),
            #{?snk_kind := multi_tenant_client_added, tns := Ns2},
            3000
        ),
    {ok, _} =
        ?block_until(#{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid1}, 3000),
    ?assertEqual({ok, [C]}, emqx_mt:list_clients(Ns2)),
    ?assertEqual({ok, []}, emqx_mt:list_clients(Ns1)),
    ok = emqtt:stop(Pid2),
    ok.
