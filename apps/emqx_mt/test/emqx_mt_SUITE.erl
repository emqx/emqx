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

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            emqx_mt
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
    %% two reaons may race
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
