%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_raft_upgrade_suite).

%% This test suite verifies backward compatibility with the old
%% releases. Compatibility is verified both in regards to BPAPI and
%% data-at-rest.
%%
%% Currently it doesn't run in CI.

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/asserts.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(wait_boot, 60_000).
-define(wait_shutdown, 5_000).

-record(peer, {
    n :: node(),
    h :: string(),
    original_release :: string() | this,
    i :: pos_integer()
}).

-define(recv(PATTERN, TIMEOUT), fun() ->
    receive
        {publish, PATTERN = __Msg} ->
            {ok, __Msg}
    after TIMEOUT ->
        timeout
    end
end).

-define(recv(PATTERN), ?recv(PATTERN, 1_000)).

bwc_test(Config) ->
    {_, WorkDir} = lists:keyfind(workdir, 1, Config),
    {_, Cluster} = lists:keyfind(cluster, 1, Config),
    ok = test_cluster_formed(Cluster),
    Sessions = connect_clients(Cluster),
    ok = create_subscriptions(Sessions),
    ok = verify_sessions(Cluster, Sessions),
    ok = verify_payloads(<<"Hello from ">>, Sessions),
    %% Perform upgrade:
    ct:pal("Upgrading cluster..."),
    disconnect_sessions(Sessions),
    ok = upgrade_cluster(WorkDir, Cluster),
    ct:sleep(10_000),
    ct:pal("Cluster upgraded."),
    %% After upgrade:
    Sessions = connect_clients(Cluster),
    ok = verify_sessions(Cluster, Sessions),
    ok = verify_payloads(<<"Hello from upgraded ">>, Sessions),
    ok.

%% Verify that DS DBs become available in the mixed cluster:
test_cluster_formed(Cluster) ->
    Timeout = 15_000,
    lists:foreach(
        fun(#peer{n = Node}) ->
            ?assertMatch(
                ok,
                peer:call(
                    whereis(Node), emqx_persistent_message, wait_readiness, [Timeout], Timeout
                )
            )
        end,
        Cluster
    ).

%% Connect clients to each node of the cluster:
connect_clients(Cluster) ->
    lists:map(
        fun(#peer{h = Host}) ->
            ClientId = iolist_to_binary(["client@", Host]),
            ct:pal("Connecting ~s", [ClientId]),
            {ok, Client} = emqtt:start_link(
                #{
                    host => Host,
                    clientid => ClientId,
                    proto_ver => v5,
                    clean_start => false,
                    will_topic => ["will/", Host],
                    will_payload => <<"will message">>,
                    reconnect => infinity,
                    properties => #{
                        'Session-Expiry-Interval' => 1000,
                        'Will-Delay-Interval' => 10
                    },
                    name => binary_to_atom(ClientId)
                }
            ),
            {ok, _} = emqtt:connect(Client),
            ClientId
        end,
        Cluster
    ).

create_subscriptions(Sessions) ->
    lists:foreach(
        fun(ClientId) ->
            Client = binary_to_atom(ClientId),
            {ok, _, _} = emqtt:subscribe(Client, <<"t/#">>, 2),
            {ok, _, _} = emqtt:subscribe(Client, <<"t1/#">>, 1),
            {ok, _, _} = emqtt:subscribe(Client, <<"t2/#">>, 1),
            {ok, _, _} = emqtt:subscribe(Client, <<"$share/grp/s/#">>, 2)
        end,
        Sessions
    ).

disconnect_sessions(Sessions) ->
    lists:foreach(
        fun(ClientId) ->
            ct:pal("Disconnecting ~s", [ClientId]),
            ok = emqtt:disconnect(binary_to_atom(ClientId))
        end,
        Sessions
    ).

verify_sessions(Cluster, Sessions) ->
    [
        ?assertMatch(
            #{
                s := #{
                    id := ClientId,
                    subscriptions :=
                        #{
                            <<"t/#">> := _,
                            <<"t1/#">> := _,
                            <<"t2/#">> := _,
                            {share, <<"grp">>, <<"s/#">>} := _
                        }
                }
            },
            peer:call(
                whereis(Node),
                emqx_persistent_session_ds,
                print_session,
                [ClientId]
            )
        )
     || ClientId <- Sessions,
        #peer{n = Node} <- Cluster
    ],
    ok.

verify_payloads(Prefix, Sessions) ->
    N = length(Sessions),
    %% Publish normal message from each client:
    lists:foreach(
        fun(Sess) ->
            Payload = <<Prefix/binary, Sess/binary>>,
            Topic = <<"t/", Sess/binary>>,
            {ok, _} = emqtt:publish(
                binary_to_atom(Sess),
                Topic,
                Payload,
                2
            ),
            %% All clients should receive the message:
            _ = wait_messages(N, ?recv(#{payload := Payload, topic := Topic}))
        end,
        Sessions
    ),
    %% %% Publish shared messages:
    %% lists:foreach(
    %%     fun(Sess) ->
    %%         Payload = <<"hello from ", Sess/binary>>,
    %%         Topic = <<"s/", Sess/binary>>,
    %%         {ok, _} = emqtt:publish(
    %%             binary_to_atom(Sess),
    %%             Topic,
    %%             Payload,
    %%             2
    %%         ),
    %%         %% One client should receive:
    %%         [_] = wait_messages(
    %%             1,
    %%             ?recv(#{payload := Payload, topic := Topic}, 5_000)
    %%         )
    %%     end,
    %%     Sessions
    %% ),
    ok.

init_per_group(Group, Config) ->
    Releases =
        case Group of
            r6_0_0 -> ["6.0.0"];
            r6_1_0 -> ["6.1.0"]
        end,
    WorkDir = emqx_cth_suite:work_dir(all, Config),
    Cluster = start_cluster(WorkDir, Releases),
    [{cluster, Cluster}, {workdir, WorkDir} | Config].

groups() ->
    TCs = [bwc_test],
    [
        {r6_0_0, [sequence], TCs},
        {r6_1_0, [sequence], TCs}
    ].

all() ->
    %% TODO: test should run in mixed cluster instead. But currently
    %% it's impossible due to license.
    [{group, r6_0_0}, {group, r6_1_0}].

end_per_group(_, Config) ->
    {_, WorkDir} = lists:keyfind(workdir, 1, Config),
    {_, Cluster} = lists:keyfind(cluster, 1, Config),
    stop_cluster(Cluster),
    emqx_cth_suite:clean_work_dir(WorkDir),
    ok.

upgrade_cluster(BaseDir, Cluster) ->
    NNodes = length(Cluster),
    NewRel = this,
    [
        begin
            ct:pal("Upgrading ~p from ~p to ~p...", [Node, OrigRel, NewRel]),
            ?assertMatch(ok, peer:stop(whereis(Node))),
            ?assertMatch({ok, _}, start_peer(NewRel, I, BaseDir, NNodes)),
            ct:pal("Done", [])
        end
     || #peer{i = I, n = Node, original_release = OrigRel} <- Cluster
    ],
    ok.

start_cluster(WorkDir, Releases) ->
    %% Start nodes running on different releases:
    {Nodes, _} = lists:mapfoldl(
        fun(Release, I) ->
            {ok, Peer} = start_peer(Release, I, WorkDir, length(Releases)),
            {Peer, I + 1}
        end,
        1,
        Releases
    ),
    %% Make a cluster:
    [Seed | Rest] = lists:reverse(Nodes),
    lists:foreach(
        fun(#peer{n = Node}) ->
            ?retry(
                1000,
                10,
                ?assertMatch(ok, peer:call(whereis(Node), ekka, join, [Seed#peer.n], 15_000))
            ),
            ?assertMatch(ok, peer:call(whereis(Node), emqx_license, unload, []))
        end,
        Rest
    ),
    Nodes.

start_peer(Release, N, BaseDir, NNodes) ->
    Host = "127.54.22." ++ integer_to_list(N),
    NodeStr = "emqx" ++ integer_to_list(N) ++ "@" ++ Host,
    {ok, Node} = start_peer(Release, NodeStr, Host, BaseDir, NNodes),
    Peer = #peer{n = Node, h = Host, original_release = Release, i = N},
    {ok, Peer}.

start_peer(this, NodeStr, Host, BaseDir, NNodes) ->
    Emqx = get_release_executable(),
    ?assert(is_list(Emqx)),
    Args = ["foreground"],
    WorkDir = workdir(BaseDir, NodeStr),
    ct:pal("~s workdir: ~s~n", [Host, WorkDir]),
    {ok, Peer, Node} = peer:start(
        #{
            name => NodeStr,
            connection => standard_io,
            exec => {Emqx, Args},
            env => env(NodeStr, Host, WorkDir, NNodes),
            wait_boot => ?wait_boot,
            longnames => true,
            shutdown => ?wait_shutdown
        }
    ),
    register(Node, Peer),
    {ok, Node};
start_peer(Release, NodeStr, Host, WorkDir, NNodes) when is_list(Release) ->
    Docker = os:find_executable("docker"),
    ?assert(is_list(Docker)),
    {ok, Peer, Node} = peer:start(
        #{
            name => NodeStr,
            connection => standard_io,
            exec => {Docker, docker_args(Release, NodeStr, Host, WorkDir, NNodes)},
            wait_boot => ?wait_boot,
            longnames => true,
            shutdown => ?wait_shutdown
        }
    ),
    register(Node, Peer),
    {ok, Node}.

%% erlfmt-ignore
docker_args(Release, Node, Host, Dir, NNodes) ->
    User = os:cmd("echo -n $(id -u):$(id -g)"),
    Env = env(Node, Host, "/data", NNodes),
    WorkDir = workdir(Dir, Node),
    ct:pal("~s workdir: ~s (docker)~n", [Host, WorkDir]),
    ok = filelib:ensure_path(WorkDir),
    ["run", "-i"] ++
     [lists:flatten(io_lib:format("-e~s=~s", [K, V])) || {K, V} <- Env] ++
    ["--user", User,
     "-v", WorkDir ++ ":/data:rw",
     "--hostname", Host,
     "--network", "host",
     "emqx/emqx:" ++ Release,
     "--", "emqx", "foreground"].

env(Node, Host, DataDir, NNodes0) ->
    NNodes = integer_to_list(NNodes0),
    ListenerConf = [
        {"EMQX_listeners__" ++ L ++ "__default__enable", "false"}
     || L <- ["ssl", "ws", "wss"]
    ],
    [
        {"EMQX_NODE__NAME", Node},
        {"EMQX_rpc__port_discovery", "stateless"},
        {"EMQX_durable_sessions__enable", "true"},
        {"EMQX_durable_sessions__shared_subs__heartbeat_interval", "100"},
        {"EMQX_durable_sessions__shared_subs__realloc_interval", "100"},
        {"EMQX_durable_sessions__shared_subs__leader_timeout", "100"},
        {"EMQX_durable_sessions__shared_subs__checkpoint_interval", "10"},
        %% Logging:
        {"EMQX_log__file__default__enable", "true"},
        {"EMQX_log__file__default__level", "notice"},
        {"EMQX_LOG_DIR", filename:join(DataDir, "debug_logs")},
        %% Durable storage:
        {"EMQX_node__data_dir", DataDir},
        {"EMQX_durable_storage__n_sites", NNodes},
        {"EMQX_durable_storage__messages__replication_factor", NNodes},
        {"EMQX_durable_storage__sessions__replication_factor", NNodes},
        {"EMQX_durable_storage__timers__replication_factor", NNodes},
        {"EMQX_durable_storage__shared_subs__replication_factor", NNodes},
        %% Listeners:
        {"EMQX_dashboard__listeners__http__bind", "0"},
        {"EMQX_listeners__tcp__default__bind", Host ++ ":1883"}
        | ListenerConf
    ].

wait_messages(N, Recv) when N >= 0 ->
    wait_messages(N, Recv, []).

wait_messages(0, Recv, Acc) ->
    case Recv() of
        {ok, Msg} ->
            error({unexpected_message, Msg, Acc});
        timeout ->
            lists:reverse(Acc)
    end;
wait_messages(N, Recv, Acc) ->
    case Recv() of
        {ok, Msg} ->
            wait_messages(N - 1, Recv, [Msg | Acc]);
        timeout ->
            error({timeout, Acc})
    end.

workdir(BaseDir, Node) ->
    filename:join(BaseDir, Node).

stop_cluster(Nodes) ->
    [
        try
            ok = peer:stop(whereis(I))
        catch
            _:_ -> ok
        end
     || #peer{n = I} <- Nodes
    ],
    ok.

get_release_executable() ->
    %% TODO: this is quite ugly:
    {_, Source} = lists:keyfind(source, 1, ?MODULE:module_info(compile)),
    filename:join(filename:dirname(Source), "../../../_build/emqx-enterprise/rel/emqx/bin/emqx").
