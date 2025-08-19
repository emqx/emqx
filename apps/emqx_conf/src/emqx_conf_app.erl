%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([get_override_config_file/0]).
-export([sync_data_from_node/0]).
-export([unset_config_loaded/0]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_conf.hrl").

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_cluster_rpc:create_tables()),
    try
        ok = init_conf()
    catch
        C:E:St ->
            ?SLOG(error, #{
                msg => "failed_to_load_config", error => C, reason => E, stacktrace => St
            }),
            exit_loop(1)
    end,
    ok = emqx_config_logger:refresh_config(),
    emqx_conf_sup:start_link().

stop(_State) ->
    ok.

exit_loop(ExitCode) ->
    timer:sleep(100),
    init:stop(ExitCode),
    exit_loop(ExitCode).

%% @doc emqx_conf relies on this flag to synchronize configuration between nodes.
%% Therefore, we must clean up this flag when emqx application is restarted by mria.
unset_config_loaded() ->
    emqx_app:unset_config_loaded().

%% Read the cluster config from the local node.
%% This function is named 'override' due to historical reasons.
get_override_config_file() ->
    Node = node(),
    Data = #{
        wall_clock => erlang:statistics(wall_clock),
        node => Node,
        release => emqx_release:version_with_prefix()
    },
    case emqx_app:init_load_done() of
        false ->
            {error, Data#{msg => "init_conf_load_not_done"}};
        true ->
            case erlang:whereis(emqx_config_handler) of
                undefined ->
                    {error, Data#{msg => "emqx_config_handler_not_ready"}};
                _ ->
                    Fun = fun() ->
                        TnxId = emqx_cluster_rpc:get_node_tnx_id(Node),
                        Conf = emqx_config_handler:get_raw_cluster_override_conf(),
                        HasDeprecateFile = emqx_config:has_deprecated_file(),
                        Data#{
                            conf => Conf,
                            tnx_id => TnxId,
                            has_deprecated_file => HasDeprecateFile
                        }
                    end,
                    case mria:ro_transaction(?CLUSTER_RPC_SHARD, Fun) of
                        {atomic, Res} -> {ok, Res};
                        {aborted, Reason} -> {error, Data#{msg => Reason}}
                    end
            end
    end.

-define(DATA_DIRS, ["authz", "certs"]).

sync_data_from_node() ->
    DataDir = emqx:data_dir(),
    Name = "data.zip",
    Files = traverse_and_collect_files(DataDir),
    case zip:zip(Name, Files, [memory, {cwd, DataDir}]) of
        {ok, {Name, Bin}} ->
            {ok, Bin};
        {error, Reason} ->
            {error, Reason}
    end.

%% ------------------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------------------

init_load(TnxId) ->
    case emqx_app:get_config_loader() of
        Module when Module == emqx; Module == emqx_conf ->
            ok = emqx_config:init_load(emqx_conf:schema_module()),
            %% Set load config done after update(init) tnx_id.
            ok = emqx_cluster_rpc:maybe_init_tnx_id(node(), TnxId),
            ok = emqx_app:set_config_loader(emqx_conf),
            ok;
        Module ->
            ?SLOG(info, #{
                msg => "skip_init_config_load",
                reason => "Some application has set another config loader",
                loader => Module
            })
    end.

init_conf() ->
    emqx_cluster_rpc:wait_for_cluster_rpc(),
    {ok, TnxId} = sync_cluster_conf(),
    ok = init_load(TnxId),
    ok.

cluster_nodes() ->
    mria:cluster_nodes(cores) -- [node()].

%% @doc Try to sync the cluster config from other core nodes.
sync_cluster_conf() ->
    case cluster_nodes() of
        [] ->
            %% The first core nodes is self.
            ?SLOG(info, #{
                msg => "skip_sync_cluster_conf",
                reason => "This is a single node, or the first node in the cluster"
            }),
            {ok, ?DEFAULT_INIT_TXN_ID};
        Nodes ->
            sync_cluster_conf2(Nodes)
    end.

%% @private Some core nodes are running, try to sync the cluster config from them.
sync_cluster_conf2(Nodes) ->
    {Results, Failed} = emqx_conf_proto_v4:get_override_config_file(Nodes),
    {Ready, NotReady} = lists:partition(fun(Res) -> element(1, Res) =:= ok end, Results),
    LogData = #{peer_nodes => Nodes, self_node => node()},
    case Failed ++ NotReady of
        [] ->
            ok;
        _ ->
            ?SLOG(
                warning,
                LogData#{
                    msg => "cluster_config_fetch_failures",
                    failed_nodes => Failed,
                    booting_nodes => NotReady
                }
            )
    end,
    MyRole = mria_rlog:role(),
    case Ready of
        [] when MyRole =:= replicant ->
            %% replicant should never boot without copying from a core node
            delay_and_retry(LogData#{role => replicant});
        [] ->
            %% none of the nodes are ready, either delay-and-retry or boot without wait
            TableStatus = tx_commit_table_status(),
            sync_cluster_conf5(TableStatus, LogData);
        _ ->
            %% copy config from the best node in the Ready list
            sync_cluster_conf3(Ready)
    end.

%% None of the peer nodes are responsive, so we have to make a decision
%% based on the commit lagging (if the commit table is loaded).
%%
%% It could be that the peer nodes are also booting up,
%% however we cannot always wait because it may run into a dead-lock.
%%
%% Giving up wait here implies that some changes made to the peer node outside
%% of cluster-rpc MFAs will be lost.
%% e.g. stop all nodes, manually change cluster.hocon in one node
%% then boot all nodes around the same time, the changed cluster.hocon may
%% get lost if the node happen to copy config from others.
sync_cluster_conf5({loaded, local}, LogData) ->
    ?SLOG(info, LogData#{
        msg => "skip_copy_cluster_config_from_peer_nodes",
        explain => "Commit table loaded locally from disk, assuming that I have the latest config"
    }),
    {ok, ?DEFAULT_INIT_TXN_ID};
sync_cluster_conf5({loaded, From}, LogData) ->
    case get_commit_lag() of
        #{my_id := MyId, latest := Latest} = Lagging when MyId >= Latest orelse Latest =:= 0 ->
            ?SLOG(info, LogData#{
                msg => "skip_copy_cluster_config_from_peer_nodes",
                explain => "I have the latest cluster config commit",
                commit_loaded_from => From,
                lagging_info => Lagging
            }),
            {ok, ?DEFAULT_INIT_TXN_ID};
        #{my_id := _MyId, latest := _Latest} = Lagging ->
            delay_and_retry(LogData#{lagging_info => Lagging, commit_loaded_from => From})
    end;
sync_cluster_conf5({waiting, Waiting}, LogData) ->
    %% this may never happen? since we waited for table before
    delay_and_retry(LogData#{table_pending => Waiting}).

get_commit_lag() ->
    emqx_cluster_rpc:get_commit_lag().

delay_and_retry(LogData) ->
    Timeout = sync_delay_timeout(),
    ?SLOG(warning, LogData#{
        msg => "sync_cluster_conf_retry",
        explain =>
            "Cannot boot alone due to potentially stale data. "
            "Will try sync cluster config again after delay",
        delay => Timeout
    }),
    timer:sleep(Timeout),
    sync_cluster_conf().

-ifdef(TEST).
sync_delay_timeout() ->
    Jitter = rand:uniform(200),
    1_000 + Jitter.
-else.
sync_delay_timeout() ->
    Jitter = rand:uniform(2000),
    10_000 + Jitter.
-endif.

%% @private Filter out the nodes which are running a newer version than this node.
sync_cluster_conf3(Ready) ->
    case lists:filter(fun is_older_or_same_version/1, Ready) of
        [] ->
            %% All available core nodes are running a newer version than this node.
            %% Start this node without syncing cluster config from them.
            %% This is likely a restart of an older version node during cluster upgrade.
            NodesAndVersions = lists:map(
                fun({ok, #{node := Node, release := Release}}) ->
                    #{node => Node, version => Release}
                end,
                Ready
            ),
            ?SLOG(warning, #{
                msg => "all_available_nodes_running_newer_version",
                explain =>
                    "Booting this node without syncing cluster config from core nodes "
                    "because other nodes are running a newer version",
                versions => NodesAndVersions
            }),
            {ok, ?DEFAULT_INIT_TXN_ID};
        Ready2 ->
            sync_cluster_conf4(Ready2)
    end.

is_older_or_same_version({ok, #{release := RemoteRelease}}) ->
    try
        emqx_release:vsn_compare(RemoteRelease) =/= newer
    catch
        _:_ ->
            %% If the version is not valid (without v or e prefix),
            %% we know it's older than v5.1.0/e5.1.0
            true
    end;
is_older_or_same_version(_) ->
    %% older version has no 'release' field
    true.

%% @private Some core nodes are running and replied with their configs successfully.
%% Try to sort the results and save the first one for local use.
sync_cluster_conf4(Ready) ->
    [{ok, Info} | _] = lists:sort(fun conf_sort/2, Ready),
    #{node := Node, conf := RawOverrideConf, tnx_id := TnxId} = Info,
    HasDeprecatedFile = has_deprecated_file(Info),
    ?SLOG(info, #{
        msg => "sync_cluster_conf_success",
        synced_from_node => Node,
        has_deprecated_file => HasDeprecatedFile,
        local_release => emqx_release:version_with_prefix(),
        remote_release => maps:get(release, Info, "before_v5.0.24|e5.0.3"),
        data_dir => emqx:data_dir(),
        tnx_id => TnxId
    }),
    ok = emqx_config:save_to_override_conf(
        HasDeprecatedFile,
        RawOverrideConf,
        #{override_to => cluster}
    ),
    ok = sync_data_from_node(Node),
    {ok, TnxId}.

tx_commit_table_status() ->
    TablesStatus = emqx_cluster_rpc:get_tables_status(),
    maps:get(?CLUSTER_COMMIT, TablesStatus).

conf_sort({ok, #{tnx_id := Id1}}, {ok, #{tnx_id := Id2}}) when Id1 > Id2 -> true;
conf_sort({ok, #{tnx_id := Id, wall_clock := W1}}, {ok, #{tnx_id := Id, wall_clock := W2}}) ->
    W1 > W2;
conf_sort({ok, _}, {ok, _}) ->
    false.

sync_data_from_node(Node) ->
    case emqx_conf_proto_v4:sync_data_from_node(Node) of
        {ok, DataBin} ->
            case zip:unzip(DataBin, [{cwd, emqx:data_dir()}]) of
                {ok, []} ->
                    ?SLOG(debug, #{node => Node, msg => "sync_data_from_node_empty_response"});
                {ok, Files} ->
                    ?SLOG(debug, #{
                        node => Node,
                        msg => "sync_data_from_node_non_empty_response",
                        files => Files
                    })
            end,
            ok;
        Error ->
            ?SLOG(emergency, #{node => Node, msg => "sync_data_from_node_failed", reason => Error}),
            error(Error)
    end.

has_deprecated_file(#{conf := Conf} = Info) ->
    case maps:find(has_deprecated_file, Info) of
        {ok, HasDeprecatedFile} ->
            HasDeprecatedFile;
        error ->
            %% The old version don't have emqx_config:has_deprecated_file/0
            %% Conf is not empty if deprecated file is found.
            Conf =/= #{}
    end.

traverse_and_collect_files(DataDir) ->
    SubDirs = lists:map(fun(D) -> filename:join(DataDir, D) end, ?DATA_DIRS),
    Prefix = ensure_trailing_slash(DataDir),
    do_traverse_and_collect_files(SubDirs, Prefix, _Acc = []).

do_traverse_and_collect_files([] = _SubDirs, _Prefix, Acc) ->
    Acc;
do_traverse_and_collect_files([SubDir | Rest], Prefix, Acc0) ->
    %% This function already drops any non-regular file, including symlinks.
    Acc = filelib:fold_files(
        SubDir,
        _Regex = "",
        _Recursive = true,
        fun(Path0, Acc) ->
            Path = to_data_dir_relative_path(Path0, Prefix),
            [Path | Acc]
        end,
        Acc0
    ),
    do_traverse_and_collect_files(Rest, Prefix, Acc).

%% Note: `Prefix' must end in `/'.
to_data_dir_relative_path(Path, Prefix) ->
    lists:flatten(string:replace(Path, Prefix, "", leading)).

ensure_trailing_slash(DataDir) ->
    case lists:suffix("/", DataDir) of
        true ->
            DataDir;
        false ->
            DataDir ++ "/"
    end.
