%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([get_override_config_file/0]).
-export([sync_data_from_node/0]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_conf.hrl").

-define(DEFAULT_INIT_TXN_ID, -1).

start(_StartType, _StartArgs) ->
    try
        ok = init_conf()
    catch
        C:E:St ->
            %% logger is not quite ready.
            io:format(standard_error, "Failed to load config~n~p~n~p~n~p~n", [C, E, St]),
            init:stop(1)
    end,
    ok = emqx_config_logger:refresh_config(),
    emqx_conf_sup:start_link().

stop(_State) ->
    ok.

%% Read the cluster config from the local node.
%% This function is named 'override' due to historical reasons.
get_override_config_file() ->
    Node = node(),
    case emqx_app:get_init_config_load_done() of
        false ->
            {error, #{node => Node, msg => "init_conf_load_not_done"}};
        true ->
            case erlang:whereis(emqx_config_handler) of
                undefined ->
                    {error, #{node => Node, msg => "emqx_config_handler_not_ready"}};
                _ ->
                    Fun = fun() ->
                        TnxId = emqx_cluster_rpc:get_node_tnx_id(Node),
                        WallClock = erlang:statistics(wall_clock),
                        Conf = emqx_config_handler:get_raw_cluster_override_conf(),
                        HasDeprecateFile = emqx_config:has_deprecated_file(),
                        #{
                            wall_clock => WallClock,
                            conf => Conf,
                            tnx_id => TnxId,
                            node => Node,
                            has_deprecated_file => HasDeprecateFile,
                            release => emqx_release:version_with_prefix()
                        }
                    end,
                    case mria:ro_transaction(?CLUSTER_RPC_SHARD, Fun) of
                        {atomic, Res} -> {ok, Res};
                        {aborted, Reason} -> {error, #{node => Node, msg => Reason}}
                    end
            end
    end.

sync_data_from_node() ->
    Dir = emqx:data_dir(),
    TargetDirs = lists:filter(fun(Type) -> filelib:is_dir(filename:join(Dir, Type)) end, [
        "authz", "certs"
    ]),
    Name = "data.zip",
    case zip:zip(Name, TargetDirs, [memory, {cwd, Dir}]) of
        {ok, {Name, Bin}} -> {ok, Bin};
        {error, Reason} -> {error, Reason}
    end.

%% ------------------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------------------

init_load() ->
    emqx_config:init_load(emqx_conf:schema_module()).

init_conf() ->
    %% Workaround for https://github.com/emqx/mria/issues/94:
    _ = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], 1000),
    _ = mria:wait_for_tables([?CLUSTER_MFA, ?CLUSTER_COMMIT]),
    {ok, TnxId} = sync_cluster_conf(),
    _ = emqx_app:set_init_tnx_id(TnxId),
    ok = init_load(),
    ok = emqx_app:set_init_config_load_done().

cluster_nodes() ->
    mria:cluster_nodes(cores) -- [node()].

%% @doc Try to sync the cluster config from other core nodes.
sync_cluster_conf() ->
    case cluster_nodes() of
        [] ->
            %% The first core nodes is self.
            ?SLOG(debug, #{
                msg => "skip_sync_cluster_conf",
                reason => "This is a single node, or the first node in the cluster"
            }),
            {ok, ?DEFAULT_INIT_TXN_ID};
        Nodes ->
            sync_cluster_conf2(Nodes)
    end.

%% @private Some core nodes are running, try to sync the cluster config from them.
sync_cluster_conf2(Nodes) ->
    {Results, Failed} = emqx_conf_proto_v2:get_override_config_file(Nodes),
    {Ready, NotReady0} = lists:partition(fun(Res) -> element(1, Res) =:= ok end, Results),
    NotReady = lists:filter(fun(Res) -> element(1, Res) =:= error end, NotReady0),
    case (Failed =/= [] orelse NotReady =/= []) of
        true when Ready =/= [] ->
            %% Some core nodes failed to reply.
            Warning = #{
                nodes => Nodes,
                failed => Failed,
                not_ready => NotReady,
                msg => "ignored_nodes_when_sync_cluster_conf"
            },
            ?SLOG(warning, Warning);
        true ->
            %% There are core nodes running but no one was able to reply.
            ?SLOG(error, #{
                msg => "failed_to_sync_cluster_conf",
                nodes => Nodes,
                failed => Failed,
                not_ready => NotReady
            });
        false ->
            ok
    end,
    case Ready of
        [] ->
            case should_proceed_with_boot() of
                true ->
                    %% Act as if this node is alone, so it can
                    %% finish the boot sequence and load the
                    %% config for other nodes to copy it.
                    ?SLOG(info, #{
                        msg => "skip_sync_cluster_conf",
                        loading_from_disk => true,
                        nodes => Nodes,
                        failed => Failed,
                        not_ready => NotReady
                    }),
                    {ok, ?DEFAULT_INIT_TXN_ID};
                false ->
                    %% retry in some time
                    Jitter = rand:uniform(2000),
                    Timeout = 10000 + Jitter,
                    timer:sleep(Timeout),
                    ?SLOG(warning, #{
                        msg => "sync_cluster_conf_retry",
                        timeout => Timeout,
                        nodes => Nodes,
                        failed => Failed,
                        not_ready => NotReady
                    }),
                    sync_cluster_conf()
            end;
        _ ->
            sync_cluster_conf3(Ready)
    end.

%% @private Filter out the nodes which are running a newer version than this node.
sync_cluster_conf3(Ready) ->
    NotNewer = fun({ok, #{release := RemoteRelease}}) ->
        try
            emqx_release:vsn_compare(RemoteRelease) =/= newer
        catch
            _:_ ->
                %% If the version is not valid (without v or e prefix),
                %% we know it's older than v5.1.0/e5.1.0
                true
        end
    end,
    case lists:filter(NotNewer, Ready) of
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
                hint =>
                    "Booting this node without syncing cluster config from peer core nodes "
                    "because other nodes are running a newer version",
                peer_nodes => NodesAndVersions
            }),
            {ok, ?DEFAULT_INIT_TXN_ID};
        Ready2 ->
            sync_cluster_conf4(Ready2)
    end.

%% @private Some core nodes are running and replied with their configs successfully.
%% Try to sort the results and save the first one for local use.
sync_cluster_conf4(Ready) ->
    [{ok, Info} | _] = lists:sort(fun conf_sort/2, Ready),
    #{node := Node, conf := RawOverrideConf, tnx_id := TnxId} = Info,
    HasDeprecatedFile = has_deprecated_file(Info),
    ?SLOG(debug, #{
        msg => "sync_cluster_conf_success",
        synced_from_node => Node,
        has_deprecated_file => HasDeprecatedFile,
        local_release => emqx_app:get_release(),
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

should_proceed_with_boot() ->
    TablesStatus = emqx_cluster_rpc:get_tables_status(),
    LocalNode = node(),
    case maps:get(?CLUSTER_COMMIT, TablesStatus) of
        {disc, LocalNode} ->
            %% Loading locally; let this node finish its boot sequence
            %% so others can copy the config from this one.
            true;
        _ ->
            %% Loading from another node or still waiting for nodes to
            %% be up.  Try again.
            false
    end.

conf_sort({ok, #{tnx_id := Id1}}, {ok, #{tnx_id := Id2}}) when Id1 > Id2 -> true;
conf_sort({ok, #{tnx_id := Id, wall_clock := W1}}, {ok, #{tnx_id := Id, wall_clock := W2}}) ->
    W1 > W2;
conf_sort({ok, _}, {ok, _}) ->
    false.

sync_data_from_node(Node) ->
    case emqx_conf_proto_v2:sync_data_from_node(Node) of
        {ok, DataBin} ->
            case zip:unzip(DataBin, [{cwd, emqx:data_dir()}]) of
                {ok, []} ->
                    ?SLOG(debug, #{node => Node, msg => "sync_data_from_node_ignore"});
                {ok, Files} ->
                    ?SLOG(debug, #{node => Node, msg => "sync_data_from_node_ok", files => Files})
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
