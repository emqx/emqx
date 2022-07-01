%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    init_conf(),
    emqx_conf_sup:start_link().

stop(_State) ->
    ok.

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
                        #{wall_clock => WallClock, conf => Conf, tnx_id => TnxId, node => Node}
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
    {ok, Zip} = zip:zip(atom_to_list(node()) ++ "_data.zip", TargetDirs, [{cwd, Dir}]),
    Res = {ok, _Bin} = file:read_file(Zip),
    _ = file:delete(Zip),
    Res.

%% ------------------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------------------

-ifdef(TEST).
init_load() ->
    emqx_config:init_load(emqx_conf:schema_module(), #{raw_with_default => false}).

-else.

init_load() ->
    emqx_config:init_load(emqx_conf:schema_module(), #{raw_with_default => true}).
-endif.

init_conf() ->
    {ok, TnxId} = copy_override_conf_from_core_node(),
    emqx_app:set_init_tnx_id(TnxId),
    init_load(),
    emqx_app:set_init_config_load_done().

cluster_nodes() ->
    maps:get(running_nodes, ekka_cluster:info()) -- [node()].

copy_override_conf_from_core_node() ->
    case cluster_nodes() of
        %% The first core nodes is self.
        [] ->
            ?SLOG(debug, #{msg => "skip_copy_overide_conf_from_core_node"}),
            {ok, ?DEFAULT_INIT_TXN_ID};
        Nodes ->
            {Results, Failed} = emqx_conf_proto_v2:get_override_config_file(Nodes),
            {Ready, NotReady0} = lists:partition(fun(Res) -> element(1, Res) =:= ok end, Results),
            NotReady = lists:filter(fun(Res) -> element(1, Res) =:= error end, NotReady0),
            case (Failed =/= [] orelse NotReady =/= []) andalso Ready =/= [] of
                true ->
                    Warning = #{
                        nodes => Nodes,
                        failed => Failed,
                        not_ready => NotReady,
                        msg => "ignored_bad_nodes_when_copy_init_config"
                    },
                    ?SLOG(warning, Warning);
                false ->
                    ok
            end,
            case Ready of
                [] ->
                    %% Other core nodes running but no one replicated it successfully.
                    ?SLOG(error, #{
                        msg => "copy_override_conf_from_core_node_failed",
                        nodes => Nodes,
                        failed => Failed,
                        not_ready => NotReady
                    }),

                    case should_proceed_with_boot() of
                        true ->
                            %% Act as if this node is alone, so it can
                            %% finish the boot sequence and load the
                            %% config for other nodes to copy it.
                            ?SLOG(info, #{
                                msg => "skip_copy_overide_conf_from_core_node",
                                loading_from_disk => true,
                                nodes => Nodes,
                                failed => Failed,
                                not_ready => NotReady
                            }),
                            {ok, ?DEFAULT_INIT_TXN_ID};
                        false ->
                            %% retry in some time
                            Jitter = rand:uniform(2_000),
                            Timeout = 10_000 + Jitter,
                            ?SLOG(info, #{
                                msg => "copy_overide_conf_from_core_node_retry",
                                timeout => Timeout,
                                nodes => Nodes,
                                failed => Failed,
                                not_ready => NotReady
                            }),
                            timer:sleep(Timeout),
                            copy_override_conf_from_core_node()
                    end;
                _ ->
                    [{ok, Info} | _] = lists:sort(fun conf_sort/2, Ready),
                    #{node := Node, conf := RawOverrideConf, tnx_id := TnxId} = Info,
                    Msg = #{
                        msg => "copy_overide_conf_from_core_node_success",
                        node => Node
                    },
                    ?SLOG(debug, Msg),
                    ok = emqx_config:save_to_override_conf(
                        RawOverrideConf,
                        #{override_to => cluster}
                    ),
                    ok = sync_data_from_node(Node),
                    {ok, TnxId}
            end
    end.

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
            {ok, Files} = zip:unzip(DataBin, [{cwd, emqx:data_dir()}]),
            ?SLOG(debug, #{node => Node, msg => "sync_data_from_node_ok", files => Files}),
            ok;
        Error ->
            ?SLOG(emergency, #{node => Node, msg => "sync_data_from_node_failed", reason => Error}),
            error(Error)
    end.
