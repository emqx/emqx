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

-include_lib("emqx/include/logger.hrl").
-include("emqx_conf.hrl").

start(_StartType, _StartArgs) ->
    init_conf(),
    emqx_conf_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
init_conf() ->
    {ok, TnxId} = copy_override_conf_from_core_node(),
    emqx_app:set_init_tnx_id(TnxId),
    emqx_config:init_load(emqx_conf:schema_module()),
    emqx_app:set_init_config_load_done().

copy_override_conf_from_core_node() ->
    case nodes() of
        [] -> %% The first core nodes is self.
            ?SLOG(debug, #{msg => "skip_copy_overide_conf_from_core_node"}),
            {ok, -1};
        Nodes ->
            {Results, Failed} = emqx_conf_proto_v1:get_override_config_file(Nodes),
            {Ready, NotReady0} = lists:partition(fun(Res) -> element(1, Res) =:= ok end, Results),
            NotReady = lists:filter(fun(Res) -> element(1, Res) =:= error end, NotReady0),
            case (Failed =/= [] orelse NotReady =/= []) andalso Ready =/= [] of
                true ->
                    Warning = #{nodes => Nodes, failed => Failed, not_ready => NotReady,
                        msg => "ignored_bad_nodes_when_copy_init_config"},
                    ?SLOG(warning, Warning);
                false -> ok
            end,
            case Ready of
                [] ->
                    %% Other core nodes running but no one replicated it successfully.
                    ?SLOG(error, #{msg => "copy_overide_conf_from_core_node_failed",
                        nodes => Nodes, failed => Failed, not_ready => NotReady}),
                    {error, "core node not ready"};
                _ ->
                    SortFun = fun({ok, #{wall_clock := W1}},
                        {ok, #{wall_clock := W2}}) -> W1 > W2 end,
                    [{ok, Info} | _] = lists:sort(SortFun, Ready),
                    #{node := Node, conf := RawOverrideConf, tnx_id := TnxId} = Info,
                    Msg = #{msg => "copy_overide_conf_from_core_node_success", node => Node},
                    ?SLOG(debug, Msg),
                    ok = emqx_config:save_to_override_conf(RawOverrideConf,
                        #{override_to => cluster}),
                    {ok, TnxId}
            end
    end.

get_override_config_file() ->
    Node = node(),
    Role = mria_rlog:role(),
    case emqx_app:get_init_config_load_done() of
        false -> {error, #{node => Node, msg => "init_conf_load_not_done"}};
        true when Role =:= core ->
            case erlang:whereis(emqx_config_handler) of
                undefined -> {error, #{node => Node, msg => "emqx_config_handler_not_ready"}};
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
            end;
        true when Role =:= replicant ->
            {ignore, #{node => Node}}
    end.
