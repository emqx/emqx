%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_config:init_load(emqx_conf_schema),
    emqx_app:set_init_config_load_done(),
    ekka:start(),
    ok = ekka_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity).

copy_override_conf_from_core_node() ->
    CoreNodes = mnesia:system_info(running_db_nodes) -- [node()],
    {Results, Failed} = rpc:multicall(CoreNodes, ?MODULE, get_override_config_file, [], 20000),
    {Ready, NotReady} = lists:splitwith(fun(Res) -> element(1, Res) =:= ok end, Results),
    LogMeta = #{core_nodes => CoreNodes, failed => Failed, not_ready => NotReady,
        msg => "copy_overide_conf_from_core_node_failed_or_not_ready"},
    Failed =/= [] andalso ?SLOG(error, LogMeta),
    NotReady =/= [] andalso ?SLOG(info, LogMeta),
    case lists:sort(fun(A, B) -> A > B end, Ready) of
        [{ok, _WallClock, Info} | _] ->
            #{node := Node, conf := RawOverrideConf, tnx_id := TnxId} = Info,
            ?SLOG(debug, #{msg => "copy_overide_conf_from_core_node_success", node => Node}),
            ok = emqx_config:save_to_override_conf(RawOverrideConf, #{override_to => cluster}),
            {ok, TnxId};
        [] when CoreNodes =:= [] -> %% The first core nodes is self.
            ?SLOG(debug, #{msg => "skip_copy_overide_conf_from_core_node"}),
            {ok, -1};
        [] -> %% Other core node running but copy failed.
            ?SLOG(error, #{msg => "copy_overide_conf_from_core_node_failed",
                core_nodes => CoreNodes, failed => Failed, not_ready => NotReady}),
            error
    end.

get_override_config_file() ->
    Init = #{node => node()},
    case emqx_app:get_init_config_load_done() of
        false -> {error, Init#{msg => "init_conf_load_not_done"}};
        true ->
            case erlang:whereis(emqx_config_handler) of
                undefined -> {error, Init#{msg => "emqx_config_handler_not_ready"}};
                _ ->
                    case emqx_cluster_rpc:get_latest_tnx_id() of
                        {atomic, TnxId} ->
                            WallClock = erlang:statistics(wall_clock),
                            %% To prevent others from updating the file while we reading.
                            %% We read override conf from emqx_config_handler.
                            Conf = emqx_config_handler:get_raw_cluster_override_conf(),
                            {ok, WallClock, Init#{conf => Conf, tnx_id => TnxId}};
                        {aborted, Reason} ->
                            {error, Init#{msg => Reason}}
                    end
            end
    end.
