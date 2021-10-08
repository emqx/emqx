%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine).

-export([ start/0
        , graceful_shutdown/0
        , is_ready/0
        ]).

-export([get_override_config_file/0]).

-include_lib("emqx/include/logger.hrl").

%% @doc EMQ X boot entrypoint.
start() ->
    case os:type() of
        {win32, nt} -> ok;
        _nix ->
            os:set_signal(sighup, ignore),
            os:set_signal(sigterm, handle) %% default is handle
    end,
    ok = set_backtrace_depth(),
    ok = print_otp_version_warning(),

    {ok, TnxId} = copy_override_conf_from_core_node(),
    emqx_app:set_init_tnx_id(TnxId),
    load_config_files().

graceful_shutdown() ->
    emqx_machine_terminator:graceful_wait().

set_backtrace_depth() ->
    {ok, Depth} = application:get_env(emqx_machine, backtrace_depth),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

%% @doc Return true if boot is complete.
is_ready() ->
    emqx_machine_terminator:is_running().

-if(?OTP_RELEASE > 22).
print_otp_version_warning() -> ok.
-else.
print_otp_version_warning() ->
    ?ULOG("WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
          [?OTP_RELEASE]).
-endif. % OTP_RELEASE > 22

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
            ok = emqx_config:save_to_override_conf(RawOverrideConf),
            {ok, TnxId};
        [] when CoreNodes =:= [] -> %% The first core nodes is self.
            ?SLOG(debug, #{msg => "skip_the_step_of_copy_overide_conf_from_core_node"}),
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
                            Conf = emqx_config_handler:get_raw_override_conf(),
                            {ok, WallClock, Init#{conf => Conf, txn_id => TnxId}};
                        {aborted, Reason} ->
                            {error, Init#{msg => Reason}}
                    end
            end
    end.

load_config_files() ->
    %% the app env 'config_files' for 'emqx` app should be set
    %% in app.time.config by boot script before starting Erlang VM
    ConfFiles = application:get_env(emqx, config_files, []),
    %% emqx_machine_schema is a superset of emqx_schema
    ok = emqx_config:init_load(emqx_machine_schema, ConfFiles),
    %% to avoid config being loaded again when emqx app starts.
    ok = emqx_app:set_init_config_load_done().
