%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    start/0,
    graceful_shutdown/0,
    is_ready/0,

    node_status/0,
    update_vips/0
]).

-include_lib("emqx/include/logger.hrl").

%% @doc EMQX boot entrypoint.
start() ->
    case os:type() of
        {win32, nt} ->
            ok;
        _Nix ->
            os:set_signal(sighup, ignore),
            %% default is handle
            os:set_signal(sigterm, handle)
    end,
    ok = set_backtrace_depth(),
    start_sysmon(),
    configure_shard_transports(),
    ekka:start(),
    ok = print_otp_version_warning().

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
    ?ULOG(
        "WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
        [?OTP_RELEASE]
    ).
% OTP_RELEASE > 22
-endif.

start_sysmon() ->
    _ = application:load(system_monitor),
    application:set_env(system_monitor, node_status_fun, {?MODULE, node_status}),
    application:set_env(system_monitor, status_checks, [{?MODULE, update_vips, false, 10}]),
    case application:get_env(system_monitor, db_hostname) of
        {ok, [_ | _]} ->
            application:set_env(system_monitor, callback_mod, system_monitor_pg),
            _ = application:ensure_all_started(system_monitor, temporary),
            ok;
        _ ->
            %% If there is no sink for the events, there is no reason
            %% to run system_monitor_top, ignore start
            ok
    end.

node_status() ->
    emqx_json:encode(#{
        backend => mria_rlog:backend(),
        role => mria_rlog:role()
    }).

update_vips() ->
    system_monitor:add_vip(mria_status:shards_up()).

configure_shard_transports() ->
    ShardTransports = application:get_env(emqx_machine, custom_shard_transports, #{}),
    lists:foreach(
        fun({ShardBin, Transport}) ->
            ShardName = binary_to_existing_atom(ShardBin),
            mria_config:set_shard_transport(ShardName, Transport)
        end,
        maps:to_list(ShardTransports)
    ).
