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
    %% emqx_config:init_load(emqx_conf_schema),
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
    ?ULOG("WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
          [?OTP_RELEASE]).
-endif. % OTP_RELEASE > 22
