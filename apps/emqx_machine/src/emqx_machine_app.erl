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

-module(emqx_machine_app).

-export([ start/2
        , stop/1
        , prep_stop/1
        ]).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

start(_Type, _Args) ->
    ok = set_backtrace_depth(),
    ok = print_otp_version_warning(),
    _ = load_modules(),

    {ok, _} = application:ensure_all_started(emqx),

    _ = emqx_plugins:load(),
    _ = start_modules(),

    ok = print_vsn(),
    emqx_machine_sup:start_link().

prep_stop(_State) ->
    application:stop(emqx).

stop(_State) ->
    ok.

set_backtrace_depth() ->
    {ok, Depth} = application:get_env(emqx_machine, backtrace_depth),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

-if(?OTP_RELEASE > 22).
print_otp_version_warning() -> ok.
-else.
print_otp_version_warning() ->
    ?ULOG("WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
          [?OTP_RELEASE]).
-endif. % OTP_RELEASE > 22

-ifdef(TEST).
print_vsn() -> ok.
-else. % TEST
print_vsn() ->
    ?ULOG("~s ~s is running now!~n", [emqx_app:get_description(), emqx_app:get_release()]).
-endif. % TEST

-ifndef(EMQX_ENTERPRISE).
load_modules() ->
    application:load(emqx_modules).
start_modules() ->
    application:ensure_all_started(emqx_modules).
-else.
load_modules() ->
    ok.
start_modules() ->
    ok.
-endif.
