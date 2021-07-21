%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_app).

-behaviour(application).

-export([ start/2
        , stop/1
        , get_description/0
        , get_release/0
        ]).

-include("emqx.hrl").

-define(APP, emqx).

-define(EMQX_SHARDS, [ ?ROUTE_SHARD
                     , ?COMMON_SHARD
                     , ?SHARED_SUB_SHARD
                     , ?RULE_ENGINE_SHARD
                     , ?MOD_DELAYED_SHARD
                     ]).

-include("emqx_release.hrl").

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    set_backtrace_depth(),
    print_otp_version_warning(),
    print_banner(),
    %% Load application first for ekka_mnesia scanner
    _ = load_ce_modules(),
    ekka:start(),
    ok = ekka_rlog:wait_for_shards(?EMQX_SHARDS, infinity),
    false == os:getenv("EMQX_NO_QUIC")
        andalso application:ensure_all_started(quicer),
    {ok, Sup} = emqx_sup:start_link(),
    ok = start_autocluster(),
    % ok = emqx_plugins:init(),
    _ = emqx_plugins:load(),
    _ = start_ce_modules(),
    emqx_boot:is_enabled(listeners) andalso (ok = emqx_listeners:start()),
    register(emqx, self()),
    ok = emqx_alarm_handler:load(),
    print_vsn(),
    {ok, Sup}.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_boot:is_enabled(listeners)
      andalso emqx_listeners:stop().

set_backtrace_depth() ->
    Depth = emqx_config:get([node, backtrace_depth]),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

-ifndef(EMQX_ENTERPRISE).
load_ce_modules() ->
    application:load(emqx_modules).
start_ce_modules() ->
    application:ensure_all_started(emqx_modules).
-else.
load_ce_modules() ->
    ok.
start_ce_modules() ->
    ok.
-endif.

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

-if(?OTP_RELEASE> 22).
print_otp_version_warning() -> ok.
-else.
print_otp_version_warning() ->
    io:format("WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
              [?OTP_RELEASE]).
-endif. % OTP_RELEASE

-ifndef(TEST).

print_banner() ->
    io:format("Starting ~s on node ~s~n", [?APP, node()]).

print_vsn() ->
    io:format("~s ~s is running now!~n", [get_description(), get_release()]).

-else. % TEST

print_vsn() ->
    ok.

print_banner() ->
    ok.

-endif. % TEST

get_description() ->
    {ok, Descr0} = application:get_key(?APP, description),
    case os:getenv("EMQX_DESCRIPTION") of
        false -> Descr0;
        "" -> Descr0;
        Str -> string:strip(Str, both, $\n)
    end.

get_release() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        false ->    %% For TEST build or depedency build.
            release_in_macro();
        {_, Vsn} -> %% For emqx release build
            VsnStr = release_in_macro(),
            case string:str(Vsn, VsnStr) of
                1 -> ok;
                _ ->
                    erlang:error(#{ reason => version_mismatch
                                  , source => VsnStr
                                  , built_for => Vsn
                                  })
            end,
            Vsn
    end.

release_in_macro() ->
    element(2, ?EMQX_RELEASE).

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------
start_autocluster() ->
    ekka:callback(prepare, fun emqx:shutdown/1),
    ekka:callback(reboot,  fun emqx:reboot/0),
    _ = ekka:autocluster(?APP), %% returns 'ok' or a pid or 'any()' as in spec
    ok.
