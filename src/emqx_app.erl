%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , prep_stop/1
        , stop/1
        , get_description/0
        , get_release/0
        ]).

%% internal exports for ad-hoc debugging.
-export([ set_alias_enrichment_module/0
        , set_special_auth_module/0
        ]).

-define(APP, emqx).

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
    {ok, Sup} = emqx_sup:start_link(),
    ok = start_autocluster(),
    %% We need to make sure that emqx's listeners start before plugins
    %% and modules. Since if the emqx-conf module/plugin is enabled, it will
    %% try to start or update the listeners with the latest configuration
    emqx_boot:is_enabled(listeners) andalso (ok = emqx_listeners:start()),
    ok = emqx_plugins:init(),
    _ = emqx_plugins:load(),
    _ = start_ce_modules(),
    set_alias_enrichment_module(),
    _ = set_special_auth_module(),
    register(emqx, self()),
    print_vsn(),
    {ok, Sup}.

prep_stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_boot:is_enabled(listeners)
      andalso emqx_listeners:stop().

stop(_State) ->
    ok.

set_backtrace_depth() ->
    Depth = application:get_env(?APP, backtrace_depth, 16),
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

set_alias_enrichment_module() ->
    case emqx:get_env(alias_enrichment_module) of
        undefined ->
            ok;
        Mod ->
            case erlang:function_exported(Mod, enrich_with_aliases, 2) of
                true ->
                    persistent_term:put(alias_enrichment_module, Mod);
                false ->
                    ok
            end
    end.

set_special_auth_module() ->
    case emqx:get_env(special_auth_module) of
        undefined ->
            ok;
        Mod ->
            case erlang:function_exported(Mod, check_authn, 2) of
                true ->
                    Priority = authn_module_priority(Mod),
                    persistent_term:put(special_auth_module, Mod),
                    emqx:hook('client.authenticate', fun Mod:check_authn/2, Priority);
                false ->
                    ok
            end
    end.

authn_module_priority(Mod) ->
    try
        Mod:authn_priority()
    catch
        _:_ ->
            10_000
    end.

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
        Str ->
            %% We replace the "EMQ X" to "EMQX" incase the description has been
            %% loaded to the OS Envs and cannot be changed without reboot.
            replace_emq_x_to_emqx(string:strip(Str, both, $\n))
    end.

get_release() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        false ->    %% For TEST build or depedency build.
            release_in_macro();
        {_, VsnCompile} -> %% For emqx release build
            VsnMacro = release_in_macro(),
            case string:str(VsnCompile, VsnMacro) of
                1 -> ok;
                _ -> error({version_not_match, VsnCompile, VsnMacro})
            end,
            VsnCompile
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

replace_emq_x_to_emqx(Str) ->
    re:replace(Str, "\\bEMQ X\\b", "EMQX", [{return,list}]).
