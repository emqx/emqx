%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_app).

-behaviour(application).

-include("emqx_exhook.hrl").

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        , prep_stop/1
        ]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_exhook_sup:start_link(),

    %% Load all dirvers
    load_all_drivers(),

    %% Register all hooks
    load_exhooks(),

    %% Register CLI
    emqx_ctl:register_command(exhook, {emqx_exhook_cli, cli}, []),
    {ok, Sup}.

prep_stop(State) ->
    emqx_ctl:unregister_command(exhook),
    unload_exhooks(),
    unload_all_drivers(),
    State.

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

load_all_drivers() ->
    load_all_drivers(application:get_env(?APP, drivers, [])).

load_all_drivers([]) ->
    ok;
load_all_drivers([{Name, Opts}|Drivers]) ->
    ok = emqx_exhook:enable(Name, Opts),
    load_all_drivers(Drivers).

unload_all_drivers() ->
    emqx_exhook:disable_all().

%%--------------------------------------------------------------------
%% Exhooks

load_exhooks() ->
    [emqx:hook(Name, {M, F, A}) || {Name, {M, F, A}} <- search_exhooks()].

unload_exhooks() ->
    [emqx:unhook(Name, {M, F}) || {Name, {M, F, _A}} <- search_exhooks()].

search_exhooks() ->
    search_exhooks(ignore_lib_apps(application:loaded_applications())).
search_exhooks(Apps) ->
    lists:flatten([ExHooks || App <- Apps, {_App, _Mod, ExHooks} <- find_attrs(App, exhooks)]).

ignore_lib_apps(Apps) ->
    LibApps = [kernel, stdlib, sasl, appmon, eldap, erts,
               syntax_tools, ssl, crypto, mnesia, os_mon,
               inets, goldrush, gproc, runtime_tools,
               snmp, otp_mibs, public_key, asn1, ssh, hipe,
               common_test, observer, webtool, xmerl, tools,
               test_server, compiler, debugger, eunit, et,
               wx],
    [AppName || {AppName, _, _} <- Apps, not lists:member(AppName, LibApps)].

find_attrs(App, Def) ->
    [{App, Mod, Attr} || {ok, Modules} <- [application:get_key(App, modules)],
                         Mod <- Modules,
                         {Name, Attrs} <- module_attributes(Mod), Name =:= Def,
                         Attr <- Attrs].

module_attributes(Module) ->
    try Module:module_info(attributes)
    catch
        error:undef -> [];
        error:Reason -> error(Reason)
    end.

