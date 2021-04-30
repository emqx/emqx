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

-module(emqx_exhook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    _ = emqx_exhook_demo_svr:start(),
    emqx_ct_helpers:start_apps([emqx_exhook], fun set_special_cfgs/1),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_exhook]),
    emqx_exhook_demo_svr:stop().

set_special_cfgs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file, undefined),
    application:set_env(emqx, modules_loaded_file, undefined);
set_special_cfgs(emqx_exhook) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_noserver_nohook(_) ->
    emqx_exhook:disable(default),
    ?assertEqual([], ets:tab2list(emqx_hooks)),

    Opts = proplists:get_value(
             default,
             application:get_env(emqx_exhook, servers, [])
            ),
    ok = emqx_exhook:enable(default, Opts),
    ?assertNotEqual([], ets:tab2list(emqx_hooks)).

t_cli_list(_) ->
    meck_print(),
    ?assertEqual( [[emqx_exhook_server:format(Svr) || Svr <- emqx_exhook:list()]]
                , emqx_exhook_cli:cli(["server", "list"])
                ),
    unmeck_print().

t_cli_enable_disable(_) ->
    meck_print(),
    ?assertEqual([already_started], emqx_exhook_cli:cli(["server", "enable", "default"])),
    ?assertEqual(ok, emqx_exhook_cli:cli(["server", "disable", "default"])),
    ?assertEqual([], emqx_exhook_cli:cli(["server", "list"])),

    ?assertEqual([not_running], emqx_exhook_cli:cli(["server", "disable", "default"])),
    ?assertEqual(ok, emqx_exhook_cli:cli(["server", "enable", "default"])),
    unmeck_print().

t_cli_stats(_) ->
    meck_print(),
    _ = emqx_exhook_cli:cli(["server", "stats"]),
    _ = emqx_exhook_cli:cli(x),
    unmeck_print().

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

meck_print() ->
    meck:new(emqx_ctl, [passthrough, no_history, no_link]),
    meck:expect(emqx_ctl, print, fun(_) -> ok end),
    meck:expect(emqx_ctl, print, fun(_, Args) -> Args end).

unmeck_print() ->
    meck:unload(emqx_ctl).
