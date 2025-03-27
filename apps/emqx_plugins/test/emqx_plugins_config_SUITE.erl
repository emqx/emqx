%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(EMQX_PLUGIN_APP_NAME_NO_AVSC, my_emqx_plugin).
-define(EMQX_PLUGIN_APP_NAME_AVSC, my_emqx_plugin_avsc).

-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.9.0-beta.1").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.9.0-beta.1").

-define(EMQX_PLUGIN_NO_AVSC, #{
    vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
    tag => ?EMQX_PLUGIN_TEMPLATE_TAG,
    release_name => atom_to_list(?EMQX_PLUGIN_APP_NAME_NO_AVSC),
    app_name => ?EMQX_PLUGIN_APP_NAME_NO_AVSC,
    git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
    shdir => emqx_plugins_fs:install_dir()
}).

-define(EMQX_PLUGIN_AVSC, #{
    vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
    tag => ?EMQX_PLUGIN_TEMPLATE_TAG,
    release_name => atom_to_list(?EMQX_PLUGIN_APP_NAME_AVSC),
    app_name => ?EMQX_PLUGIN_APP_NAME_AVSC,
    git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
    shdir => emqx_plugins_fs:install_dir()
}).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_ctl,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}}
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(InstallDir),
    [{suite_apps, Apps}, {install_dir, InstallDir} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_plugins_test_helpers:purge_plugins(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_plugins_test_helpers:purge_plugins().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_plugin_with_avsc_config(_Config) ->
    #{name_vsn := NameVsn} = emqx_plugins_test_helpers:get_demo_plugin_package(?EMQX_PLUGIN_AVSC),
    ok = emqx_plugins:ensure_installed(NameVsn),

    ?assertMatch(
        #{<<"hostname">> := <<"localhost">>},
        emqx_plugins:get_config(NameVsn)
    ),

    ok = emqx_plugins:ensure_uninstalled(NameVsn).


t_plugin_with_no_avsc_config(_Config) ->
    #{name_vsn := NameVsn} = emqx_plugins_test_helpers:get_demo_plugin_package(?EMQX_PLUGIN_NO_AVSC),
    ok = emqx_plugins:ensure_installed(NameVsn),

    ?assertMatch(
        #{<<"hostname">> := <<"localhost">>},
        emqx_plugins:get_config(NameVsn)
    ),

    ok = emqx_plugins:ensure_uninstalled(NameVsn).


