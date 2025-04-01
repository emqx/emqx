%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_plugins/include/emqx_plugins.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(EMQX_PLUGIN_APP_NAME_NO_AVSC, my_emqx_plugin).
-define(EMQX_PLUGIN_APP_NAME_AVSC, my_emqx_plugin_avsc).

-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.9.0-beta.3").

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
    [{group, avsc}, {group, no_avsc}].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [{avsc, All}, {no_avsc, All}].

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

init_per_group(avsc, Config) ->
    [{plugin_download_options, ?EMQX_PLUGIN_AVSC} | Config];
init_per_group(no_avsc, Config) ->
    [{plugin_download_options, ?EMQX_PLUGIN_NO_AVSC} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    emqx_plugins_test_helpers:purge_plugins(),
    PluginDownloadOptions = ?config(plugin_download_options, Config),
    #{name_vsn := NameVsn} = emqx_plugins_test_helpers:get_demo_plugin_package(
        PluginDownloadOptions
    ),
    [{name_vsn, NameVsn} | Config].

end_per_testcase(_TestCase, _Config) ->
    emqx_plugins_test_helpers:purge_plugins().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_update_config(Config) ->
    NameVsn = ?config(name_vsn, Config),

    %% Plugin is not installed, so we cannot update the config
    ?assertMatch(
        {error, _},
        emqx_plugins:update_config(NameVsn, #{<<"foo">> => <<"bar">>})
    ),

    %% Install the plugin, verify that we have a config
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch(
        #{<<"hostname">> := <<"localhost">>},
        emqx_plugins:get_config(NameVsn)
    ),

    %% Verify that callbacks are called even if the plugin is not running
    OldConfig = emqx_plugins:get_config(NameVsn),
    BadConfig = OldConfig#{<<"hostname">> => <<"bad.host">>},
    %% The error for non-localhost is baked into the plugin
    ?assertMatch(
        {error, <<"Invalid host: bad.host">>}, emqx_plugins:update_config(NameVsn, BadConfig)
    ),
    ?assertMatch(ok, emqx_plugins:update_config(NameVsn, OldConfig)),

    %% Verify that callbacks are called if the plugin is running
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertMatch(
        {error, <<"Invalid host: bad.host">>}, emqx_plugins:update_config(NameVsn, BadConfig)
    ),
    ?assertMatch(ok, emqx_plugins:update_config(NameVsn, OldConfig)),

    %% Verify that callbacks are called if the plugin is stopped
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertMatch(
        {error, <<"Invalid host: bad.host">>}, emqx_plugins:update_config(NameVsn, BadConfig)
    ),
    ?assertMatch(ok, emqx_plugins:update_config(NameVsn, OldConfig)),

    %% Verify that config cannot be updated if the plugin is uninstalled
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertMatch({error, _}, emqx_plugins:update_config(NameVsn, OldConfig)).
