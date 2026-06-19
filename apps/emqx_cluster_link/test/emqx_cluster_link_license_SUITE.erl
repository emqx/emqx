%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_license_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_cluster_link
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = set_mode(normal),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TC, Config) ->
    ok = set_mode(normal),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = set_mode(normal),
    cleanup_links(),
    ok.

%%

-doc """
Booting cluster_link under a community license leaves the supervisor up but
does not spawn per-link actors, even when the config has enabled links.
""".
t_boot_under_community_license_skips_actors(_Config) ->
    %% Configure an enabled link via the raw schema and bypass the gate by
    %% writing to the config directly (simulating a config carried over from
    %% before the license downgraded).
    Link = link_raw(<<"lic_boot">>, true),
    ok = write_links_directly([Link]),
    ok = restart_app(singleton),
    ?assertEqual(#{}, link_resources()),
    %% Re-booting under normal mode starts the resource.
    ok = restart_app(normal),
    ?assertMatch(#{<<"lic_boot">> := _}, link_resources()).

-doc "Creating an enabled link via the REST/config layer is rejected under community license.".
t_reject_enabled_create_under_community(_Config) ->
    ok = set_mode(singleton),
    Link = link_raw(<<"lic_create">>, true),
    ?assertEqual({error, single_node_license}, emqx_cluster_link_config:create_link(Link)).

-doc "Updating a link to enabled is rejected; deleting/disabling is allowed.".
t_reject_enable_toggle_under_community(_Config) ->
    %% Start in normal mode so we can create a disabled link.
    Link = link_raw(<<"lic_toggle">>, false),
    {ok, _} = emqx_cluster_link_config:create_link(Link),
    ok = set_mode(singleton),
    ?assertEqual(
        {error, single_node_license},
        emqx_cluster_link_config:update_link(Link#{<<"enable">> => true})
    ),
    %% Toggling to disabled is allowed (it was disabled to begin with, but
    %% the gate must accept it explicitly).
    {ok, _} = emqx_cluster_link_config:update_link(Link#{<<"enable">> => false}),
    %% Deleting is allowed too.
    ?assertEqual(ok, emqx_cluster_link_config:delete_link(<<"lic_toggle">>)).

-doc "After license upgrade, the user can toggle enable via the same REST path.".
t_upgrade_license_then_enable(_Config) ->
    ok = set_mode(singleton),
    %% Disabled-link create is fine under community.
    Link = link_raw(<<"lic_upgrade">>, false),
    {ok, _} = emqx_cluster_link_config:create_link(Link),
    %% Simulate a license upgrade.
    ok = set_mode(normal),
    %% Now the toggle goes through and the message forwarding resource starts.
    {ok, _} = emqx_cluster_link_config:update_link(Link#{<<"enable">> => true}),
    ?assertMatch(#{<<"lic_upgrade">> := _}, link_resources()).

%%

%% NOTE: `emqx_cluster:ensure_singleton_mode/0' resolves `?DEFAULT_MODE' which
%% is defined as `normal' under the TEST profile, so we set the env directly
%% to drive the gate from inside CT.
set_mode(singleton) ->
    application:set_env(emqx, cluster_mode, singleton),
    ok;
set_mode(normal) ->
    application:set_env(emqx, cluster_mode, normal),
    ok.

restart_app(Mode) ->
    _ = application:stop(emqx_cluster_link),
    ok = set_mode(Mode),
    {ok, _} = application:ensure_all_started(emqx_cluster_link),
    ok.

link_resources() ->
    emqx_cluster_link_mqtt:get_all_resources_local_v1().

write_links_directly(RawLinks) ->
    {ok, _} = emqx_conf:update(
        [cluster, links],
        RawLinks,
        #{rawconf_with_defaults => true, override_to => cluster}
    ),
    ok.

cleanup_links() ->
    lists:foreach(
        fun(#{name := Name}) ->
            _ = emqx_cluster_link_config:delete_link(Name)
        end,
        emqx_cluster_link_config:get_links()
    ).

link_raw(Name, Enable) ->
    #{
        <<"name">> => Name,
        <<"enable">> => Enable,
        <<"server">> => <<"localhost:31883">>,
        <<"topics">> => [<<"t/#">>],
        <<"pool_size">> => 1,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => 1000,
            <<"worker_pool_size">> => 1
        }
    }.
