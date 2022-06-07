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
-module(emqx_machine_boot).

-include_lib("emqx/include/logger.hrl").

-export([post_boot/0]).
-export([stop_apps/0, ensure_apps_started/0]).
-export([sorted_reboot_apps/0]).
-export([start_autocluster/0]).

-dialyzer({no_match, [basic_reboot_apps/0]}).

-ifdef(TEST).
-export([sorted_reboot_apps/1]).
-endif.

%% these apps are always (re)started by emqx_machine
-define(BASIC_REBOOT_APPS, [gproc, esockd, ranch, cowboy, emqx]).

%% If any of these applications crash, the entire EMQX node shuts down
-define(BASIC_PERMANENT_APPS, [mria, ekka, esockd, emqx]).

post_boot() ->
    ok = ensure_apps_started(),
    ok = print_vsn(),
    ok = start_autocluster(),
    ignore.

-ifdef(TEST).
print_vsn() -> ok.
% TEST
-else.
print_vsn() ->
    ?ULOG("~ts ~ts is running now!~n", [emqx_app:get_description(), emqx_app:get_release()]).
% TEST
-endif.

start_autocluster() ->
    ekka:callback(stop, fun emqx_machine_boot:stop_apps/0),
    ekka:callback(start, fun emqx_machine_boot:ensure_apps_started/0),
    %% returns 'ok' or a pid or 'any()' as in spec
    _ = ekka:autocluster(emqx),
    ok.

stop_apps() ->
    ?SLOG(notice, #{msg => "stopping_emqx_apps"}),
    _ = emqx_alarm_handler:unload(),
    lists:foreach(fun stop_one_app/1, lists:reverse(sorted_reboot_apps())).

stop_one_app(App) ->
    ?SLOG(debug, #{msg => "stopping_app", app => App}),
    try
        _ = application:stop(App)
    catch
        C:E ->
            ?SLOG(error, #{
                msg => "failed_to_stop_app",
                app => App,
                exception => C,
                reason => E
            })
    end.

ensure_apps_started() ->
    ?SLOG(notice, #{msg => "(re)starting_emqx_apps"}),
    lists:foreach(fun start_one_app/1, sorted_reboot_apps()).

start_one_app(App) ->
    ?SLOG(debug, #{msg => "starting_app", app => App}),
    case application:ensure_all_started(App, restart_type(App)) of
        {ok, Apps} ->
            ?SLOG(debug, #{msg => "started_apps", apps => Apps});
        {error, Reason} ->
            ?SLOG(critical, #{msg => "failed_to_start_app", app => App, reason => Reason}),
            error({failed_to_start_app, App, Reason})
    end.

restart_type(App) ->
    PermanentApps =
        ?BASIC_PERMANENT_APPS ++ application:get_env(emqx_machine, permanent_applications, []),
    case lists:member(App, PermanentApps) of
        true ->
            permanent;
        false ->
            temporary
    end.

%% list of app names which should be rebooted when:
%% 1. due to static config change
%% 2. after join a cluster

%% the list of (re)started apps depends on release type/edition
reboot_apps() ->
    {ok, ConfigApps0} = application:get_env(emqx_machine, applications),
    BaseRebootApps = basic_reboot_apps(),
    ConfigApps = lists:filter(fun(App) -> not lists:member(App, BaseRebootApps) end, ConfigApps0),
    BaseRebootApps ++ ConfigApps.

basic_reboot_apps() ->
    CE =
        ?BASIC_REBOOT_APPS ++
            [
                emqx_prometheus,
                emqx_modules,
                emqx_dashboard,
                emqx_connector,
                emqx_gateway,
                emqx_statsd,
                emqx_resource,
                emqx_rule_engine,
                emqx_bridge,
                emqx_plugin_libs,
                emqx_management,
                emqx_retainer,
                emqx_exhook,
                emqx_authn,
                emqx_authz,
                emqx_slow_subs,
                emqx_auto_subscribe,
                emqx_plugins
            ],
    case emqx_release:edition() of
        ce -> CE;
        ee -> CE ++ []
    end.

sorted_reboot_apps() ->
    Apps = [{App, app_deps(App)} || App <- reboot_apps()],
    sorted_reboot_apps(Apps).

app_deps(App) ->
    case application:get_key(App, applications) of
        undefined -> undefined;
        {ok, List} -> lists:filter(fun(A) -> lists:member(A, reboot_apps()) end, List)
    end.

sorted_reboot_apps(Apps) ->
    G = digraph:new(),
    try
        NoDepApps = add_apps_to_digraph(G, Apps),
        case digraph_utils:topsort(G) of
            Sorted when is_list(Sorted) ->
                %% ensure emqx_conf boot up first
                [emqx_conf | Sorted ++ (NoDepApps -- Sorted)];
            false ->
                Loops = find_loops(G),
                error({circular_application_dependency, Loops})
        end
    after
        digraph:delete(G)
    end.

%% Build a dependency graph from the provided application list.
%% Return top-sort result of the apps.
%% Isolated apps without which are not dependency of any other apps are
%% put to the end of the list in the original order.
add_apps_to_digraph(G, Apps) ->
    lists:foldl(
        fun
            ({App, undefined}, Acc) ->
                ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
                Acc;
            ({App, []}, Acc) ->
                %% use '++' to keep the original order
                Acc ++ [App];
            ({App, Deps}, Acc) ->
                add_app_deps_to_digraph(G, App, Deps),
                Acc
        end,
        [],
        Apps
    ).

add_app_deps_to_digraph(G, App, undefined) ->
    ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
    %% not loaded
    add_app_deps_to_digraph(G, App, []);
add_app_deps_to_digraph(_G, _App, []) ->
    ok;
add_app_deps_to_digraph(G, App, [Dep | Deps]) ->
    digraph:add_vertex(G, App),
    digraph:add_vertex(G, Dep),
    %% dep -> app as dependency
    digraph:add_edge(G, Dep, App),
    add_app_deps_to_digraph(G, App, Deps).

find_loops(G) ->
    lists:filtermap(
        fun(App) ->
            case digraph:get_short_cycle(G, App) of
                false -> false;
                Apps -> {true, Apps}
            end
        end,
        digraph:vertices(G)
    ).
