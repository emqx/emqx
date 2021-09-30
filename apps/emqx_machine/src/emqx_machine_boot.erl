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
-module(emqx_machine_boot).

-include_lib("emqx/include/logger.hrl").

-export([post_boot/0]).
-export([stop_apps/1, ensure_apps_started/0]).
-export([sorted_reboot_apps/0]).
-export([start_autocluster/0]).

-ifdef(TEST).
-export([sorted_reboot_apps/1]).
-endif.

post_boot() ->
    ok = ensure_apps_started(),
    _ = emqx_plugins:load(),
    ok = print_vsn(),
    ok = start_autocluster(),
    ignore.

-ifdef(TEST).
print_vsn() -> ok.
-else. % TEST
print_vsn() ->
    ?ULOG("~s ~s is running now!~n", [emqx_app:get_description(), emqx_app:get_release()]).
-endif. % TEST


start_autocluster() ->
    ekka:callback(prepare, fun ?MODULE:stop_apps/1),
    ekka:callback(reboot,  fun ?MODULE:ensure_apps_started/0),
    _ = ekka:autocluster(emqx), %% returns 'ok' or a pid or 'any()' as in spec
    ok.

stop_apps(Reason) ->
    ?SLOG(info, #{msg => "stopping_apps", reason => Reason}),
    _ = emqx_alarm_handler:unload(),
    lists:foreach(fun stop_one_app/1, lists:reverse(sorted_reboot_apps())).

stop_one_app(App) ->
    ?SLOG(debug, #{msg => "stopping_app", app => App}),
    try
        _ = application:stop(App)
    catch
        C : E ->
            ?SLOG(error, #{msg => "failed_to_stop_app",
                app => App,
                exception => C,
                reason => E})
    end.


ensure_apps_started() ->
    lists:foreach(fun start_one_app/1, sorted_reboot_apps()).

start_one_app(App) ->
    ?SLOG(debug, #{msg => "starting_app", app => App}),
    case application:ensure_all_started(App) of
        {ok, Apps} ->
            ?SLOG(debug, #{msg => "started_apps", apps => Apps});
        {error, Reason} ->
            ?SLOG(critical, #{msg => "failed_to_start_app", app => App, reason => Reason}),
            error({failed_to_start_app, App, Reason})
    end.

%% list of app names which should be rebooted when:
%% 1. due to static static config change
%% 2. after join a cluster
reboot_apps() ->
    [ gproc
    , esockd
    , ranch
    , cowboy
    , emqx
    , emqx_prometheus
    , emqx_modules
    , emqx_dashboard
    , emqx_connector
    , emqx_gateway
    , emqx_statsd
    , emqx_resource
    , emqx_rule_engine
    , emqx_bridge
    , emqx_bridge_mqtt
    , emqx_plugin_libs
    , emqx_management
    , emqx_retainer
    , emqx_exhook
    , emqx_authn
    , emqx_authz
    ].

sorted_reboot_apps() ->
    Apps = [{App, app_deps(App)} || App <- reboot_apps()],
    sorted_reboot_apps(Apps).

app_deps(App) ->
    case application:get_key(App, applications) of
        undefined -> [];
        {ok, List} -> lists:filter(fun(A) -> lists:member(A, reboot_apps()) end, List)
    end.

sorted_reboot_apps(Apps) ->
    G = digraph:new(),
    try
        lists:foreach(fun({App, Deps}) -> add_app(G, App, Deps) end, Apps),
        case digraph_utils:topsort(G) of
            Sorted when is_list(Sorted) ->
                Sorted;
            false ->
                Loops = find_loops(G),
                error({circular_application_dependency, Loops})
        end
    after
        digraph:delete(G)
    end.

add_app(G, App, undefined) ->
    ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
    %% not loaded
    add_app(G, App, []);
add_app(_G, _App, []) ->
    ok;
add_app(G, App, [Dep | Deps]) ->
    digraph:add_vertex(G, App),
    digraph:add_vertex(G, Dep),
    digraph:add_edge(G, Dep, App), %% dep -> app as dependency
    add_app(G, App, Deps).

find_loops(G) ->
    lists:filtermap(
        fun (App) ->
            case digraph:get_short_cycle(G, App) of
                false -> false;
                Apps -> {true, Apps}
            end
        end, digraph:vertices(G)).
