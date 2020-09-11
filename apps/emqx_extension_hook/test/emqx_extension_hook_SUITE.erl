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

-module(emqx_extension_hook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_extension_hook], fun set_special_cfgs/1),
    emqx_logger:set_log_level(warning),
    Cfg.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_extension_hook]).

set_special_cfgs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_cfgs(emqx_extension_hook) ->
    application:set_env(emqx_extension_hook, drivers, []),
    ok.

reload_plugin_with(_DriverName = python3) ->
    application:stop(emqx_extension_hook),
    Path = emqx_ct_helpers:deps_path(emqx_extension_hook, "test/scripts"),
    Drivers = [{python3, [{init_module, main},
                          {python_path, Path},
                          {call_timeout, 5000}]}],
    application:set_env(emqx_extension_hook, drivers, Drivers),
    application:ensure_all_started(emqx_extension_hook);

reload_plugin_with(_DriverName = java) ->
    application:stop(emqx_extension_hook),

    ErlPortJar = emqx_ct_helpers:deps_path(erlport, "priv/java/_pkgs/erlport.jar"),
    Path = emqx_ct_helpers:deps_path(emqx_extension_hook, "test/scripts"),
    Drivers = [{java, [{init_module, 'Main'},
                       {java_path, Path},
                       {call_timeout, 5000}]}],

    %% Compile it
    ct:pal(os:cmd(lists:concat(["cd ", Path, " && ",
                                "rm -rf Main.class State.class && ",
                                "javac -cp ", ErlPortJar, " Main.java"]))),

    application:set_env(emqx_extension_hook, drivers, Drivers),
    application:ensure_all_started(emqx_extension_hook).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_python3(_) ->
    reload_plugin_with(python3),
    schedule_all_hooks().

t_java(_) ->
    reload_plugin_with(java),
    schedule_all_hooks().

schedule_all_hooks() ->
    ok = emqx_extension_hook_handler:on_client_connect(conninfo(), #{}),
    ok = emqx_extension_hook_handler:on_client_connack(conninfo(), success,#{}),
    ok = emqx_extension_hook_handler:on_client_connected(clientinfo(), conninfo()),
    ok = emqx_extension_hook_handler:on_client_disconnected(clientinfo(), takeovered, conninfo()),
    {stop, #{auth_result := success,
             anonymous := false}} = emqx_extension_hook_handler:on_client_authenticate(clientinfo(), #{auth_result => not_authorised, anonymous => true}),
    {stop, allow} = emqx_extension_hook_handler:on_client_check_acl(clientinfo(), publish, <<"t/a">>, deny),
    ok = emqx_extension_hook_handler:on_client_subscribe(clientinfo(), #{}, sub_topicfilters()),
    ok = emqx_extension_hook_handler:on_client_unsubscribe(clientinfo(), #{}, unsub_topicfilters()),

    ok = emqx_extension_hook_handler:on_session_created(clientinfo(), sessinfo()),
    ok = emqx_extension_hook_handler:on_session_subscribed(clientinfo(), <<"t/a">>, subopts()),
    ok = emqx_extension_hook_handler:on_session_unsubscribed(clientinfo(), <<"t/a">>, subopts()),
    ok = emqx_extension_hook_handler:on_session_resumed(clientinfo(), sessinfo()),
    ok = emqx_extension_hook_handler:on_session_discarded(clientinfo(), sessinfo()),
    ok = emqx_extension_hook_handler:on_session_takeovered(clientinfo(), sessinfo()),
    ok = emqx_extension_hook_handler:on_session_terminated(clientinfo(), sockerr, sessinfo()).

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------

conninfo() ->
    #{clientid => <<"123">>,
      username => <<"abc">>,
      peername => {{127,0,0,1}, 2341},
      sockname => {{0,0,0,0}, 1883},
      proto_name => <<"MQTT">>,
      proto_ver => 4,
      keepalive => 60
     }.

clientinfo() ->
    #{clientid => <<"123">>,
      username => <<"abc">>,
      peerhost => {127,0,0,1},
      sockport => 1883,
      protocol => 'mqtt',
      mountpoint => undefined
     }.

sub_topicfilters() ->
    [{<<"t/a">>, #{qos => 1}}].

unsub_topicfilters() ->
    [<<"t/a">>].

sessinfo() ->
    {session,xxx,yyy}.

subopts() ->
    #{qos => 1, rh => 0, rap => 0, nl => 0}.

