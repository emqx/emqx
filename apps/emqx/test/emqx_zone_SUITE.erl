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

-module(emqx_zone_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(ENVS, [{use_username_as_clientid, false},
               {server_keepalive, 60},
               {upgrade_qos, false},
               {session_expiry_interval, 7200},
               {retry_interval, 20},
               {mqueue_store_qos0, true},
               {mqueue_priorities, none},
               {mqueue_default_priority, highest},
               {max_subscriptions, 0},
               {max_mqueue_len, 1000},
               {max_inflight, 32},
               {max_awaiting_rel, 100},
               {keepalive_backoff, 0.75},
               {ignore_loop_deliver, false},
               {idle_timeout, 15000},
               {force_shutdown_policy, #{max_heap_size => 838860800,
                                         message_queue_len => 8000}},
               {force_gc_policy, #{bytes => 1048576, count => 1000}},
               {enable_stats, true},
               {enable_flapping_detect, false},
               {enable_ban, true},
               {enable_acl, true},
               {await_rel_timeout, 300},
               {acl_deny_action, ignore}
              ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx),
    application:set_env(emqx, zone_env, val),
    application:set_env(emqx, zones, [{zone, ?ENVS}]),
    Config.

end_per_suite(_Config) ->
    emqx_zone:unset_all_env(),
    application:unset_env(emqx, zone_env),
    application:unset_env(emqx, zones).

t_zone_env_func(_) ->
    lists:foreach(fun({Env, Val}) ->
                          case erlang:function_exported(emqx_zone, Env, 1) of
                              true ->
                                  ?assertEqual(Val, erlang:apply(emqx_zone, Env, [zone]));
                              false -> ok
                          end
                  end, ?ENVS).

t_get_env(_) ->
    ?assertEqual(val, emqx_zone:get_env(undefined, zone_env)),
    ?assertEqual(val, emqx_zone:get_env(undefined, zone_env, def)),
    ?assert(emqx_zone:get_env(zone, enable_acl)),
    ?assert(emqx_zone:get_env(zone, enable_ban)),
    ?assertEqual(defval, emqx_zone:get_env(extenal, key, defval)),
    ?assertEqual(undefined, emqx_zone:get_env(external, key)),
    ?assertEqual(undefined, emqx_zone:get_env(internal, key)),
    ?assertEqual(def, emqx_zone:get_env(internal, key, def)).

t_get_set_env(_) ->
    ok = emqx_zone:set_env(zone, key, val),
    ?assertEqual(val, emqx_zone:get_env(zone, key)),
    true = emqx_zone:unset_env(zone, key),
    ?assertEqual(undefined, emqx_zone:get_env(zone, key)).

t_force_reload(_) ->
    {ok, _} = emqx_zone:start_link(),
    ?assertEqual(undefined, emqx_zone:get_env(xzone, key)),
    application:set_env(emqx, zones, [{xzone, [{key, val}]}]),
    ok = emqx_zone:force_reload(),
    ?assertEqual(val, emqx_zone:get_env(xzone, key)),
    emqx_zone:stop().

t_uncovered_func(_) ->
    {ok, Pid} = emqx_zone:start_link(),
    ignored = gen_server:call(Pid, unexpected_call),
    ok = gen_server:cast(Pid, unexpected_cast),
    ok = Pid ! ok,
    emqx_zone:stop().

t_frame_options(_) ->
    ?assertMatch(#{strict_mode := _, max_size := _ }, emqx_zone:mqtt_frame_options(zone)).
