%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sm_SUITE).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(ATTRS, #{clean_start         => true,
                 client_id           => <<"client">>,
                 zone                => internal,
                 username            => <<"emqx">>,
                 expiry_interval     => 0,
                 max_inflight        => 0,
                 topic_alias_maximum => 0,
                 will_msg            => undefined}).

all() -> [{group, registry}, {group, ets}].

groups() ->
    Cases =
        [ t_resume_session,
          t_discard_session,
          t_register_unregister_session,
          t_get_set_session_attrs,
          t_get_set_session_stats,
          t_lookup_session_pids],
    [ {registry, [non_parallel_tests], Cases},
      {ets, [non_parallel_tests], Cases}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(registry, Config) ->
    emqx_ct_helpers:start_apps([], fun enable_session_registry/1),
    Config;
init_per_group(ets, Config) ->
    emqx_ct_helpers:start_apps([], fun disable_session_registry/1),
    Config.

end_per_group(_, _Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(_All, Config) ->
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => self()}),
    [{session_pid, SPid}|Config].

end_per_testcase(_All, Config) ->
    emqx_sm:close_session(?config(session_pid, Config)),
    receive
        {shutdown, normal} -> ok
    after 500 -> ct:fail({timeout, wait_session_shutdown})
    end.

enable_session_registry(_) ->
    application:set_env(emqx, enable_session_registry, true),
    ok.

disable_session_registry(_) ->
    application:set_env(emqx, enable_session_registry, false),
    ok.

t_resume_session(Config) ->
    ?assertEqual({ok, ?config(session_pid, Config)}, emqx_sm:resume_session(<<"client">>, ?ATTRS#{conn_pid => self()})).

t_discard_session(_) ->
    ?assertEqual(ok, emqx_sm:discard_session(<<"client1">>)).

t_register_unregister_session(_) ->
    Pid = self(),
    ?assertEqual(ok, emqx_sm:register_session(<<"client">>)),
    ?assertEqual(ok, emqx_sm:register_session(<<"client">>, Pid)),
    ?assertEqual(ok, emqx_sm:unregister_session(<<"client">>)),
    ?assertEqual(ok, emqx_sm:unregister_session(<<"client">>), Pid).

t_get_set_session_attrs(Config) ->
    SPid = ?config(session_pid, Config),
    ClientPid0 = spawn(fun() -> receive _ -> ok end end),
    ?assertEqual(true, emqx_sm:set_session_attrs(<<"client">>, [?ATTRS#{conn_pid => ClientPid0}])),
    ?assertEqual(true, emqx_sm:set_session_attrs(<<"client">>, SPid, [?ATTRS#{conn_pid => ClientPid0}])),
    [SAttr0] = emqx_sm:get_session_attrs(<<"client">>, SPid),
    ?assertEqual(ClientPid0, maps:get(conn_pid, SAttr0)),
    ?assertEqual(true, emqx_sm:set_session_attrs(<<"client">>, SPid, [?ATTRS#{conn_pid => self()}])),
    [SAttr1] = emqx_sm:get_session_attrs(<<"client">>, SPid),
    ?assertEqual(self(), maps:get(conn_pid, SAttr1)).

t_get_set_session_stats(Config) ->
    SPid = ?config(session_pid, Config),
    ?assertEqual(true, emqx_sm:set_session_stats(<<"client">>, [{inflight, 10}])),
    ?assertEqual(true, emqx_sm:set_session_stats(<<"client">>, SPid, [{inflight, 10}])),
    ?assertEqual([{inflight, 10}], emqx_sm:get_session_stats(<<"client">>, SPid)).

t_lookup_session_pids(Config) ->
    SPid = ?config(session_pid, Config),
    ?assertEqual([SPid], emqx_sm:lookup_session_pids(<<"client">>)).
