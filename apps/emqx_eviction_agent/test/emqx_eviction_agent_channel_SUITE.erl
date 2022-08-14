%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CLIENT_ID, <<"client_with_session">>).

-import(emqx_eviction_agent_test_helpers,
        [emqtt_connect/2]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_eviction_agent]).

t_start_no_session(_Config) ->
    Opts = #{clientinfo => #{clientid => ?CLIENT_ID,
                             zone => internal},
             conninfo => #{clientid => ?CLIENT_ID,
                           receive_maximum => 32}},
    ?assertMatch(
       {error, {no_session, _}},
       emqx_eviction_agent_channel:start_supervised(Opts)).

t_start_no_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),

    Opts = #{clientinfo => #{clientid => ?CLIENT_ID,
                             zone => internal},
             conninfo => #{clientid => ?CLIENT_ID,
                           receive_maximum => 32,
                           expiry_interval => 0}},
    ?assertMatch(
       {error, {should_be_expired, _}},
       emqx_eviction_agent_channel:start_supervised(Opts)).

t_start_infinite_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),

    Opts = #{clientinfo => #{clientid => ?CLIENT_ID,
                             zone => internal},
             conninfo => #{clientid => ?CLIENT_ID,
                           receive_maximum => 32,
                           expiry_interval => ?UINT_MAX}},
    ?assertMatch(
       {ok, _},
       emqx_eviction_agent_channel:start_supervised(Opts)).

t_kick(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
       ok,
       emqx_eviction_agent_channel:call(Pid, kick)).

t_discard(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
       ok,
       emqx_eviction_agent_channel:call(Pid, discard)).

t_stop(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ?assertEqual(
       ok,
       emqx_eviction_agent_channel:stop(Pid)).


t_ignored_calls(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    Opts = evict_session_opts(?CLIENT_ID),

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts),

    ok = emqx_eviction_agent_channel:cast(Pid, unknown),
    Pid ! unknown,

    ?assertEqual(
       [],
       emqx_eviction_agent_channel:call(Pid, list_acl_cache)),

    ?assertEqual(
       ok,
       emqx_eviction_agent_channel:call(Pid, {quota, quota})),

    ?assertEqual(
       ignored,
       emqx_eviction_agent_channel:call(Pid, unknown)).

t_expire(_Config) ->
    erlang:process_flag(trap_exit, true),

    _ = emqtt_connect(?CLIENT_ID, false),
    #{conninfo := ConnInfo} = Opts0 = evict_session_opts(?CLIENT_ID),
    Opts1 = Opts0#{conninfo => ConnInfo#{expiry_interval => 1}},

    {ok, Pid} = emqx_eviction_agent_channel:start_supervised(Opts1),

    ct:sleep(1500),

    ?assertNot(is_process_alive(Pid)).

evict_session_opts(ClientId) ->
    maps:with(
      [conninfo, clientinfo],
      emqx_cm:get_chan_info(ClientId)).
