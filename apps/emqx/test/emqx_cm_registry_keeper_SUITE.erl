%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cm_registry_keeper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_cm.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    AppConfig = "broker.session_history_retain = 2s",
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => AppConfig}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_cleanup_after_retain(_) ->
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ClientId = <<"clientid">>,
    ClientId2 = <<"clientid2">>,
    emqx_cm_registry:register_channel({ClientId, Pid}),
    emqx_cm_registry:register_channel({ClientId2, Pid}),
    ?assertEqual([Pid], emqx_cm_registry:lookup_channels(ClientId)),
    ?assertEqual([Pid], emqx_cm_registry:lookup_channels(ClientId2)),
    ?assertEqual(2, emqx_cm_registry_keeper:count(0)),
    T0 = erlang:system_time(seconds),
    exit(Pid, kill),
    %% lookup_channel chesk if the channel is still alive
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId)),
    ?assertEqual([], emqx_cm_registry:lookup_channels(ClientId2)),
    %% simulate a DOWN message which causes emqx_cm to call clean_down
    %% to clean the channels for real
    ok = emqx_cm:clean_down({Pid, ClientId}),
    ok = emqx_cm:clean_down({Pid, ClientId2}),
    ?assertEqual(2, emqx_cm_registry_keeper:count(T0)),
    ?assertEqual(2, emqx_cm_registry_keeper:count(0)),
    ?retry(_Interval = 1000, _Attempts = 4, begin
        ?assertEqual(0, emqx_cm_registry_keeper:count(T0)),
        ?assertEqual(0, emqx_cm_registry_keeper:count(0))
    end),
    ok.

%% count is cached when the number of entries is greater than 1000
t_count_cache(_) ->
    Pid = self(),
    ClientsCount = 999,
    ClientIds = lists:map(fun erlang:integer_to_binary/1, lists:seq(1, ClientsCount)),
    Channels = lists:map(fun(ClientId) -> {ClientId, Pid} end, ClientIds),
    lists:foreach(
        fun emqx_cm_registry:register_channel/1,
        Channels
    ),
    T0 = erlang:system_time(seconds),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(0)),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(T0)),
    %% insert another one to trigger the cache threshold
    emqx_cm_registry:register_channel({<<"-1">>, Pid}),
    ?assertEqual(ClientsCount + 1, emqx_cm_registry_keeper:count(0)),
    ?assertEqual(ClientsCount, emqx_cm_registry_keeper:count(T0)),
    mnesia:clear_table(?CHAN_REG_TAB),
    ok.

channel(Id, Pid) ->
    #channel{chid = Id, pid = Pid}.
