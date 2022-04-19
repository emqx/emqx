%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-define(NOW, erlang:system_time(millisecond)).
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(BASE_CONF, <<
    ""
    "\n"
    "slow_subs {\n"
    "    enable = true\n"
    "	top_k_num = 5,\n"
    "    expire_interval = 5m\n"
    "    stats_type = whole\n"
    "    }"
    ""
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    ok = emqx_common_test_helpers:load_config(emqx_slow_subs_schema, ?BASE_CONF),
    emqx_common_test_helpers:start_apps([emqx_slow_subs]),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    emqx_common_test_helpers:stop_apps([emqx_slow_subs]).

init_per_testcase(t_expire, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    Cfg = emqx_config:get([slow_subs]),
    emqx_slow_subs:update_settings(Cfg#{expire_interval := 1500}),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _) ->
    case erlang:whereis(node()) of
        undefined ->
            ok;
        P ->
            erlang:unlink(P),
            erlang:exit(P, kill)
    end,
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_pub(_) ->
    %% Sub topic first
    Subs = [{<<"/test1/+">>, ?QOS_1}, {<<"/test2/+">>, ?QOS_2}],
    Clients = start_client(Subs),
    timer:sleep(1000),
    Now = ?NOW,
    %% publish

    lists:foreach(
        fun(I) ->
            Topic = list_to_binary(io_lib:format("/test1/~p", [I])),
            Msg = emqx_message:make(undefined, ?QOS_1, Topic, <<"Hello">>),
            emqx:publish(Msg#message{timestamp = Now - 500}),
            timer:sleep(100)
        end,
        lists:seq(1, 10)
    ),

    lists:foreach(
        fun(I) ->
            Topic = list_to_binary(io_lib:format("/test2/~p", [I])),
            Msg = emqx_message:make(undefined, ?QOS_2, Topic, <<"Hello">>),
            emqx:publish(Msg#message{timestamp = Now - 500}),
            timer:sleep(100)
        end,
        lists:seq(1, 10)
    ),

    timer:sleep(1000),
    Size = ets:info(?TOPK_TAB, size),
    ?assert(Size =< 10 andalso Size >= 3, io_lib:format("the size is :~p~n", [Size])),

    [Client ! stop || Client <- Clients],
    ok.

t_expire(_) ->
    Now = ?NOW,
    Each = fun(I) ->
        ClientId = erlang:list_to_binary(io_lib:format("test_~p", [I])),
        ets:insert(?TOPK_TAB, #top_k{
            index = ?TOPK_INDEX(1, ?ID(ClientId, <<"topic">>)),
            last_update_time = Now - timer:minutes(5)
        })
    end,

    lists:foreach(Each, lists:seq(1, 5)),

    timer:sleep(3000),
    Size = ets:info(?TOPK_TAB, size),
    ?assertEqual(0, Size),
    ok.

start_client(Subs) ->
    [spawn(fun() -> client(I, Subs) end) || I <- lists:seq(1, 10)].

client(I, Subs) ->
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {clientid, io_lib:format("slow_subs_~p", [I])},
        {username, <<"plain">>},
        {password, <<"plain">>}
    ]),
    {ok, _} = emqtt:connect(C),

    Len = erlang:length(Subs),
    Sub = lists:nth(I rem Len + 1, Subs),
    _ = emqtt:subscribe(C, Sub),

    receive
        stop ->
            ok
    end.
