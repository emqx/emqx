%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(TOPK_TAB, emqx_slow_subs_topk).
-define(NOW, erlang:system_time(millisecond)).

-define(BASE_CONF, <<"""
emqx_slow_subs {
    enable = true
	top_k_num = 5,
    expire_interval = 3000
    notice_interval = 1500
    notice_qos = 0
    notice_batch_size = 3
}""">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_slow_subs_schema, ?BASE_CONF),
    emqx_common_test_helpers:start_apps([emqx_slow_subs]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_slow_subs]).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_log_and_pub(_) ->
    %% Sub topic first
    Subs = [{<<"/test1/+">>, ?QOS_1}, {<<"/test2/+">>, ?QOS_2}],
    Clients = start_client(Subs),
    emqx:subscribe("$SYS/brokers/+/slow_subs"),
    timer:sleep(1000),
    Now = ?NOW,
    %% publish

    lists:foreach(fun(I) ->
                          Topic = list_to_binary(io_lib:format("/test1/~p", [I])),
                          Msg = emqx_message:make(undefined, ?QOS_1, Topic, <<"Hello">>),
                          emqx:publish(Msg#message{timestamp = Now - 500})
                  end,
                  lists:seq(1, 10)),

    lists:foreach(fun(I) ->
                          Topic = list_to_binary(io_lib:format("/test2/~p", [I])),
                          Msg = emqx_message:make(undefined, ?QOS_2, Topic, <<"Hello">>),
                          emqx:publish(Msg#message{timestamp = Now - 500})
                  end,
                  lists:seq(1, 10)),

    timer:sleep(1000),
    Size = ets:info(?TOPK_TAB, size),
    %% some time record maybe delete due to it expired
    ?assert(Size =< 6 andalso Size >= 4),

    timer:sleep(1500),
    Recs = try_receive([]),
    RecSum = lists:sum(Recs),
    ?assert(RecSum >= 5),
    ?assert(lists:all(fun(E) -> E =< 3 end, Recs)),

    timer:sleep(3000),
    ?assert(ets:info(?TOPK_TAB, size) =:= 0),
    [Client ! stop || Client <- Clients],
    ok.

start_client(Subs) ->
    [spawn(fun() -> client(I, Subs) end) || I <- lists:seq(1, 10)].

client(I, Subs) ->
    {ok, C} = emqtt:start_link([{host,      "localhost"},
                                {clientid,  io_lib:format("slow_subs_~p", [I])},
                                {username,  <<"plain">>},
                                {password,  <<"plain">>}]),
    {ok, _} = emqtt:connect(C),

    Len = erlang:length(Subs),
    Sub = lists:nth(I rem Len + 1, Subs),
    _ = emqtt:subscribe(C, Sub),

    receive
        stop ->
            ok
    end.

try_receive(Acc) ->
    receive
        {deliver, _, #message{payload = Payload}} ->
            #{<<"logs">> := Logs} =  emqx_json:decode(Payload, [return_maps]),
            try_receive([length(Logs) | Acc])
    after 500 ->
            Acc
    end.
