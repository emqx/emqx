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

-module(emqx_st_statistics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("include/emqx.hrl").

%-define(LOGT(Format, Args), ct:pal(Format, Args)).

-define(LOG_TAB, emqx_st_statistics_log).
-define(TOPK_TAB, emqx_st_statistics_topk).
-define(NOW, erlang:system_time(millisecond)).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx]),
    Config.

init_per_testcase(_, Config) ->
    emqx_mod_st_statistics:load(base_conf()),
    Config.

end_per_testcase(_, _) ->
    emqx_mod_st_statistics:unload(undefined),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_log_and_pub(_) ->
    %% Sub topic first
    SubBase = "/test",
    emqx:subscribe("$slow_topics"),
    Clients = start_client(SubBase),
    timer:sleep(1000),
    Now = ?NOW,
    %% publish
    ?assert(ets:info(?LOG_TAB, size) =:= 0),
    lists:foreach(fun(I) ->
                          Topic = list_to_binary(io_lib:format("~s~p", [SubBase, I])),
                          Msg = emqx_message:make(Topic, <<"Hello">>),
                          emqx:publish(Msg#message{timestamp = Now - 1000})
                  end,
                  lists:seq(1, 10)),

    timer:sleep(100),

    case ets:info(?LOG_TAB, size) of
        5 ->
            ok;
        _ ->
            ?assert(ets:info(?TOPK_TAB, size) =/= 0)
    end,

    timer:sleep(2400),

    ?assert(ets:info(?LOG_TAB, size) =:= 0),
    ?assert(ets:info(?TOPK_TAB, size) =:= 3),
    try_receive(3),
    try_receive(2),
    [Client ! stop || Client <- Clients],
    ok.

base_conf() ->
    [{top_k_num, 3},
     {threshold_time, 10},
     {notice_qos, 0},
     {notice_batch_size, 3},
     {notice_topic,"$slow_topics"},
     {time_window, 2000},
     {max_log_num, 5}].

start_client(Base) ->
    [spawn(fun() ->
                   Topic = list_to_binary(io_lib:format("~s~p", [Base, I])),
                   client(Topic)
           end)
     || I <- lists:seq(1, 10)].

client(Topic) ->
    {ok, C} = emqtt:start_link([{host,      "localhost"},
                                {clientid,  Topic},
                                {username,  <<"plain">>},
                                {password,  <<"plain">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, Topic),
    receive
        stop ->
            ok
    end.

try_receive(L) ->
    receive
        {deliver, _, #message{payload = Payload}} ->
            #{<<"logs">> := Logs} =  emqx_json:decode(Payload, [return_maps]),
            ?assertEqual(length(Logs), L)
    after 500 ->
            ?assert(false)
    end.
