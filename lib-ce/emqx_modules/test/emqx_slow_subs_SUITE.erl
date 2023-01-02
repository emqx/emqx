%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("include/emqx_mqtt.hrl").
-include_lib("include/emqx.hrl").
-define(LANTENCY, 101).

%-define(LOGT(Format, Args), ct:pal(Format, Args)).

-define(TOPK_TAB, emqx_slow_subs_topk).
-define(NOW, erlang:system_time(millisecond)).

all() ->
    [ {group, whole}
    , {group, internal}
    , {group, response}
    ].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [ {whole, [], Cases}
    , {internal, [], Cases}
    , {response, [], Cases}
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx]),
    Config.

init_per_testcase(_, Config) ->
    Group = proplists:get_value(name, proplists:get_value(tc_group_properties, Config)),
    emqx_mod_slow_subs:load(base_conf(Group)),
    Config.

end_per_testcase(_, _) ->
    emqx_mod_slow_subs:unload([]),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_log_and_pub(Config) ->
    %% Sub topic first
    Subs = [{<<"/test1/+">>, ?QOS_1}, {<<"/test2/+">>, ?QOS_2}],
    Clients = start_client(Subs, Config),
    timer:sleep(1500),
    Now = ?NOW,

    %% publish
    lists:foreach(fun(I) ->
                          Topic = list_to_binary(io_lib:format("/test1/~p", [I])),
                          Msg = emqx_message:make(undefined, ?QOS_1, Topic, <<"Hello">>),
                          emqx:publish(Msg#message{timestamp = Now - ?LANTENCY})
                  end,
                  lists:seq(1, 10)),

    lists:foreach(fun(I) ->
                          Topic = list_to_binary(io_lib:format("/test2/~p", [I])),
                          Msg = emqx_message:make(undefined, ?QOS_2, Topic, <<"Hello">>),
                          emqx:publish(Msg#message{timestamp = Now - ?LANTENCY})
                  end,
                  lists:seq(1, 10)),

    timer:sleep(2000),
    Size = ets:info(?TOPK_TAB, size),
    %% some time record maybe delete due to it expired or the ets size exceeds 5 due to race conditions
    ?assert(Size =< 8 andalso Size >= 3,
            unicode:characters_to_binary(io_lib:format("size is :~p~n", [Size]))),

    ?assert(
        lists:all(
            fun(#{timespan := Ts}) ->
                Ts >= 101 andalso Ts < ?NOW - Now
            end,
            emqx_slow_subs_api:get_history()
        )
    ),

    timer:sleep(3000),
    ?assert(ets:info(?TOPK_TAB, size) =:= 0),
    [Client ! stop || Client <- Clients],
    ok.
base_conf(Type) ->
    [ {threshold, 100}
    , {top_k_num, 5}
    , {expire_interval, timer:seconds(3)}
    , {stats_type, Type}
    ].

start_client(Subs, Config) ->
    [spawn(fun() -> client(I, Subs, Config) end) || I <- lists:seq(1, 10)].

client(I, Subs, Config) ->
    Group = proplists:get_value(name, proplists:get_value(tc_group_properties, Config)),
    ConnOptions = make_conn_options(Group, I),
    {ok, C} = emqtt:start_link(ConnOptions),
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

make_conn_options(response, I) ->
    [ {msg_handler,
       #{publish => fun(_) -> timer:sleep(50) end,
         disconnected => fun(_) -> ok end}}
    | make_conn_options(whole, I)];
make_conn_options(_, I) ->
    [{host,      "localhost"},
     {clientid,  io_lib:format("slow_subs_~p", [I])},
     {username,  <<"plain">>},
     {password,  <<"plain">>}].
