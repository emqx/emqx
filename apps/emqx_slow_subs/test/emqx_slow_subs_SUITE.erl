%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-define(NOW, erlang:system_time(millisecond)).
-define(LANTENCY, 101).

-define(BASE_CONF, <<
    "slow_subs {\n"
    "    enable = true\n"
    "	 top_k_num = 5\n"
    "	 threshold = 100ms\n"
    "    expire_interval = 5m\n"
    "    stats_type = whole\n"
    "}"
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_slow_subs, ?BASE_CONF}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_pub(_) ->
    _ = [stats_with_type(Type) || Type <- [whole, internal, response]],
    ok.

t_expire(_) ->
    _ = update_config(<<"expire_interval">>, <<"1500ms">>),
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

start_client(Type, Subs) ->
    [spawn(fun() -> client(I, Type, Subs) end) || I <- lists:seq(1, 10)].

client(I, Type, Subs) ->
    ConnOptions = make_conn_options(Type, I),
    {ok, C} = emqtt:start_link(ConnOptions),
    {ok, _} = emqtt:connect(C),

    Len = erlang:length(Subs),
    Sub = lists:nth(I rem Len + 1, Subs),
    _ = emqtt:subscribe(C, Sub),

    receive
        stop ->
            ok
    end.

stats_with_type(Type) ->
    emqx_slow_subs:clear_history(),
    update_stats_type(Type),
    %% Sub topic first
    Subs = [{<<"/test1/+">>, ?QOS_1}, {<<"/test2/+">>, ?QOS_2}],
    Clients = start_client(Type, Subs),
    timer:sleep(1000),
    Now = ?NOW,
    %% publish

    lists:foreach(
        fun(I) ->
            Topic = list_to_binary(io_lib:format("/test1/~p", [I])),
            Msg = emqx_message:make(undefined, ?QOS_1, Topic, <<"Hello">>),
            emqx:publish(Msg#message{timestamp = Now - ?LANTENCY}),
            timer:sleep(100)
        end,
        lists:seq(1, 10)
    ),

    lists:foreach(
        fun(I) ->
            Topic = list_to_binary(io_lib:format("/test2/~p", [I])),
            Msg = emqx_message:make(undefined, ?QOS_2, Topic, <<"Hello">>),
            emqx:publish(Msg#message{timestamp = Now - ?LANTENCY}),
            timer:sleep(100)
        end,
        lists:seq(1, 10)
    ),

    timer:sleep(1000),
    Size = ets:info(?TOPK_TAB, size),
    ?assert(
        Size =< 10 andalso Size >= 3,
        lists:flatten(io_lib:format("with_type:~p, the size is :~p~n", [Type, Size]))
    ),

    ?assert(
        lists:all(
            fun(#{timespan := Ts}) ->
                Ts >= 101 andalso Ts < ?NOW - Now
            end,
            emqx_slow_subs_api:get_history()
        )
    ),

    [Client ! stop || Client <- Clients],
    ok.

update_stats_type(Type) ->
    update_config(<<"stats_type">>, erlang:atom_to_binary(Type)).

update_config(Key, Value) ->
    Raw = #{
        <<"enable">> => true,
        <<"expire_interval">> => <<"5m">>,
        <<"stats_type">> => <<"whole">>,
        <<"threshold">> => <<"100ms">>,
        <<"top_k_num">> => 5
    },
    emqx_slow_subs:update_settings(Raw#{Key => Value}).

make_conn_options(response, I) ->
    [
        {msg_handler, #{
            publish => fun(_) -> timer:sleep(?LANTENCY) end,
            disconnected => fun(_) -> ok end
        }}
        | make_conn_options(whole, I)
    ];
make_conn_options(_, I) ->
    [
        {host, "localhost"},
        {clientid, io_lib:format("slow_subs_~p", [I])},
        {username, <<"plain">>},
        {password, <<"plain">>}
    ].
