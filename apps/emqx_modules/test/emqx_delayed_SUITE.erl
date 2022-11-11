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

-module(emqx_delayed_SUITE).

-import(emqx_delayed, [on_message_publish/1]).

-compile(export_all).
-compile(nowarn_export_all).

-record(delayed_message, {key, delayed, msg}).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
-define(BASE_CONF, #{
    <<"dealyed">> => <<"true">>,
    <<"max_delayed_messages">> => <<"0">>
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_modules]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_modules, emqx_conf]).

init_per_testcase(t_load_case, Config) ->
    Config;
init_per_testcase(_Case, Config) ->
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:load(),
    Config.

end_per_testcase(_Case, _Config) ->
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:unload().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_enable_disable_case(_) ->
    emqx_delayed:unload(),
    timer:sleep(100),
    Hooks = emqx_hooks:lookup('message.publish'),
    MFA = {emqx_delayed, on_message_publish, []},
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),

    ok = emqx_delayed:load(),
    Hooks1 = emqx_hooks:lookup('message.publish'),
    ?assertNotEqual(false, lists:keyfind(MFA, 2, Hooks1)),

    Ts0 = integer_to_binary(erlang:system_time(second) + 10),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),
    ?assertMatch(#{data := Datas} when Datas =/= [], emqx_delayed:list(#{})),

    emqx_delayed:unload(),
    timer:sleep(100),
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),
    ?assertMatch(#{data := []}, emqx_delayed:list(#{})),
    ok.

t_delayed_message(_) ->
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed_m">>),
    ?assertEqual(
        {stop, DelayedMsg#message{topic = <<"publish">>, headers = #{allow_publish => false}}},
        on_message_publish(DelayedMsg)
    ),

    Msg = emqx_message:make(?MODULE, 1, <<"no_delayed_msg">>, <<"no_delayed">>),
    ?assertEqual({ok, Msg}, on_message_publish(Msg)),

    [#delayed_message{msg = #message{payload = Payload}}] = ets:tab2list(emqx_delayed),
    ?assertEqual(<<"delayed_m">>, Payload),
    ct:sleep(2500),

    EmptyKey = mnesia:dirty_all_keys(emqx_delayed),
    ?assertEqual([], EmptyKey).

t_delayed_message_abs_time(_) ->
    Ts0 = integer_to_binary(erlang:system_time(second) + 1),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),

    ?assertMatch(
        [#delayed_message{msg = #message{payload = <<"delayed_abs">>}}],
        ets:tab2list(emqx_delayed)
    ),

    ct:sleep(2000),

    ?assertMatch(
        [],
        ets:tab2list(emqx_delayed)
    ),

    Ts1 = integer_to_binary(erlang:system_time(second) + 10000000),
    DelayedMsg1 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts1/binary, "/publish">>, <<"delayed_abs">>
    ),

    ?assertError(
        invalid_delayed_timestamp,
        on_message_publish(DelayedMsg1)
    ).

t_list(_) ->
    Ts0 = integer_to_binary(erlang:system_time(second) + 1),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),

    ?assertMatch(
        #{data := [#{topic := <<"publish">>}]},
        emqx_delayed:list(#{})
    ).

t_max(_) ->
    emqx:update_config([delayed, max_delayed_messages], 1),

    DelayedMsg0 = emqx_message:make(?MODULE, 1, <<"$delayed/10/t0">>, <<"delayed0">>),
    DelayedMsg1 = emqx_message:make(?MODULE, 1, <<"$delayed/10/t1">>, <<"delayed1">>),
    _ = on_message_publish(DelayedMsg0),
    _ = on_message_publish(DelayedMsg1),

    ?assertMatch(
        #{data := [#{topic := <<"t0">>}]},
        emqx_delayed:list(#{})
    ).

t_cluster(_) ->
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed">>),
    Id = emqx_message:id(DelayedMsg),
    _ = on_message_publish(DelayedMsg),

    ?assertMatch(
        {ok, _},
        emqx_delayed_proto_v1:get_delayed_message(node(), Id)
    ),

    ?assertEqual(
        emqx_delayed:get_delayed_message(Id),
        emqx_delayed_proto_v1:get_delayed_message(node(), Id)
    ),

    ok = emqx_delayed_proto_v1:delete_delayed_message(node(), Id),

    ?assertMatch(
        {error, _},
        emqx_delayed:get_delayed_message(Id)
    ).

t_unknown_messages(_) ->
    OldPid = whereis(emqx_delayed),
    OldPid ! unknown,
    ok = gen_server:cast(OldPid, unknown),
    ?assertEqual(
        ignored,
        gen_server:call(OldPid, unknown)
    ).

t_get_basic_usage_info(_Config) ->
    emqx:update_config([delayed, max_delayed_messages], 10000),
    ?assertEqual(#{delayed_message_count => 0}, emqx_delayed:get_basic_usage_info()),
    lists:foreach(
        fun(N) ->
            Num = integer_to_binary(N),
            Message = emqx_message:make(<<"$delayed/", Num/binary, "/delayed">>, <<"payload">>),
            {stop, _} = emqx_delayed:on_message_publish(Message)
        end,
        lists:seq(1, 4)
    ),
    ?assertEqual(#{delayed_message_count => 4}, emqx_delayed:get_basic_usage_info()),
    ok.

t_delayed_precision(_) ->
    MaxSpan = 1250,
    FutureDiff = subscribe_proc(),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/1/delayed/test">>, <<"delayed/test">>
    ),
    _ = on_message_publish(DelayedMsg0),
    ?assert(FutureDiff() =< MaxSpan).

t_banned_delayed(_) ->
    emqx:update_config([delayed, max_delayed_messages], 10000),
    ClientId1 = <<"bc1">>,
    ClientId2 = <<"bc2">>,

    Now = erlang:system_time(second),
    Who = {clientid, ClientId2},
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    snabbkaffe:start_trace(),
    lists:foreach(
        fun(ClientId) ->
            Msg = emqx_message:make(ClientId, <<"$delayed/1/bc">>, <<"payload">>),
            emqx_delayed:on_message_publish(Msg)
        end,
        [ClientId1, ClientId1, ClientId1, ClientId2, ClientId2]
    ),

    timer:sleep(2000),
    Trace = snabbkaffe:collect_trace(),
    snabbkaffe:stop(),
    emqx_banned:delete(Who),
    mnesia:clear_table(emqx_delayed),

    ?assertEqual(2, length(?of_kind(ignore_delayed_message_publish, Trace))).

subscribe_proc() ->
    Self = self(),
    Ref = erlang:make_ref(),
    erlang:spawn(fun() ->
        Topic = <<"delayed/+">>,
        emqx_broker:subscribe(Topic),
        Self !
            {Ref,
                receive
                    {deliver, Topic, Msg} ->
                        erlang:system_time(milli_seconds) - Msg#message.timestamp
                after 2000 ->
                    2000
                end},
        emqx_broker:unsubscribe(Topic)
    end),
    fun() ->
        receive
            {Ref, Diff} ->
                Diff
        after 2000 ->
            2000
        end
    end.
