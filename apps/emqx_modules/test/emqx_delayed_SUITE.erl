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

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_modules]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_modules, emqx_conf]).

init_per_testcase(t_load_case, Config) ->
    Config;
init_per_testcase(_Case, Config) ->
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:enable(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_delayed:disable().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_case(_) ->
    Hooks = emqx_hooks:lookup('message.publish'),
    MFA = {emqx_delayed, on_message_publish, []},
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),
    ok = emqx_delayed:enable(),
    Hooks1 = emqx_hooks:lookup('message.publish'),
    ?assertNotEqual(false, lists:keyfind(MFA, 2, Hooks1)),
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
    ct:sleep(2000),

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
    emqx_delayed:set_max_delayed_messages(1),

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
