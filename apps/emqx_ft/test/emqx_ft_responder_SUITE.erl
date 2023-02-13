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

-module(emqx_ft_responder_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_ft]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft]),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_register_unregister(_Config) ->
    Key = <<"test">>,
    DefaultAction = fun(_) -> ok end,
    ?assertEqual(
        ok,
        emqx_ft_responder:register(Key, DefaultAction, 1000)
    ),
    ?assertEqual(
        {error, already_registered},
        emqx_ft_responder:register(Key, DefaultAction, 1000)
    ),
    ?assertEqual(
        ok,
        emqx_ft_responder:unregister(Key)
    ),
    ?assertEqual(
        {error, not_found},
        emqx_ft_responder:unregister(Key)
    ).

t_timeout(_Config) ->
    Key = <<"test">>,
    Self = self(),
    DefaultAction = fun(K) -> Self ! {timeout, K} end,
    ok = emqx_ft_responder:register(Key, DefaultAction, 20),
    receive
        {timeout, Key} ->
            ok
    after 100 ->
        ct:fail("emqx_ft_responder not called")
    end,
    ?assertEqual(
        {error, not_found},
        emqx_ft_responder:unregister(Key)
    ).

t_action_exception(_Config) ->
    Key = <<"test">>,
    DefaultAction = fun(K) -> error({oops, K}) end,

    ?assertWaitEvent(
        emqx_ft_responder:register(Key, DefaultAction, 10),
        #{?snk_kind := ft_timeout_action_applied, key := <<"test">>},
        1000
    ),
    ?assertEqual(
        {error, not_found},
        emqx_ft_responder:unregister(Key)
    ).

t_unknown_msgs(_Config) ->
    Pid = whereis(emqx_ft_responder),
    Pid ! {unknown_msg, <<"test">>},
    ok = gen_server:cast(Pid, {unknown_msg, <<"test">>}),
    ?assertEqual(
        {error, unknown_call},
        gen_server:call(Pid, {unknown_call, <<"test">>})
    ).
