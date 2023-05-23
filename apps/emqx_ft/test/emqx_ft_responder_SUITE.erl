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

-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_ft], emqx_ft_test_helpers:env_handler(Config)),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft]),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_start_ack(_Config) ->
    Key = <<"test">>,
    DefaultAction = fun({ack, Ref}) -> Ref end,
    ?assertMatch(
        {ok, _Pid},
        emqx_ft_responder:start(Key, DefaultAction, 1000)
    ),
    ?assertMatch(
        {error, {already_started, _Pid}},
        emqx_ft_responder:start(Key, DefaultAction, 1000)
    ),
    Ref = make_ref(),
    ?assertEqual(
        Ref,
        emqx_ft_responder:ack(Key, Ref)
    ),
    ?assertExit(
        {noproc, _},
        emqx_ft_responder:ack(Key, Ref)
    ).

t_timeout(_Config) ->
    Key = <<"test">>,
    Self = self(),
    DefaultAction = fun(timeout) -> Self ! {timeout, Key} end,
    {ok, _Pid} = emqx_ft_responder:start(Key, DefaultAction, 20),
    receive
        {timeout, Key} ->
            ok
    after 100 ->
        ct:fail("emqx_ft_responder not called")
    end,
    ?assertExit(
        {noproc, _},
        emqx_ft_responder:ack(Key, oops)
    ).

t_unknown_msgs(_Config) ->
    {ok, Pid} = emqx_ft_responder:start(make_ref(), fun(_) -> ok end, 100),
    Pid ! {unknown_msg, <<"test">>},
    ok = gen_server:cast(Pid, {unknown_msg, <<"test">>}),
    ?assertEqual(
        {error, unknown_call},
        gen_server:call(Pid, {unknown_call, <<"test">>})
    ).
