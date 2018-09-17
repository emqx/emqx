%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pool_SUITE).

-compile(export_all).

-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() -> [
          {group, submit_case},
          {group, async_submit_case}
         ].

groups() ->
    [
     {submit_case, [sequence], [submit_mfa, submit_fa]},
     {async_submit_case, [sequence], [async_submit_mfa]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(gproc),
    Config.

end_per_suite(_Config) ->
    ok.

submit_mfa(_Config) ->
    erlang:process_flag(trap_exit, true),
    {ok, Pid} =  emqx_pool:start_link(),
    Result = emqx_pool:submit({?MODULE, test_mfa, []}),
    ?assertEqual(15, Result),
    gen_server:stop(Pid, normal, 3000),
    ok.

submit_fa(_Config) ->
    {ok, Pid} =  emqx_pool:start_link(),
    Fun = fun(X) -> case X rem 2 of 0 -> {true, X div 2}; _ -> false end end,
    Result = emqx_pool:submit(Fun, [2]),
    ?assertEqual({true, 1}, Result),
    exit(Pid, normal).

test_mfa() ->
    lists:foldl(fun(X, Sum) -> X + Sum end, 0, [1,2,3,4,5]).

async_submit_mfa(_Config) ->
    {ok, Pid} =  emqx_pool:start_link(),
    emqx_pool:async_submit({?MODULE, test_mfa, []}),
    exit(Pid, normal).

