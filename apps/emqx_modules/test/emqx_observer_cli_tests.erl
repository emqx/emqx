%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_observer_cli_tests).

-include_lib("eunit/include/eunit.hrl").

start_observer_cli_test() ->
    meck:new(observer_cli, [passthrough, no_history, no_link, no_passthrough_cover]),
    meck:expect(observer_cli, start, fun() -> ok end),
    try
        ok = emqx_observer_cli:cmd(["status"])
    after
        meck:unload(observer_cli)
    end.

bin_leak_test() ->
    ok = emqx_observer_cli:cmd(["bin_leak"]).

load_observer_cli_test() ->
    ok = emqx_observer_cli:cmd(["load", "lists"]).

unknown_command_test() ->
    meck_emqx_ctl(),
    try
        ok = emqx_observer_cli:cmd(dummy),
        receive
            {usage, [_ | _]} -> ok
        end
    after
        unmeck_emqx_ctl()
    end.

meck_emqx_ctl() ->
    Pid = self(),
    meck:new(emqx_ctl, [passthrough, no_history, no_link, no_passthrough_cover]),
    meck:expect(emqx_ctl, usage, fun(Tuples) ->
        Pid ! {usage, Tuples},
        ok
    end).

unmeck_emqx_ctl() ->
    meck:unload(emqx_ctl).
