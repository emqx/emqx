%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
