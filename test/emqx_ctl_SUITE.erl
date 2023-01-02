%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ctl_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_logger:set_log_level(emergency),
    Config.

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_reg_unreg_command(_) ->
    with_ctl_server(
      fun(_CtlSrv) ->
            emqx_ctl:register_command(cmd1, {?MODULE, cmd1_fun}),
            emqx_ctl:register_command(cmd2, {?MODULE, cmd2_fun}),
            ?assertEqual([{?MODULE, cmd1_fun}], emqx_ctl:lookup_command(cmd1)),
            ?assertEqual([{?MODULE, cmd2_fun}], emqx_ctl:lookup_command(cmd2)),
            ?assertEqual([{cmd1, ?MODULE, cmd1_fun}, {cmd2, ?MODULE, cmd2_fun}],
                         emqx_ctl:get_commands()),
            emqx_ctl:unregister_command(cmd1),
            emqx_ctl:unregister_command(cmd2),
            ct:sleep(100),
            ?assertEqual([], emqx_ctl:lookup_command(cmd1)),
            ?assertEqual([], emqx_ctl:lookup_command(cmd2)),
            ?assertEqual([], emqx_ctl:get_commands())
      end).

t_run_commands(_) ->
    with_ctl_server(
      fun(_CtlSrv) ->
            ?assertEqual({error, cmd_not_found}, emqx_ctl:run_command(["cmd", "arg"])),
            emqx_ctl:register_command(cmd1, {?MODULE, cmd1_fun}),
            emqx_ctl:register_command(cmd2, {?MODULE, cmd2_fun}),
            ok = emqx_ctl:run_command(["cmd1", "arg"]),
            {error, badarg} = emqx_ctl:run_command(["cmd1", "badarg"]),
            ok = emqx_ctl:run_command(["cmd2", "arg1", "arg2"]),
            {error, badarg} = emqx_ctl:run_command(["cmd2", "arg1", "badarg"])
      end).

t_print(_) ->
    ok = emqx_ctl:print("help"),
    ok = emqx_ctl:print("~s", [help]),
    ok = emqx_ctl:print("~s", [<<"~!@#$%^&*()">>]),
    % - check the output of the usage
    mock_print(),
    ?assertEqual("help\n", emqx_ctl:print("help~n")),
    ?assertEqual("help", emqx_ctl:print("~s", [help])),
    ?assertEqual("~!@#$%^&*()", emqx_ctl:print("~s", [<<"~!@#$%^&*()">>])),
    unmock_print().

t_usage(_) ->
    CmdParams1 = "emqx_cmd_1 param1 param2",
    CmdDescr1 = "emqx_cmd_1 is a test command means nothing",
    % - usage/1,2 should return ok
    ok = emqx_ctl:usage([{CmdParams1, CmdDescr1}, {CmdParams1, CmdDescr1}]),
    ok = emqx_ctl:usage(CmdParams1, CmdDescr1).

t_unexpected(_) ->
    with_ctl_server(
      fun(CtlSrv) ->
              ignored = gen_server:call(CtlSrv, unexpected_call),
              ok = gen_server:cast(CtlSrv, unexpected_cast),
              CtlSrv ! unexpected_info,
              ?assert(is_process_alive(CtlSrv))
      end).

%%--------------------------------------------------------------------
%% Cmds for test
%%--------------------------------------------------------------------

cmd1_fun(["arg"]) -> ok;
cmd1_fun(["badarg"]) -> error(badarg).

cmd2_fun(["arg1", "arg2"]) -> ok;
cmd2_fun(["arg1", "badarg"]) -> error(badarg).

with_ctl_server(Fun) ->
    {ok, Pid} = emqx_ctl:start_link(),
    _ = Fun(Pid),
    ok = emqx_ctl:stop().

mock_print() ->
    %% proxy usage/1,2 and print/1,2 to format_xx/1,2 funcs
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(CmdParams, CmdDescr) ->
                                         emqx_ctl:format_usage(CmdParams, CmdDescr) end).

unmock_print() ->
    meck:unload(emqx_ctl).
