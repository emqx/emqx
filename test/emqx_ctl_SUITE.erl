%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_ctl:print("help").

t_usage(_) ->
    emqx_ctl:usage([{cmd1, "Cmd1 usage"}, {cmd2, "Cmd2 usage"}]),
    emqx_ctl:usage(cmd1, "Cmd1 usage"),
    emqx_ctl:usage(cmd2, "Cmd2 usage").

t_format(_) ->
    emqx_ctl:format("help"),
    emqx_ctl:format("~s", [help]).

t_format_usage(_) ->
    emqx_ctl:format_usage(cmd1, "Cmd1 usage"),
    emqx_ctl:format_usage([{cmd1, "Cmd1 usage"}, {cmd2, "Cmd2 usage"}]).

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

