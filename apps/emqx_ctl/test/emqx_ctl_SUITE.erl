%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> [t_reg_unreg_command, t_run_commands, t_audit_redaction, t_print, t_usage, t_unexpected].

init_per_suite(Config) ->
    application:stop(emqx_ctl),
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
            ?assertEqual({?MODULE, cmd1_fun}, lookup_command(cmd1)),
            ?assertEqual({?MODULE, cmd2_fun}, lookup_command(cmd2)),
            ?assertEqual(
                [{cmd1, ?MODULE, cmd1_fun}, {cmd2, ?MODULE, cmd2_fun}],
                emqx_ctl:get_commands()
            ),
            emqx_ctl:unregister_command(cmd1),
            emqx_ctl:unregister_command(cmd2),
            ct:sleep(100),
            ?assertEqual({error, cmd_not_found}, lookup_command(cmd1)),
            ?assertEqual({error, cmd_not_found}, lookup_command(cmd2)),
            ?assertEqual([], emqx_ctl:get_commands())
        end
    ).

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
        end
    ).

t_audit_redaction(_) ->
    with_ctl_server(
        fun(_CtlSrv) ->
            emqx_ctl:register_command(audit, {?MODULE, audit_fun}),
            emqx_ctl:register_command(cmd3, {?MODULE, cmd3_fun}),
            emqx_ctl:register_command(cmd4, {?MODULE, cmd4_fun}),

            ?assertEqual(
                {error, cmd_not_found},
                emqx_ctl:run_command(unregistered_cmd, ["secret", "value"])
            ),
            ?assertEqual(undefined, get(audit_log)),
            ?assertEqual(
                {error, cmd_not_found},
                emqx_ctl:run_command(
                    ["unknown_cli_command_7f42d9", "secret", "value"]
                )
            ),
            ?assertEqual(undefined, get(audit_log)),

            ok = emqx_ctl:run_command(["cmd3", "secret", "value"]),
            ?assertEqual(["secret", "value"], get(command_args)),
            ?assertEqual(["secret", "value"], get(callback_args)),
            ?assertMatch(
                #{args := [<<"secret">>, <<"******">>]},
                get(audit_log)
            ),

            ok = emqx_ctl:run_command(["cmd4", "value"]),
            ?assertMatch(
                #{args := [<<"selected-audit-callback">>]},
                get(audit_log)
            ),

            emqx_ctl:register_command(missing_callback, {lists, reverse}),
            ["value", "plain"] = emqx_ctl:run_command(["missing_callback", "plain", "value"]),
            ?assertMatch(
                #{args := [<<"******">>, <<"******">>]},
                get(audit_log)
            ),

            ok = emqx_ctl:run_command(["cmd3", "crash", "value"]),
            ?assertEqual(["crash", "value"], get(command_args)),
            ?assertMatch(
                #{args := [<<"******">>, <<"******">>]},
                get(audit_log)
            ),

            ok = emqx_ctl:run_command(["cmd3", "invalid", "value"]),
            ?assertEqual(["invalid", "value"], get(command_args)),
            ?assertMatch(
                #{args := [<<"******">>, <<"******">>]},
                get(audit_log)
            ),

            EvalExprs = parse_exprs("node()."),
            {ok, _} = emqx_ctl:run_command(eval_erl, EvalExprs),
            ExpectedEval = unicode:characters_to_binary(
                erl_pp:exprs(EvalExprs, [{linewidth, 10000}])
            ),
            ?assertMatch(
                #{args := [ExpectedEval]},
                get(audit_log)
            ),

            erase(audit_log),
            {ok, _} = emqx_ctl:run_command(eval_erl, parse_exprs("emqx:is_running().")),
            ?assertEqual(undefined, get(audit_log))
        end
    ).

t_print(_) ->
    ok = emqx_ctl:print("help"),
    ok = emqx_ctl:print("~ts", [help]),
    ok = emqx_ctl:print("~ts", [<<"~!@#$%^&*()">>]),
    % - check the output of the usage
    mock_print(),
    ?assertEqual("help", emqx_ctl:print("help")),
    ?assertEqual("help", emqx_ctl:print("~ts", [help])),
    ?assertEqual("~!@#$%^&*()", emqx_ctl:print("~ts", [<<"~!@#$%^&*()">>])),
    unmock_print().

t_eval_erl(_) ->
    mock_print(),
    Expected = atom_to_list(node()) ++ "\n",
    ?assertEqual(Expected, emqx_ctl:run_command(["eval_erl", "node()"])),
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
        end
    ).

%%--------------------------------------------------------------------
%% Cmds for test
%%--------------------------------------------------------------------

cmd1_fun(["arg"]) -> ok;
cmd1_fun(["badarg"]) -> error(badarg).

cmd2_fun(["arg1", "arg2"]) -> ok;
cmd2_fun(["arg1", "badarg"]) -> error(badarg).

cmd3_fun(Args) ->
    put(command_args, Args),
    ok.

cmd3_fun_audit_args(["secret", _Value] = Args) ->
    put(callback_args, Args),
    ["secret", "******"];
cmd3_fun_audit_args(["crash" | _]) ->
    error({redaction_failed, <<"raw-secret">>});
cmd3_fun_audit_args(["invalid" | _]) ->
    invalid_return;
cmd3_fun_audit_args(Args) ->
    Args.

cmd4_fun(_Args) ->
    ok.

cmd4_fun_audit_args(_Args) ->
    ["selected-audit-callback"].

audit_fun(usage) ->
    ok.

audit_fun(_Level, _From, Log) ->
    put(audit_log, Log),
    ok.

parse_exprs(String) ->
    {ok, Scanned, _} = erl_scan:string(String),
    {ok, Exprs} = erl_parse:parse_exprs(Scanned),
    Exprs.

with_ctl_server(Fun) ->
    ok = emqx_ctl:stop(),
    {ok, Pid} = emqx_ctl:start_link(),
    try
        _ = Fun(Pid),
        ok
    after
        ok = emqx_ctl:stop()
    end.

mock_print() ->
    %% proxy usage/1,2 and print/1,2 to format_xx/1,2 funcs
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(CmdParams, CmdDescr) ->
        emqx_ctl:format_usage(CmdParams, CmdDescr)
    end).

unmock_print() ->
    meck:unload(emqx_ctl).

lookup_command(Cmd) ->
    case emqx_ctl:lookup_command(Cmd) of
        {ok, {Mod, Fun}} -> {Mod, Fun};
        Error -> Error
    end.
