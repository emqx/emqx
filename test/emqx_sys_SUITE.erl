%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(mock_modules,
        [ emqx_metrics
        , emqx_stats
        , emqx_broker
        , ekka_mnesia
        ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx),
    ok = application:set_env(emqx, broker_sys_interval, 1),
    ok = application:set_env(emqx, broker_sys_heartbeat, 1),
    ok = emqx_logger:set_log_level(emergency),
    Config.

end_per_suite(_Config) ->
    application:unload(emqx),
    ok = emqx_logger:set_log_level(error),
    ok.
    
% t_version(_) ->
%     error('TODO').

% t_sysdescr(_) ->
%     error('TODO').

t_uptime(_) ->
    ?assertEqual(<<"1 seconds">>, iolist_to_binary(emqx_sys:uptime(seconds, 1))),
    ?assertEqual(<<"1 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(seconds, 60))),
    ?assertEqual(<<"1 hours, 0 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(seconds, 3600))),
    ?assertEqual(<<"1 days, 0 hours, 0 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(seconds, 86400))).

% t_datetime(_) ->
%     error('TODO').

% t_sys_interval(_) ->
%     error('TODO').

% t_sys_heatbeat_interval(_) ->
%     error('TODO').

% t_info(_) ->
%     error('TODO').

t_prop_sys(_) ->
    Opts = [{numtests, 100}, {to_file, user}],
    ok = load(?mock_modules),
    ?assert(proper:quickcheck(prop_sys(), Opts)),
    ok = unload(?mock_modules).

prop_sys() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {ok, _Pid} = emqx_sys:start_link(),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            ok = emqx_sys:stop(),
            ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                [History,State,Result]),
                      aggregate(command_names(Cmds), true))
        end).

load(Modules) ->
    [mock(Module) || Module <- Modules],
    ok.

unload(Modules) ->
    lists:foreach(fun(Module) ->
                          ok = meck:unload(Module)
                  end, Modules).

mock(Module) ->
    ok = meck:new(Module, [passthrough, no_history]),
    do_mock(Module).

do_mock(emqx_broker) ->
    meck:expect(emqx_broker, publish,
                fun(Msg) -> {node(), <<"test">>, Msg} end),
    meck:expect(emqx_broker, safe_publish,
                fun(Msg) -> {node(), <<"test">>, Msg} end);
do_mock(emqx_stats) ->
    meck:expect(emqx_stats, getstats, fun() -> [0] end);
do_mock(ekka_mnesia) ->
    meck:expect(ekka_mnesia, running_nodes, fun() -> [node()] end);
do_mock(emqx_metrics) ->
    meck:expect(emqx_metrics, all, fun() -> [{hello, 3}] end).

unmock() ->
    meck:unload(emqx_broker).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
    #{}.

%% @doc List of possible commands to run against the system
command(_State) ->
    oneof([{call, emqx_sys, info, []},
           {call, emqx_sys, version, []},
           {call, emqx_sys, uptime, []},
           {call, emqx_sys, datetime, []},
           {call, emqx_sys, sysdescr, []},
           {call, emqx_sys, sys_interval, []},
           {call, emqx_sys, sys_heatbeat_interval, []},
           %------------ unexpected message ----------------------%
           {call, emqx_sys, handle_call, [emqx_sys, other, state]},
           {call, emqx_sys, handle_cast, [emqx_sys, other]},
           {call, emqx_sys, handle_info, [info, state]}
          ]).

precondition(_State, {call, _Mod, _Fun, _Args}) ->
    timer:sleep(1),
    true.

postcondition(_State, {call, emqx_sys, info, []}, Info) ->
    is_list(Info) andalso length(Info) =:= 4;
postcondition(_State, {call, emqx_sys, version, []}, Version) ->
    is_list(Version);
postcondition(_State, {call, emqx_sys, uptime, []}, Uptime) ->
    is_list(Uptime);
postcondition(_State, {call, emqx_sys, datetime, []}, Datetime) ->
    is_list(Datetime);
postcondition(_State, {call, emqx_sys, sysdescr, []}, Sysdescr) ->
    is_list(Sysdescr);
postcondition(_State, {call, emqx_sys, sys_interval, []}, SysInterval) ->
    is_integer(SysInterval) andalso SysInterval > 0;
postcondition(_State, {call, emqx_sys, sys_heartbeat_interval, []}, SysHeartInterval) ->
    is_integer(SysHeartInterval) andalso SysHeartInterval > 0;
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    NewState = State,
    NewState.
