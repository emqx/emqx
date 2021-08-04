%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine_terminator).

-export([ start/0
        , graceful/0
        , terminator_loop/0
        ]).

-define(TERMINATOR, ?MODULE).

%% @doc This API is called to shutdown the Erlang VM by RPC call from remote shell node.
%% The shutown of apps is delegated to a to a process instead of doing it in the RPC spawned
%% process which has a remote group leader.
start() ->
    _ = spawn_link(
          fun() ->
                  register(?TERMINATOR, self()),
                  terminator_loop()
          end),
    ok.

%% internal use
terminator_loop() ->
    receive
        graceful_shutdown ->
            ok = emqx_machine_app:stop_apps(normal),
            exit_loop()
    after
        1000 ->
            %% keep looping for beam reload
            ?MODULE:terminator_loop()
    end.

%% @doc Shutdown the Erlang VM.
graceful() ->
    case whereis(?TERMINATOR) of
        undefined ->
            exit(emqx_machine_not_started);
        Pid ->
            Pid ! graceful_shutdown,
            Ref = monitor(process, Pid),
            %% NOTE: not exactly sure, but maybe there is a chance that
            %% Erlang VM goes down before this receive.
            %% In which case, the remote caller will get {badrpc, nodedown}
            receive {'DOWN', Ref, process, Pid, _} -> ok end
    end.

%% Loop until Erlang VM exits
exit_loop() ->
    init:stop(),
    timer:sleep(100),
    exit_loop().
