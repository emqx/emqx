%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(gen_server).

-export([
    start_link/0,
    graceful/0,
    graceful_wait/0,
    is_running/0
]).

-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2
]).

-include_lib("emqx/include/logger.hrl").

-define(TERMINATOR, ?MODULE).
-define(DO_IT, graceful_shutdown).

%% @doc This API is called to shutdown the Erlang VM by RPC call from remote shell node.
%% The shutdown of apps is delegated to a to a process instead of doing it in the RPC spawned
%% process which has a remote group leader.
start_link() ->
    {ok, _} = gen_server:start_link({local, ?TERMINATOR}, ?MODULE, [], []).

is_running() -> is_pid(whereis(?TERMINATOR)).

%% @doc Call `emqx_machine_terminator' to stop applications
%% then call init:stop() stop beam.
graceful() ->
    try
        _ = gen_server:call(?TERMINATOR, ?DO_IT, infinity)
    catch
        _:_ ->
            %% failed to notify terminator, probably due to not started yet
            %% or node is going down, either case, the caller
            %% should issue a shutdown to be sure
            %% NOTE: not exit_loop here because we do not want to
            %% block erl_signal_server
            ?ELOG("Shutdown before node is ready?~n", []),
            init:stop()
    end,
    ok.

%% @doc Shutdown the Erlang VM and wait indefinitely.
graceful_wait() ->
    ?AUDIT(alert, #{
        cmd => emqx,
        args => [<<"stop">>],
        version => emqx_release:version(),
        from => cli,
        duration_ms => element(1, erlang:statistics(wall_clock))
    }),
    ok = graceful(),
    exit_loop().

exit_loop() ->
    timer:sleep(100),
    init:stop(),
    exit_loop().

init(_) ->
    ok = emqx_machine_signal_handler:start(),
    {ok, #{}}.

handle_info(_, State) ->
    {noreply, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_call(?DO_IT, _From, State) ->
    try
        %% stop port apps before stopping other apps.
        emqx_machine_boot:stop_port_apps(),
        emqx_machine_boot:stop_apps()
    catch
        C:E:St ->
            Apps = [element(1, A) || A <- application:which_applications()],
            ?SLOG(error, #{
                msg => "failed_to_stop_apps",
                exception => C,
                reason => E,
                stacktrace => St,
                remaining_apps => Apps
            })
    after
        init:stop()
    end,
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {noreply, State}.

terminate(_Args, _State) ->
    ok.
