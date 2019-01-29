%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_sup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

all() -> [t_child_all].

start_link() ->
    Pid = spawn_link(?MODULE, echo, [0]),
    {ok, Pid}.

echo(State) ->
    receive
        {From, Req} ->
            ct:pal("======from:~p, req:~p", [From, Req]),
            From ! Req,
            echo(State)
    end.

t_child_all(_) ->
    {ok, Pid} = emqx_mod_sup:start_link(),
    {ok, Child} = emqx_mod_sup:start_child(?MODULE, worker),
    timer:sleep(10),
    Child ! {self(), hi},
    receive hi -> ok after 100 -> ct:fail({timeout, wait_echo}) end,
    ok = emqx_mod_sup:stop_child(?MODULE),
    exit(Pid, normal).
