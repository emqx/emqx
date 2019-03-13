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

-module(emqx_ct_helpers).

-export([ensure_mnesia_stopped/0, wait_for/4]).

ensure_mnesia_stopped() ->
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema().

%% Help function to wait for Fun to yield 'true'.
wait_for(Fn, Ln, F, Timeout) ->
    {Pid, Mref} = erlang:spawn_monitor(fun() -> wait_loop(F, catch_call(F)) end),
    wait_for_down(Fn, Ln, Timeout, Pid, Mref, false).

wait_for_down(Fn, Ln, Timeout, Pid, Mref, Kill) ->
    receive
        {'DOWN', Mref, process, Pid, normal} ->
            ok;
        {'DOWN', Mref, process, Pid, {unexpected, Result}} ->
            erlang:error({unexpected, Fn, Ln, Result});
        {'DOWN', Mref, process, Pid, {crashed, {C, E, S}}} ->
            erlang:raise(C, {Fn, Ln, E}, S)
    after
        Timeout ->
            case Kill of
                true ->
                    erlang:demonitor(Mref, [flush]),
                    erlang:exit(Pid, kill),
                    erlang:error({Fn, Ln, timeout});
                false ->
                    Pid ! stop,
                    wait_for_down(Fn, Ln, Timeout, Pid, Mref, true)
            end
    end.

wait_loop(_F, ok) -> exit(normal);
wait_loop(F, LastRes) ->
    receive
        stop -> erlang:exit(LastRes)
    after
        100 ->
            Res = catch_call(F),
            wait_loop(F, Res)
    end.

catch_call(F) ->
    try
        case F() of
            true -> ok;
            Other -> {unexpected, Other}
        end
    catch
        C : E : S ->
            {crashed, {C, E, S}}
    end.
