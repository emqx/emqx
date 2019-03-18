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

-module(emqx_misc_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SOCKOPTS, [binary,
                   {packet,    raw},
                   {reuseaddr, true},
                   {backlog,   512},
                   {nodelay,   true}]).


t_merge_opts_test() ->
    Opts = emqx_misc:merge_opts(?SOCKOPTS, [raw,
                                            binary,
                                            {backlog, 1024},
                                            {nodelay, false},
                                            {max_clients, 1024},
                                            {acceptors, 16}]),
    ?assertEqual(1024, proplists:get_value(backlog, Opts)),
    ?assertEqual(1024, proplists:get_value(max_clients, Opts)),
    [binary, raw,
     {acceptors, 16},
     {backlog, 1024},
     {max_clients, 1024},
     {nodelay, false},
     {packet, raw},
     {reuseaddr, true}] = lists:sort(Opts).

timer_cancel_flush_test() ->
    Timer = emqx_misc:start_timer(0, foo),
    ok = emqx_misc:cancel_timer(Timer),
    receive {timeout, Timer, foo} -> error(unexpected)
    after 0 -> ok
    end.

shutdown_disabled_test() ->
    ok = drain(),
    self() ! foo,
    ?assertEqual(continue, conn_proc_mng_policy(0)),
    receive foo -> ok end,
    ?assertEqual(hibernate, conn_proc_mng_policy(0)).

message_queue_too_long_test() ->
    ok = drain(),
    self() ! foo,
    self() ! bar,
    ?assertEqual({shutdown, message_queue_too_long},
                 conn_proc_mng_policy(1)),
    receive foo -> ok end,
    ?assertEqual(continue, conn_proc_mng_policy(1)),
    receive bar -> ok end.

conn_proc_mng_policy(L) ->
    emqx_misc:conn_proc_mng_policy(#{message_queue_len => L}).

%% drain self() msg queue for deterministic test behavior
drain() ->
    _ = drain([]), % maybe log
    ok.

drain(Acc) ->
    receive
        Msg ->
            drain([Msg | Acc])
    after
        0 ->
            lists:reverse(Acc)
    end.

