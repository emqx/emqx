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

-module(emqx_misc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SOCKOPTS, [binary,
                   {packet,    raw},
                   {reuseaddr, true},
                   {backlog,   512},
                   {nodelay,   true}]).

all() -> emqx_ct:all(?MODULE).

t_merge_opts() ->
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

t_timer_cancel_flush() ->
    Timer = emqx_misc:start_timer(0, foo),
    ok = emqx_misc:cancel_timer(Timer),
    receive
        {timeout, Timer, foo} ->
            error(unexpected)
    after 0 -> ok
    end.

t_proc_name(_) ->
    'TODO'.

t_proc_stats(_) ->
    'TODO'.

t_drain_deliver(_) ->
    'TODO'.

t_drain_down(_) ->
    'TODO'.

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

