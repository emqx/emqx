%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_vm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_load(_Config) ->
    lists:foreach(
        fun({Avg, LoadKey, Int}) ->
            emqx_common_test_helpers:with_mock(
                cpu_sup,
                Avg,
                fun() -> Int end,
                fun() ->
                    Load = proplists:get_value(LoadKey, emqx_vm:loads()),
                    ?assertEqual(Int / 256, Load)
                end
            )
        end,
        [{avg1, load1, 0}, {avg5, load5, 128}, {avg15, load15, 256}]
    ),
    ?assertMatch(
        [{load1, _}, {load5, _}, {load15, _}],
        emqx_vm:loads()
    ).

t_systeminfo(_Config) ->
    ?assertEqual(
        emqx_vm:system_info_keys(),
        [Key || {Key, _} <- emqx_vm:get_system_info()]
    ),
    ?assertEqual(undefined, emqx_vm:get_system_info(undefined)).

t_process_info(_Config) ->
    ProcessInfo = emqx_vm:get_process_info(),
    ?assertEqual(emqx_vm:process_info_keys(), [K || {K, _V} <- ProcessInfo]).

t_process_gc(_Config) ->
    GcInfo = emqx_vm:get_process_gc_info(),
    ?assertEqual(emqx_vm:process_gc_info_keys(), [K || {K, _V} <- GcInfo]).

t_get_ets_list(_Config) ->
    ets:new(test, [named_table]),
    Ets = emqx_vm:get_ets_list(),
    true = lists:member(test, Ets).

t_get_ets_info(_Config) ->
    ets:new(test, [named_table]),
    [] = emqx_vm:get_ets_info(test1),
    EtsInfo = emqx_vm:get_ets_info(test),
    test = proplists:get_value(name, EtsInfo),
    Tid = proplists:get_value(id, EtsInfo),
    EtsInfos = emqx_vm:get_ets_info(),
    ?assertEqual(
        true,
        lists:foldl(
            fun(Info, Acc) ->
                case proplists:get_value(id, Info) of
                    Tid -> true;
                    _ -> Acc
                end
            end,
            false,
            EtsInfos
        )
    ).

t_scheduler_usage(_Config) ->
    emqx_vm:scheduler_usage(5000).

t_get_memory(_Config) ->
    emqx_vm:get_memory().

t_schedulers(_Config) ->
    emqx_vm:schedulers().

t_get_process_limit(_Config) ->
    emqx_vm:get_process_limit().

t_cpu_util(_Config) ->
    ?assertMatch(Val when is_number(Val), emqx_vm:cpu_util()).

easy_server() ->
    {ok, LSock} = gen_tcp:listen(5678, [binary, {packet, 0}, {active, false}]),
    {ok, Sock} = gen_tcp:accept(LSock),
    ok = do_recv(Sock),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(LSock).

do_recv(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, _} ->
            do_recv(Sock);
        {error, closed} ->
            ok
    end.
