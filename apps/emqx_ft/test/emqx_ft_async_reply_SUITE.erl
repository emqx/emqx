%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_async_reply_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{override_env => [{boot_modules, [broker, listeners]}]}},
            {emqx_ft, "file_transfer { enable = true, assemble_timeout = 1s }"}
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = snabbkaffe:stop(),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_register(_Config) ->
    PacketId = 1,
    MRef = make_ref(),
    TRef = make_ref(),
    ok = emqx_ft_async_reply:register(PacketId, MRef, TRef, somedata),

    ?assertEqual(
        undefined,
        emqx_ft_async_reply:with_new_packet(PacketId, fun() -> ok end, undefined)
    ),

    ?assertEqual(
        ok,
        emqx_ft_async_reply:with_new_packet(2, fun() -> ok end, undefined)
    ),

    ?assertEqual(
        {ok, PacketId, TRef, somedata},
        emqx_ft_async_reply:take_by_mref(MRef)
    ).

t_process_independence(_Config) ->
    PacketId = 1,
    MRef = make_ref(),
    TRef = make_ref(),
    ok = emqx_ft_async_reply:register(PacketId, MRef, TRef, somedata),

    Self = self(),

    spawn_link(fun() ->
        Self ! emqx_ft_async_reply:take_by_mref(MRef)
    end),

    Res1 =
        receive
            Msg1 -> Msg1
        end,

    ?assertEqual(
        not_found,
        Res1
    ),

    spawn_link(fun() ->
        Self ! emqx_ft_async_reply:with_new_packet(PacketId, fun() -> ok end, undefined)
    end),

    Res2 =
        receive
            Msg2 -> Msg2
        end,

    ?assertEqual(
        ok,
        Res2
    ).

t_take(_Config) ->
    PacketId = 1,
    MRef = make_ref(),
    TRef = make_ref(),
    ok = emqx_ft_async_reply:register(PacketId, MRef, TRef, somedata),

    ?assertEqual(
        {ok, PacketId, TRef, somedata},
        emqx_ft_async_reply:take_by_mref(MRef)
    ),

    ?assertEqual(
        not_found,
        emqx_ft_async_reply:take_by_mref(MRef)
    ),

    ?assertEqual(
        ok,
        emqx_ft_async_reply:with_new_packet(2, fun() -> ok end, undefined)
    ).

t_cleanup(_Config) ->
    PacketId = 1,
    MRef0 = make_ref(),
    TRef0 = make_ref(),
    MRef1 = make_ref(),
    TRef1 = make_ref(),
    ok = emqx_ft_async_reply:register(PacketId, MRef0, TRef0, somedata0),

    Self = self(),

    Pid = spawn_link(fun() ->
        ok = emqx_ft_async_reply:register(PacketId, MRef1, TRef1, somedata1),
        receive
            kickoff ->
                ?assertEqual(
                    undefined,
                    emqx_ft_async_reply:with_new_packet(PacketId, fun() -> ok end, undefined)
                ),

                ?assertEqual(
                    {ok, PacketId, TRef1, somedata1},
                    emqx_ft_async_reply:take_by_mref(MRef1)
                ),

                Self ! done
        end
    end),

    ?assertEqual(
        undefined,
        emqx_ft_async_reply:with_new_packet(PacketId, fun() -> ok end, undefined)
    ),

    ok = emqx_ft_async_reply:deregister_all(Self),

    ?assertEqual(
        ok,
        emqx_ft_async_reply:with_new_packet(PacketId, fun() -> ok end, undefined)
    ),

    Pid ! kickoff,

    receive
        done -> ok
    end.

t_reply_by_tiemout(_Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = emqx_ft_test_helpers:start_client(ClientId, node()),

    SleepForever = fun() ->
        Ref = make_ref(),
        receive
            Ref -> ok
        end
    end,

    ok = meck:new(emqx_ft_storage, [passthrough]),
    meck:expect(emqx_ft_storage, assemble, fun(_, _, _) -> {async, spawn_link(SleepForever)} end),

    FinTopic = <<"$file/fakeid/fin/999999">>,

    ?assertMatch(
        {ok, #{reason_code_name := unspecified_error}},
        emqtt:publish(C, FinTopic, <<>>, 1)
    ),

    meck:unload(emqx_ft_storage),
    emqtt:stop(C).

t_cleanup_by_cm(_Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = emqx_ft_test_helpers:start_client(ClientId, node()),

    ok = meck:new(emqx_ft_storage, [passthrough]),
    meck:expect(emqx_ft_storage, kickoff, fun(_) -> meck:exception(error, oops) end),

    FinTopic = <<"$file/fakeid/fin/999999">>,

    [ClientPid] = emqx_cm:lookup_channels(ClientId),

    ?assertWaitEvent(
        begin
            emqtt:publish(C, FinTopic, <<>>, 1),
            exit(ClientPid, kill)
        end,
        #{?snk_kind := emqx_cm_clean_down, client_id := ClientId},
        1000
    ),

    ?assertEqual(
        {0, 0},
        emqx_ft_async_reply:info()
    ),

    meck:unload(emqx_ft_storage).

t_unrelated_events(_Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = emqx_ft_test_helpers:start_client(ClientId, node()),
    [ClientPid] = emqx_cm:lookup_channels(ClientId),

    erlang:monitor(process, ClientPid),

    ClientPid ! {'DOWN', make_ref(), process, self(), normal},
    ClientPid ! {timeout, make_ref(), unknown_timer_event},

    ?assertNotReceive(
        {'DOWN', _Ref, process, ClientPid, _Reason},
        500
    ),

    emqtt:stop(C).
