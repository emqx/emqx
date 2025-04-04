%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_responder_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_ft, "file_transfer {enable = true}"}
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_start_ack(_Config) ->
    Key = <<"test">>,
    DefaultAction = fun({ack, Ref}) -> Ref end,
    {ok, ResponderPid} = emqx_ft_responder:start(Key, DefaultAction, 1000),
    erlang:monitor(process, ResponderPid),
    ?assertMatch(
        {error, {already_started, _Pid}},
        emqx_ft_responder:start(Key, DefaultAction, 1000)
    ),
    Ref = make_ref(),
    ?assertEqual(
        Ref,
        emqx_ft_responder:ack(Key, Ref)
    ),
    ?assertExit(
        {noproc, _},
        emqx_ft_responder:ack(Key, Ref)
    ),
    ?assertReceive(
        {'DOWN', _, process, ResponderPid, {shutdown, _}},
        1000
    ).

t_timeout(_Config) ->
    Key = <<"test">>,
    Self = self(),
    DefaultAction = fun(timeout) -> Self ! {timeout, Key} end,
    {ok, _Pid} = emqx_ft_responder:start(Key, DefaultAction, 20),
    receive
        {timeout, Key} ->
            ok
    after 100 ->
        ct:fail("emqx_ft_responder not called")
    end,
    ?assertExit(
        {noproc, _},
        emqx_ft_responder:ack(Key, oops)
    ).

t_unknown_msgs(_Config) ->
    {ok, Pid} = emqx_ft_responder:start(make_ref(), fun(_) -> ok end, 100),
    Pid ! {unknown_msg, <<"test">>},
    ok = gen_server:cast(Pid, {unknown_msg, <<"test">>}),
    ?assertEqual(
        {error, unknown_call},
        gen_server:call(Pid, {unknown_call, <<"test">>})
    ).
