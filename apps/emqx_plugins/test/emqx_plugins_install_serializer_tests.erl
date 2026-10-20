%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_plugins_install_serializer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SERIALIZER, emqx_plugins_install_serializer).

run_returns_the_callback_result_test() ->
    with_serializer(fun() ->
        ?assertEqual({ok, 42}, ?SERIALIZER:run("plugin_a-1.0", fun() -> {ok, 42} end))
    end).

run_releases_the_lock_when_the_callback_returns_test() ->
    with_serializer(fun() ->
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end)),
        ?assertEqual(ok, ?SERIALIZER:run(<<"plugin_a-1.0">>, fun() -> ok end))
    end).

run_releases_the_lock_when_the_callback_throws_test() ->
    with_serializer(fun() ->
        ?assertThrow(boom, ?SERIALIZER:run("plugin_a-1.0", fun() -> throw(boom) end)),
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
    end).

%% The steps of one installation are composed by taking the lock once around the
%% whole sequence: the nested calls of that process run in the same critical
%% section instead of being refused by the lock the process holds itself.
run_is_reentrant_for_the_holder_test() ->
    with_serializer(fun() ->
        ?assertEqual(
            {ok, done},
            ?SERIALIZER:run("plugin_a-1.0", fun() ->
                {ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> done end)}
            end)
        )
    end).

a_second_process_is_refused_while_the_lock_is_held_test() ->
    with_serializer(fun() ->
        Holder = hold_lock("plugin_a-1.0"),
        try
            ?assertMatch(
                {error, #{reason := installation_in_progress}},
                ?SERIALIZER:run("plugin_a-1.0", fun() -> error(unexpected_callback) end)
            )
        after
            release_holder(Holder)
        end
    end).

%% The server is not running: the installation can not be ordered, and the
%% refusal names the installation which asked for the lock.
server_not_running_is_reported_as_an_occupied_lock_test() ->
    ?assertEqual(undefined, whereis(?SERIALIZER)),
    ?assertMatch(
        {error, #{reason := installation_in_progress, name_vsn := <<"not_running-1.0">>}},
        ?SERIALIZER:run("not_running-1.0", fun() -> error(unexpected_callback) end)
    ).

%% The refusal names the installation which asked for the lock, and the plugin
%% whose lock is occupied.
busy_error_names_the_refused_installation_test() ->
    with_serializer(fun() ->
        Holder = hold_lock("plugin_a-1.0"),
        try
            ?assertMatch(
                {error, #{
                    reason := installation_in_progress,
                    name_vsn := <<"plugin_a-2.0">>,
                    plugin := <<"plugin_a">>
                }},
                ?SERIALIZER:run("plugin_a-2.0", fun() -> error(unexpected_callback) end)
            )
        after
            release_holder(Holder)
        end
    end).

%% The identifier may come from a request, so the lock key is taken from its
%% text: this must not create an atom.
lock_key_does_not_create_an_atom_test() ->
    with_serializer(fun() ->
        Name = <<
            "textual_plugin_",
            (integer_to_binary(erlang:unique_integer([positive])))/binary
        >>,
        NameVsn = <<Name/binary, "-1.0">>,
        ?assertError(badarg, binary_to_existing_atom(Name, utf8)),
        ?assertEqual(ok, ?SERIALIZER:run(NameVsn, fun() -> ok end)),
        ?assertError(badarg, binary_to_existing_atom(Name, utf8))
    end).

%% Two versions of one plugin publish and roll back the same installation tree,
%% so they share a lock.
every_version_of_a_plugin_shares_one_lock_test() ->
    with_serializer(fun() ->
        Holder = hold_lock("plugin_a-1.0"),
        try
            ?assertMatch(
                {error, #{reason := installation_in_progress}},
                ?SERIALIZER:run("plugin_a-2.0", fun() -> error(unexpected_callback) end)
            )
        after
            release_holder(Holder)
        end
    end).

%% Installations of different plugins write different directories, so one of
%% them does not hold back the other.
installations_of_different_plugins_do_not_block_each_other_test() ->
    with_serializer(fun() ->
        Holder = hold_lock("plugin_a-1.0"),
        try
            ?assertEqual(ok, ?SERIALIZER:run("plugin_b-1.0", fun() -> ok end))
        after
            release_holder(Holder)
        end
    end).

%% Taking the lock of another plugin inside one critical section must not drop
%% the lock of the first one when the nested frame finishes.
run_of_another_plugin_keeps_the_first_lock_test() ->
    with_serializer(fun() ->
        ok = ?SERIALIZER:run("plugin_a-1.0", fun() ->
            ok = ?SERIALIZER:run("plugin_b-1.0", fun() -> ok end),
            ?assertMatch(
                {error, #{reason := installation_in_progress}},
                run_in_another_process("plugin_a-1.0")
            ),
            ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
        end)
    end).

the_lock_is_released_when_the_holder_dies_test() ->
    with_serializer(fun() ->
        {Holder, Ref} = hold_lock("plugin_a-1.0"),
        exit(Holder, kill),
        receive
            {'DOWN', Ref, process, Holder, _} -> ok
        after 2000 ->
            error(holder_did_not_die)
        end,
        wait_until_free()
    end).

with_serializer(Fun) ->
    case whereis(?SERIALIZER) of
        undefined ->
            {ok, Pid} = ?SERIALIZER:start_link(),
            try
                Fun()
            after
                gen_server:stop(Pid)
            end;
        _Started ->
            Fun()
    end.

%% Hold the lock in another process and keep it until the caller releases it.
hold_lock(NameVsn) ->
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
        ok = ?SERIALIZER:run(NameVsn, fun() ->
            Parent ! {locked, self()},
            receive
                release -> ok
            end
        end)
    end),
    receive
        {locked, Pid} -> {Pid, Ref}
    after 2000 ->
        error(lock_not_taken)
    end.

release_holder({Pid, Ref}) ->
    Pid ! release,
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 2000 ->
        error(holder_did_not_finish)
    end.

run_in_another_process(NameVsn) ->
    Parent = self(),
    _ = spawn(fun() ->
        Parent ! {result, ?SERIALIZER:run(NameVsn, fun() -> ok end)}
    end),
    receive
        {result, Result} -> Result
    after 2000 ->
        error(run_timeout)
    end.

wait_until_free() ->
    wait_until_free(100).

wait_until_free(0) ->
    error(lock_was_not_released);
wait_until_free(Attempts) ->
    case ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end) of
        ok ->
            ok;
        {error, _Refused} ->
            timer:sleep(20),
            wait_until_free(Attempts - 1)
    end.
