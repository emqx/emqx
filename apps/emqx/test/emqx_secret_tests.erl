%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_secret_tests).

-include_lib("eunit/include/eunit.hrl").

wrap_unwrap_test() ->
    ?assertEqual(
        42,
        emqx_secret:unwrap(emqx_secret:wrap(42))
    ).

unwrap_immediate_test() ->
    ?assertEqual(
        42,
        emqx_secret:unwrap(42)
    ).

wrap_unwrap_load_test_() ->
    Secret = <<"foobaz">>,
    {
        setup,
        fun() -> write_temp_file(Secret) end,
        fun(Filename) -> file:delete(Filename) end,
        fun(Filename) ->
            ?_assertEqual(
                Secret,
                emqx_secret:unwrap(emqx_secret:wrap_load(file_ref(Filename)))
            )
        end
    }.

wrap_unwrap_load_path_env_interpolate_test_() ->
    Secret = <<"111">>,
    {
        setup,
        fun() -> write_temp_file(Secret) end,
        fun(Filename) -> file:delete(Filename) end,
        fun(Filename) ->
            fun() ->
                os:putenv("SECRETFILEPATH", Filename),
                File = "file://${SECRETFILEPATH}",
                try
                    ?assertEqual(
                        Secret,
                        emqx_secret:unwrap(emqx_secret:wrap_load({file, File}))
                    )
                after
                    os:unsetenv("SECRETFILEPATH")
                end
            end
        end
    }.

wrap_load_term_test() ->
    Ref = file_ref("no/such/file/i/swear"),
    ?assertEqual(Ref, emqx_secret:term(emqx_secret:wrap_load(Ref))).

wrap_unwrap_missing_file_test() ->
    ?assertThrow(
        #{msg := failed_to_read_secret_file, reason := "No such file or directory"},
        emqx_secret:unwrap(emqx_secret:wrap_load(file_ref("no/such/file/i/swear")))
    ).

wrap_term_test() ->
    ?assertEqual(
        42,
        emqx_secret:term(emqx_secret:wrap(42))
    ).

external_fun_term_error_test() ->
    Term = {foo, bar},
    ?assertError(
        badarg,
        emqx_secret:term(fun() -> Term end)
    ).

write_temp_file(Bytes) ->
    Ts = erlang:system_time(millisecond),
    Filename = filename:join("/tmp", ?MODULE_STRING ++ integer_to_list(-Ts)),
    ok = file:write_file(Filename, Bytes),
    Filename.

file_ref(Path) ->
    {file, "file://" ++ Path}.
