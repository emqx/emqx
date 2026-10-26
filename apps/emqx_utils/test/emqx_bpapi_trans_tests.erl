%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Tests of the BPAPI parse transform on transaction proto modules
%% (`<api>_tx_proto_v<N>').
-module(emqx_bpapi_trans_tests).

-include_lib("eunit/include/eunit.hrl").

tx_proto_records_implementation_as_target_test() ->
    {ok, Mod, Bin} = compile_proto(
        emqx_test_tx_proto_v1,
        [
            "-export([introduced_in/0, f/2, g/0]).",
            "introduced_in() -> \"6.3.3\".",
            "f(A, B) -> emqx_test_impl:f_tx(A, B).",
            "g() -> emqx_test_impl:g_tx()."
        ]
    ),
    {module, Mod} = code:load_binary(Mod, "emqx_test_tx_proto_v1.erl", Bin),
    try
        ?assertEqual(
            #{
                api => emqx_test_tx,
                version => 1,
                calls => [
                    {{Mod, g, []}, {emqx_test_impl, g_tx, []}},
                    {{Mod, f, ['A', 'B']}, {emqx_test_impl, f_tx, ['A', 'B']}}
                ],
                casts => []
            },
            Mod:bpapi_meta()
        )
    after
        code:purge(Mod),
        code:delete(Mod)
    end.

tx_proto_rejects_reordered_arguments_test() ->
    ?assertMatch(
        {error, [{_, [{_, emqx_bpapi_trans, {invalid_tx_fun, f, 2}}]}], _},
        compile_proto(
            emqx_test_tx_proto_v1,
            [
                "-export([introduced_in/0, f/2]).",
                "introduced_in() -> \"6.3.3\".",
                "f(A, B) -> emqx_test_impl:f_tx(B, A)."
            ]
        )
    ).

tx_proto_rejects_a_body_that_is_not_one_call_test() ->
    ?assertMatch(
        {error, [{_, [{_, emqx_bpapi_trans, {invalid_tx_fun, f, 1}}]}], _},
        compile_proto(
            emqx_test_tx_proto_v1,
            [
                "-export([introduced_in/0, f/1]).",
                "introduced_in() -> \"6.3.3\".",
                "f(A) -> B = A, emqx_test_impl:f_tx(B)."
            ]
        )
    ).

tx_proto_rejects_rpc_test() ->
    ?assertMatch(
        {error, [{_, [{_, emqx_bpapi_trans, {invalid_tx_fun, f, 4}}]}], _},
        compile_proto(
            emqx_test_tx_proto_v1,
            [
                "-export([introduced_in/0, f/4]).",
                "introduced_in() -> \"6.3.3\".",
                "f(Node, M, F, A) -> erpc:call(Node, M, F, A)."
            ]
        )
    ).

rpc_proto_still_rejects_a_plain_call_test() ->
    ?assertMatch(
        {error, [{_, [{_, emqx_bpapi_trans, {invalid_fun, f, 2}}]}], _},
        compile_proto(
            emqx_test_proto_v1,
            [
                "-export([introduced_in/0, f/2]).",
                "introduced_in() -> \"6.3.3\".",
                "f(A, B) -> emqx_test_impl:f_tx(A, B)."
            ]
        )
    ).

%% Compile a proto module given as source lines, one form per line.
compile_proto(Module, Lines) ->
    Forms = [
        form("-module(" ++ atom_to_list(Module) ++ ").")
        | [form(L) || L <- Lines]
    ],
    compile:forms(Forms, [{parse_transform, emqx_bpapi_trans}, return_errors, binary]).

form(String) ->
    {ok, Tokens, _} = erl_scan:string(String),
    {ok, Form} = erl_parse:parse_form(Tokens),
    Form.
