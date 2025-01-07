%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_ext).

-include_lib("emqx/include/emqx_schema.hrl").

-on_load(on_load/0).

-export([]).

-spec on_load() -> ok.
on_load() ->
    init_ssl_fun_cb().

init_ssl_fun_cb() ->
    lists:foreach(
        fun({FunName, {_, _, _} = MFA}) ->
            persistent_term:put(
                ?EMQX_SSL_FUN_MFA(FunName),
                MFA
            )
        end,
        [
            {root_fun, {emqx_auth_ext_tls_lib, opt_partial_chain, []}},
            {verify_fun, {emqx_auth_ext_tls_lib, opt_verify_fun, []}}
        ]
    ).
