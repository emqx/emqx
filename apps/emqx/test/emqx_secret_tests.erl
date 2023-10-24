%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_secret_tests).

-export([ident/1]).

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

wrap_unwrap_external_test() ->
    ?assertEqual(
        ident({foo, bar}),
        emqx_secret:unwrap(emqx_secret:wrap(?MODULE, ident, {foo, bar}))
    ).

wrap_unwrap_transform_test() ->
    ?assertEqual(
        <<"this_was_an_atom">>,
        emqx_secret:unwrap(emqx_secret:wrap(erlang, atom_to_binary, this_was_an_atom))
    ).

wrap_term_test() ->
    ?assertEqual(
        42,
        emqx_secret:term(emqx_secret:wrap(42))
    ).

wrap_external_term_test() ->
    ?assertEqual(
        this_was_an_atom,
        emqx_secret:term(emqx_secret:wrap(erlang, atom_to_binary, this_was_an_atom))
    ).

external_fun_term_error_test() ->
    Term = {foo, bar},
    ?assertError(
        badarg,
        emqx_secret:term(fun() -> Term end)
    ).

%%

ident(X) ->
    X.
