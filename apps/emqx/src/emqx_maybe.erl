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

-module(emqx_maybe).

-include_lib("emqx/include/types.hrl").

-export([to_list/1]).
-export([from_list/1]).
-export([define/2]).
-export([apply/2]).

-type t(T) :: option(T).
-export_type([t/1]).

-spec to_list(option(A)) -> [A].
to_list(undefined) ->
    [];
to_list(Term) ->
    [Term].

-spec from_list([A]) -> option(A).
from_list([]) ->
    undefined;
from_list([Term]) ->
    Term.

-spec define(option(A), B) -> A | B.
define(undefined, Term) ->
    Term;
define(Term, _) ->
    Term.

%% @doc Apply a function to a maybe argument.
-spec apply(fun((A) -> B), option(A)) ->
    option(B).
apply(_Fun, undefined) ->
    undefined;
apply(Fun, Term) when is_function(Fun) ->
    erlang:apply(Fun, [Term]).

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

to_list_test_() ->
    [
        ?_assertEqual([], to_list(undefined)),
        ?_assertEqual([42], to_list(42))
    ].

from_list_test_() ->
    [
        ?_assertEqual(undefined, from_list([])),
        ?_assertEqual(3.1415, from_list([3.1415])),
        ?_assertError(_, from_list([1, 2, 3]))
    ].

define_test_() ->
    [
        ?_assertEqual(42, define(42, undefined)),
        ?_assertEqual(<<"default">>, define(undefined, <<"default">>)),
        ?_assertEqual(undefined, define(undefined, undefined))
    ].

apply_test_() ->
    [
        ?_assertEqual(<<"42">>, ?MODULE:apply(fun erlang:integer_to_binary/1, 42)),
        ?_assertEqual(undefined, ?MODULE:apply(fun erlang:integer_to_binary/1, undefined)),
        ?_assertEqual(undefined, ?MODULE:apply(fun crash/1, undefined))
    ].

crash(_) ->
    erlang:error(crashed).

-endif.
