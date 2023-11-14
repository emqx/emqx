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

-module(emqx_utils_stream).

%% Constructors / Combinators
-export([
    empty/0,
    list/1,
    map/2,
    chain/2
]).

%% Evaluating
-export([
    next/1,
    take/2,
    consume/1
]).

%% Streams from ETS tables
-export([
    ets/1
]).

-export_type([stream/1]).

%% @doc A stream is essentially a lazy list.
-type stream(T) :: fun(() -> next(T) | []).
-type next(T) :: nonempty_improper_list(T, stream(T)).

-dialyzer(no_improper_lists).

%%

-spec empty() -> stream(none()).
empty() ->
    fun() -> [] end.

-spec list([T]) -> stream(T).
list([]) ->
    empty();
list([X | Rest]) ->
    fun() -> [X | list(Rest)] end.

-spec map(fun((X) -> Y), stream(X)) -> stream(Y).
map(F, S) ->
    fun() ->
        case next(S) of
            [X | Rest] ->
                [F(X) | map(F, Rest)];
            [] ->
                []
        end
    end.

-spec chain(stream(X), stream(Y)) -> stream(X | Y).
chain(SFirst, SThen) ->
    fun() ->
        case next(SFirst) of
            [X | SRest] ->
                [X | chain(SRest, SThen)];
            [] ->
                next(SThen)
        end
    end.

%%

-spec next(stream(T)) -> next(T) | [].
next(S) ->
    S().

-spec take(non_neg_integer(), stream(T)) -> {[T], stream(T)} | [T].
take(N, S) ->
    take(N, S, []).

take(0, S, Acc) ->
    {lists:reverse(Acc), S};
take(N, S, Acc) ->
    case next(S) of
        [X | SRest] ->
            take(N - 1, SRest, [X | Acc]);
        [] ->
            lists:reverse(Acc)
    end.

-spec consume(stream(T)) -> [T].
consume(S) ->
    case next(S) of
        [X | SRest] ->
            [X | consume(SRest)];
        [] ->
            []
    end.

%%

-type select_result(Record, Cont) ::
    {[Record], Cont}
    | {[Record], '$end_of_table'}
    | '$end_of_table'.

-spec ets(fun((Cont) -> select_result(Record, Cont))) -> stream(Record).
ets(ContF) ->
    ets(undefined, ContF).

ets(Cont, ContF) ->
    fun() ->
        case ContF(Cont) of
            {Records, '$end_of_table'} ->
                next(list(Records));
            {Records, NCont} ->
                next(chain(list(Records), ets(NCont, ContF)));
            '$end_of_table' ->
                []
        end
    end.
