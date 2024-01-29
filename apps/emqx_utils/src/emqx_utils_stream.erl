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
    mqueue/1,
    map/2,
    chain/2
]).

%% Evaluating
-export([
    next/1,
    consume/1,
    consume/2
]).

%% Streams from ETS tables
-export([
    ets/1
]).

%% Streams from .csv data
-export([
    csv/1
]).

-export_type([stream/1]).

%% @doc A stream is essentially a lazy list.
-type stream(T) :: fun(() -> next(T) | []).
-type next(T) :: nonempty_improper_list(T, stream(T)).

-dialyzer(no_improper_lists).

-elvis([{elvis_style, nesting_level, disable}]).

%%

%% @doc Make a stream that produces no values.
-spec empty() -> stream(none()).
empty() ->
    fun() -> [] end.

%% @doc Make a stream out of the given list.
%% Essentially it's an opposite of `consume/1`, i.e. `L = consume(list(L))`.
-spec list([T]) -> stream(T).
list([]) ->
    empty();
list([X | Rest]) ->
    fun() -> [X | list(Rest)] end.

%% @doc Make a stream out of process message queue.
-spec mqueue(timeout()) -> stream(any()).
mqueue(Timeout) ->
    fun() ->
        receive
            X ->
                [X | mqueue(Timeout)]
        after Timeout ->
            []
        end
    end.

%% @doc Make a stream by applying a function to each element of the underlying stream.
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

%% @doc Make a stream by chaining (concatenating) two streams.
%% The second stream begins to produce values only after the first one is exhausted.
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

%% @doc Produce the next value from the stream.
-spec next(stream(T)) -> next(T) | [].
next(S) ->
    S().

%% @doc Consume the stream and return a list of all produced values.
-spec consume(stream(T)) -> [T].
consume(S) ->
    case next(S) of
        [X | SRest] ->
            [X | consume(SRest)];
        [] ->
            []
    end.

%% @doc Consume N values from the stream and return a list of them and the rest of the stream.
%% If the stream is exhausted before N values are produced, return just a list of these values.
-spec consume(non_neg_integer(), stream(T)) -> {[T], stream(T)} | [T].
consume(N, S) ->
    consume(N, S, []).

consume(0, S, Acc) ->
    {lists:reverse(Acc), S};
consume(N, S, Acc) ->
    case next(S) of
        [X | SRest] ->
            consume(N - 1, SRest, [X | Acc]);
        [] ->
            lists:reverse(Acc)
    end.

%%

-type select_result(Record, Cont) ::
    {[Record], Cont}
    | {[Record], '$end_of_table'}
    | '$end_of_table'.

%% @doc Make a stream out of an ETS table, where the ETS table is scanned through in chunks,
%% with the given continuation function. The function is assumed to return a result of a call to:
%% * `ets:select/1` / `ets:select/3`
%% * `ets:match/1` / `ets:match/3`
%% * `ets:match_object/1` / `ets:match_object/3`
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

%% @doc Make a stream out of a .csv binary, where the .csv binary is loaded in all at once.
%% The .csv binary is assumed to be in UTF-8 encoding and to have a header row.
-spec csv(binary()) -> stream(map()).
csv(Bin) when is_binary(Bin) ->
    Reader = fun _Iter(Headers, Lines) ->
        case csv_read_line(Lines) of
            {Fields, Rest} ->
                case length(Fields) == length(Headers) of
                    true ->
                        User = maps:from_list(lists:zip(Headers, Fields)),
                        [User | fun() -> _Iter(Headers, Rest) end];
                    false ->
                        error(bad_format)
                end;
            eof ->
                []
        end
    end,
    HeadersAndLines = binary:split(Bin, [<<"\r">>, <<"\n">>], [global, trim_all]),
    case csv_read_line(HeadersAndLines) of
        {CSVHeaders, CSVLines} ->
            fun() -> Reader(CSVHeaders, CSVLines) end;
        eof ->
            empty()
    end.

csv_read_line([Line | Lines]) ->
    %% XXX: not support ' ' for the field value
    Fields = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
    {Fields, Lines};
csv_read_line([]) ->
    eof.
