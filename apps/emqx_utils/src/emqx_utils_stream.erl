%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    const/1,
    mqueue/1,
    map/2,
    transpose/1,
    chain/1,
    chain/2,
    repeat/1,
    interleave/2,
    limit_length/2,
    filter/2,
    drop/2,
    chainmap/2
]).

%% Evaluating
-export([
    next/1,
    consume/1,
    consume/2,
    foreach/2,
    fold/3,
    fold/4,
    sweep/2
]).

%% Streams from ETS tables
-export([
    ets/1
]).

%% Streams from .csv data
-export([
    csv/1,
    csv/2
]).

-export_type([stream/1, csv_parse_opts/0]).

%% @doc A stream is essentially a lazy list.
-type stream_tail(T) :: fun(() -> next(T) | []).
-type stream(T) :: list(T) | nonempty_improper_list(T, stream_tail(T)) | stream_tail(T).
-type next(T) :: nonempty_improper_list(T, stream_tail(T)).

-type csv_parse_opts() :: #{nullable => boolean(), filter_null => boolean()}.

-dialyzer(no_improper_lists).

-elvis([{elvis_style, nesting_level, disable}]).

%%

%% @doc Make a stream that produces no values.
-spec empty() -> stream(none()).
empty() ->
    [].

%% @doc Make a stream out of the given list.
%% Essentially it's an opposite of `consume/1`, i.e. `L = consume(list(L))`.
-spec list([T]) -> stream(T).
list(L) -> L.

%% @doc Make a stream with a single element infinitely repeated
-spec const(T) -> stream(T).
const(T) ->
    fun() -> [T | const(T)] end.

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

%% @doc Make a stream by filtering the underlying stream with a predicate function.
-spec filter(fun((X) -> boolean()), stream(X)) -> stream(X).
filter(F, S) ->
    FilterNext = fun FilterNext(St) ->
        case next(St) of
            [X | Rest] ->
                case F(X) of
                    true ->
                        [X | filter(F, Rest)];
                    false ->
                        FilterNext(Rest)
                end;
            [] ->
                []
        end
    end,
    fun() -> FilterNext(S) end.

%% @doc Drops N first elements from the stream
-spec drop(non_neg_integer(), stream(T)) -> stream(T).
drop(N, S) ->
    DropNext = fun DropNext(M, St) ->
        case next(St) of
            [_X | Rest] when M > 0 ->
                DropNext(M - 1, Rest);
            Next ->
                Next
        end
    end,
    fun() -> DropNext(N, S) end.

%% @doc Stream version of flatmap.
-spec chainmap(fun((X) -> stream(Y)), stream(X)) -> stream(Y).
chainmap(F, S) ->
    ChainNext = fun ChainNext(St) ->
        case next(St) of
            [X | Rest] ->
                case next(F(X)) of
                    [Y | YRest] ->
                        [Y | chain(YRest, chainmap(F, Rest))];
                    [] ->
                        ChainNext(Rest)
                end;
            [] ->
                []
        end
    end,
    fun() -> ChainNext(S) end.

%% @doc Transpose a list of streams into a stream producing lists of their respective values.
%% The resulting stream is as long as the shortest of the input streams.
-spec transpose([stream(X)]) -> stream([X]).
transpose([S]) ->
    map(fun(X) -> [X] end, S);
transpose([S | Streams]) ->
    transpose_tail(S, transpose(Streams));
transpose([]) ->
    empty().

transpose_tail(S, Tail) ->
    fun() ->
        case next(S) of
            [X | SRest] ->
                case next(Tail) of
                    [Xs | TailRest] ->
                        [[X | Xs] | transpose_tail(SRest, TailRest)];
                    [] ->
                        []
                end;
            [] ->
                []
        end
    end.

%% @doc Make a stream by concatenating multiple streams.
-spec chain([stream(X)]) -> stream(X).
chain(L) ->
    lists:foldr(fun chain/2, empty(), L).

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

%% @doc Make an infinite stream out of repeats of given stream.
%% If the given stream is empty, the resulting stream is also empty.
-spec repeat(stream(X)) -> stream(X).
repeat(S) ->
    fun() ->
        case next(S) of
            [X | SRest] ->
                [X | chain(SRest, repeat(S))];
            [] ->
                []
        end
    end.

%% @doc Interleave the elements of the streams.
%%
%% This function accepts a list of tuples where the first element
%% specifies size of the "batch" to be consumed from the stream at a
%% time (stream is the second tuple element). If element of the list
%% is a plain stream, then the batch size is assumed to be 1.
%%
%% If `ContinueAtEmpty' is `false', and one of the streams returns
%% `[]', then the function will return `[]' as well. Otherwise, it
%% will continue consuming data from the remaining streams.
-spec interleave([stream(X) | {non_neg_integer(), stream(X)}], boolean()) -> stream(X).
interleave(L0, ContinueAtEmpty) ->
    L = lists:map(
        fun
            (Stream) when is_function(Stream) or is_list(Stream) ->
                {1, Stream};
            (A = {N, _}) when N >= 0 ->
                A
        end,
        L0
    ),
    fun() ->
        do_interleave(ContinueAtEmpty, 0, L, [])
    end.

%% @doc Truncate list to the given length
-spec limit_length(non_neg_integer(), stream(X)) -> stream(X).
limit_length(0, _) ->
    fun() -> [] end;
limit_length(N, S) when N >= 0 ->
    fun() ->
        case next(S) of
            [] ->
                [];
            [X | S1] ->
                [X | limit_length(N - 1, S1)]
        end
    end.

%%

%% @doc Produce the next value from the stream.
-spec next(stream(T)) -> next(T) | [].
next(EvalNext) when is_function(EvalNext) ->
    EvalNext();
next([_ | _Rest] = EvaluatedNext) ->
    EvaluatedNext;
next([]) ->
    [].

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

%% @doc Consumes the stream and applies the given function to each element.
-spec foreach(fun((X) -> _), stream(X)) -> ok.
foreach(F, S) ->
    case next(S) of
        [X | Rest] ->
            F(X),
            foreach(F, Rest);
        [] ->
            ok
    end.

%% @doc Folds the whole stream, accumulating the result of given function applied
%% to each element.
-spec fold(fun((X, Acc) -> Acc), Acc, stream(X)) -> Acc.
fold(F, Acc, S) ->
    case next(S) of
        [X | Rest] ->
            fold(F, F(X, Acc), Rest);
        [] ->
            Acc
    end.

%% @doc Folds the first N element of the stream, accumulating the result of given
%% function applied to each element. If there's less than N elements in the given
%% stream, returns `[]` (a.k.a. empty stream) along with the accumulated value.
-spec fold(fun((X, Acc) -> Acc), Acc, non_neg_integer(), stream(X)) -> {Acc, stream(X)}.
fold(_, Acc, 0, S) ->
    {Acc, S};
fold(F, Acc, N, S) when N > 0 ->
    case next(S) of
        [X | Rest] ->
            fold(F, F(X, Acc), N - 1, Rest);
        [] ->
            {Acc, []}
    end.

%% @doc Same as `consume/2` but discard the consumed values.
-spec sweep(non_neg_integer(), stream(X)) -> stream(X).
sweep(0, S) ->
    S;
sweep(N, S) when N > 0 ->
    case next(S) of
        [_ | Rest] ->
            sweep(N - 1, Rest);
        [] ->
            []
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
ets(ContF) when is_function(ContF) ->
    ets(undefined, ContF).

ets(Cont, ContF) ->
    fun() ->
        case ContF(Cont) of
            {Records, '$end_of_table'} ->
                next(Records);
            {Records, NCont} ->
                next(chain(Records, ets(NCont, ContF)));
            '$end_of_table' ->
                []
        end
    end.

%% @doc Make a stream out of a .csv binary, where the .csv binary is loaded in all at once.
%% The .csv binary is assumed to be in UTF-8 encoding and to have a header row.
-spec csv(binary()) -> stream(map()).
csv(Bin) ->
    csv(Bin, #{}).

-spec csv(binary(), csv_parse_opts()) -> stream(map()).
csv(Bin, Opts) when is_binary(Bin) ->
    Liner =
        case Opts of
            #{nullable := true} ->
                fun csv_read_nullable_line/1;
            _ ->
                fun csv_read_line/1
        end,
    Maper =
        case Opts of
            #{filter_null := true} ->
                fun(Headers, Fields) ->
                    maps:from_list(
                        lists:filter(
                            fun({_, Value}) ->
                                Value =/= undefined
                            end,
                            lists:zip(Headers, Fields)
                        )
                    )
                end;
            _ ->
                fun(Headers, Fields) ->
                    maps:from_list(lists:zip(Headers, Fields))
                end
        end,
    Reader = fun _Iter(Headers, Lines) ->
        case Liner(Lines) of
            {Fields, Rest} ->
                case length(Fields) == length(Headers) of
                    true ->
                        User = Maper(Headers, Fields),
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

csv_read_nullable_line([Line | Lines]) ->
    %% XXX: not support ' ' for the field value
    Fields = lists:map(
        fun(Bin) ->
            case string:trim(Bin, both) of
                <<>> ->
                    undefined;
                Any ->
                    Any
            end
        end,
        binary:split(Line, [<<",">>], [global])
    ),
    {Fields, Lines};
csv_read_nullable_line([]) ->
    eof.

do_interleave(_Cont, _, [], []) ->
    [];
do_interleave(Cont, N, [{N, S} | Rest], Rev) ->
    do_interleave(Cont, 0, Rest, [{N, S} | Rev]);
do_interleave(Cont, _, [], Rev) ->
    do_interleave(Cont, 0, lists:reverse(Rev), []);
do_interleave(Cont, I, [{N, S} | Rest], Rev) when I < N ->
    case next(S) of
        [] when Cont ->
            do_interleave(Cont, 0, Rest, Rev);
        [] ->
            [];
        [X | S1] ->
            [
                X
                | fun() ->
                    do_interleave(Cont, I + 1, [{N, S1} | Rest], Rev)
                end
            ]
    end.
