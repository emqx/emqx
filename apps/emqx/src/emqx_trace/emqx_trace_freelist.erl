%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_trace_freelist).

-export([
    %% Constructor:
    range/2,
    %% Tracking:
    occupy/2,
    %% Accessors:
    first/1,
    to_list/1
]).

-export_type([t/0]).

-opaque t() :: [integer()].

%%

-spec range(integer(), integer()) -> t().
range(A, B) when A =< B ->
    [A, B].

-spec first(t()) -> integer() | {error, full}.
first([A | _]) ->
    A;
first([]) ->
    {error, full}.

-spec to_list(t()) -> [integer()].
to_list([A, B | Rest]) ->
    lists:seq(A, B) ++ to_list(Rest);
to_list([]) ->
    [].

-spec occupy(integer(), t()) -> t().
occupy(X, [A | _] = List) when X < A ->
    List;
occupy(X, [A, B | Rest]) ->
    case X of
        A -> compress([A + 1, B | Rest]);
        B -> compress([A, B - 1 | Rest]);
        _ when X > B -> [A, B | occupy(X, Rest)];
        _ when X > A -> compress([A, X - 1, X + 1, B | Rest])
    end;
occupy(_, []) ->
    [].

compress([A, B | Rest]) when A > B ->
    compress(Rest);
compress(Range) ->
    Range.

%%

-ifdef(TEST).

-include_lib("proper/include/proper_common.hrl").
-include_lib("eunit/include/eunit.hrl").

free_range_test() ->
    ?assertEqual(10, first(range(10, 42))).

full_range_test() ->
    L0 = range(10, 20),
    L1 = lists:foldl(fun occupy/2, L0, lists:seq(10, 20)),
    ?assertEqual([], to_list(L1)),
    ?assertEqual({error, full}, first(L1)).

prop_test() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    proper:quickcheck(prop_consistency(), Opts).

prop_consistency() ->
    ?FORALL(
        {{A, B}, Occupied},
        {t_range(), proper_types:list(t_integer())},
        begin
            FL0 = range(A, B),
            FL = lists:foldl(fun occupy/2, FL0, Occupied),
            Seq0 = lists:seq(A, B),
            Seq = lists:foldl(fun(X, Seq) -> Seq -- [X] end, Seq0, Occupied),
            to_list(FL) =:= Seq
        end
    ).

t_range() ->
    ?SUCHTHAT({A, B}, {t_integer(), t_integer()}, A =< B).

t_integer() ->
    proper_types:integer().

-endif.
