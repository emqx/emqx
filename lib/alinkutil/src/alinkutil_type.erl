%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 上午10:32
%%%-------------------------------------------------------------------
-module(alinkutil_type).
-author("yqfclid").

%% API
-export([
    to_atom/1,
    to_list/1,
    to_force_list/1,
    to_binary/1,
    to_force_binary/1,
    to_integer/1,
    to_float/1
]).


%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec(to_atom(S :: any()) -> atom()).
to_atom(S) when is_atom(S) ->
    S;
to_atom(S) when is_list(S) ->
    list_to_atom(S);
to_atom(S) when is_integer(S) ->
    list_to_atom(integer_to_list(S));
to_atom(S) when is_float(S) ->
    list_to_atom(float_to_list(S));
to_atom(S) when is_binary(S) ->
    binary_to_atom(S);
to_atom(S) ->
    throw({badarg, S}).


-spec(to_list(S :: any()) -> list()).
to_list(S) when is_atom(S) ->
    atom_to_list(S);
to_list(S) when is_list(S) ->
    S;
to_list(S) when is_integer(S) ->
    integer_to_list(S);
to_list(S) when is_float(S) ->
    float_to_list(S);
to_list(S) when is_binary(S) ->
    binary_to_list(S);
to_list(S) ->
    throw({badarg, S}).


-spec(to_force_list(S :: any()) -> list()).
to_force_list(S) when is_atom(S) ->
    atom_to_list(S);
to_force_list(S) when is_list(S) ->
    S;
to_force_list(S) when is_integer(S) ->
    integer_to_list(S);
to_force_list(S) when is_float(S) ->
    float_to_list(S);
to_force_list(S) when is_binary(S) ->
    binary_to_list(S);
to_force_list(S) ->
    io_lib:format("~p", [S]).

-spec(to_binary(S :: any()) -> binary()).
to_binary(S) when is_atom(S) ->
    atom_to_binary(S);
to_binary(S) when is_list(S) ->
    list_to_binary(S);
to_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(S) when is_float(S) ->
    float_to_binary(S);
to_binary(S) when is_binary(S) ->
    S;
to_binary(S) ->
    throw({badarg, S}).

-spec(to_force_binary(S :: any()) -> binary()).
to_force_binary(S) when is_atom(S) ->
    atom_to_binary(S);
to_force_binary(S) when is_list(S) ->
    list_to_binary(S);
to_force_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_force_binary(S) when is_float(S) ->
    float_to_binary(S);
to_force_binary(S) when is_binary(S) ->
    S;
to_force_binary(S) ->
    list_to_binary(io_lib:format("~p", [S])).


-spec(to_integer(S :: any()) -> integer()).
to_integer(S) when is_atom(S) ->
    list_to_integer(atom_to_list(S));
to_integer(S) when is_list(S) ->
    list_to_integer(S);
to_integer(S) when is_integer(S) ->
    S;
to_integer(S) when is_binary(S) ->
    binary_to_integer(S);
to_integer(S) ->
    throw({badarg, S}).

-spec(to_float(S :: any()) -> float()).
to_float(S) when is_atom(S) ->
    list_to_float(atom_to_list(S));
to_float(S) when is_list(S) ->
    list_to_float(S);
to_float(S) when is_integer(S) ->
    S + 0.0;
to_float(S) when is_float(S) ->
    S;
to_float(S) when is_binary(S) ->
    binary_to_float(S);
to_float(S) ->
    throw({badarg, S}).





