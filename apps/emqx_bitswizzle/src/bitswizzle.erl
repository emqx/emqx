-module(bitswizzle).

-export([make_sparse_matrix/2, sm_v_multiply/2, dump_sparse_matrix/1]).

-export_type([smatrix/0, elem/0]).

-nifs([make_sparse_matrix/2, sm_v_multiply/2, dump_sparse_matrix/1]).

-on_load(init/0).

-type elem() :: {non_neg_integer(), non_neg_integer()}.

-opaque smatrix() :: reference().

init() ->
    ok = erlang:load_nif(
        filename:join(filename:dirname(code:which(?MODULE)), "../priv/libbitswizzle"), 0
    ).

%% @doc Make a square sparse matrix from a list of coordinates of
%% non-zero elements. Size of the matrix must be multiple of 64
-spec make_sparse_matrix(non_neg_integer(), [elem()]) -> smatrix().
make_sparse_matrix(_, _) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Multiply sparse matrix to a bit vector represented as a
%% binary. Size of the vector in bits must be equal to the dimension
%% of the matrix.
-spec sm_v_multiply(smatrix(), binary()) -> binary().
sm_v_multiply(_, _) ->
    erlang:nif_error(nif_library_not_loaded).

%% Dump the contents of the sparse matrix for debugging
-spec dump_sparse_matrix(smatrix()) -> tuple().
dump_sparse_matrix(_) ->
    erlang:nif_error(nif_library_not_loaded).
