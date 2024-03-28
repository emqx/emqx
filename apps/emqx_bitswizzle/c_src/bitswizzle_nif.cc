#include "bitswizzle.h"
#include <erl_nif.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

static ErlNifResourceType* RES_SPARSE_MATRIX;

static ERL_NIF_TERM ATOM_SPARSE_MATRIX;
static ERL_NIF_TERM ATOM_SMATRIX_CELL;
static ERL_NIF_TERM ATOM_SMATRIX_DIAG;
static ERL_NIF_TERM ATOM_UNDEFINED;
static ERL_NIF_TERM ATOM_BADARG;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_ECOORD;
static ERL_NIF_TERM ATOM_EMATRIX_SIZE;
static ERL_NIF_TERM ATOM_EVECTOR_SIZE;
static ERL_NIF_TERM ATOM_EBADMTRIAX;

#define INIT_ATOMS \
   ATOM(ATOM_SPARSE_MATRIX, sparse_matrix);      \
   ATOM(ATOM_SMATRIX_CELL, smatrix_cell);        \
   ATOM(ATOM_SMATRIX_DIAG, smatrix_diag);        \
   ATOM(ATOM_UNDEFINED, undefined);              \
   ATOM(ATOM_BADARG, badarg);                    \
   ATOM(ATOM_ENOMEM, enomem);                    \
   ATOM(ATOM_ECOORD, ecoord);                    \
   ATOM(ATOM_EMATRIX_SIZE, ematrix_size);        \
   ATOM(ATOM_EVECTOR_SIZE, evector_size);        \
   ATOM(ATOM_EBADMTRIAX, ebadmatrix);

ERL_NIF_TERM translate_error(int error) {
  switch (error) {
  case BSWZ_ENOMEM:
    return ATOM_ENOMEM;
  case BSWZ_ECOORD:
    return ATOM_ECOORD;
  case BSWZ_EMATRIX_SIZE:
    return ATOM_EMATRIX_SIZE;
  case BSWZ_EVECTOR_SIZE:
    return ATOM_EVECTOR_SIZE;
  case BSWZ_EBADMATRIX:
    return ATOM_EBADMTRIAX;
  default:
    return ATOM_BADARG;
  }
}

static ERL_NIF_TERM bswz_sm_v_multiply(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ERL_NIF_TERM result = ATOM_UNDEFINED;
  Smatrix* matrix;
  ErlNifBinary input_vector;
  if (enif_get_resource(env, argv[0], RES_SPARSE_MATRIX, (void**)&matrix) &&
      enif_inspect_binary(env, argv[1], &input_vector)) {
    size_t vec_size = input_vector.size;
    uint8_t* result_vector = (uint8_t*)enif_make_new_binary(env, vec_size, &result);
    if(result_vector == NULL) {
      enif_raise_exception(env, ATOM_ENOMEM);
      return result;
    }
    int error = sm_v_multiply(matrix, vec_size, input_vector.data, result_vector);
    if(error) {
      enif_raise_exception(env, translate_error(error));
    }
  }
  else
    enif_raise_exception(env, ATOM_BADARG);
  return result;
}


static ERL_NIF_TERM bswz_dump_sparse_matrix(ErlNifEnv* env, int args, const ERL_NIF_TERM argv[]) {
  ERL_NIF_TERM result = ATOM_UNDEFINED;
  Smatrix* matrix;
  if(enif_get_resource(env, argv[0], RES_SPARSE_MATRIX, (void**)&matrix)) {
    // Get matrix size:
    size_t n_dim = enif_make_int64(env, matrix->n_dim);
    // Get matrix cells:
    ERL_NIF_TERM cells = enif_make_list(env, 0);
    for(size_t i = 0; i < matrix->n_cells; ++i) {
      Cell cell = matrix->cells[i];
      ERL_NIF_TERM diags = enif_make_list(env, 0);
      for(size_t j = cell.diags_offset; j<cell.diags_offset + cell.size; ++j) {
        Diaglet diag = matrix->diags[j];
        ERL_NIF_TERM diag_term = enif_make_tuple3(env,
                                                  ATOM_SMATRIX_DIAG,
                                                  enif_make_uint(env, diag.offset),
                                                  enif_make_uint64(env, diag.mask));
        diags = enif_make_list_cell(env, diag_term, diags);
      }
      ERL_NIF_TERM head = enif_make_tuple5(env,
                                           ATOM_SMATRIX_CELL,
                                           enif_make_uint64(env, cell.row),
                                           enif_make_uint64(env, cell.column),
                                           enif_make_uint(env, cell.size),
                                           diags);
      cells = enif_make_list_cell(env, head, cells);
    }
    // Make result:
    result = enif_make_tuple3(env, ATOM_SPARSE_MATRIX, n_dim, cells);
  }
  else
    enif_raise_exception(env, ATOM_BADARG);
  return result;
}


/* Traverse the list and transform Erlang tuples to elems. Return true on success. */
int parse_elem_list(ErlNifEnv* env, unsigned n_elems, ERL_NIF_TERM list, Elem* elems) {
  ERL_NIF_TERM head;
  ERL_NIF_TERM tail = list;
  int tuple_arity;
  const ERL_NIF_TERM* tuple;
  int elem_idx = 0;
  Elem elem;
  while(enif_get_list_cell(env, tail, &head, &tail)) {
    if( enif_get_tuple(env, head, &tuple_arity, &tuple) &&
        tuple_arity == 2 &&
        enif_get_uint64(env, tuple[0], &(elem.row)) &&
        enif_get_uint64(env, tuple[1], &(elem.column))) {
      elems[elem_idx++] = elem;
    }
    else
      return false;
  }
  return true;
}

static ERL_NIF_TERM bswz_make_sparse_matrix(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  uint64_t n_dim;
  unsigned n_elems;
  Elem* elems = NULL;
  ERL_NIF_TERM result = ATOM_UNDEFINED;
  if(enif_get_uint64(env, argv[0], &n_dim) &&
     enif_get_list_length(env, argv[1], &n_elems)) {
    /* Create a buffer for the elems: */
    elems = (Elem*)enif_alloc(n_elems * sizeof(Elem));
    if(elems == NULL) {
      enif_raise_exception(env, ATOM_ENOMEM);
      return result;
    }
    if(!parse_elem_list(env, n_elems, argv[1], elems)) {
      enif_raise_exception(env, ATOM_BADARG);
      goto cleanup1;
    }
    /* Create the sparse matrix: */
    Smatrix mtx;
    int error = make_sparse_matrix(n_dim, n_elems, elems, &mtx);
    if(error) {
      enif_raise_exception(env, translate_error(error));
      goto cleanup1;
    }
    Smatrix* resource = (Smatrix*)enif_alloc_resource(RES_SPARSE_MATRIX, sizeof(Smatrix));
    if(resource == NULL) {
      destroy_sparse_matrix(&mtx);
      enif_raise_exception(env, ATOM_ENOMEM);
      goto cleanup1;
    }
    /* Success: */
    *resource = mtx;
    result = enif_make_resource(env, resource);
    enif_release_resource(resource);
  }
  else {
    enif_raise_exception(env, ATOM_BADARG);
  }
 cleanup1:
  enif_free(elems);
  return result;
}

static void bswz_destroy_smatrix(ErlNifEnv* env, void* res) {
  if(res != NULL) {
    destroy_sparse_matrix((Smatrix*)res);
  }
}

static ErlNifFunc nif_funcs[] = {
  {"make_sparse_matrix", 2, bswz_make_sparse_matrix},
  {"sm_v_multiply", 2, bswz_sm_v_multiply},
  {"dump_sparse_matrix", 1, bswz_dump_sparse_matrix}
};

static int on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM loadinfo) {
  ErlNifResourceFlags res_flags = ERL_NIF_RT_CREATE;
  RES_SPARSE_MATRIX = enif_open_resource_type(env, NULL, "bitswizzle_sparse_matrix", &bswz_destroy_smatrix, res_flags, NULL);
#define ATOM(name, val)                                                 \
  {                                                                     \
    (name) = enif_make_atom(env, #val);                                 \
  }
  INIT_ATOMS
#undef ATOM
  return 0;
}

ERL_NIF_INIT(bitswizzle, nif_funcs, &on_load, NULL, NULL, NULL)
