#include "bitswizzle.h"
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include <stdio.h>

/* MAX_SIZE is the square root of 0xffffffffffffffff, the maximum
   number that fits in uint64_t. As long as dimensions of the vectors
   and matrices dont't exceed MAX_SIZE, uint64_t can fit number of
   matrix elements in _bits_.

   So most of the arithmetic that we do on the coordinates shouldn't
   overflow.
*/
#define MAX_SIZE 0x100000000

/* Sparse matrix is split into cells of fixed width that fits into the
   machine register. Height of the cell may vary. */
#define CELL_WIDTH 64

/*** Matrix multiplication: ***/

/* Rotate the bits in the input by the `rot'. Value of rot should be
   =< 64 */
inline uint64_t rotate_bits(DiagOffset rot, uint64_t input) {
  if(rot % CELL_WIDTH)
    return (input << rot) | (input >> (CELL_WIDTH - rot));
  else
    return input;
}

/** Multiply one cell of a sparse matrix to the corresponding part of
    the vector: */
uint64_t sm_v_multiply_cell(uint8_t n_diags, const Diaglet* diags, uint64_t input) {
  uint64_t result = 0;
  for(int i = 0; i < n_diags; ++i) {
    Diaglet d = diags[i];
    result |= rotate_bits(d.offset, input) & d.mask;
#ifdef DEBUG2
    fprintf(stderr, "mul_cell: offset=%d mask=0x%016lx input=0x%016lx -> 0x%016lx\n",
            d.offset, d.mask, input, rotate_bits(d.offset, input));
#endif
  }
  return result;
}

/** In-place matrix to vector multiplication.

 * vec_n_bytes is size of the input vector in BYTES (not bits!)
 * Resulting product will be stored in `result'.
 * Both `vector' and `result' must be at least as large as `vec_n_bytes'

   Returns 0 on success, or non-zero error code on error.
 */
int sm_v_multiply(const Smatrix* matrix, size_t vec_n_bytes, const uint8_t* vector, uint8_t* result) {
  if(vec_n_bytes >= (MAX_SIZE / 8)  ||    /* Avoid overflow in the following checks */
     vec_n_bytes % 8 != 0           ||    /* Conversion from uint8_t to uint64_t would be unsafe */
     vec_n_bytes * 8 != matrix->n_dim) {  /* Mismatched size */
    return BSWZ_EVECTOR_SIZE;
  }
  /* Zero the result vector: */
  memset(result, 0, vec_n_bytes);
  /* Loop over occupied matrix cells: */
  /* TODO: support Big Endian: */
  uint64_t* res = (uint64_t*)result;
  const uint64_t* vec = (uint64_t*)vector;
  size_t max_idx = vec_n_bytes / 8;
  for(size_t i = 0; i < matrix->n_cells; ++i) {
    Cell cell = matrix->cells[i];
    if(cell.row >= max_idx || cell.column >= max_idx) {
      return BSWZ_EBADMATRIX;
    }
    uint64_t src = vec[cell.column];
    size_t res_idx = cell.row;
    uint64_t p = sm_v_multiply_cell(cell.size, matrix->diags + cell.diags_offset, src);
#ifdef DEBUG
    fprintf(stderr, "cell[%ld %ld] vec[%lu] = 0x%016lx (result) 0x%016lx |= (p) 0x%016lx\n",
            cell.row, cell.column, cell.column, src, res[res_idx], p);
#endif
    res[res_idx] |= p;
  }
  return 0;
}

/*** Making sparse matrix: ***/

Coord cell_column(Elem e) {
  return e.column / CELL_WIDTH;
}

Coord cell_row(Elem e) {
  return e.row / CELL_WIDTH;
}

/** Calculate offset of the e's diagonal from the main diagonal of the
    cell:
*/
DiagOffset diag_of_elem(Elem e) {
  /* Calculate coordinates of elems within the cell: */
  int row = e.row % CELL_WIDTH;
  int col = e.column % CELL_WIDTH;
  /* Calculate offset of the diagonal where the element is stored: */
  if(row < col)
    return CELL_WIDTH - col + row;
  else
    return row - col;
}

/** Calculate bitmask that should be applied to the diaglet: */
DiagMask mask_of_elem(Elem e) {
  DiagOffset row = e.row % CELL_WIDTH;
  return (DiagMask)1 << row;
}

/** Create a cell of a sparse matrix, and store diagonals data in
    `diags' buffer at the positions starting from `diags_offset'.

    Important: the caller must ensure that `diags' array size is at
    least `diags_offset + 64'.

    Return index of the last element written to the `diags' buffer.
*/
size_t fill_cell(size_t row, size_t column,
                 const Elem* elems, size_t start_elem, size_t last_elem,
                 size_t diags_offset, Diaglet* diags,
                 Cell* cell) {
  uint64_t dense_cell[CELL_WIDTH] = {0};
  /* 1. Apply bitmasks of all elems to the dense matrix cell: */
  for(size_t i = start_elem; i < last_elem; ++i) {
    DiagOffset offset = diag_of_elem(elems[i]);
    DiagMask mask = mask_of_elem(elems[i]);
    dense_cell[offset] |= mask;
  }
  /* 2. Store non-zero diaglets in the output array: */
  size_t n = diags_offset;
  for(DiagOffset offset = 0; offset < CELL_WIDTH; ++offset) {
    uint64_t mask = dense_cell[offset];
    if(mask) {
      Diaglet d = {offset, mask};
      diags[n++] = d;
    }
  }
  *cell = {row, column, (DiagOffset)(n - diags_offset), diags_offset};
  return n;
}

/** Try to create a sparse matrix from an array of coordinates of
    occupied positions.

    The caller must supply the buffers for the cells and diagonals.

    Returns `false' on success, or `true' if the the caller needs to
    retry with a larger buffer.

    The resulting size of cells and diags buffer is written to
    `n_cells' and `n_diags' respectively.
*/
int do_fill_sparse_matrix(int n_dim,
                          size_t n_elems, const Elem* elems,
                          size_t* n_cells, Cell* cells,
                          size_t* n_diags, Diaglet* diags) {
  size_t cell_buf_size = *n_cells;
  size_t diag_buf_size = *n_diags;
  /* Reset counters: */
  *n_cells = 0;
  *n_diags = 0;
  /* Index of the element at the beginning of the cell: */
  size_t start_elem = 0;
  size_t prev_row = cell_row(elems[0]);
  size_t prev_column = cell_column(elems[0]);
  /* Loop over elements, looking for the cell boundaries: */
  for(size_t i = 0; i < n_elems; ++i) {
    size_t row = cell_row(elems[i]);
    size_t column = cell_column(elems[i]);
    bool last_cell = i + 1 == n_elems;
    /* Is it the end of the previos cell? */
    if(last_cell || prev_row != row || prev_column != column) {
      /* This is a new cell, or the last element */
      /* Check if the buffers are large enough to fit the cell
         data: */
      if((*n_cells) >= cell_buf_size ||
         (*n_diags) + CELL_WIDTH >= diag_buf_size) {
        /* Caller must retry with larger buffers: */
        *n_cells = cell_buf_size + 1;
        *n_diags = diag_buf_size + CELL_WIDTH;
        return true;
      }
      /* Finalize current cell: */
      Cell c = {0};
      (*n_diags) = fill_cell(prev_row, prev_column,
                             elems, start_elem, (last_cell)? i + 1 : i,
                             *n_diags, diags,
                             &c);
      cells[(*n_cells)++] = c;
      /* Start new cell: */
      prev_row = row;
      prev_column = column;
      start_elem = i;
    }
  }
  return false;
}

/** Version of realloc that frees the original buffer on failure,
    instead of leaving it to linger.
*/
void* try_realloc(void* ptr, size_t size, size_t elem_size) {
  void *new_ptr = realloc(ptr, size * elem_size);
  if(new_ptr == NULL) {
    // Realloc acts as free when size = 0. Avoid double free:
    if(size > 0)
      free(ptr);
  }
  return new_ptr;
}

/** `elems' is an array of coordinates of non-zero matrix elements,
    grouped by the cell.

    Returns 0 on success, or non-zero error code.
 */
int fill_sparse_matrix(size_t n_dim, size_t n_elems, const Elem* elems, Smatrix* smatrix) {
  /* Note on the implementation: size of the compressed sparse matrix
     is not known in advance. To work around this problem, we split
     the procedure into two subroutines:

     - This outer function only deals with the memory allocation. It
     starts with a buffer of certain length that is presented to
     the inner function that implements the business logic.

     - If the inner function finds out that the buffer size is not
     sufficient for the data, it updates the values of suggested
     buffer size and exits.

     - The outer function then retries.

     - Finally, size of the buffer is shrunk to fit the data
     perfectly.

     This procedure might be relatively heavy in some pathological
     cases, but we currently don't expect matrix creation to occur
     often.

     In theory, it should be possible to restart the inner routine
     where it left off, but it's currently not done for simplicity. */

  /* Increase buffer by these values: */
  #ifdef TEST_REALLOC
  /* Excercise the loop in test mode: */
  size_t cell_buf_incr = 0;
  size_t diag_buf_incr = 0;
  #else
  /* Use sane values otherwise */
  size_t cell_buf_incr = 16;
  size_t diag_buf_incr = 256;
  #endif
  /* Initial values of buffers: */
  size_t n_cells = 0;
  size_t n_diags = 0;
  /* Buffers: */
  Cell *cells = NULL;
  Diaglet *diags = NULL;
  /* Try to fill the buffers: */
  do{
    /* Grow cell buffer: */
    n_cells += cell_buf_incr;
    cells = (Cell*)try_realloc((void*)cells, n_cells, sizeof(Cell));
    if(cells == NULL) goto fail;
    /* Grow diag buffer: */
    n_diags += diag_buf_incr;
    diags = (Diaglet*)try_realloc((void*)diags, n_diags, sizeof(Diaglet));
    if(diags == NULL) goto fail;
    /* Run business logic: */
  } while(do_fill_sparse_matrix(n_dim,
                                n_elems, elems,
                                &n_cells, cells,
                                &n_diags, diags));
  /* Success. Shrink the buffers according to the actual sizes: */
  cells = (Cell*)try_realloc((void*)cells, n_cells, sizeof(Cell));
  if(cells == NULL) goto fail;
  diags = (Diaglet*)try_realloc((void*)diags, n_diags, sizeof(Diaglet));
  if(diags == NULL) goto fail;
  /* Matrix object takes the ownership of the buffers: */
  (*smatrix) = {n_dim, n_cells, cells, n_diags, diags};
  return BSWZ_SUCCESS;
 fail:
  /* Matrix object hasn't taken ownership of the buffers, so we need
     to dispose of them: */
  free(diags);
  free(cells);
  return BSWZ_ENOMEM;
}

/** Compare matrix elements according to the position of their cell: */
static int compare_elems(const void* ap, const void* bp) {
  /* Group elements according to the coordinates of their cell: */
  Elem a = *(const Elem*)ap;
  Elem b = *(const Elem*)bp;
  Coord a_row = cell_row(a);
  Coord a_col = cell_column(a);
  Coord b_row = cell_row(b);
  Coord b_col = cell_column(b);

  if(a_row < b_row) return -1;
  if(a_row > b_row) return 1;
  if(a_col < b_col) return -1;
  if(a_col > b_col) return 1;
  return 0;
}

/** Check sanity of the input: */
int make_sparse_matrix_validate_input(size_t n_dim, size_t n_elems, const Elem* elems) {
  /* Should be first to avoid badarith in the following checks: */
  if(n_dim >= MAX_SIZE)
    return BSWZ_EMATRIX_SIZE;
  /* Vector size doesn't align with the cell size; it's up to the user
     to pad vectors with zeros and trim them */
  if(n_dim % CELL_WIDTH)
    return BSWZ_EMATRIX_SIZE;
  /* Something's wrong with the input elems: */
  if(n_elems < 0)
    return BSWZ_EBADMATRIX;
  /* Check elements: */
  for(size_t i = 0; i < n_elems; ++i) {
    Elem e = elems[i];
    if(e.row >= n_dim || e.column >= n_dim)
      return BSWZ_ECOORD;
  }
  return 0;
}

int make_sparse_matrix(size_t n_dim, size_t n_elems, const Elem* elems, Smatrix* smatrix) {
  int error = make_sparse_matrix_validate_input(n_dim, n_elems, elems);
  if(error)
    return error;
  /* Early cutoff of trivial cases: */
  if(n_elems == 0 || n_dim == 0) {
    *smatrix = {n_dim, 0, NULL, 0, NULL};
    return 0;
  }
  /* Real logic starts here: */
  /* 1. Sort elements according to their cell (actually, just grouping
     them would be good enough): */
  Elem* sorted_elems = (Elem*)malloc(n_elems * sizeof(Elem));
  if(sorted_elems == NULL)
    return BSWZ_ENOMEM;
  memcpy(sorted_elems, elems, n_elems * sizeof(Elem));
  qsort(sorted_elems, n_elems, sizeof(Elem), compare_elems);
  /* 2. Fill the matrix: */
  error = fill_sparse_matrix(n_dim, n_elems, sorted_elems, smatrix);
  free(sorted_elems);
  return error;
}

void destroy_sparse_matrix(Smatrix* matrix) {
  if(matrix != NULL) {
    free(matrix->cells);
    free(matrix->diags);
    matrix->cells = NULL;
    matrix->diags = NULL;
  }
}

/** Debug */

void print_sparse_matrix(const Smatrix* matrix) {
  if(matrix != NULL) {
    fprintf(stderr, "Matrix @ 0x%p: n_dim = %ld n_cells = %ld\n",
            (void*)matrix, matrix->n_dim, matrix->n_cells);
    for(size_t i = 0; i < matrix->n_cells; ++i) {
      Cell cell = matrix->cells[i];
      fprintf(stderr,
              "  Cell[%lx] position={%lx %lx} size = %u:\n",
              i, cell.row, cell.column, cell.size);
      for(DiagOffset j = 0; j < cell.size; ++j) {
        Diaglet diag = matrix->diags[cell.diags_offset + j];
        fprintf(stderr, "    d[%02d] %02d 0x%016lx\n",
                j, diag.offset, diag.mask);
      }
    }
  }
}
