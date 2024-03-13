#include <stdlib.h>

#include <criterion/criterion.h>
#include <criterion/new/assert.h>
#include <criterion/theories.h>

#include "../bitswizzle.h"

/* Helper functions */

#define NELEMS(x)  (sizeof(x) / sizeof((x)[0]))

union Vec1 {
  uint64_t number;
  uint8_t bytes[sizeof(uint64_t)];
};

typedef size_t (*MatrixType)(size_t size, size_t column);

void make_orthogonal_matrix(MatrixType row_fun, size_t size, Smatrix* matrix, char transposed) {
  Elem* elems = (Elem*)malloc(size * sizeof(Elem));
  cr_assert(elems != NULL);
  for(size_t i = 0; i < size; ++i) {
    Coord row = row_fun(size, i);
    Coord col = i;
    if(transposed)
      elems[i] = {col, row};
    else
      elems[i] = {row, col};
  }
  cr_assert(zero(make_sparse_matrix(size, size, elems, matrix)),
            "Failed to create a matrix");
  free(elems);
}

void mvmul(const Smatrix* mtx, size_t n_cells, const Vec1* src, Vec1* result) {
  cr_assert(zero(sm_v_multiply(mtx, n_cells * 8, (uint8_t*)src, (uint8_t*)result)),
            "Failed to multiply");
}


/* Functions that define various orthogonal matrices */

static Coord identity(size_t size, Coord column) {
  return column;
}

static Coord inverse(size_t size, Coord column) {
  return size - column - 1;
}

static Coord swap_bytes(size_t size, Coord column) {
  Coord half = size / 2;
  if(column < half)
    return column + half;
  else
    return column - half;
}

/* Lebesgue curve, also known as Morton order, or z-order curve: */
static Coord lebesgue(size_t size, Coord column) {
  Coord half = size / 2;
  if(column < half)
    return column * 2;
  else
    return (column - half) * 2 + 1;
}

/* Tests */

Test(make, empty) {
  Smatrix mat = {0};
  cr_expect(eq(int, make_sparse_matrix(64, 0, NULL, &mat), 0), "Error code");
  destroy_sparse_matrix(&mat);
}

Test(make, nil) {
  Smatrix mat = {0};
  cr_expect(eq(int, make_sparse_matrix(0, 0, NULL, &mat), 0), "Error code");
  destroy_sparse_matrix(&mat);
}

Test(make, singleton) {
  Smatrix mat = {0};
  Elem elems[1] = {0, 0};
  cr_assert(zero(int, make_sparse_matrix(64, 1, elems, &mat)),
            "Error code");
  cr_expect(eq(int, mat.n_cells, 1));
  cr_assert(eq(int, mat.n_diags, 1));
  cr_expect(eq(int, mat.diags[0].mask, 1));
  cr_expect(eq(int, mat.diags[0].offset, 0));
  destroy_sparse_matrix(&mat);
}

Test(make, identity) {
  Smatrix mat = {0};
  make_orthogonal_matrix(identity, 64, &mat, false);
  cr_expect(eq(int, mat.n_dim, 64),
            "Dimension of the matrix is incorrect");
  cr_expect(eq(int, mat.n_cells, 1),
            "Non-zero matrix of size 64 should always have 1 cell");
  /* Check the cell: */
  cr_expect(eq(int, mat.cells[0].column, 0));
  cr_expect(eq(int, mat.cells[0].row, 0));
  cr_expect(eq(int, mat.cells[0].size, 1),
            "Only one diagonal must be occupied in the identity matrix");
  cr_expect(eq(int, mat.cells[0].diags_offset, 0));
  /* Check the diaglet: */
  cr_expect(eq(int, mat.diags[0].offset, 0));
  uint64_t mask = mat.diags[0].mask;
  cr_expect(eq(u64, mask, 0xffffffffffffffff),
            "Unexpected mask: %016lx", mask);
  destroy_sparse_matrix(&mat);
}

Test(make, identity2cell) {
  /* This test makes a diagonal identity matrix, transforms it to a
     compressed sparse matrix with 2 occupied cells, and then verfies
     the contents of each component of the resulting sparse matrix */
  Smatrix mat = {0};
  make_orthogonal_matrix(identity, 128, &mat, false);
  print_sparse_matrix(&mat);
  cr_expect(eq(int, mat.n_dim, 128),
            "Dimension of matrix is incorrect");
  cr_expect(eq(int, mat.n_cells, 2),
            "Number of cells in a diagonal matrix of size 128 = 2");
  /* Check the cells: */
  for(int c = 0; c <= 1; ++c) {
    cr_expect(eq(int, mat.cells[c].column, c));
    cr_expect(eq(int, mat.cells[c].row, c));
    cr_expect(eq(int, mat.cells[c].size, 1),
              "Only one diagonal must be occupied in the identity matrix");
    cr_expect(eq(int, mat.cells[c].diags_offset, c));
  }
  /* Check the diaglets: */
  for(int d = 0; d <= 1; ++d) {
    cr_expect(eq(int, mat.diags[d].offset, 0),
              "Offset [%d]", d);
    uint64_t mask = mat.diags[d].mask;
    cr_expect(eq(u64, mask, 0xffffffffffffffff),
              "Unexpected mask [%d]: %016lx", d, mask);
  }
  destroy_sparse_matrix(&mat);
}


Test(make, 4cell) {
  /* Test creation of a matrix where all cells are occupied: */
  Smatrix mat = {0};
  Elem elems[7] = {{0, 0}, {0, 64}, {0, 127}, {64, 0}, {64, 64}, {64, 127}, {127, 127}};
  cr_assert(zero(make_sparse_matrix(128, NELEMS(elems), elems, &mat)),
            "Error code");
  cr_expect(eq(int, mat.n_cells, 4));
  destroy_sparse_matrix(&mat);
}

Test(make, inverse) {
  Smatrix mat = {0};
  make_orthogonal_matrix(inverse, 64, &mat, false);
  cr_expect(eq(int, mat.n_dim, 64));
  cr_assert(eq(int, mat.n_cells, 1));
  cr_expect(eq(int, mat.cells[0].size, 32),
            "All odd-numbered diagonals should be occupied in the inversed identity matrix");
  destroy_sparse_matrix(&mat);
}

Test(make_fail, matrix_size) {
  Smatrix mat = {0};
  Elem elems[1] = {{0, 0}};
  cr_expect(eq(int,
               make_sparse_matrix(32, 1, elems, &mat),
               BSWZ_EMATRIX_SIZE),
            "Matrix size should be aligned to cell size");
  cr_expect(eq(int,
               make_sparse_matrix(-64, 1, elems, &mat),
               BSWZ_EMATRIX_SIZE),
            "make_sparse_matrix should reject input");
}

Test(make_fail, elem) {
  Smatrix mat = {0};
  Elem elems[1] = {{128, 0}};
  cr_expect(eq(int,
               make_sparse_matrix(64, 1, elems, &mat),
               BSWZ_ECOORD),
            "Negative row id");
  elems[0] = {0, 128};
  cr_expect(eq(int,
               make_sparse_matrix(64, 1, elems, &mat),
               BSWZ_ECOORD),
            "Negative column id");
  elems[0] = {64, 0};
  cr_expect(eq(int,
               make_sparse_matrix(64, 1, elems, &mat),
               BSWZ_ECOORD),
            "Row id exceeds matrix size");
  elems[0] = {0, 64};
  cr_expect(eq(int,
               make_sparse_matrix(64, 1, elems, &mat),
               BSWZ_ECOORD),
            "Column id exceeds matrix size");
}

TheoryDataPoints(mvmul, single_bit) = {
  DataPoints(uint8_t, 0, 1, 7, 63),
  DataPoints(uint8_t, 0, 1, 7, 63),
};

Theory((uint8_t from, uint8_t to), mvmul, single_bit) {
  Smatrix mtx = {0};
  Elem elems[1] = {to, from};
  cr_assert(zero(int, make_sparse_matrix(64, 1, elems, &mtx)),
            "Failed to create matrix");
  Vec1 vec[1] = {(uint64_t)1 << from};
  Vec1 out[1];
  mvmul(&mtx, 1, vec, out);
  cr_expect(eq(int, out[0].number, (uint64_t)1 << to),
            "%d-th bit of the input vector must be 1", to);
  destroy_sparse_matrix(&mtx);
}


Test(mvmul, swap_bytes) {
  // This is a simple sanity check of matrix*vector multiplication
  // when it comes to cells located off main diagonal.
   Smatrix mtx = {0};
  make_orthogonal_matrix(swap_bytes, 128, &mtx, false);
  print_sparse_matrix(&mtx);
  //              LSB                 MSB
  Vec1 src[2] = { 0xaaaaaaaaaaaaaaaa, 0xbbbbbbbbbbbbbbbb};
  Vec1 out[2];
  mvmul(&mtx, 2, src, out);
  cr_expect(eq(u64, out[0].number, 0xbbbbbbbbbbbbbbbb));
  cr_expect(eq(u64, out[1].number, 0xaaaaaaaaaaaaaaaa));
  destroy_sparse_matrix(&mtx);
}


Test(mvmul, lebesgue) {
  Smatrix mtx = {0};
  make_orthogonal_matrix(lebesgue, 64, &mtx, false);
  print_sparse_matrix(&mtx);

  Vec1 src[1] = { (uint64_t)0xffffffff << 32 };
  Vec1 out[1];
  mvmul(&mtx, 1, src, out);
  cr_expect(eq(u64, out[0].number, 0xaaaaaaaaaaaaaaaa));

  src[0].number = 0x00000000ffffffff;
  mvmul(&mtx, 1, src, out);
  cr_expect(eq(u64, out[0].number, 0x5555555555555555));
  destroy_sparse_matrix(&mtx);
}

Test(mvmul, lebesgue2c) {
  Smatrix mtx = {0};
  make_orthogonal_matrix(lebesgue, 128, &mtx, false);
  print_sparse_matrix(&mtx);
  //             LSB  MSB
  Vec1 src[2] = { 0, 0xffffffffffffffff};
  Vec1 out[2];
  mvmul(&mtx, 2, src, out);
  cr_expect(eq(u64, out[0].number, 0xaaaaaaaaaaaaaaaa));
  cr_expect(eq(u64, out[1].number, 0xaaaaaaaaaaaaaaaa));

  src[0].number = 0xffffffffffffffff; // LSB
  src[1].number = 0;                  // MSB
  mvmul(&mtx, 2, src, out);
  cr_expect(eq(u64, out[0].number, 0x5555555555555555));
  cr_expect(eq(u64, out[1].number, 0x5555555555555555));

  destroy_sparse_matrix(&mtx);
}


#define VEC_DATAPOINTS 0, 0xffffffffffffffff

#define VEC_DATAPOINTS_RAND               \
    0xff0f0f0f0f0f0f0f,                   \
    0xf0f0f0f0f0f0f0ff,                   \
    0xaaaaaaaaaaaaaaaa,                   \
    0x5555555555555555,                   \
    0x21696e686572656e,                   \
    0x7420617574686f72,                   \
    0x6974797c7c6f626c,                   \
    0x69676174696f6e2e

TheoryDataPoints(mvmul, ortho_trans) = {
  DataPoints(MatrixType, identity, inverse, swap_bytes),
  /* Size of the matrix, in cells */
  DataPoints(int, 0, 1, 2),
  /* Following arke components of the vector */
  DataPoints(uint64_t, VEC_DATAPOINTS, VEC_DATAPOINTS_RAND),
  DataPoints(uint64_t, VEC_DATAPOINTS, VEC_DATAPOINTS_RAND)
};

Theory((MatrixType type, int n_cells, uint64_t v1, uint64_t v2), mvmul, ortho_trans) {
  /* Input vector: */
  Vec1 input_vector[3];
  Vec1 out_vector1[3];
  Vec1 out_vector2[3];
  input_vector[0].number = v1;
  input_vector[1].number = v2;

  /* Make matrices: */
  Smatrix mtx;
  make_orthogonal_matrix(type, n_cells * 64, &mtx, false);
  Smatrix inv;
  make_orthogonal_matrix(type, n_cells * 64, &inv, true);

  /* Multiply: */
  mvmul(&mtx, n_cells, input_vector, out_vector1);
  mvmul(&mtx, n_cells, out_vector1, out_vector2);
  /* Check that out_vector2 = src: */
  for(int i = 0; i<n_cells; ++i) {
    uint64_t src = input_vector[i].number;
    uint64_t res = out_vector2[i].number;
    cr_expect(eq(u64, res, src),
              "Non orthogonal transform chain for cell %d:\n"
              "0: 0x%016lx\n"
              "1: 0x%016lx\n"
              "2: 0x%016lx", i, src, out_vector1[i], res);
  }

  /* Cleanup: */
  destroy_sparse_matrix(&mtx);
  destroy_sparse_matrix(&inv);
}
