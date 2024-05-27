#ifndef BITSWIZZLE_H
#define BITSWIZZLE_H

#include <stdlib.h>
#include <stdint.h>

/*  Errors: */
#define BSWZ_SUCCESS 0
/* Failed to allocate memory: */
#define BSWZ_ENOMEM 1
/* Coordinate is out of range: */
#define BSWZ_ECOORD 2
/* Size of the matrix is invalid */
#define BSWZ_EMATRIX_SIZE 3
/* Size of the vector is invalid */
#define BSWZ_EVECTOR_SIZE 4
/* Something is wrong with the matrix */
#define BSWZ_EBADMATRIX 5

typedef size_t Coord;

typedef struct {
  Coord row;
  Coord column;
} Elem;

/* Offset of the subdiagonal from the main diagonal of the cell: */
typedef uint8_t DiagOffset;
/* Bitmask that represents a diagonal of a cell: */
typedef uint64_t DiagMask;

/* Packed diagonal of the cell: */
typedef struct {
  DiagOffset offset;
  DiagMask mask;
} Diaglet;

typedef struct {
  /* Position of cell in the matrix: */
  Coord row;
  Coord column;
  /* Number of occupied diagonals in the cell: */
  DiagOffset size;
  /* Offset in the diags array where cell's data starts: */
  size_t diags_offset;
} Cell;

/* Sparse matrix */
typedef struct {
  size_t n_dim;
  size_t n_cells;
  Cell* cells;
  size_t n_diags;
  Diaglet* diags;
} Smatrix;

/** Create a sparse matrix of the transformation.

    Parameters:

    - `n_dim': Number of rows and columns in the matrix in _bits_. It
    should be a multiple of 64.

    - `n_elems': Size of the `elems' array.

    - `elems': Array that contains coordinates of the non-zero
      elements of the matrix (0-based).

    - `smatrix': Pointer to where the result will be stored.

    Return value:
    - BSWZ_SUCCESS on success
    - Other value on error
*/
int make_sparse_matrix(size_t n_dim, size_t n_elems, const Elem* elems, Smatrix* smatrix);

/** Destructor for the Smatrix object */
void destroy_sparse_matrix(Smatrix* matrix);

/** Multiply a sparse matrix to a vector.

    Parameters:

    - `matrix': Pointer to the matrix of transformation
    - `vec_n_bytes': Size of the input and result vectors, in bytes
    - `vector': Input vector
    - `result': Pointer to an array where the result will be stored
*/
int sm_v_multiply(const Smatrix* matrix, size_t vec_n_bytes, const uint8_t* vector, uint8_t* result);

/** Debugging: Print the sparse matrix to stderr */
void print_sparse_matrix(const Smatrix* matrix);

#endif
