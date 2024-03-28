# This file is licensed under the WTFPL version 2 -- you can see the full
# license over at http://www.wtfpl.net/txt/copying/
#
# - Try to find Criterion
#
# Once done this will define
#  CRITERION_FOUND - System has Criterion
#  CRITERION_INCLUDE_DIRS - The Criterion include directories
#  CRITERION_LIBRARIES - The libraries needed to use Criterion

find_package(PkgConfig)

find_path(CRITERION_INCLUDE_DIR criterion/criterion.h
          PATH_SUFFIXES criterion)

find_library(CRITERION_LIBRARY NAMES criterion libcriterion)

set(CRITERION_LIBRARIES ${CRITERION_LIBRARY})
set(CRITERION_INCLUDE_DIRS ${CRITERION_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIET and REQUIRED arguments and set CRITERION_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Criterion DEFAULT_MSG
                                  CRITERION_LIBRARY CRITERION_INCLUDE_DIR)

mark_as_advanced(CRITERION_INCLUDE_DIR CRITERION_LIBRARY)
