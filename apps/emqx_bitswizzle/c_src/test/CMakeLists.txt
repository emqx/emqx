cmake_minimum_required(VERSION 3.10)

include(FindCriterion.cmake)

if(CRITERION_FOUND)
  message("Criterion found, will add 'make test' target to NIF")
  add_executable(run_test ../bitswizzle.cc test.cc)
  # target_compile_options(run_test PUBLIC -fno-omit-frame-pointer -g -O1 -fsanitize=address,leak,undefined)
  # target_link_options(run_test PUBLIC -fsanitize=address,leak,undefined)
  target_link_libraries(run_test -lcriterion)

  add_custom_target(test COMMAND ./run_test --timeout 1 DEPENDS run_test)
else()
  message(WARNING "Criterion NOT found, internal unit tests WON'T be run.
You can get it here: https://github.com/Snaipe/Criterion, or from your distro.
  Debian/Ubuntu: apt-get install libcriterion-dev
  Gentoo:        emerge -av dev-libs/criterion")

  add_custom_target(test COMMAND true)
endif()
