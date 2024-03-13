# -*- cmake -*-
cmake_minimum_required(VERSION 3.10)

project(bitswizzle)

add_compile_options(-Wall -Werror -pedantic -fdiagnostics-path-format=separate-events)
set(CMAKE_C_STANDARD 99)

set(LIBRARY_OUTPUT_DIRECTORY "${PROJECT_DIR}/_priv")

if (DEFINED ENV{Erlang_OTP_ROOT_DIR})
  set(Erlang_OTP_ROOT_DIR $ENV{Erlang_OTP_ROOT_DIR})
else()
  execute_process(
    COMMAND         erl -noshell -eval "io:format(\"~s\", [code:root_dir()])" -s erlang halt
    OUTPUT_VARIABLE Erlang_OTP_ROOT_DIR
  )
endif()

option(ENABLE_NIF "Enable Erlang NIF generations" ON)
option(TEST "Build in test mode" OFF)

set(MySrcList bitswizzle.cc)
if(ENABLE_NIF)
  list(APPEND MySrcList bitswizzle_nif.cc)
endif()


add_library(bitswizzle SHARED ${MySrcList})

if(ENABLE_NIF)
  target_include_directories(bitswizzle PRIVATE ${Erlang_OTP_ROOT_DIR}/usr/include/)
endif()

# Testing:
if(TEST)
  add_compile_options(-fno-omit-frame-pointer -g -O1 -fsanitize=address,leak,undefined)
  add_compile_definitions(TEST)
  add_link_options(-fsanitize=address,leak,undefined)
  add_subdirectory(test)
endif()
