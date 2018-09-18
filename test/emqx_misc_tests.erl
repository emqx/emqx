%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_misc_tests).
-include_lib("eunit/include/eunit.hrl").

shutdown_disabled_test() ->
    with_env(
      [{conn_max_msg_queue_len, 0},
       {conn_max_total_heap_size, 0}],
      fun() ->
          self() ! foo,
          ?assertEqual(continue, conn_proc_mng_policy()),
          receive foo -> ok end,
          ?assertEqual(hibernate, conn_proc_mng_policy())
      end).

message_queue_too_long_test() ->
    with_env(
      [{conn_max_msg_queue_len, 1},
       {conn_max_total_heap_size, 0}],
      fun() ->
          self() ! foo,
          self() ! bar,
          ?assertEqual({shutdown, message_queue_too_long},
                       conn_proc_mng_policy()),
          receive foo -> ok end,
          ?assertEqual(continue, conn_proc_mng_policy()),
          receive bar -> ok end
      end).

total_heap_size_too_large_test() ->
    with_env(
      [{conn_max_msg_queue_len, 0},
       {conn_max_total_heap_size, 1}],
      fun() ->
          ?assertEqual({shutdown, total_heap_size_too_large},
                       conn_proc_mng_policy())
      end).

with_env(Envs, F) -> emqx_test_lib:with_env(Envs, F).

conn_proc_mng_policy() -> emqx_misc:conn_proc_mng_policy().
