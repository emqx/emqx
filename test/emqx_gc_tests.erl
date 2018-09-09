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

-module(emqx_gc_tests).

-include_lib("eunit/include/eunit.hrl").

trigger_by_cnt_test() ->
    Args = #{count => 2, bytes => 0},
    ok = emqx_gc:init(Args),
    ok = emqx_gc:inc(1, 1000),
    St1 = inspect(),
    ?assertMatch({_, Remain} when Remain > 0, maps:get(cnt, St1)),
    ok = emqx_gc:inc(2, 2),
    St2 = inspect(),
    ok = emqx_gc:inc(0, 2000),
    St3 = inspect(),
    ?assertEqual(St2, St3),
    ?assertMatch({N, N}, maps:get(cnt, St2)),
    ?assertNot(maps:is_key(oct, St2)),
    ok.

trigger_by_oct_test() ->
    Args = #{count => 2, bytes => 2},
    ok = emqx_gc:init(Args),
    ok = emqx_gc:inc(1, 1),
    St1 = inspect(),
    ?assertMatch({_, Remain} when Remain > 0, maps:get(oct, St1)),
    ok = emqx_gc:inc(2, 2),
    St2 = inspect(),
    ?assertMatch({N, N}, maps:get(oct, St2)),
    ?assertMatch({M, M}, maps:get(cnt, St2)),
    ok.

disabled_test() ->
    Args = #{count => -1, bytes => false},
    ok = emqx_gc:init(Args),
    ok = emqx_gc:inc(1, 1),
    ?assertEqual(#{}, inspect()),
    ok.

inspect() -> erlang:get(emqx_gc).
