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

-module(emqx_sequence_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_sequence, [generate/1, reclaim/1]).

all() ->
    [sequence_generate].

sequence_generate(_) ->
    ok = emqx_sequence:create(),
    ?assertEqual(1, generate(key)),
    ?assertEqual(2, generate(key)),
    ?assertEqual(3, generate(key)),
    ?assertEqual(2, reclaim(key)),
    ?assertEqual(1, reclaim(key)),
    ?assertEqual(0, reclaim(key)),
    ?assertEqual(false, ets:member(emqx_sequence, key)),
    ?assertEqual(1, generate(key)).

