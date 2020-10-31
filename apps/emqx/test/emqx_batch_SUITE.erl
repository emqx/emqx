%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------

-module(emqx_batch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_batch_full_commit(_) ->
    B0 = emqx_batch:init(#{batch_size => 3,
                           linger_ms => 2000,
                           commit_fun => fun(_) -> ok end
                          }),
    B3 = lists:foldl(fun(E, B) -> emqx_batch:push(E, B) end, B0, [a, b, c]),
    ?assertEqual(3, emqx_batch:size(B3)),
    ?assertEqual([a, b, c], emqx_batch:items(B3)),
    %% Trigger commit fun.
    B4 = emqx_batch:push(a, B3),
    ?assertEqual(0, emqx_batch:size(B4)),
    ?assertEqual([], emqx_batch:items(B4)).

t_batch_linger_commit(_) ->
    CommitFun = fun(Q) -> ?assertEqual(3, length(Q)) end,
    B0 = emqx_batch:init(#{batch_size => 3,
                           linger_ms => 500,
                           commit_fun => CommitFun
                          }),
    B3 = lists:foldl(fun(E, B) -> emqx_batch:push(E, B) end, B0, [a, b, c]),
    ?assertEqual(3, emqx_batch:size(B3)),
    ?assertEqual([a, b, c], emqx_batch:items(B3)),
    receive
        batch_linger_expired ->
            B4 = emqx_batch:commit(B3),
            ?assertEqual(0, emqx_batch:size(B4)),
            ?assertEqual([], emqx_batch:items(B4))
    after
        1000 ->
            error(linger_timer_not_triggered)
    end.

