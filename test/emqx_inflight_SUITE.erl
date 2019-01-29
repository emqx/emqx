%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_inflight_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> [t_inflight_all].

t_inflight_all(_) ->
    Empty = emqx_inflight:new(2),
    true = emqx_inflight:is_empty(Empty),
    2 = emqx_inflight:max_size(Empty),
    false = emqx_inflight:contain(a, Empty),
    none = emqx_inflight:lookup(a, Empty),
    try emqx_inflight:update(a, 1, Empty) catch
        error:Reason -> io:format("Reason: ~w~n", [Reason])
    end,
    0 = emqx_inflight:size(Empty),
    Inflight1 = emqx_inflight:insert(a, 1, Empty),
    Inflight2 = emqx_inflight:insert(b, 2, Inflight1),
    2 = emqx_inflight:size(Inflight2),
    true = emqx_inflight:is_full(Inflight2),
    {value, 1} = emqx_inflight:lookup(a, Inflight1),
    {value, 2} = emqx_inflight:lookup(a, emqx_inflight:update(a, 2, Inflight1)),
    false = emqx_inflight:contain(a, emqx_inflight:delete(a, Inflight1)),
    [1, 2] = emqx_inflight:values(Inflight2),
    [{a, 1}, {b ,2}] = emqx_inflight:to_list(Inflight2),
    [a, b] = emqx_inflight:window(Inflight2).
