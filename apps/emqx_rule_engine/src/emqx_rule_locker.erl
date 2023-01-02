%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_locker).

-export([start_link/0]).

-export([ lock/1
        , unlock/1
        ]).

start_link() ->
    ekka_locker:start_link(?MODULE).

-spec(lock(binary()) -> ekka_locker:lock_result()).
lock(Id) ->
    ekka_locker:acquire(?MODULE, Id, local).

-spec(unlock(binary()) -> {boolean(), [node()]}).
unlock(Id) ->
    ekka_locker:release(?MODULE, Id, local).
