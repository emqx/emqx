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

-module(emqx_ft_storage_dummy).

-behaviour(emqx_ft_storage).

-export([
    store_filemeta/3,
    store_segment/3,
    assemble/3
]).

store_filemeta(_Storage, _Transfer, _Meta) ->
    ok.

store_segment(_Storage, _Transfer, _Segment) ->
    ok.

assemble(_Storage, _Transfer, Callback) ->
    Pid = spawn(fun() -> Callback({error, not_implemented}) end),
    {ok, Pid}.
