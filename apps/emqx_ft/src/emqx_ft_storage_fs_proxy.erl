%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This methods are called via rpc by `emqx_ft_storage_fs`
%% They populate the call with actual storage which may be configured differently
%% on a concrete node.

-module(emqx_ft_storage_fs_proxy).

-export([
    list_local/2,
    pread_local/4,
    lookup_local_assembler/1
]).

list_local(Transfer, What) ->
    emqx_ft_storage:with_storage_type(local, list, [Transfer, What]).

pread_local(Transfer, Frag, Offset, Size) ->
    emqx_ft_storage:with_storage_type(local, pread, [Transfer, Frag, Offset, Size]).

lookup_local_assembler(Transfer) ->
    emqx_ft_storage:with_storage_type(local, lookup_local_assembler, [Transfer]).
