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

%% This methods are called via rpc by `emqx_ft_storage_fs`
%% They populate the call with actual storage which may be configured differently
%% on a concrete node.

-module(emqx_ft_storage_fs_proxy).

-export([
    list_local/2,
    pread_local/4,
    list_exports_local/0,
    read_export_file_local/2
]).

list_local(Transfer, What) ->
    emqx_ft_storage:with_storage_type(local, list, [Transfer, What]).

pread_local(Transfer, Frag, Offset, Size) ->
    emqx_ft_storage:with_storage_type(local, pread, [Transfer, Frag, Offset, Size]).

list_exports_local() ->
    case emqx_ft_storage:with_storage_type(local, exporter, []) of
        {emqx_ft_storage_exporter_fs, Options} ->
            emqx_ft_storage_exporter_fs:list_local(Options);
        InvalidExporter ->
            {error, {invalid_exporter, InvalidExporter}}
    end.

read_export_file_local(Filepath, CallerPid) ->
    case emqx_ft_storage:with_storage_type(local, exporter, []) of
        {emqx_ft_storage_exporter_fs, Options} ->
            emqx_ft_storage_exporter_fs:start_reader(Options, Filepath, CallerPid);
        InvalidExporter ->
            {error, {invalid_exporter, InvalidExporter}}
    end.
