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

%% This methods are called via rpc by `emqx_ft_storage_exporter_fs`
%% They populate the call with actual storage which may be configured differently
%% on a concrete node.

-module(emqx_ft_storage_exporter_fs_proxy).

-export([
    list_exports_local/1,
    read_export_file_local/2
]).

list_exports_local(Query) ->
    emqx_ft_storage:with_storage_type(local, fun(Storage) ->
        case emqx_ft_storage_exporter:exporter(Storage) of
            {emqx_ft_storage_exporter_fs, Options} ->
                emqx_ft_storage_exporter_fs:list_local(Options, Query)
            % NOTE
            % This case clause is currently deemed unreachable by dialyzer.
            % InvalidExporter ->
            %     {error, {invalid_exporter, InvalidExporter}}
        end
    end).

read_export_file_local(Filepath, CallerPid) ->
    emqx_ft_storage:with_storage_type(local, fun(Storage) ->
        case emqx_ft_storage_exporter:exporter(Storage) of
            {emqx_ft_storage_exporter_fs, Options} ->
                emqx_ft_storage_exporter_fs:start_reader(Options, Filepath, CallerPid)
            % NOTE
            % This case clause is currently deemed unreachable by dialyzer.
            % InvalidExporter ->
            %     {error, {invalid_exporter, InvalidExporter}}
        end
    end).
