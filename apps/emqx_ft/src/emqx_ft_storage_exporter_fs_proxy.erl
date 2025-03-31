%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
