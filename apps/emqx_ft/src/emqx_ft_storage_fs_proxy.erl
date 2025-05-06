%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
