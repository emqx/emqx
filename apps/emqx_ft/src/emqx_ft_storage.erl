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

-module(emqx_ft_storage).

-export(
    [
        store_filemeta/2,
        store_segment/2,
        assemble/2
    ]
).

-export([list_local/2]).
-export([pread_local/4]).

-export([local_transfers/0]).

-type offset() :: emqx_ft:offset().
-type transfer() :: emqx_ft:transfer().

-type storage() :: emqx_config:config().

-export_type([assemble_callback/0]).

-type assemble_callback() :: fun((ok | {error, term()}) -> any()).

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

-callback store_filemeta(storage(), emqx_ft:transfer(), emqx_ft:filemeta()) ->
    ok | {error, term()}.
-callback store_segment(storage(), emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {error, term()}.
-callback assemble(storage(), emqx_ft:transfer(), assemble_callback()) ->
    {ok, pid()} | {error, term()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_filemeta(emqx_ft:transfer(), emqx_ft:filemeta()) ->
    ok | {error, term()}.
store_filemeta(Transfer, FileMeta) ->
    Mod = mod(),
    Mod:store_filemeta(storage(), Transfer, FileMeta).

-spec store_segment(emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {error, term()}.
store_segment(Transfer, Segment) ->
    Mod = mod(),
    Mod:store_segment(storage(), Transfer, Segment).

-spec assemble(emqx_ft:transfer(), assemble_callback()) ->
    {ok, pid()} | {error, term()}.
assemble(Transfer, Callback) ->
    Mod = mod(),
    Mod:assemble(storage(), Transfer, Callback).

%%--------------------------------------------------------------------
%% Local FS API
%%--------------------------------------------------------------------

-type filefrag() :: emqx_ft_storage_fs:filefrag().
-type transferinfo() :: emqx_ft_storage_fs:transferinfo().

-spec list_local(transfer(), fragment | result) ->
    {ok, [filefrag()]} | {error, term()}.
list_local(Transfer, What) ->
    with_local_storage(
        fun(Mod, Storage) -> Mod:list(Storage, Transfer, What) end
    ).

-spec pread_local(transfer(), filefrag(), offset(), _Size :: non_neg_integer()) ->
    {ok, [filefrag()]} | {error, term()}.
pread_local(Transfer, Frag, Offset, Size) ->
    with_local_storage(
        fun(Mod, Storage) -> Mod:pread(Storage, Transfer, Frag, Offset, Size) end
    ).

-spec local_transfers() ->
    {ok, node(), #{transfer() => transferinfo()}} | {error, term()}.
local_transfers() ->
    with_local_storage(
        fun(Mod, Storage) -> Mod:transfers(Storage) end
    ).

%%

mod() ->
    mod(storage()).

mod(Storage) ->
    case Storage of
        #{type := local} ->
            emqx_ft_storage_fs
        % emqx_ft_storage_dummy
    end.

storage() ->
    emqx_config:get([file_transfer, storage]).

with_local_storage(Fun) ->
    case storage() of
        #{type := local} = Storage ->
            Fun(mod(Storage), Storage);
        #{type := Type} ->
            {error, {unsupported_storage_type, Type}}
    end.
