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

mod() ->
    case storage() of
        #{type := local} ->
            emqx_ft_storage_fs
        % emqx_ft_storage_dummy
    end.

storage() ->
    emqx_config:get([file_transfer, storage]).
