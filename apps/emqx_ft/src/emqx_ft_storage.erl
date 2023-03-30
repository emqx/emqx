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
        child_spec/0,

        store_filemeta/2,
        store_segment/2,
        assemble/2,

        files/0,

        with_storage_type/2,
        with_storage_type/3
    ]
).

-type storage() :: emqx_config:config().

-export_type([assemble_callback/0]).
-export_type([file_info/0]).
-export_type([export_data/0]).
-export_type([reader/0]).

-type assemble_callback() :: fun((ok | {error, term()}) -> any()).

-type file_info() :: #{
    transfer := emqx_ft:transfer(),
    name := file:name(),
    size := _Bytes :: non_neg_integer(),
    uri => uri_string:uri_string(),
    meta => emqx_ft:filemeta()
}.

-type export_data() :: binary() | qlc:query_handle().
-type reader() :: pid().

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

%% NOTE
%% An async task will wait for a `kickoff` message to start processing, to give some time
%% to set up monitors, etc. Async task will not explicitly report the processing result,
%% you are expected to receive and handle exit reason of the process, which is
%% -type result() :: `{shutdown, ok | {error, _}}`.

-callback store_filemeta(storage(), emqx_ft:transfer(), emqx_ft:filemeta()) ->
    ok | {async, pid()} | {error, term()}.
-callback store_segment(storage(), emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {async, pid()} | {error, term()}.
-callback assemble(storage(), emqx_ft:transfer(), _Size :: emqx_ft:bytes()) ->
    ok | {async, pid()} | {error, term()}.

-callback files(storage()) ->
    {ok, [file_info()]} | {error, term()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec child_spec() ->
    [supervisor:child_spec()].
child_spec() ->
    try
        Mod = mod(),
        Mod:child_spec(storage())
    catch
        error:disabled -> [];
        error:undef -> []
    end.

-spec store_filemeta(emqx_ft:transfer(), emqx_ft:filemeta()) ->
    ok | {async, pid()} | {error, term()}.
store_filemeta(Transfer, FileMeta) ->
    Mod = mod(),
    Mod:store_filemeta(storage(), Transfer, FileMeta).

-spec store_segment(emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {async, pid()} | {error, term()}.
store_segment(Transfer, Segment) ->
    Mod = mod(),
    Mod:store_segment(storage(), Transfer, Segment).

-spec assemble(emqx_ft:transfer(), emqx_ft:bytes()) ->
    ok | {async, pid()} | {error, term()}.
assemble(Transfer, Size) ->
    Mod = mod(),
    Mod:assemble(storage(), Transfer, Size).

-spec files() ->
    {ok, [file_info()]} | {error, term()}.
files() ->
    Mod = mod(),
    Mod:files(storage()).

-spec with_storage_type(atom(), atom() | function()) -> any().
with_storage_type(Type, Fun) ->
    with_storage_type(Type, Fun, []).

-spec with_storage_type(atom(), atom() | function(), list(term())) -> any().
with_storage_type(Type, Fun, Args) ->
    Storage = storage(),
    case Storage of
        #{type := Type} when is_function(Fun) ->
            apply(Fun, [Storage | Args]);
        #{type := Type} when is_atom(Fun) ->
            Mod = mod(Storage),
            apply(Mod, Fun, [Storage | Args]);
        disabled ->
            {error, disabled};
        _ ->
            {error, {invalid_storage_type, Type}}
    end.

%%--------------------------------------------------------------------
%% Local FS API
%%--------------------------------------------------------------------

storage() ->
    emqx_ft_conf:storage().

mod() ->
    mod(storage()).

mod(Storage) ->
    case Storage of
        #{type := local} ->
            emqx_ft_storage_fs;
        disabled ->
            error(disabled)
        % emqx_ft_storage_dummy
    end.
