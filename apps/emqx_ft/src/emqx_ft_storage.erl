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
        with_storage_type/3,

        on_config_update/2
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
    timestamp := emqx_datetime:epoch_second(),
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
    with_storage(store_filemeta, [Transfer, FileMeta]).

-spec store_segment(emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {async, pid()} | {error, term()}.
store_segment(Transfer, Segment) ->
    with_storage(store_segment, [Transfer, Segment]).

-spec assemble(emqx_ft:transfer(), emqx_ft:bytes()) ->
    ok | {async, pid()} | {error, term()}.
assemble(Transfer, Size) ->
    with_storage(assemble, [Transfer, Size]).

-spec files() ->
    {ok, [file_info()]} | {error, term()}.
files() ->
    with_storage(files, []).

-spec with_storage(atom() | function()) -> any().
with_storage(Fun) ->
    with_storage(Fun, []).

-spec with_storage(atom() | function(), list(term())) -> any().
with_storage(Fun, Args) ->
    case storage() of
        Storage = #{} ->
            apply_storage(Storage, Fun, Args);
        undefined ->
            {error, disabled}
    end.

-spec with_storage_type(atom(), atom() | function()) -> any().
with_storage_type(Type, Fun) ->
    with_storage_type(Type, Fun, []).

-spec with_storage_type(atom(), atom() | function(), list(term())) -> any().
with_storage_type(Type, Fun, Args) ->
    with_storage(fun(Storage) ->
        case Storage of
            #{type := Type} ->
                apply_storage(Storage, Fun, Args);
            _ ->
                {error, {invalid_storage_type, Storage}}
        end
    end).

apply_storage(Storage, Fun, Args) when is_atom(Fun) ->
    apply(mod(Storage), Fun, [Storage | Args]);
apply_storage(Storage, Fun, Args) when is_function(Fun) ->
    apply(Fun, [Storage | Args]).

%%

-spec on_config_update(_Old :: emqx_maybe:t(storage()), _New :: emqx_maybe:t(storage())) ->
    ok.
on_config_update(Storage, Storage) ->
    ok;
on_config_update(#{type := Type} = StorageOld, #{type := Type} = StorageNew) ->
    ok = (mod(StorageNew)):on_config_update(StorageOld, StorageNew);
on_config_update(StorageOld, StorageNew) ->
    _ = emqx_maybe:apply(fun on_storage_stop/1, StorageOld),
    _ = emqx_maybe:apply(fun on_storage_start/1, StorageNew),
    _ = emqx_maybe:apply(
        fun(Storage) -> (mod(Storage)):on_config_update(StorageOld, StorageNew) end,
        StorageNew
    ),
    ok.

on_storage_start(Storage = #{type := _}) ->
    lists:foreach(
        fun(ChildSpec) ->
            {ok, _Child} = supervisor:start_child(emqx_ft_sup, ChildSpec)
        end,
        child_spec(Storage)
    ).

on_storage_stop(Storage = #{type := _}) ->
    lists:foreach(
        fun(#{id := ChildId}) ->
            _ = supervisor:terminate_child(emqx_ft_sup, ChildId),
            ok = supervisor:delete_child(emqx_ft_sup, ChildId)
        end,
        child_spec(Storage)
    ).

child_spec(Storage) ->
    try
        Mod = mod(Storage),
        Mod:child_spec(Storage)
    catch
        error:disabled -> [];
        error:undef -> []
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
        undefined ->
            error(disabled)
    end.
