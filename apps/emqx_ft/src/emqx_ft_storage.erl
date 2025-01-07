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

-module(emqx_ft_storage).

-include_lib("emqx/include/types.hrl").

-export(
    [
        store_filemeta/2,
        store_segment/2,
        assemble/3,
        kickoff/1,

        files/0,
        files/1,

        with_storage_type/2,
        with_storage_type/3,

        backend/0,
        update_config/2
    ]
).

-type type() :: local.
-type backend() :: {type(), storage()}.
-type storage() :: config().
-type config() :: emqx_config:config().

-export_type([backend/0]).

-export_type([assemble_callback/0]).

-export_type([query/1]).
-export_type([page/2]).
-export_type([file_info/0]).
-export_type([export_data/0]).
-export_type([reader/0]).

-type assemble_callback() :: fun((ok | {error, term()}) -> any()).

-type query(Cursor) ::
    #{transfer => emqx_ft:transfer()}
    | #{
        limit => non_neg_integer(),
        following => Cursor
    }.

-type page(Item, Cursor) :: #{
    items := [Item],
    cursor => Cursor
}.

-type file_info() :: #{
    transfer := emqx_ft:transfer(),
    name := file:name(),
    size := _Bytes :: non_neg_integer(),
    timestamp := emqx_utils_calendar:epoch_second(),
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
-callback assemble(storage(), emqx_ft:transfer(), _Size :: emqx_ft:bytes(), emqx_ft:finopts()) ->
    ok | {async, pid()} | {error, term()}.

-callback files(storage(), query(Cursor)) ->
    {ok, page(file_info(), Cursor)} | {error, term()}.

-callback start(storage()) -> any().
-callback stop(storage()) -> any().

-callback update_config(_OldConfig :: option(storage()), _NewConfig :: option(storage())) ->
    any().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_filemeta(emqx_ft:transfer(), emqx_ft:filemeta()) ->
    ok | {async, pid()} | {error, term()}.
store_filemeta(Transfer, FileMeta) ->
    dispatch(store_filemeta, [Transfer, FileMeta]).

-spec store_segment(emqx_ft:transfer(), emqx_ft:segment()) ->
    ok | {async, pid()} | {error, term()}.
store_segment(Transfer, Segment) ->
    dispatch(store_segment, [Transfer, Segment]).

-spec assemble(emqx_ft:transfer(), emqx_ft:bytes(), emqx_ft:finopts()) ->
    ok | {async, pid()} | {error, term()}.
assemble(Transfer, Size, FinOpts) ->
    dispatch(assemble, [Transfer, Size, FinOpts]).

-spec kickoff(pid()) -> ok.
kickoff(Pid) ->
    _ = erlang:send(Pid, kickoff),
    ok.

%%

-spec files() ->
    {ok, page(file_info(), _)} | {error, term()}.
files() ->
    files(#{}).

-spec files(query(Cursor)) ->
    {ok, page(file_info(), Cursor)} | {error, term()}.
files(Query) ->
    dispatch(files, [Query]).

-spec dispatch(atom(), list(term())) -> any().
dispatch(Fun, Args) when is_atom(Fun) ->
    {Type, Storage} = backend(),
    apply(mod(Type), Fun, [Storage | Args]).

%%

-spec with_storage_type(atom(), atom() | function()) -> any().
with_storage_type(Type, Fun) ->
    with_storage_type(Type, Fun, []).

-spec with_storage_type(atom(), atom() | function(), list(term())) -> any().
with_storage_type(Type, Fun, Args) ->
    case backend() of
        {Type, Storage} when is_atom(Fun) ->
            apply(mod(Type), Fun, [Storage | Args]);
        {Type, Storage} when is_function(Fun) ->
            apply(Fun, [Storage | Args]);
        {_, _} = Backend ->
            {error, {invalid_storage_backend, Backend}}
    end.

%%

-spec backend() -> backend().
backend() ->
    backend(emqx_ft_conf:storage()).

-spec update_config(_Old :: emqx_maybe:t(config()), _New :: emqx_maybe:t(config())) ->
    ok.
update_config(ConfigOld, ConfigNew) ->
    on_backend_update(
        emqx_maybe:apply(fun backend/1, ConfigOld),
        emqx_maybe:apply(fun backend/1, ConfigNew)
    ).

on_backend_update({Type, _} = Backend, {Type, _} = Backend) ->
    ok;
on_backend_update({Type, StorageOld}, {Type, StorageNew}) ->
    ok = (mod(Type)):update_config(StorageOld, StorageNew);
on_backend_update(BackendOld, BackendNew) when
    (BackendOld =:= undefined orelse is_tuple(BackendOld)) andalso
        (BackendNew =:= undefined orelse is_tuple(BackendNew))
->
    _ = emqx_maybe:apply(fun stop_backend/1, BackendOld),
    _ = emqx_maybe:apply(fun start_backend/1, BackendNew),
    ok.

%%--------------------------------------------------------------------
%% Local API
%%--------------------------------------------------------------------

-spec backend(config()) -> backend().
backend(Config) ->
    emqx_ft_schema:backend(Config).

start_backend({Type, Storage}) ->
    (mod(Type)):start(Storage).

stop_backend({Type, Storage}) ->
    (mod(Type)):stop(Storage).

mod(local) ->
    emqx_ft_storage_fs.
