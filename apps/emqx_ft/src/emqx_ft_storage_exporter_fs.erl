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

-module(emqx_ft_storage_exporter_fs).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").

%% Exporter API
-behaviour(emqx_ft_storage_exporter).

-export([start_export/3]).
-export([write/2]).
-export([complete/2]).
-export([discard/1]).
-export([list/1]).

%% Internal API for RPC
-export([list_local/1]).
-export([list_local/2]).
-export([start_reader/3]).

% TODO
% -export([list/2]).

-type options() :: _TODO.
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type exportinfo() :: #{
    transfer := transfer(),
    name := file:name(),
    uri := uri_string:uri_string(),
    timestamp := emqx_datetime:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    meta => filemeta()
}.

-type file_error() :: emqx_ft_storage_fs:file_error().

-type export_st() :: #{
    path := file:name(),
    handle := io:device(),
    result := file:name(),
    meta := filemeta()
}.

-type reader() :: pid().

-define(TEMPDIR, "tmp").
-define(MANIFEST, ".MANIFEST.json").

%% NOTE
%% Bucketing of resulting files to accomodate the storage backend for considerably
%% large (e.g. > 10s of millions) amount of files.
-define(BUCKET_HASH, sha).

%% 2 symbols = at most 256 directories on the upper level
-define(BUCKET1_LEN, 2).
%% 2 symbols = at most 256 directories on the second level
-define(BUCKET2_LEN, 2).

-define(SLOG_UNEXPECTED(RelFilepath, Fileinfo, Options),
    ?SLOG(notice, "filesystem_object_unexpected", #{
        relpath => RelFilepath,
        fileinfo => Fileinfo,
        options => Options
    })
).

-define(SLOG_INACCESSIBLE(RelFilepath, Reason, Options),
    ?SLOG(warning, "filesystem_object_inaccessible", #{
        relpath => RelFilepath,
        reason => Reason,
        options => Options
    })
).

%%

-spec start_export(options(), transfer(), filemeta()) ->
    {ok, export_st()} | {error, file_error()}.
start_export(Options, Transfer, Filemeta = #{name := Filename}) ->
    TempFilepath = mk_temp_absfilepath(Options, Transfer, Filename),
    ResultFilepath = mk_absfilepath(Options, Transfer, result, Filename),
    _ = filelib:ensure_dir(TempFilepath),
    case file:open(TempFilepath, [write, raw, binary]) of
        {ok, Handle} ->
            {ok, #{
                path => TempFilepath,
                handle => Handle,
                result => ResultFilepath,
                meta => Filemeta
            }};
        {error, _} = Error ->
            Error
    end.

-spec write(export_st(), iodata()) ->
    {ok, export_st()} | {error, file_error()}.
write(ExportSt = #{handle := Handle}, IoData) ->
    case file:write(Handle, IoData) of
        ok ->
            {ok, ExportSt};
        {error, _} = Error ->
            _ = discard(ExportSt),
            Error
    end.

-spec complete(export_st(), emqx_ft:checksum()) ->
    ok | {error, {checksum, _Algo, _Computed}} | {error, file_error()}.
complete(
    #{
        path := Filepath,
        handle := Handle,
        result := ResultFilepath,
        meta := FilemetaIn
    },
    Checksum
) ->
    Filemeta = FilemetaIn#{checksum => Checksum},
    ok = file:close(Handle),
    _ = filelib:ensure_dir(ResultFilepath),
    _ = file:write_file(mk_manifest_filename(ResultFilepath), encode_filemeta(Filemeta)),
    file:rename(Filepath, ResultFilepath).

-spec discard(export_st()) ->
    ok.
discard(#{path := Filepath, handle := Handle}) ->
    ok = file:close(Handle),
    file:delete(Filepath).

%%

-spec list_local(options(), transfer()) ->
    {ok, [exportinfo(), ...]} | {error, file_error()}.
list_local(Options, Transfer) ->
    TransferRoot = mk_absdir(Options, Transfer, result),
    case
        emqx_ft_fs_util:fold(
            fun
                (_Path, {error, Reason}, [], []) ->
                    {error, Reason};
                (_Path, Fileinfo = #file_info{type = regular}, [Filename | _], Acc) ->
                    RelFilepath = filename:join(mk_result_reldir(Transfer) ++ [Filename]),
                    Info = mk_exportinfo(Options, Filename, RelFilepath, Transfer, Fileinfo),
                    [Info | Acc];
                (RelFilepath, Fileinfo = #file_info{}, _, Acc) ->
                    ?SLOG_UNEXPECTED(RelFilepath, Fileinfo, Options),
                    Acc;
                (RelFilepath, {error, Reason}, _, Acc) ->
                    ?SLOG_INACCESSIBLE(RelFilepath, Reason, Options),
                    Acc
            end,
            [],
            TransferRoot,
            [fun filter_manifest/1]
        )
    of
        Infos = [_ | _] ->
            {ok, Infos};
        [] ->
            {error, enoent};
        {error, Reason} ->
            {error, Reason}
    end.

-spec list_local(options()) ->
    {ok, #{transfer() => [exportinfo(), ...]}}.
list_local(Options) ->
    Pattern = [
        _Bucket1 = '*',
        _Bucket2 = '*',
        _Rest = '*',
        _ClientId = '*',
        _FileId = '*',
        fun filter_manifest/1
    ],
    Root = get_storage_root(Options),
    {ok,
        emqx_ft_fs_util:fold(
            fun(RelFilepath, Info, Stack, Acc) ->
                read_exportinfo(Options, RelFilepath, Info, Stack, Acc)
            end,
            [],
            Root,
            Pattern
        )}.

filter_manifest(?MANIFEST) ->
    % Filename equals `?MANIFEST`, there should also be a manifest for it.
    false;
filter_manifest(Filename) ->
    ?MANIFEST =/= string:find(Filename, ?MANIFEST, trailing).

read_exportinfo(Options, RelFilepath, Fileinfo = #file_info{type = regular}, Stack, Acc) ->
    [Filename, FileId, ClientId | _] = Stack,
    Transfer = dirnames_to_transfer(ClientId, FileId),
    Info = mk_exportinfo(Options, Filename, RelFilepath, Transfer, Fileinfo),
    [Info | Acc];
read_exportinfo(Options, RelFilepath, Fileinfo = #file_info{}, _Stack, Acc) ->
    ?SLOG_UNEXPECTED(RelFilepath, Fileinfo, Options),
    Acc;
read_exportinfo(Options, RelFilepath, {error, Reason}, _Stack, Acc) ->
    ?SLOG_INACCESSIBLE(RelFilepath, Reason, Options),
    Acc.

mk_exportinfo(Options, Filename, RelFilepath, Transfer, Fileinfo) ->
    Root = get_storage_root(Options),
    try_read_filemeta(
        filename:join(Root, mk_manifest_filename(RelFilepath)),
        #{
            transfer => Transfer,
            name => Filename,
            uri => mk_export_uri(RelFilepath),
            timestamp => Fileinfo#file_info.mtime,
            size => Fileinfo#file_info.size,
            path => filename:join(Root, RelFilepath)
        }
    ).

try_read_filemeta(Filepath, Info) ->
    case emqx_ft_fs_util:read_decode_file(Filepath, fun decode_filemeta/1) of
        {ok, Filemeta} ->
            Info#{meta => Filemeta};
        {error, Reason} ->
            ?SLOG(warning, "filemeta_inaccessible", #{
                path => Filepath,
                reason => Reason
            }),
            Info
    end.

mk_export_uri(RelFilepath) ->
    emqx_ft_storage_exporter_fs_api:mk_export_uri(node(), RelFilepath).

-spec start_reader(options(), file:name(), _Caller :: pid()) ->
    {ok, reader()} | {error, enoent}.
start_reader(Options, RelFilepath, CallerPid) ->
    Root = get_storage_root(Options),
    case filelib:safe_relative_path(RelFilepath, Root) of
        SafeFilepath when SafeFilepath /= unsafe ->
            AbsFilepath = filename:join(Root, SafeFilepath),
            emqx_ft_storage_fs_reader:start_supervised(CallerPid, AbsFilepath);
        unsafe ->
            {error, enoent}
    end.

%%

-spec list(options()) ->
    {ok, [exportinfo(), ...]} | {error, [{node(), _Reason}]}.
list(_Options) ->
    Nodes = mria_mnesia:running_nodes(),
    Replies = emqx_ft_storage_exporter_fs_proto_v1:list_exports(Nodes),
    {Results, Errors} = lists:foldl(
        fun
            ({_Node, {ok, {ok, Files}}}, {Acc, Errors}) ->
                {Files ++ Acc, Errors};
            ({Node, {ok, {error, _} = Error}}, {Acc, Errors}) ->
                {Acc, [{Node, Error} | Errors]};
            ({Node, Error}, {Acc, Errors}) ->
                {Acc, [{Node, Error} | Errors]}
        end,
        {[], []},
        lists:zip(Nodes, Replies)
    ),
    length(Errors) > 0 andalso
        ?SLOG(warning, #{msg => "list_remote_exports_failed", errors => Errors}),
    case Results of
        [_ | _] ->
            {ok, Results};
        [] when Errors =:= [] ->
            {ok, Results};
        [] ->
            {error, Errors}
    end.

%%

-define(PRELUDE(Vsn, Meta), [<<"filemeta">>, Vsn, Meta]).

encode_filemeta(Meta) ->
    emqx_json:encode(?PRELUDE(_Vsn = 1, emqx_ft:encode_filemeta(Meta))).

decode_filemeta(Binary) when is_binary(Binary) ->
    ?PRELUDE(_Vsn = 1, Map) = emqx_json:decode(Binary, [return_maps]),
    case emqx_ft:decode_filemeta(Map) of
        {ok, Meta} ->
            Meta;
        {error, Reason} ->
            error(Reason)
    end.

mk_manifest_filename(Filename) when is_list(Filename) ->
    Filename ++ ?MANIFEST;
mk_manifest_filename(Filename) when is_binary(Filename) ->
    <<Filename/binary, ?MANIFEST>>.

mk_temp_absfilepath(Options, Transfer, Filename) ->
    Unique = erlang:unique_integer([positive]),
    TempFilename = integer_to_list(Unique) ++ "." ++ Filename,
    filename:join(mk_absdir(Options, Transfer, temporary), TempFilename).

mk_absdir(Options, _Transfer, temporary) ->
    filename:join([get_storage_root(Options), ?TEMPDIR]);
mk_absdir(Options, Transfer, result) ->
    filename:join([get_storage_root(Options) | mk_result_reldir(Transfer)]).

mk_absfilepath(Options, Transfer, What, Filename) ->
    filename:join(mk_absdir(Options, Transfer, What), Filename).

mk_result_reldir(Transfer = {ClientId, FileId}) ->
    Hash = mk_transfer_hash(Transfer),
    <<
        Bucket1:?BUCKET1_LEN/binary,
        Bucket2:?BUCKET2_LEN/binary,
        BucketRest/binary
    >> = binary:encode_hex(Hash),
    [
        Bucket1,
        Bucket2,
        BucketRest,
        emqx_ft_fs_util:escape_filename(ClientId),
        emqx_ft_fs_util:escape_filename(FileId)
    ].

dirnames_to_transfer(ClientId, FileId) ->
    {emqx_ft_fs_util:unescape_filename(ClientId), emqx_ft_fs_util:unescape_filename(FileId)}.

mk_transfer_hash(Transfer) ->
    crypto:hash(?BUCKET_HASH, term_to_binary(Transfer)).

get_storage_root(Options) ->
    maps:get(root, Options, filename:join([emqx:data_dir(), file_transfer, exports])).
