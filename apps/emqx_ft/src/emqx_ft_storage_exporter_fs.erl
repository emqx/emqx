%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    start/1,
    stop/1,
    update/2
]).

%% Internal API for RPC
-export([list_local/1]).
-export([list_local/2]).
-export([list_local_transfer/2]).
-export([start_reader/3]).

-export([list/2]).

-export_type([export_st/0]).
-export_type([options/0]).

-type options() :: #{
    enable := true,
    root => file:name(),
    _ => _
}.

-type query() :: emqx_ft_storage:query(cursor()).
-type page(T) :: emqx_ft_storage:page(T, cursor()).
-type cursor() :: iodata().

-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type exportinfo() :: emqx_ft_storage:file_info().
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

%%--------------------------------------------------------------------
%% Exporter behaviour
%%--------------------------------------------------------------------

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
    ManifestFilepath = mk_manifest_filename(ResultFilepath),
    case file:write_file(ManifestFilepath, encode_filemeta(Filemeta)) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(warning, "filemeta_write_failed", #{
                path => ManifestFilepath,
                meta => Filemeta,
                reason => Reason
            })
    end,
    file:rename(Filepath, ResultFilepath).

-spec discard(export_st()) ->
    ok.
discard(#{path := Filepath, handle := Handle}) ->
    ok = file:close(Handle),
    file:delete(Filepath).

%%--------------------------------------------------------------------
%% Exporter behaviour (lifecycle)
%%--------------------------------------------------------------------

%% FS Exporter does not have require any stateful entities,
%% so lifecycle callbacks are no-op.

-spec start(options()) -> ok.
start(_Options) -> ok.

-spec stop(options()) -> ok.
stop(_Options) -> ok.

-spec update(options(), options()) -> ok.
update(_OldOptions, _NewOptions) -> ok.

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

-type local_query() :: emqx_ft_storage:query({transfer(), file:name()}).

-spec list_local_transfer(options(), transfer()) ->
    {ok, [exportinfo()]} | {error, file_error()}.
list_local_transfer(Options, Transfer) ->
    It = emqx_ft_fs_iterator:new(
        mk_absdir(Options, Transfer, result),
        [fun filter_manifest/1]
    ),
    Result = emqx_ft_fs_iterator:fold(
        fun
            ({leaf, _Path, Fileinfo = #file_info{type = regular}, [Filename | _]}, Acc) ->
                RelFilepath = filename:join(mk_result_reldir(Transfer) ++ [Filename]),
                Info = mk_exportinfo(Options, Filename, RelFilepath, Transfer, Fileinfo),
                [Info | Acc];
            ({node, _Path, {error, Reason}, []}, []) ->
                {error, Reason};
            (Entry, Acc) ->
                ok = log_invalid_entry(Options, Entry),
                Acc
        end,
        [],
        It
    ),
    case Result of
        Infos = [_ | _] ->
            {ok, lists:reverse(Infos)};
        [] ->
            {error, enoent};
        {error, Reason} ->
            {error, Reason}
    end.

-spec list_local(options()) ->
    {ok, [exportinfo()]} | {error, file_error()}.
list_local(Options) ->
    list_local(Options, #{}).

-spec list_local(options(), local_query()) ->
    {ok, [exportinfo()]} | {error, file_error()}.
list_local(Options, #{transfer := Transfer}) ->
    list_local_transfer(Options, Transfer);
list_local(Options, #{} = Query) ->
    Root = get_storage_root(Options),
    Glob = [
        _Bucket1 = '*',
        _Bucket2 = '*',
        _Rest = '*',
        _ClientId = '*',
        _FileId = '*',
        fun filter_manifest/1
    ],
    It =
        case Query of
            #{following := Cursor} ->
                emqx_ft_fs_iterator:seek(mk_path_seek(Cursor), Root, Glob);
            #{} ->
                emqx_ft_fs_iterator:new(Root, Glob)
        end,
    % NOTE
    % In the rare case when some transfer contain more than one file, the paging mechanic
    % here may skip over some files, when the cursor is transfer-only.
    Limit = maps:get(limit, Query, -1),
    {Exports, _} = emqx_ft_fs_iterator:fold_n(
        fun(Entry, Acc) -> read_exportinfo(Options, Entry, Acc) end,
        [],
        It,
        Limit
    ),
    {ok, Exports}.

mk_path_seek(#{transfer := Transfer, name := Filename}) ->
    mk_result_reldir(Transfer) ++ [Filename];
mk_path_seek(#{transfer := Transfer}) ->
    % NOTE: Any bitstring is greater than any list.
    mk_result_reldir(Transfer) ++ [<<>>].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

filter_manifest(?MANIFEST) ->
    % Filename equals `?MANIFEST`, there should also be a manifest for it.
    false;
filter_manifest(Filename) ->
    ?MANIFEST =/= string:find(Filename, ?MANIFEST, trailing).

read_exportinfo(
    Options,
    {leaf, RelFilepath, Fileinfo = #file_info{type = regular}, [Filename, FileId, ClientId | _]},
    Acc
) ->
    % NOTE
    % There might be more than one file for a single transfer (though
    % extremely bad luck is needed for that, e.g. concurrent assemblers with
    % different filemetas from different nodes). This might be unexpected for a
    % client given the current protocol, yet might be helpful in the future.
    Transfer = dirnames_to_transfer(ClientId, FileId),
    Info = mk_exportinfo(Options, Filename, RelFilepath, Transfer, Fileinfo),
    [Info | Acc];
read_exportinfo(_Options, {node, _Root = "", {error, enoent}, []}, Acc) ->
    % NOTE: Root directory does not exist, this is not an error.
    Acc;
read_exportinfo(Options, Entry, Acc) ->
    ok = log_invalid_entry(Options, Entry),
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

log_invalid_entry(Options, {_Type, RelFilepath, Fileinfo = #file_info{}, _Stack}) ->
    ?SLOG(notice, "filesystem_object_unexpected", #{
        relpath => RelFilepath,
        fileinfo => Fileinfo,
        options => Options
    });
log_invalid_entry(Options, {_Type, RelFilepath, {error, Reason}, _Stack}) ->
    ?SLOG(warning, "filesystem_object_inaccessible", #{
        relpath => RelFilepath,
        reason => Reason,
        options => Options
    }).

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

-spec list(options(), query()) ->
    {ok, page(exportinfo())} | {error, [{node(), _Reason}]}.
list(_Options, Query = #{transfer := _Transfer}) ->
    case list(Query) of
        #{items := Exports = [_ | _]} ->
            {ok, #{items => Exports}};
        #{items := [], errors := NodeErrors} ->
            {error, NodeErrors};
        #{items := []} ->
            {ok, #{items => []}}
    end;
list(_Options, Query) ->
    Result = list(Query),
    case Result of
        #{errors := NodeErrors} ->
            ?SLOG(warning, "list_exports_errors", #{
                query => Query,
                errors => NodeErrors
            });
        #{} ->
            ok
    end,
    case Result of
        #{items := Exports, cursor := Cursor} ->
            {ok, #{items => lists:reverse(Exports), cursor => encode_cursor(Cursor)}};
        #{items := Exports} ->
            {ok, #{items => lists:reverse(Exports)}}
    end.

list(QueryIn) ->
    {Nodes, NodeQuery} = decode_query(QueryIn, lists:sort(emqx:running_nodes())),
    list_nodes(NodeQuery, Nodes, #{items => []}).

list_nodes(Query, Nodes = [Node | Rest], Acc) ->
    case emqx_ft_storage_exporter_fs_proto_v1:list_exports([Node], Query) of
        [{ok, Result}] ->
            list_accumulate(Result, Query, Nodes, Acc);
        [Failure] ->
            ?SLOG(warning, #{
                msg => "list_remote_exports_failed",
                node => Node,
                query => Query,
                failure => Failure
            }),
            list_next(Query, Rest, Acc)
    end;
list_nodes(_Query, [], Acc) ->
    Acc.

list_accumulate({ok, Exports}, Query, [Node | Rest], Acc = #{items := EAcc}) ->
    NExports = length(Exports),
    AccNext = Acc#{items := Exports ++ EAcc},
    case Query of
        #{limit := Limit} when NExports < Limit ->
            list_next(Query#{limit => Limit - NExports}, Rest, AccNext);
        #{limit := _} ->
            AccNext#{cursor => mk_cursor(Node, Exports)};
        #{} ->
            list_next(Query, Rest, AccNext)
    end;
list_accumulate({error, Reason}, Query, [Node | Rest], Acc) ->
    EAcc = maps:get(errors, Acc, []),
    list_next(Query, Rest, Acc#{errors => [{Node, Reason} | EAcc]}).

list_next(Query, Nodes, Acc) ->
    list_nodes(maps:remove(following, Query), Nodes, Acc).

decode_query(Query = #{following := Cursor}, Nodes) ->
    {Node, NodeCursor} = decode_cursor(Cursor),
    {skip_query_nodes(Node, Nodes), Query#{following => NodeCursor}};
decode_query(Query = #{}, Nodes) ->
    {Nodes, Query}.

skip_query_nodes(CNode, Nodes) ->
    lists:dropwhile(fun(N) -> N < CNode end, Nodes).

mk_cursor(Node, [_Last = #{transfer := Transfer, name := Name} | _]) ->
    {Node, #{transfer => Transfer, name => Name}}.

encode_cursor({Node, #{transfer := {ClientId, FileId}, name := Name}}) ->
    emqx_utils_json:encode(#{
        <<"n">> => Node,
        <<"cid">> => ClientId,
        <<"fid">> => FileId,
        <<"fn">> => unicode:characters_to_binary(Name)
    }).

decode_cursor(Cursor) ->
    try
        #{
            <<"n">> := NodeIn,
            <<"cid">> := ClientId,
            <<"fid">> := FileId,
            <<"fn">> := NameIn
        } = emqx_utils_json:decode(Cursor),
        true = is_binary(ClientId),
        true = is_binary(FileId),
        Node = binary_to_existing_atom(NodeIn),
        Name = unicode:characters_to_list(NameIn),
        true = is_list(Name),
        {Node, #{transfer => {ClientId, FileId}, name => Name}}
    catch
        error:{Loc, JsonError} when is_integer(Loc), is_atom(JsonError) ->
            error({badarg, cursor});
        error:{badmatch, _} ->
            error({badarg, cursor});
        error:badarg ->
            error({badarg, cursor})
    end.

%%

-define(PRELUDE(Vsn, Meta), [<<"filemeta">>, Vsn, Meta]).

encode_filemeta(Meta) ->
    emqx_utils_json:encode(?PRELUDE(_Vsn = 1, emqx_ft:encode_filemeta(Meta))).

decode_filemeta(Binary) when is_binary(Binary) ->
    ?PRELUDE(_Vsn = 1, Map) = emqx_utils_json:decode(Binary, [return_maps]),
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
    TempFilename = emqx_ft_fs_util:mk_temp_filename(Filename),
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
        binary_to_list(Bucket1),
        binary_to_list(Bucket2),
        binary_to_list(BucketRest),
        emqx_ft_fs_util:escape_filename(ClientId),
        emqx_ft_fs_util:escape_filename(FileId)
    ].

dirnames_to_transfer(ClientId, FileId) ->
    {emqx_ft_fs_util:unescape_filename(ClientId), emqx_ft_fs_util:unescape_filename(FileId)}.

mk_transfer_hash(Transfer) ->
    crypto:hash(?BUCKET_HASH, term_to_binary(Transfer)).

get_storage_root(Options) ->
    maps:get(root, Options, filename:join([emqx:data_dir(), file_transfer, exports])).
