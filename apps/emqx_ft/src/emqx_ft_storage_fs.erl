%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Filesystem storage backend
%%
%% NOTE
%% If you plan to change storage layout please consult `emqx_ft_storage_fs_gc`
%% to see how much it would break or impair GC.

-module(emqx_ft_storage_fs).

-behaviour(emqx_ft_storage).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([child_spec/1]).

% Segments-related API
-export([store_filemeta/3]).
-export([store_segment/3]).
-export([read_filemeta/2]).
-export([list/3]).
-export([pread/5]).
-export([lookup_local_assembler/1]).
-export([assemble/4]).

-export([transfers/1]).

% GC API
% TODO: This is quickly becomes hairy.
-export([get_root/1]).
-export([get_subdir/2]).
-export([get_subdir/3]).

-export([files/2]).

-export([start/1]).
-export([stop/1]).
-export([update_config/2]).

-export_type([storage/0]).
-export_type([filefrag/1]).
-export_type([filefrag/0]).
-export_type([transferinfo/0]).
-export_type([segmentinfo/0]).

-export_type([file_error/0]).

-type transfer() :: emqx_ft:transfer().
-type offset() :: emqx_ft:offset().
-type filemeta() :: emqx_ft:filemeta().
-type segment() :: emqx_ft:segment().

-type segmentinfo() :: #{
    offset := offset(),
    size := _Bytes :: non_neg_integer()
}.

-type transferinfo() :: #{
    filemeta => filemeta()
}.

% TODO naming
-type filefrag(T) :: #{
    path := file:name(),
    timestamp := emqx_utils_calendar:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    fragment := T
}.

-type filefrag() :: filefrag(
    {filemeta, filemeta()}
    | {segment, segmentinfo()}
).

-define(FRAGDIR, frags).
-define(TEMPDIR, tmp).
-define(MANIFEST, "MANIFEST.json").
-define(SEGMENT, "SEG").

-type segments() :: #{
    root := file:name(),
    gc := #{
        interval := non_neg_integer(),
        maximum_segments_ttl := non_neg_integer(),
        minimum_segments_ttl := non_neg_integer()
    }
}.

-type storage() :: #{
    type := 'local',
    enable := true,
    segments := segments(),
    exporter := emqx_ft_storage_exporter:exporter_conf()
}.

-type file_error() ::
    file:posix()
    %% Filename is incompatible with the backing filesystem.
    | badarg
    %% System limit (e.g. number of ports) reached.
    | system_limit.

%% Related resources childspecs
-spec child_spec(storage()) ->
    [supervisor:child_spec()].
child_spec(Storage) ->
    [
        #{
            id => emqx_ft_storage_fs_gc,
            start => {emqx_ft_storage_fs_gc, start_link, [Storage]},
            restart => permanent
        }
    ].

%% Store manifest in the backing filesystem.
%% Atomic operation.
-spec store_filemeta(storage(), transfer(), filemeta()) ->
    % Quota? Some lower level errors?
    ok | {error, conflict} | {error, file_error()}.
store_filemeta(Storage, Transfer, Meta) ->
    Filepath = mk_filepath(Storage, Transfer, get_subdirs_for(fragment), ?MANIFEST),
    case read_file(Filepath, fun decode_filemeta/1) of
        {ok, Meta} ->
            _ = touch_file(Filepath),
            ok;
        {ok, Conflict} ->
            ?SLOG(warning, #{
                msg => "filemeta_conflict", transfer => Transfer, new => Meta, old => Conflict
            }),
            % TODO
            % We won't see conflicts in case of concurrent `store_filemeta`
            % requests. It's rather odd scenario so it's fine not to worry
            % about it too much now.
            {error, filemeta_conflict};
        {error, Reason} when Reason =:= notfound; Reason =:= corrupted; Reason =:= enoent ->
            write_file_atomic(Storage, Transfer, Filepath, encode_filemeta(Meta));
        {error, _} = Error ->
            Error
    end.

%% Store a segment in the backing filesystem.
%% Atomic operation.
-spec store_segment(storage(), transfer(), segment()) ->
    % Where is the checksum gets verified? Upper level probably.
    % Quota? Some lower level errors?
    ok | {error, file_error()}.
store_segment(Storage, Transfer, Segment = {_Offset, Content}) ->
    Filename = mk_segment_filename(Segment),
    Filepath = mk_filepath(Storage, Transfer, get_subdirs_for(fragment), Filename),
    write_file_atomic(Storage, Transfer, Filepath, Content).

-spec read_filemeta(storage(), transfer()) ->
    {ok, filemeta()} | {error, corrupted} | {error, file_error()}.
read_filemeta(Storage, Transfer) ->
    Filepath = mk_filepath(Storage, Transfer, get_subdirs_for(fragment), ?MANIFEST),
    read_file(Filepath, fun decode_filemeta/1).

-spec list(storage(), transfer(), _What :: fragment) ->
    % Some lower level errors? {error, notfound}?
    % Result will contain zero or only one filemeta.
    {ok, [filefrag({filemeta, filemeta()} | {segment, segmentinfo()})]}
    | {error, file_error()}.
list(Storage, Transfer, What = fragment) ->
    Dirname = mk_filedir(Storage, Transfer, get_subdirs_for(What)),
    case file:list_dir(Dirname) of
        {ok, Filenames} ->
            % TODO
            % In case of `What = result` there might be more than one file (though
            % extremely bad luck is needed for that, e.g. concurrent assemblers with
            % different filemetas from different nodes). This might be unexpected for a
            % client given the current protocol, yet might be helpful in the future.
            {ok, filtermap_files(fun mk_filefrag/2, Dirname, Filenames)};
        {error, enoent} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

-spec pread(storage(), transfer(), filefrag(), offset(), _Size :: non_neg_integer()) ->
    {ok, _Content :: iodata()} | {error, eof} | {error, file_error()}.
pread(_Storage, _Transfer, Frag, Offset, Size) ->
    Filepath = maps:get(path, Frag),
    case file:open(Filepath, [read, raw, binary]) of
        {ok, IoDevice} ->
            % NOTE
            % Reading empty file is always `eof`.
            Read = file:pread(IoDevice, Offset, Size),
            ok = file:close(IoDevice),
            case Read of
                {ok, Content} ->
                    {ok, Content};
                eof ->
                    {error, eof};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec assemble(storage(), transfer(), emqx_ft:bytes(), emqx_ft:finopts()) ->
    {async, _Assembler :: pid()} | ok | {error, _TODO}.
assemble(Storage, Transfer, Size, Opts) ->
    LookupSources = [
        fun() -> lookup_local_assembler(Transfer) end,
        fun() -> lookup_remote_assembler(Transfer) end,
        fun() -> check_if_already_exported(Storage, Transfer) end,
        fun() -> ensure_local_assembler(Storage, Transfer, Size, Opts) end
    ],
    lookup_assembler(LookupSources).

%%

files(Storage, Query) ->
    emqx_ft_storage_exporter:list(Storage, Query).

%%

update_config(StorageOld, StorageNew) ->
    % NOTE: this will reset GC timer, frequent changes would postpone GC indefinitely
    ok = emqx_ft_storage_fs_gc:reset(StorageNew),
    emqx_ft_storage_exporter:update_config(StorageOld, StorageNew).

start(Storage) ->
    ok = lists:foreach(
        fun(ChildSpec) ->
            {ok, _Child} = supervisor:start_child(emqx_ft_sup, ChildSpec)
        end,
        child_spec(Storage)
    ),
    ok = emqx_ft_storage_exporter:update_config(undefined, Storage),
    ok.

stop(Storage) ->
    ok = emqx_ft_storage_exporter:update_config(Storage, undefined),
    ok = lists:foreach(
        fun(#{id := ChildId}) ->
            _ = supervisor:terminate_child(emqx_ft_sup, ChildId),
            ok = supervisor:delete_child(emqx_ft_sup, ChildId)
        end,
        child_spec(Storage)
    ),
    ok.

%%

lookup_assembler([LastSource]) ->
    LastSource();
lookup_assembler([Source | Sources]) ->
    case Source() of
        {error, not_found} -> lookup_assembler(Sources);
        Result -> Result
    end.

check_if_already_exported(Storage, Transfer) ->
    case files(Storage, #{transfer => Transfer}) of
        {ok, #{items := [_ | _]}} ->
            % NOTE: we don't know coverage here, let's just clean up locally.
            _ = emqx_ft_storage_fs_gc:collect(Storage, Transfer, [node()]),
            ok;
        _ ->
            {error, not_found}
    end.

lookup_local_assembler(Transfer) ->
    case emqx_ft_assembler:where(Transfer) of
        Pid when is_pid(Pid) -> {async, Pid};
        _ -> {error, not_found}
    end.

lookup_remote_assembler(Transfer) ->
    Nodes = emqx:running_nodes() -- [node()],
    Assemblers = lists:flatmap(
        fun
            ({ok, {async, Pid}}) -> [Pid];
            (_) -> []
        end,
        emqx_ft_storage_fs_proto_v1:list_assemblers(Nodes, Transfer)
    ),
    case Assemblers of
        [Pid | _] -> {async, Pid};
        _ -> {error, not_found}
    end.

ensure_local_assembler(Storage, Transfer, Size, Opts) ->
    {ok, Pid} = emqx_ft_assembler_sup:ensure_child(Storage, Transfer, Size, Opts),
    {async, Pid}.

-spec transfers(storage()) ->
    {ok, #{transfer() => transferinfo()}}.
transfers(Storage) ->
    % TODO `Continuation`
    % There might be millions of transfers on the node, we need a protocol and
    % storage schema to iterate through them effectively.
    ClientIds = try_list_dir(get_root(Storage)),
    {ok,
        lists:foldl(
            fun(ClientId, Acc) -> transfers(Storage, ClientId, Acc) end,
            #{},
            ClientIds
        )}.

transfers(Storage, ClientId, AccIn) ->
    Dirname = filename:join(get_root(Storage), ClientId),
    case file:list_dir(Dirname) of
        {ok, FileIds} ->
            lists:foldl(
                fun(FileId, Acc) ->
                    Transfer = dirnames_to_transfer(ClientId, FileId),
                    read_transferinfo(Storage, Transfer, Acc)
                end,
                AccIn,
                FileIds
            );
        {error, _Reason} ->
            ?tp(warning, "list_dir_failed", #{
                storage => Storage,
                directory => Dirname
            }),
            AccIn
    end.

read_transferinfo(Storage, Transfer, Acc) ->
    case read_filemeta(Storage, Transfer) of
        {ok, Filemeta} ->
            Acc#{Transfer => #{filemeta => Filemeta}};
        {error, enoent} ->
            Acc#{Transfer => #{}};
        {error, Reason} ->
            ?tp(warning, "read_transferinfo_failed", #{
                storage => Storage,
                transfer => Transfer,
                reason => Reason
            }),
            Acc
    end.

-spec get_root(storage()) ->
    file:name().
get_root(Storage) ->
    case emqx_utils_maps:deep_find([segments, root], Storage) of
        {ok, Root} ->
            Root;
        {not_found, _, _} ->
            filename:join([emqx:data_dir(), file_transfer, segments])
    end.

-spec get_subdir(storage(), transfer()) ->
    file:name().
get_subdir(Storage, Transfer) ->
    mk_filedir(Storage, Transfer, []).

-spec get_subdir(storage(), transfer(), fragment | temporary) ->
    file:name().
get_subdir(Storage, Transfer, What) ->
    mk_filedir(Storage, Transfer, get_subdirs_for(What)).

get_subdirs_for(fragment) ->
    [?FRAGDIR];
get_subdirs_for(temporary) ->
    [?TEMPDIR].

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

mk_segment_filename({Offset, Content}) ->
    lists:concat([?SEGMENT, ".", Offset, ".", byte_size(Content)]).

break_segment_filename(Filename) ->
    Regex = "^" ?SEGMENT "[.]([0-9]+)[.]([0-9]+)$",
    Result = re:run(Filename, Regex, [{capture, all_but_first, list}]),
    case Result of
        {match, [Offset, Size]} ->
            {ok, #{offset => list_to_integer(Offset), size => list_to_integer(Size)}};
        nomatch ->
            {error, invalid}
    end.

mk_filedir(Storage, {ClientId, FileId}, SubDirs) ->
    filename:join([
        get_root(Storage),
        emqx_ft_fs_util:escape_filename(ClientId),
        emqx_ft_fs_util:escape_filename(FileId)
        | SubDirs
    ]).

dirnames_to_transfer(ClientId, FileId) ->
    {emqx_ft_fs_util:unescape_filename(ClientId), emqx_ft_fs_util:unescape_filename(FileId)}.

mk_filepath(Storage, Transfer, SubDirs, Filename) ->
    filename:join(mk_filedir(Storage, Transfer, SubDirs), Filename).

try_list_dir(Dirname) ->
    case file:list_dir(Dirname) of
        {ok, List} -> List;
        {error, _} -> []
    end.

-include_lib("kernel/include/file.hrl").

read_file(Filepath, DecodeFun) ->
    emqx_ft_fs_util:read_decode_file(Filepath, DecodeFun).

write_file_atomic(Storage, Transfer, Filepath, Content) when is_binary(Content) ->
    TempFilepath = mk_temp_filepath(Storage, Transfer, filename:basename(Filepath)),
    Result = emqx_utils:pipeline(
        [
            fun filelib:ensure_dir/1,
            fun write_contents/2,
            fun(_) -> mv_temp_file(TempFilepath, Filepath) end
        ],
        TempFilepath,
        Content
    ),
    case Result of
        {ok, _, _} ->
            _ = file:delete(TempFilepath),
            ok;
        {error, Reason, _} ->
            {error, Reason}
    end.

mk_temp_filepath(Storage, Transfer, Filename) ->
    TempFilename = emqx_ft_fs_util:mk_temp_filename(Filename),
    filename:join(get_subdir(Storage, Transfer, temporary), TempFilename).

write_contents(Filepath, Content) ->
    file:write_file(Filepath, Content).

mv_temp_file(TempFilepath, Filepath) ->
    _ = filelib:ensure_dir(Filepath),
    file:rename(TempFilepath, Filepath).

touch_file(Filepath) ->
    Now = erlang:localtime(),
    file:change_time(Filepath, _Mtime = Now, _Atime = Now).

filtermap_files(Fun, Dirname, Filenames) ->
    lists:filtermap(fun(Filename) -> Fun(Dirname, Filename) end, Filenames).

mk_filefrag(Dirname, Filename = ?MANIFEST) ->
    mk_filefrag(Dirname, Filename, filemeta, fun read_frag_filemeta/2);
mk_filefrag(Dirname, Filename = ?SEGMENT ++ _) ->
    mk_filefrag(Dirname, Filename, segment, fun read_frag_segmentinfo/2);
mk_filefrag(_Dirname, _Filename) ->
    ?tp(warning, "rogue_file_found", #{
        directory => _Dirname,
        filename => _Filename
    }),
    false.

mk_filefrag(Dirname, Filename, Tag, Fun) ->
    Filepath = filename:join(Dirname, Filename),
    % TODO error handling?
    {ok, Fileinfo} = file:read_file_info(Filepath),
    case Fun(Filename, Filepath) of
        {ok, Frag} ->
            {true, #{
                path => Filepath,
                timestamp => Fileinfo#file_info.mtime,
                size => Fileinfo#file_info.size,
                fragment => {Tag, Frag}
            }};
        {error, _Reason} ->
            ?tp(warning, "mk_filefrag_failed", #{
                directory => Dirname,
                filename => Filename,
                type => Tag,
                reason => _Reason
            }),
            false
    end.

read_frag_filemeta(_Filename, Filepath) ->
    read_file(Filepath, fun decode_filemeta/1).

read_frag_segmentinfo(Filename, _Filepath) ->
    break_segment_filename(Filename).
