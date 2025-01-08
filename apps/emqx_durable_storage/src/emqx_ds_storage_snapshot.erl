%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_storage_snapshot).

-include_lib("kernel/include/file.hrl").

-export([
    new_reader/1,
    read_chunk/2,
    abort_reader/1,
    release_reader/1,
    reader_info/2
]).

-export([
    new_writer/1,
    write_chunk/2,
    abort_writer/1,
    release_writer/1,
    writer_info/2
]).

-export_type([
    reader/0,
    writer/0
]).

%%

-define(FILECHUNK(RELPATH, POS, MORE), #{
    '$' => chunk,
    rp => RELPATH,
    pos => POS,
    more => MORE
}).
-define(PAT_FILECHUNK(RELPATH, POS, MORE), #{
    '$' := chunk,
    rp := RELPATH,
    pos := POS,
    more := MORE
}).

-define(EOS(), #{
    '$' => eos
}).
-define(PAT_EOS(), #{
    '$' := eos
}).

-define(PAT_HEADER(), #{'$' := _}).

%%

-record(reader, {
    dirpath :: file:filename(),
    files :: #{_RelPath => reader_file()},
    queue :: [_RelPath :: file:filename()]
}).

-record(rfile, {
    abspath :: file:filename(),
    fd :: file:io_device() | eof,
    pos :: non_neg_integer()
}).

-opaque reader() :: #reader{}.
-type reader_file() :: #rfile{}.

-type reason() :: {atom(), _AbsPath :: file:filename(), _Details :: term()}.

%% @doc Initialize a reader for a snapshot directory.
%% Snapshot directory is a directory containing arbitrary number of regular
%% files in arbitrary subdirectory structure. Files are read in indeterminate
%% order. It's an error to have non-regular files in the directory (e.g. symlinks).
-spec new_reader(_Dir :: file:filename()) -> {ok, reader()}.
new_reader(DirPath) ->
    %% NOTE
    %% Opening all files at once, so there would be less error handling later
    %% during transfer.
    %% TODO
    %% Beware of how errors are handled: if one file fails to open, the whole
    %% process will exit. This is fine for the purpose of replication (because
    %% ra spawns separate process for each transfer), but may not be suitable
    %% for other use cases.
    Files = emqx_utils_fs:traverse_dir(
        fun(Path, Info, Acc) -> new_reader_file(Path, Info, DirPath, Acc) end,
        #{},
        DirPath
    ),
    {ok, #reader{
        dirpath = DirPath,
        files = Files,
        queue = maps:keys(Files)
    }}.

new_reader_file(Path, #file_info{type = regular}, DirPath, Acc) ->
    case file:open(Path, [read, binary, raw]) of
        {ok, IoDev} ->
            RelPath = emqx_utils_fs:find_relpath(Path, DirPath),
            File = #rfile{abspath = Path, fd = IoDev, pos = 0},
            Acc#{RelPath => File};
        {error, Reason} ->
            error({open_failed, Path, Reason})
    end;
new_reader_file(Path, #file_info{type = Type}, _, _Acc) ->
    error({bad_file_type, Path, Type});
new_reader_file(Path, {error, Reason}, _, _Acc) ->
    error({inaccessible, Path, Reason}).

%% @doc Read a chunk of data from the snapshot.
%% Returns `{last, Chunk, Reader}` when the last chunk is read. After that, one
%% should call `release_reader/1` to finalize the process (or `abort_reader/1` if
%% keeping the snapshot is desired).
-spec read_chunk(reader(), _Size :: non_neg_integer()) ->
    {last | next, _Chunk :: iodata(), reader()} | {error, reason()}.
read_chunk(R = #reader{files = Files, queue = [RelPath | Rest]}, Size) ->
    File = maps:get(RelPath, Files),
    case read_chunk_file(RelPath, File, Size) of
        {last, Chunk, FileRest} ->
            {next, Chunk, R#reader{files = Files#{RelPath := FileRest}, queue = Rest}};
        {next, Chunk, FileRest} ->
            {next, Chunk, R#reader{files = Files#{RelPath := FileRest}}};
        Error ->
            Error
    end;
read_chunk(R = #reader{queue = []}, _Size) ->
    {last, make_packet(?EOS()), R}.

read_chunk_file(RelPath, RFile0 = #rfile{fd = IoDev, pos = Pos, abspath = AbsPath}, Size) ->
    case file:read(IoDev, Size) of
        {ok, Chunk} ->
            ChunkSize = byte_size(Chunk),
            HasMore = ChunkSize div Size,
            RFile1 = RFile0#rfile{pos = Pos + ChunkSize},
            case HasMore of
                _Yes = 1 ->
                    Status = next,
                    RFile = RFile1;
                _No = 0 ->
                    Status = last,
                    RFile = release_reader_file(RFile1)
            end,
            Packet = make_packet(?FILECHUNK(RelPath, Pos, HasMore), Chunk),
            {Status, Packet, RFile};
        eof ->
            Packet = make_packet(?FILECHUNK(RelPath, Pos, 0)),
            {last, Packet, release_reader_file(RFile0)};
        {error, Reason} ->
            {error, {read_failed, AbsPath, Reason}}
    end.

%% @doc Aborts the snapshot reader, but does not release the snapshot files.
-spec abort_reader(reader()) -> ok.
abort_reader(#reader{files = Files}) ->
    lists:foreach(fun release_reader_file/1, maps:values(Files)).

%% @doc Aborts the snapshot reader and deletes the snapshot files.
-spec release_reader(reader()) -> ok.
release_reader(R = #reader{dirpath = DirPath}) ->
    ok = abort_reader(R),
    file:del_dir_r(DirPath).

release_reader_file(RFile = #rfile{fd = eof}) ->
    RFile;
release_reader_file(RFile = #rfile{fd = IoDev}) ->
    _ = file:close(IoDev),
    RFile#rfile{fd = eof}.

-spec reader_info(bytes_read, reader()) -> _Bytes :: non_neg_integer().
reader_info(bytes_read, #reader{files = Files}) ->
    maps:fold(fun(_, RFile, Sum) -> Sum + RFile#rfile.pos end, 0, Files).

%%

-record(writer, {
    dirpath :: file:filename(),
    files :: #{_RelPath :: file:filename() => writer_file()}
}).

-record(wfile, {
    abspath :: file:filename(),
    fd :: file:io_device() | eof,
    pos :: non_neg_integer()
}).

-opaque writer() :: #writer{}.
-type writer_file() :: #wfile{}.

%% @doc Initialize a writer into a snapshot directory.
%% The directory needs not to exist, it will be created if it doesn't.
%% Having non-empty directory is not an error, existing files will be
%% overwritten.
-spec new_writer(_Dir :: file:filename()) -> {ok, writer()} | {error, reason()}.
new_writer(DirPath) ->
    case filelib:ensure_path(DirPath) of
        ok ->
            {ok, #writer{dirpath = DirPath, files = #{}}};
        {error, Reason} ->
            {error, {mkdir_failed, DirPath, Reason}}
    end.

%% @doc Write a chunk of data to the snapshot.
%% Returns `{last, Writer}` when the last chunk is written. After that, one
%% should call `release_writer/1` to finalize the process.
-spec write_chunk(writer(), _Chunk :: binary()) ->
    {last | next, writer()} | {error, _Reason}.
write_chunk(W, Packet) ->
    case parse_packet(Packet) of
        {?PAT_FILECHUNK(RelPath, Pos, More), Chunk} ->
            write_chunk(W, RelPath, Pos, More, Chunk);
        {?PAT_EOS(), _Rest} ->
            %% TODO: Verify all files are `eof` at this point?
            {last, W};
        Error ->
            Error
    end.

write_chunk(W = #writer{files = Files}, RelPath, Pos, More, Chunk) ->
    case Files of
        #{RelPath := WFile} ->
            write_chunk(W, WFile, RelPath, Pos, More, Chunk);
        #{} when Pos == 0 ->
            case new_writer_file(W, RelPath) of
                WFile = #wfile{} ->
                    write_chunk(W, WFile, RelPath, Pos, More, Chunk);
                Error ->
                    Error
            end;
        #{} ->
            {error, {bad_chunk, RelPath, Pos}}
    end.

write_chunk(W = #writer{files = Files}, WFile0, RelPath, Pos, More, Chunk) ->
    case write_chunk_file(WFile0, Pos, More, Chunk) of
        WFile = #wfile{} ->
            {next, W#writer{files = Files#{RelPath => WFile}}};
        Error ->
            Error
    end.

new_writer_file(#writer{dirpath = DirPath}, RelPath) ->
    AbsPath = filename:join(DirPath, RelPath),
    _ = filelib:ensure_dir(AbsPath),
    case file:open(AbsPath, [write, binary, raw]) of
        {ok, IoDev} ->
            #wfile{
                abspath = AbsPath,
                fd = IoDev,
                pos = 0
            };
        {error, Reason} ->
            {error, {open_failed, AbsPath, Reason}}
    end.

write_chunk_file(WFile0 = #wfile{fd = IoDev, pos = Pos, abspath = AbsPath}, Pos, More, Chunk) ->
    ChunkSize = byte_size(Chunk),
    case (ChunkSize > 0) andalso file:write(IoDev, Chunk) of
        false ->
            WFile0;
        ok ->
            WFile1 = WFile0#wfile{pos = Pos + ChunkSize},
            case More of
                0 -> release_writer_file(WFile1);
                _ -> WFile1
            end;
        {error, Reason} ->
            {error, {write_failed, AbsPath, Reason}}
    end;
write_chunk_file(WFile = #wfile{pos = WPos}, Pos, _More, _Chunk) when Pos < WPos ->
    WFile;
write_chunk_file(#wfile{abspath = AbsPath}, Pos, _More, _Chunk) ->
    {error, {bad_chunk, AbsPath, Pos}}.

%% @doc Abort the writer and clean up unfinished snapshot files.
-spec abort_writer(writer()) -> ok | {error, file:posix()}.
abort_writer(W = #writer{dirpath = DirPath}) ->
    ok = release_writer(W),
    file:del_dir_r(DirPath).

%% @doc Release the writer and close all snapshot files.
-spec release_writer(writer()) -> ok.
release_writer(#writer{files = Files}) ->
    ok = lists:foreach(fun release_writer_file/1, maps:values(Files)).

release_writer_file(WFile = #wfile{fd = eof}) ->
    WFile;
release_writer_file(WFile = #wfile{fd = IoDev}) ->
    _ = file:close(IoDev),
    WFile#wfile{fd = eof}.

-spec writer_info(bytes_written, writer()) -> _Bytes :: non_neg_integer().
writer_info(bytes_written, #writer{files = Files}) ->
    maps:fold(fun(_, WFile, Sum) -> Sum + WFile#wfile.pos end, 0, Files).

%%

make_packet(Header) ->
    term_to_binary(Header).

make_packet(Header, Rest) ->
    HeaderBytes = term_to_binary(Header),
    <<HeaderBytes/binary, Rest/binary>>.

parse_packet(Packet) ->
    try binary_to_term(Packet, [safe, used]) of
        {Header = ?PAT_HEADER(), Length} ->
            {_, Rest} = split_binary(Packet, Length),
            {Header, Rest};
        {Header, _} ->
            {error, {bad_header, Header}}
    catch
        error:badarg ->
            {error, bad_packet}
    end.
