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

-module(emqx_ft_storage_fs).

-behaviour(emqx_ft_storage).

-export([store_filemeta/3]).
-export([store_segment/3]).
-export([list/3]).
-export([pread/5]).
-export([assemble/3]).

-export([transfers/1]).

-export([open_file/3]).
-export([complete/4]).
-export([write/2]).
-export([discard/1]).

-type transfer() :: emqx_ft:transfer().
-type offset() :: emqx_ft:offset().
-type filemeta() :: emqx_ft:filemeta().
-type segment() :: emqx_ft:segment().

-type segmentinfo() :: #{
    offset := offset(),
    size := _Bytes :: non_neg_integer()
}.

-type transferinfo() :: #{
    status := complete | incomplete,
    result => [filefrag({result, #{}})]
}.

% TODO naming
-type filefrag(T) :: #{
    path := file:name(),
    timestamp := emqx_datetime:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    fragment := T
}.

-type filefrag() :: filefrag(
    {filemeta, filemeta()}
    | {segment, segmentinfo()}
    | {result, #{}}
).

-define(FRAGDIR, frags).
-define(TEMPDIR, tmp).
-define(RESULTDIR, result).
-define(MANIFEST, "MANIFEST.json").
-define(SEGMENT, "SEG").

-type root() :: file:name().

% -record(st, {
%     root :: file:name()
% }).

%% TODO
-type storage() :: root().

%%

% -define(PROCREF(Root), {via, gproc, {n, l, {?MODULE, Root}}}).

% -spec start_link(root()) ->
%     {ok, pid()} | {error, already_started}.
% start_link(Root) ->
%     gen_server:start_link(?PROCREF(Root), ?MODULE, [], []).

%% Store manifest in the backing filesystem.
%% Atomic operation.
-spec store_filemeta(storage(), transfer(), filemeta()) ->
    % Quota? Some lower level errors?
    {ok, emqx_ft_storage:ctx()} | {error, conflict} | {error, _TODO}.
store_filemeta(Storage, Transfer, Meta) ->
    % TODO safeguard against bad clientids / fileids.
    Filepath = mk_filepath(Storage, Transfer, [?FRAGDIR], ?MANIFEST),
    case read_file(Filepath, fun decode_filemeta/1) of
        {ok, Meta} ->
            _ = touch_file(Filepath),
            ok;
        {ok, _Conflict} ->
            % TODO
            % We won't see conflicts in case of concurrent `store_filemeta`
            % requests. It's rather odd scenario so it's fine not to worry
            % about it too much now.
            {error, conflict};
        {error, Reason} when Reason =:= notfound; Reason =:= corrupted; Reason =:= enoent ->
            write_file_atomic(Storage, Transfer, Filepath, encode_filemeta(Meta))
    end.

%% Store a segment in the backing filesystem.
%% Atomic operation.
-spec store_segment(storage(), transfer(), segment()) ->
    % Where is the checksum gets verified? Upper level probably.
    % Quota? Some lower level errors?
    ok | {error, _TODO}.
store_segment(Storage, Transfer, Segment = {_Offset, Content}) ->
    Filepath = mk_filepath(Storage, Transfer, [?FRAGDIR], mk_segment_filename(Segment)),
    write_file_atomic(Storage, Transfer, Filepath, Content).

-spec list(storage(), transfer(), _What :: fragment | result) ->
    % Some lower level errors? {error, notfound}?
    % Result will contain zero or only one filemeta.
    {ok, [filefrag({filemeta, filemeta()} | {segment, segmentinfo()})]} | {error, _TODO}.
list(Storage, Transfer, What) ->
    Dirname = mk_filedir(Storage, Transfer, get_subdirs_for(What)),
    case file:list_dir(Dirname) of
        {ok, Filenames} ->
            % TODO
            % In case of `What = result` there might be more than one file (though
            % extremely bad luck is needed for that, e.g. concurrent assemblers with
            % different filemetas from different nodes). This might be unexpected for a
            % client given the current protocol, yet might be helpful in the future.
            {ok, filtermap_files(get_filefrag_fun_for(What), Dirname, Filenames)};
        {error, enoent} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

get_subdirs_for(fragment) ->
    [?FRAGDIR];
get_subdirs_for(result) ->
    [?RESULTDIR].

get_filefrag_fun_for(fragment) ->
    fun mk_filefrag/2;
get_filefrag_fun_for(result) ->
    fun mk_result_filefrag/2.

-spec pread(storage(), transfer(), filefrag(), offset(), _Size :: non_neg_integer()) ->
    {ok, _Content :: iodata()} | {error, _TODO}.
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

-spec assemble(storage(), transfer(), fun((ok | {error, term()}) -> any())) ->
    % {ok, _Assembler :: pid()} | {error, incomplete} | {error, badrpc} | {error, _TODO}.
    {ok, _Assembler :: pid()} | {error, _TODO}.
assemble(Storage, Transfer, Callback) ->
    emqx_ft_assembler_sup:start_child(Storage, Transfer, Callback).

%%

-spec transfers(storage()) ->
    {ok, #{transfer() => transferinfo()}}.
transfers(Storage) ->
    % TODO `Continuation`
    % There might be millions of transfers on the node, we need a protocol and
    % storage schema to iterate through them effectively.
    ClientIds = try_list_dir(get_storage_root(Storage)),
    {ok,
        lists:foldl(
            fun(ClientId, Acc) -> transfers(Storage, ClientId, Acc) end,
            #{},
            ClientIds
        )}.

transfers(Storage, ClientId, AccIn) ->
    Dirname = mk_client_filedir(Storage, ClientId),
    case file:list_dir(Dirname) of
        {ok, FileIds} ->
            lists:foldl(
                fun(FileId, Acc) ->
                    Transfer = {filename_to_binary(ClientId), filename_to_binary(FileId)},
                    read_transferinfo(Storage, Transfer, Acc)
                end,
                AccIn,
                FileIds
            );
        {error, _Reason} ->
            % TODO worth logging
            AccIn
    end.

read_transferinfo(Storage, Transfer, Acc) ->
    case list(Storage, Transfer, result) of
        {ok, Result = [_ | _]} ->
            Info = #{status => complete, result => Result},
            Acc#{Transfer => Info};
        {ok, []} ->
            Info = #{status => incomplete},
            Acc#{Transfer => Info};
        {error, _Reason} ->
            % TODO worth logging
            Acc
    end.

%%

-type handle() :: {file:name(), io:device(), crypto:hash_state()}.

-spec open_file(storage(), transfer(), filemeta()) ->
    {ok, handle()} | {error, _TODO}.
open_file(Storage, Transfer, Filemeta) ->
    Filename = maps:get(name, Filemeta),
    TempFilepath = mk_temp_filepath(Storage, Transfer, Filename),
    _ = filelib:ensure_dir(TempFilepath),
    case file:open(TempFilepath, [write, raw, binary]) of
        {ok, Handle} ->
            _ = file:truncate(Handle),
            {ok, {TempFilepath, Handle, init_checksum(Filemeta)}};
        {error, _} = Error ->
            Error
    end.

-spec write(handle(), iodata()) ->
    ok | {error, _TODO}.
write({Filepath, IoDevice, Ctx}, IoData) ->
    case file:write(IoDevice, IoData) of
        ok ->
            {ok, {Filepath, IoDevice, update_checksum(Ctx, IoData)}};
        {error, _} = Error ->
            Error
    end.

-spec complete(storage(), transfer(), filemeta(), handle()) ->
    ok | {error, {checksum, _Algo, _Computed}} | {error, _TODO}.
complete(Storage, Transfer, Filemeta, Handle = {Filepath, IoDevice, Ctx}) ->
    TargetFilepath = mk_filepath(Storage, Transfer, [?RESULTDIR], maps:get(name, Filemeta)),
    case verify_checksum(Ctx, Filemeta) of
        ok ->
            ok = file:close(IoDevice),
            mv_temp_file(Filepath, TargetFilepath);
        {error, _} = Error ->
            _ = discard(Handle),
            Error
    end.

-spec discard(handle()) ->
    ok.
discard({Filepath, IoDevice, _Ctx}) ->
    ok = file:close(IoDevice),
    file:delete(Filepath).

init_checksum(#{checksum := {Algo, _}}) ->
    crypto:hash_init(Algo);
init_checksum(#{}) ->
    undefined.

update_checksum(Ctx, IoData) when Ctx /= undefined ->
    crypto:hash_update(Ctx, IoData);
update_checksum(undefined, _IoData) ->
    undefined.

verify_checksum(Ctx, #{checksum := {Algo, Digest}}) when Ctx /= undefined ->
    case crypto:hash_final(Ctx) of
        Digest ->
            ok;
        Mismatch ->
            {error, {checksum, Algo, binary:encode_hex(Mismatch)}}
    end;
verify_checksum(undefined, _) ->
    ok.

%%

% -spec init(root()) -> {ok, storage()}.
% init(Root) ->
%     % TODO: garbage_collect(...)
%     {ok, Root}.

% %%

-define(PRELUDE(Vsn, Meta), [<<"filemeta">>, Vsn, Meta]).

% encode_filemeta(Meta) ->
%     emqx_json:encode(
%         ?PRELUDE(
%             _Vsn = 1,
%             maps:map(
%                 fun
%                     (name, Name) ->
%                         {<<"name">>, Name};
%                     (size, Size) ->
%                         {<<"size">>, Size};
%                     (checksum, {sha256, Hash}) ->
%                         {<<"checksum">>, <<"sha256:", (binary:encode_hex(Hash))/binary>>};
%                     (expire_at, ExpiresAt) ->
%                         {<<"expire_at">>, ExpiresAt};
%                     (segments_ttl, TTL) ->
%                         {<<"segments_ttl">>, TTL};
%                     (user_data, UserData) ->
%                         {<<"user_data">>, UserData}
%                 end,
%                 Meta
%             )
%         )
%     ).

encode_filemeta(Meta) ->
    % TODO: Looks like this should be hocon's responsibility.
    Schema = emqx_ft_schema:schema(filemeta),
    Term = hocon_tconf:make_serializable(Schema, emqx_map_lib:binary_key_map(Meta), #{}),
    emqx_json:encode(?PRELUDE(_Vsn = 1, Term)).

decode_filemeta(Binary) ->
    Schema = emqx_ft_schema:schema(filemeta),
    ?PRELUDE(_Vsn = 1, Term) = emqx_json:decode(Binary, [return_maps]),
    hocon_tconf:check_plain(Schema, Term, #{atom_key => true, required => false}).

% map_into(Fun, Into, Ks, Map) ->
%     map_foldr(map_into_fn(Fun, Into), Into, Ks, Map).

% map_into_fn(Fun, L) when is_list(L) ->
%     fun(K, V, Acc) -> [{K, Fun(K, V)} || Acc] end.

% map_foldr(_Fun, Acc, [], _) ->
%     Acc;
% map_foldr(Fun, Acc, [K | Ks], Map) when is_map_key(K, Map) ->
%     Fun(K, maps:get(K, Map), map_foldr(Fun, Acc, Ks, Map));
% map_foldr(Fun, Acc, [_ | Ks], Map) ->
%     map_foldr(Fun, Acc, Ks, Map).

%%

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
    filename:join([get_storage_root(Storage), ClientId, FileId | SubDirs]).

mk_client_filedir(Storage, ClientId) ->
    filename:join([get_storage_root(Storage), ClientId]).

mk_filepath(Storage, Transfer, SubDirs, Filename) ->
    filename:join(mk_filedir(Storage, Transfer, SubDirs), Filename).

try_list_dir(Dirname) ->
    case file:list_dir(Dirname) of
        {ok, List} -> List;
        {error, _} -> []
    end.

get_storage_root(Storage) ->
    maps:get(root, Storage, filename:join(emqx:data_dir(), "file_transfer")).

-include_lib("kernel/include/file.hrl").

read_file(Filepath) ->
    file:read_file(Filepath).

read_file(Filepath, DecodeFun) ->
    case read_file(Filepath) of
        {ok, Content} ->
            safe_decode(Content, DecodeFun);
        {error, _} = Error ->
            Error
    end.

safe_decode(Content, DecodeFun) ->
    try
        {ok, DecodeFun(Content)}
    catch
        _C:_R:_Stacktrace ->
            % TODO: Log?
            {error, corrupted}
    end.

write_file_atomic(Storage, Transfer, Filepath, Content) when is_binary(Content) ->
    TempFilepath = mk_temp_filepath(Storage, Transfer, filename:basename(Filepath)),
    Result = emqx_misc:pipeline(
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
    Unique = erlang:unique_integer([positive]),
    filename:join(mk_filedir(Storage, Transfer, [?TEMPDIR]), mk_filename([Unique, ".", Filename])).

mk_filename(Comps) ->
    lists:append(lists:map(fun mk_filename_component/1, Comps)).

mk_filename_component(I) when is_integer(I) -> integer_to_list(I);
mk_filename_component(A) when is_atom(A) -> atom_to_list(A);
mk_filename_component(B) when is_binary(B) -> unicode:characters_to_list(B);
mk_filename_component(S) when is_list(S) -> S.

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
    mk_filefrag(Dirname, Filename, filemeta, fun read_filemeta/2);
mk_filefrag(Dirname, Filename = ?SEGMENT ++ _) ->
    mk_filefrag(Dirname, Filename, segment, fun read_segmentinfo/2);
mk_filefrag(_Dirname, _Filename) ->
    % TODO this is unexpected, worth logging?
    false.

mk_result_filefrag(Dirname, Filename) ->
    % NOTE
    % Any file in the `?RESULTDIR` subdir is currently considered the result of
    % the file transfer.
    mk_filefrag(Dirname, Filename, result, fun(_, _) -> {ok, #{}} end).

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
            % TODO loss of information
            false
    end.

read_filemeta(_Filename, Filepath) ->
    read_file(Filepath, fun decode_filemeta/1).

read_segmentinfo(Filename, _Filepath) ->
    break_segment_filename(Filename).

filename_to_binary(S) when is_list(S) -> unicode:characters_to_binary(S);
filename_to_binary(B) when is_binary(B) -> B.
