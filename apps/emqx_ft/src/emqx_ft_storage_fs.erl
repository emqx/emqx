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

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

% -compile(export_all).

-export([store_filemeta/3]).
-export([store_segment/3]).
-export([list/2]).
-export([assemble/3]).

-export([open_file/3]).
-export([complete/4]).
-export([write/2]).
-export([discard/1]).

% -behaviour(gen_server).
% -export([init/1]).
% -export([handle_call/3]).
% -export([handle_cast/2]).

-type json_value() ::
    null
    | boolean()
    | binary()
    | number()
    | [json_value()]
    | #{binary() => json_value()}.

-reflect_type([json_value/0]).

-type transfer() :: emqx_ft:transfer().
-type offset() :: emqx_ft:offset().

%% TODO: move to `emqx_ft` interface module
% -type sha256_hex() :: <<_:512>>.

-type filemeta() :: #{
    %% Display name
    name := string(),
    %% Size in bytes, as advertised by the client.
    %% Client is free to specify here whatever it wants, which means we can end
    %% up with a file of different size after assembly. It's not clear from
    %% specification what that means (e.g. what are clients' expectations), we
    %% currently do not condider that an error (or, specifically, a signal that
    %% the resulting file is corrupted during transmission).
    size => _Bytes :: non_neg_integer(),
    checksum => {sha256, <<_:256>>},
    expire_at := emqx_datetime:epoch_second(),
    %% TTL of individual segments
    %% Somewhat confusing that we won't know it on the nodes where the filemeta
    %% is missing.
    segments_ttl => _Seconds :: pos_integer(),
    user_data => json_value()
}.

-type segment() :: {offset(), _Content :: binary()}.

-type segmentinfo() :: #{
    offset := offset(),
    size := _Bytes :: non_neg_integer()
}.

-type filefrag(T) :: #{
    path := file:name(),
    timestamp := emqx_datetime:epoch_second(),
    fragment := T
}.

-type filefrag() :: filefrag({filemeta, filemeta()} | {segment, segmentinfo()}).

-define(MANIFEST, "MANIFEST.json").
-define(SEGMENT, "SEG").
-define(TEMP, "TMP").

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
    ok | {error, conflict} | {error, _TODO}.
store_filemeta(Storage, Transfer, Meta) ->
    Filepath = mk_filepath(Storage, Transfer, ?MANIFEST),
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
            write_file_atomic(Filepath, encode_filemeta(Meta))
    end.

%% Store a segment in the backing filesystem.
%% Atomic operation.
-spec store_segment(storage(), transfer(), segment()) ->
    % Where is the checksum gets verified? Upper level probably.
    % Quota? Some lower level errors?
    ok | {error, _TODO}.
store_segment(Storage, Transfer, Segment = {_Offset, Content}) ->
    Filepath = mk_filepath(Storage, Transfer, mk_segment_filename(Segment)),
    write_file_atomic(Filepath, Content).

-spec list(storage(), transfer()) ->
    % Some lower level errors? {error, notfound}?
    % Result will contain zero or only one filemeta.
    {ok, list(filefrag())} | {error, _TODO}.
list(Storage, Transfer) ->
    Dirname = mk_filedir(Storage, Transfer),
    case file:list_dir(Dirname) of
        {ok, Filenames} ->
            {ok, filtermap_files(fun mk_filefrag/2, Dirname, Filenames)};
        {error, enoent} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

-spec assemble(storage(), transfer(), fun((ok | {error, term()}) -> any())) ->
    % {ok, _Assembler :: pid()} | {error, incomplete} | {error, badrpc} | {error, _TODO}.
    {ok, _Assembler :: pid()} | {error, _TODO}.
assemble(Storage, Transfer, Callback) ->
    emqx_ft_assembler_sup:start_child(Storage, Transfer, Callback).

%%

-type handle() :: {file:name(), io:device(), crypto:hash_state()}.

-spec open_file(storage(), transfer(), filemeta()) ->
    {ok, handle()} | {error, _TODO}.
open_file(Storage, Transfer, Filemeta) ->
    Filename = maps:get(name, Filemeta),
    Filepath = mk_filepath(Storage, Transfer, Filename),
    TempFilepath = mk_temp_filepath(Filepath),
    case file:open(TempFilepath, [write, raw]) of
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
    TargetFilepath = mk_filepath(Storage, Transfer, maps:get(name, Filemeta)),
    case verify_checksum(Ctx, Filemeta) of
        ok ->
            ok = file:close(IoDevice),
            file:rename(Filepath, TargetFilepath);
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

schema() ->
    #{
        roots => [
            {name, hoconsc:mk(string(), #{required => true})},
            {size, hoconsc:mk(non_neg_integer())},
            {expire_at, hoconsc:mk(non_neg_integer())},
            {checksum, hoconsc:mk({atom(), binary()}, #{converter => converter(checksum)})},
            {segments_ttl, hoconsc:mk(pos_integer())},
            {user_data, hoconsc:mk(json_value())}
        ]
    }.

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
    Term = hocon_tconf:make_serializable(schema(), emqx_map_lib:binary_key_map(Meta), #{}),
    emqx_json:encode(?PRELUDE(_Vsn = 1, Term)).

decode_filemeta(Binary) ->
    ?PRELUDE(_Vsn = 1, Term) = emqx_json:decode(Binary, [return_maps]),
    hocon_tconf:check_plain(schema(), Term, #{atom_key => true, required => false}).

converter(checksum) ->
    fun
        (undefined, #{}) ->
            undefined;
        ({sha256, Bin}, #{make_serializable := true}) ->
            _ = is_binary(Bin) orelse throw({expected_type, string}),
            _ = byte_size(Bin) =:= 32 orelse throw({expected_length, 32}),
            binary:encode_hex(Bin);
        (Hex, #{}) ->
            _ = is_binary(Hex) orelse throw({expected_type, string}),
            _ = byte_size(Hex) =:= 64 orelse throw({expected_length, 64}),
            {sha256, binary:decode_hex(Hex)}
    end.

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

mk_filedir(Storage, {ClientId, FileId}) ->
    filename:join([get_storage_root(Storage), ClientId, FileId]).

mk_filepath(Storage, Transfer, Filename) ->
    filename:join(mk_filedir(Storage, Transfer), Filename).

get_storage_root(Storage) ->
    Storage.

%%

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

write_file_atomic(Filepath, Content) when is_binary(Content) ->
    Result = emqx_misc:pipeline(
        [
            fun filelib:ensure_dir/1,
            fun mk_temp_filepath/1,
            fun write_contents/2,
            fun mv_temp_file/1
        ],
        Filepath,
        Content
    ),
    case Result of
        {ok, {Filepath, TempFilepath}, _} ->
            _ = file:delete(TempFilepath),
            ok;
        {error, Reason, _} ->
            {error, Reason}
    end.

mk_temp_filepath(Filepath) ->
    Dirname = filename:dirname(Filepath),
    Filename = filename:basename(Filepath),
    Unique = erlang:unique_integer([positive]),
    TempFilepath = filename:join(Dirname, ?TEMP ++ integer_to_list(Unique) ++ "." ++ Filename),
    {Filepath, TempFilepath}.

write_contents({_Filepath, TempFilepath}, Content) ->
    file:write_file(TempFilepath, Content).

mv_temp_file({Filepath, TempFilepath}) ->
    file:rename(TempFilepath, Filepath).

touch_file(Filepath) ->
    Now = erlang:localtime(),
    file:change_time(Filepath, _Mtime = Now, _Atime = Now).

filtermap_files(Fun, Dirname, Filenames) ->
    lists:filtermap(fun(Filename) -> Fun(Dirname, Filename) end, Filenames).

mk_filefrag(Dirname, Filename = ?MANIFEST) ->
    mk_filefrag(Dirname, Filename, fun read_filemeta/2);
mk_filefrag(Dirname, Filename = ?SEGMENT ++ _) ->
    mk_filefrag(Dirname, Filename, fun read_segmentinfo/2);
mk_filefrag(_Dirname, _) ->
    false.

mk_filefrag(Dirname, Filename, Fun) ->
    Filepath = filename:join(Dirname, Filename),
    Fileinfo = file:read_file_info(Filepath),
    case Fun(Filename, Filepath) of
        {ok, Frag} ->
            {true, #{
                path => Filepath,
                timestamp => Fileinfo#file_info.mtime,
                fragment => Frag
            }};
        {error, _Reason} ->
            false
    end.

read_filemeta(_Filename, Filepath) ->
    read_file(Filepath, fun decode_filemeta/1).

read_segmentinfo(Filename, _Filepath) ->
    break_segment_filename(Filename).
