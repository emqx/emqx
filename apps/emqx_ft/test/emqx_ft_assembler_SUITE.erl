%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_assembler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    % {ok, Apps} = application:ensure_all_started(emqx_ft),
    % [{suite_apps, Apps} | Config].
    % ok = emqx_common_test_helpers:start_apps([emqx_ft]),
    Config.

end_per_suite(_Config) ->
    % lists:foreach(fun application:stop/1, lists:reverse(?config(suite_apps, Config))).
    % ok = emqx_common_test_helpers:stop_apps([emqx_ft]),
    ok.

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Root = filename:join(["roots", TC]),
    {ok, Pid} = emqx_ft_assembler_sup:start_link(),
    [{storage_root, Root}, {assembler_sup, Pid} | Config].

end_per_testcase(_TC, Config) ->
    ok = inspect_storage_root(Config),
    ok = gen:stop(?config(assembler_sup, Config)),
    ok = snabbkaffe:stop(),
    ok.

%%

-define(CLIENTID, <<"thatsme">>).

t_assemble_empty_transfer(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID, mk_fileid()},
    Filename = "important.pdf",
    Meta = #{
        name => Filename,
        size => 0,
        expire_at => 42
    },
    ok = emqx_ft_storage_fs:store_filemeta(Storage, Transfer, Meta),
    ?assertMatch(
        {ok, [
            #{
                path := _,
                timestamp := {{_, _, _}, {_, _, _}},
                fragment := {filemeta, Meta}
            }
        ]},
        emqx_ft_storage_fs:list(Storage, Transfer)
    ),
    {ok, _AsmPid} = emqx_ft_storage_fs:assemble(Storage, Transfer, fun on_assembly_finished/1),
    {ok, Event} = ?block_until(#{?snk_kind := test_assembly_finished}),
    ?assertMatch(#{result := ok}, Event),
    ?assertEqual(
        {ok, <<>>},
        % TODO
        file:read_file(mk_assembly_filename(Config, Transfer, Filename))
    ),
    ok.

t_assemble_complete_local_transfer(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID, mk_fileid()},
    Filename = "topsecret.pdf",
    TransferSize = 10000 + rand:uniform(50000),
    SegmentSize = 4096,
    Gen = emqx_ft_content_gen:new({Transfer, TransferSize}, SegmentSize),
    Hash = emqx_ft_content_gen:hash(Gen, crypto:hash_init(sha256)),
    Meta = #{
        name => Filename,
        checksum => {sha256, Hash},
        expire_at => 42
    },

    ok = emqx_ft_storage_fs:store_filemeta(Storage, Transfer, Meta),
    _ = emqx_ft_content_gen:consume(
        Gen,
        fun({Content, SegmentNum, _SegmentCount}) ->
            Offset = (SegmentNum - 1) * SegmentSize,
            ?assertEqual(
                ok,
                emqx_ft_storage_fs:store_segment(Storage, Transfer, {Offset, Content})
            )
        end
    ),

    {ok, Fragments} = emqx_ft_storage_fs:list(Storage, Transfer),
    ?assertEqual((TransferSize div SegmentSize) + 1 + 1, length(Fragments)),
    ?assertEqual(
        [Meta],
        [FM || #{fragment := {filemeta, FM}} <- Fragments],
        Fragments
    ),

    {ok, _AsmPid} = emqx_ft_storage_fs:assemble(Storage, Transfer, fun on_assembly_finished/1),
    {ok, Event} = ?block_until(#{?snk_kind := test_assembly_finished}),
    ?assertMatch(#{result := ok}, Event),

    AssemblyFilename = mk_assembly_filename(Config, Transfer, Filename),
    ?assertMatch(
        {ok, #file_info{type = regular, size = TransferSize}},
        file:read_file_info(AssemblyFilename)
    ),
    ok = emqx_ft_content_gen:check_file_consistency(
        {Transfer, TransferSize},
        100,
        AssemblyFilename
    ).

mk_assembly_filename(Config, {ClientID, FileID}, Filename) ->
    filename:join([?config(storage_root, Config), ClientID, FileID, result, Filename]).

on_assembly_finished(Result) ->
    ?tp(test_assembly_finished, #{result => Result}).

%%

-include_lib("kernel/include/file.hrl").

inspect_storage_root(Config) ->
    inspect_dir(?config(storage_root, Config)).

inspect_dir(Dir) ->
    FileInfos = filelib:fold_files(
        Dir,
        ".*",
        true,
        fun(Filename, Acc) -> orddict:store(Filename, inspect_file(Filename), Acc) end,
        orddict:new()
    ),
    ct:pal("inspect '~s': ~p", [Dir, FileInfos]).

inspect_file(Filename) ->
    {ok, Info} = file:read_file_info(Filename),
    {Info#file_info.type, Info#file_info.size, Info#file_info.mtime}.

mk_fileid() ->
    integer_to_binary(erlang:system_time(millisecond)).

storage(Config) ->
    #{
        root => ?config(storage_root, Config)
    }.
