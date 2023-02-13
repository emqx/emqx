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

-module(emqx_ft_assembler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    [
        t_assemble_empty_transfer,
        t_assemble_complete_local_transfer,
        t_assemble_incomplete_transfer,
        t_assemble_no_meta,

        % NOTE
        % It depends on the side effects of all previous testcases.
        t_list_transfers
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    {ok, Pid} = emqx_ft_assembler_sup:start_link(),
    [
        {storage_root, "file_transfer_root"},
        {file_id, atom_to_binary(TC)},
        {assembler_sup, Pid}
        | Config
    ].

end_per_testcase(_TC, Config) ->
    ok = inspect_storage_root(Config),
    ok = gen:stop(?config(assembler_sup, Config)),
    ok = snabbkaffe:stop(),
    ok.

%%

-define(CLIENTID1, <<"thatsme">>).
-define(CLIENTID2, <<"thatsnotme">>).

t_assemble_empty_transfer(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID1, ?config(file_id, Config)},
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
        emqx_ft_storage_fs:list(Storage, Transfer, fragment)
    ),
    {ok, _AsmPid} = emqx_ft_storage_fs:assemble(Storage, Transfer, fun on_assembly_finished/1),
    {ok, Event} = ?block_until(#{?snk_kind := test_assembly_finished}),
    ?assertMatch(#{result := ok}, Event),
    ?assertEqual(
        {ok, <<>>},
        % TODO
        file:read_file(mk_assembly_filename(Config, Transfer, Filename))
    ),
    {ok, [Result = #{size := Size = 0}]} = emqx_ft_storage_fs:list(Storage, Transfer, result),
    ?assertEqual(
        {error, eof},
        emqx_ft_storage_fs:pread(Storage, Transfer, Result, 0, Size)
    ),
    ok.

t_assemble_complete_local_transfer(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID2, ?config(file_id, Config)},
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

    {ok, Fragments} = emqx_ft_storage_fs:list(Storage, Transfer, fragment),
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
        {ok, [
            #{
                path := AssemblyFilename,
                size := TransferSize,
                fragment := {result, #{}}
            }
        ]},
        emqx_ft_storage_fs:list(Storage, Transfer, result)
    ),
    ?assertMatch(
        {ok, #file_info{type = regular, size = TransferSize}},
        file:read_file_info(AssemblyFilename)
    ),
    ok = emqx_ft_content_gen:check_file_consistency(
        {Transfer, TransferSize},
        100,
        AssemblyFilename
    ).

t_assemble_incomplete_transfer(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID2, ?config(file_id, Config)},
    Filename = "incomplete.pdf",
    TransferSize = 10000 + rand:uniform(50000),
    SegmentSize = 4096,
    Gen = emqx_ft_content_gen:new({Transfer, TransferSize}, SegmentSize),
    Hash = emqx_ft_content_gen:hash(Gen, crypto:hash_init(sha256)),
    Meta = #{
        name => Filename,
        checksum => {sha256, Hash},
        size => TransferSize,
        expire_at => 42
    },
    ok = emqx_ft_storage_fs:store_filemeta(Storage, Transfer, Meta),
    Self = self(),
    {ok, _AsmPid} = emqx_ft_storage_fs:assemble(Storage, Transfer, fun(Result) ->
        Self ! {test_assembly_finished, Result}
    end),
    receive
        {test_assembly_finished, Result} ->
            ?assertMatch({error, _}, Result)
    after 1000 ->
        ct:fail("Assembler did not called callback")
    end.

t_assemble_no_meta(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID2, ?config(file_id, Config)},
    Self = self(),
    {ok, _AsmPid} = emqx_ft_storage_fs:assemble(Storage, Transfer, fun(Result) ->
        Self ! {test_assembly_finished, Result}
    end),
    receive
        {test_assembly_finished, Result} ->
            ?assertMatch({error, _}, Result)
    after 1000 ->
        ct:fail("Assembler did not called callback")
    end.

mk_assembly_filename(Config, {ClientID, FileID}, Filename) ->
    filename:join([?config(storage_root, Config), ClientID, FileID, result, Filename]).

on_assembly_finished(Result) ->
    ?tp(test_assembly_finished, #{result => Result}).

%%

t_list_transfers(Config) ->
    Storage = storage(Config),
    ?assertMatch(
        {ok, #{
            {?CLIENTID1, <<"t_assemble_empty_transfer">>} := #{
                status := complete,
                result := [#{path := _, size := 0, fragment := {result, _}}]
            },
            {?CLIENTID2, <<"t_assemble_complete_local_transfer">>} := #{
                status := complete,
                result := [#{path := _, size := Size, fragment := {result, _}}]
            }
        }} when Size > 0,
        emqx_ft_storage_fs:transfers(Storage)
    ).

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
        type => local,
        root => ?config(storage_root, Config)
    }.
