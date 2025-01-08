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

-module(emqx_ft_assembler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/file.hrl").

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
    {ok, Apps} = application:ensure_all_started(gproc),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop_apps(?config(suite_apps, Config)).

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    {ok, Pid} = emqx_ft_assembler_sup:start_link(),
    [
        {storage_root, <<"file_transfer_root">>},
        {exports_root, <<"file_transfer_exports">>},
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
    Status = complete_assemble(Storage, Transfer, 0),
    ?assertEqual({shutdown, ok}, Status),
    {ok, [_Result = #{size := _Size = 0}]} = list_exports(Config, Transfer),
    % ?assertEqual(
    %     {error, eof},
    %     emqx_ft_storage_fs:pread(Storage, Transfer, Result, 0, Size)
    % ),
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
        fun({Content, SegmentNum, _Meta}) ->
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

    Status = complete_assemble(Storage, Transfer, TransferSize),
    ?assertEqual({shutdown, ok}, Status),

    ?assertMatch(
        {ok, [
            #{
                size := TransferSize,
                meta := #{}
            }
        ]},
        list_exports(Config, Transfer)
    ),
    {ok, [#{path := AssemblyFilename}]} = list_exports(Config, Transfer),
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
    Status = complete_assemble(Storage, Transfer, TransferSize),
    ?assertMatch({shutdown, {error, _}}, Status).

t_assemble_no_meta(Config) ->
    Storage = storage(Config),
    Transfer = {?CLIENTID2, ?config(file_id, Config)},
    Status = complete_assemble(Storage, Transfer, 42),
    ?assertMatch({shutdown, {error, {incomplete, _}}}, Status).

complete_assemble(Storage, Transfer, Size) ->
    complete_assemble(Storage, Transfer, Size, 1000).

complete_assemble(Storage, Transfer, Size, Timeout) ->
    {async, Pid} = emqx_ft_storage_fs:assemble(Storage, Transfer, Size, #{}),
    MRef = erlang:monitor(process, Pid),
    Pid ! kickoff,
    receive
        {'DOWN', MRef, process, Pid, Result} ->
            Result
    after Timeout ->
        ct:fail("Assembler did not finish in time")
    end.

%%

t_list_transfers(Config) ->
    {ok, Exports} = list_exports(Config),
    ?assertMatch(
        [
            #{
                transfer := {?CLIENTID2, <<"t_assemble_complete_local_transfer">>},
                path := _,
                size := Size,
                meta := #{name := "topsecret.pdf"}
            },
            #{
                transfer := {?CLIENTID1, <<"t_assemble_empty_transfer">>},
                path := _,
                size := 0,
                meta := #{name := "important.pdf"}
            }
        ] when Size > 0,
        lists:sort(Exports)
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

list_exports(Config) ->
    {emqx_ft_storage_exporter_fs, Options} = exporter(Config),
    emqx_ft_storage_exporter_fs:list_local(Options).

list_exports(Config, Transfer) ->
    {emqx_ft_storage_exporter_fs, Options} = exporter(Config),
    emqx_ft_storage_exporter_fs:list_local_transfer(Options, Transfer).

exporter(Config) ->
    emqx_ft_storage_exporter:exporter(storage(Config)).

storage(Config) ->
    emqx_utils_maps:deep_get(
        [storage, local],
        emqx_ft_schema:translate(#{
            <<"storage">> => #{
                <<"local">> => #{
                    <<"segments">> => #{
                        <<"root">> => ?config(storage_root, Config)
                    },
                    <<"exporter">> => #{
                        <<"local">> => #{
                            <<"enable">> => true,
                            <<"root">> => ?config(exports_root, Config)
                        }
                    }
                }
            }
        })
    ).
