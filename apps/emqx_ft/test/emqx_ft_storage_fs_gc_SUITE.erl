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

-module(emqx_ft_storage_fs_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_ft/include/emqx_ft_storage_fs.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_ft),
    ok = emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([]),
    ok.

init_per_testcase(TC, Config) ->
    _ = application:unset_env(emqx_ft, gc_interval),
    _ = application:unset_env(emqx_ft, min_segments_ttl),
    _ = application:unset_env(emqx_ft, max_segments_ttl),
    ok = emqx_common_test_helpers:start_app(
        emqx_ft,
        fun(emqx_ft) ->
            ok = emqx_config:put([file_transfer, storage], #{
                type => local,
                root => mk_root(TC, Config)
            })
        end
    ),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = application:stop(emqx_ft),
    ok.

mk_root(TC, Config) ->
    filename:join([?config(priv_dir, Config), <<"file_transfer">>, TC, atom_to_binary(node())]).

%%

t_gc_triggers_periodically(_Config) ->
    Interval = 1000,
    ok = application:set_env(emqx_ft, gc_interval, Interval),
    ok = emqx_ft_storage_fs_gc:reset(emqx_ft_conf:storage()),
    ?check_trace(
        timer:sleep(Interval * 3),
        fun(Trace) ->
            [Event, _ | _] = ?of_kind(garbage_collection, Trace),
            ?assertMatch(
                #{
                    stats := #gcstats{
                        files = 0,
                        directories = 0,
                        space = 0,
                        errors = #{} = Errors
                    }
                } when map_size(Errors) == 0,
                Event
            )
        end
    ).

t_gc_triggers_manually(_Config) ->
    ?check_trace(
        ?assertMatch(
            #gcstats{files = 0, directories = 0, space = 0, errors = #{} = Errors} when
                map_size(Errors) == 0,
            emqx_ft_storage_fs_gc:collect(emqx_ft_conf:storage())
        ),
        fun(Trace) ->
            [Event] = ?of_kind(garbage_collection, Trace),
            ?assertMatch(
                #{stats := #gcstats{}},
                Event
            )
        end
    ).

t_gc_complete_transfers(_Config) ->
    Storage = emqx_ft_conf:storage(),
    Transfers = [
        {
            T1 = {<<"client1">>, mk_file_id()},
            "cat.cur",
            emqx_ft_content_gen:new({?LINE, S1 = 42}, SS1 = 16)
        },
        {
            T2 = {<<"client2">>, mk_file_id()},
            "cat.ico",
            emqx_ft_content_gen:new({?LINE, S2 = 420}, SS2 = 64)
        },
        {
            T3 = {<<"client42">>, mk_file_id()},
            "cat.jpg",
            emqx_ft_content_gen:new({?LINE, S3 = 42000}, SS3 = 1024)
        }
    ],
    % 1. Start all transfers
    TransferSizes = emqx_misc:pmap(
        fun(Transfer) -> start_transfer(Storage, Transfer) end,
        Transfers
    ),
    ?assertEqual([S1, S2, S3], TransferSizes),
    ?assertMatch(
        #gcstats{files = 0, directories = 0, errors = #{} = Es} when map_size(Es) == 0,
        emqx_ft_storage_fs_gc:collect(Storage)
    ),
    % 2. Complete just the first transfer
    ?assertEqual(
        ok,
        complete_transfer(Storage, T1, S1)
    ),
    GCFiles1 = ceil(S1 / SS1) + 1,
    ?assertMatch(
        #gcstats{
            files = GCFiles1,
            directories = 2,
            space = Space,
            errors = #{} = Es
        } when Space > S1 andalso map_size(Es) == 0,
        emqx_ft_storage_fs_gc:collect(Storage)
    ),
    % 3. Complete rest of transfers
    ?assertEqual(
        [ok, ok],
        emqx_misc:pmap(
            fun({Transfer, Size}) -> complete_transfer(Storage, Transfer, Size) end,
            [{T2, S2}, {T3, S3}]
        )
    ),
    GCFiles2 = ceil(S2 / SS2) + 1,
    GCFiles3 = ceil(S3 / SS3) + 1,
    ?assertMatch(
        #gcstats{
            files = Files,
            directories = 4,
            space = Space,
            errors = #{} = Es
        } when
            Files == (GCFiles2 + GCFiles3) andalso
                Space > (S2 + S3) andalso
                map_size(Es) == 0,
        emqx_ft_storage_fs_gc:collect(Storage)
    ).

start_transfer(Storage, {Transfer, Name, Gen}) ->
    Meta = #{
        name => Name,
        segments_ttl => 10
    },
    ?assertEqual(
        ok,
        emqx_ft_storage_fs:store_filemeta(Storage, Transfer, Meta)
    ),
    emqx_ft_content_gen:fold(
        fun({Content, SegmentNum, #{chunk_size := SegmentSize}}, _Transferred) ->
            Offset = (SegmentNum - 1) * SegmentSize,
            ?assertEqual(
                ok,
                emqx_ft_storage_fs:store_segment(Storage, Transfer, {Offset, Content})
            ),
            Offset + byte_size(Content)
        end,
        0,
        Gen
    ).

complete_transfer(Storage, Transfer, Size) ->
    complete_transfer(Storage, Transfer, Size, 100).

complete_transfer(Storage, Transfer, Size, Timeout) ->
    {async, Pid} = emqx_ft_storage_fs:assemble(Storage, Transfer, Size),
    MRef = erlang:monitor(process, Pid),
    Pid ! kickoff,
    receive
        {'DOWN', MRef, process, Pid, {shutdown, Result}} ->
            Result
    after Timeout ->
        ct:fail("Assembler did not finish in time")
    end.

mk_file_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).
