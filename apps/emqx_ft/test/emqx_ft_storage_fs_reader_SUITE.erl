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

-module(emqx_ft_storage_fs_reader_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Storage = emqx_ft_test_helpers:local_storage(Config),
    Apps = emqx_cth_suite:start(
        [
            {emqx_ft, #{config => emqx_ft_test_helpers:config(Storage)}}
        ],
        #{work_dir => WorkDir}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    file:make_dir(?config(data_dir, Config)),
    Data = <<"hello world">>,
    Path = expand_path(Config, "test_file"),
    ok = mk_test_file(Path, Data),
    [{path, Path} | Config].

end_per_testcase(_Case, _Config) ->
    ok.

t_successful_read(Config) ->
    Path = ?config(path, Config),

    {ok, ReaderPid} = emqx_ft_storage_fs_reader:start_link(self(), Path),
    ?assertEqual(
        {ok, <<"hello ">>},
        emqx_ft_storage_fs_reader:read(ReaderPid, 6)
    ),
    ?assertEqual(
        {ok, <<"world">>},
        emqx_ft_storage_fs_reader:read(ReaderPid, 6)
    ),
    ?assertEqual(
        eof,
        emqx_ft_storage_fs_reader:read(ReaderPid, 6)
    ),
    ?assertNot(is_process_alive(ReaderPid)).

t_caller_dead(Config) ->
    erlang:process_flag(trap_exit, true),

    Path = ?config(path, Config),

    CallerPid = spawn_link(
        fun() ->
            receive
                stop -> ok
            end
        end
    ),
    {ok, ReaderPid} = emqx_ft_storage_fs_reader:start_link(CallerPid, Path),
    _ = erlang:monitor(process, ReaderPid),
    ?assertEqual(
        {ok, <<"hello ">>},
        emqx_ft_storage_fs_reader:read(ReaderPid, 6)
    ),
    CallerPid ! stop,
    receive
        {'DOWN', _, process, ReaderPid, _} -> ok
    after 1000 ->
        ct:fail("Reader process did not die")
    end.

t_tables(Config) ->
    Path = ?config(path, Config),

    {ok, ReaderPid0} = emqx_ft_storage_fs_reader:start_link(self(), Path),

    ReaderQH0 = emqx_ft_storage_fs_reader:table(ReaderPid0, 6),
    ?assertEqual(
        [<<"hello ">>, <<"world">>],
        qlc:eval(ReaderQH0)
    ),

    {ok, ReaderPid1} = emqx_ft_storage_fs_reader:start_link(self(), Path),

    ReaderQH1 = emqx_ft_storage_fs_reader:table(ReaderPid1),
    ?assertEqual(
        [<<"hello world">>],
        qlc:eval(ReaderQH1)
    ).

t_bad_messages(Config) ->
    Path = ?config(path, Config),

    {ok, ReaderPid} = emqx_ft_storage_fs_reader:start_link(self(), Path),

    ReaderPid ! {bad, message},
    gen_server:cast(ReaderPid, {bad, message}),

    ?assertEqual(
        {error, {bad_call, {bad, message}}},
        gen_server:call(ReaderPid, {bad, message})
    ).

t_nonexistent_file(_Config) ->
    erlang:process_flag(trap_exit, true),
    ?assertEqual(
        {error, enoent},
        emqx_ft_storage_fs_reader:start_link(self(), "/a/b/c/bar")
    ).

t_start_supervised(Config) ->
    Path = ?config(path, Config),

    {ok, ReaderPid} = emqx_ft_storage_fs_reader:start_supervised(self(), Path),
    ?assertEqual(
        {ok, <<"hello ">>},
        emqx_ft_storage_fs_reader:read(ReaderPid, 6)
    ).

t_rpc_error(_Config) ->
    ReaderQH = emqx_ft_storage_fs_reader:table(fake_remote_pid('dummy@127.0.0.1'), 6),
    ?assertEqual(
        [],
        qlc:eval(ReaderQH)
    ).

mk_test_file(Path, Data) ->
    ok = file:write_file(Path, Data).

expand_path(Config, Filename) ->
    filename:join([?config(data_dir, Config), Filename]).

%% This is a hack to create a pid that is not registered on the local node.
%% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html#new_pid_ext
fake_remote_pid(Node) ->
    <<131, NodeAtom/binary>> = term_to_binary(Node),
    PidBin = <<131, 88, NodeAtom/binary, 1:32/big, 1:32/big, 1:32/big>>,
    binary_to_term(PidBin).
