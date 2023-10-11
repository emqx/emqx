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
-module(emqx_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% A simple smoke test that verifies that opening/closing the DB
%% doesn't crash, and not much else
t_00_smoke_open_drop(_Config) ->
    DB = 'DB',
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    ?assertMatch(ok, emqx_ds:drop_db(DB)).

%% A simple smoke test that verifies that storing the messages doesn't
%% crash
t_01_smoke_store(_Config) ->
    DB = default,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    Msg = message(<<"foo/bar">>, <<"foo">>, 0),
    ?assertMatch(ok, emqx_ds:store_batch(DB, [Msg])).

%% A simple smoke test that verifies that getting the list of streams
%% doesn't crash and that iterators can be opened.
t_02_smoke_get_streams_start_iter(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    StartTime = 0,
    TopicFilter = ['#'],
    [{Rank, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    ?assertMatch({_, _}, Rank),
    ?assertMatch({ok, _Iter}, emqx_ds:make_iterator(Stream, TopicFilter, StartTime)).

%% A simple smoke test that verifies that it's possible to iterate
%% over messages.
t_03_smoke_iterate(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    StartTime = 0,
    TopicFilter = ['#'],
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(Stream, TopicFilter, StartTime),
    {ok, Iter, Batch} = iterate(Iter0, 1),
    ?assertEqual(Msgs, Batch, {Iter0, Iter}).

%% Verify that iterators survive restart of the application. This is
%% an important property, since the lifetime of the iterators is tied
%% to the external resources, such as clients' sessions, and they
%% should always be able to continue replaying the topics from where
%% they are left off.
t_04_restart(_Config) ->
    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, #{})),
    TopicFilter = ['#'],
    StartTime = 0,
    Msgs = [
        message(<<"foo/bar">>, <<"1">>, 0),
        message(<<"foo">>, <<"2">>, 1),
        message(<<"bar/bar">>, <<"3">>, 2)
    ],
    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),
    [{_, Stream}] = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    {ok, Iter0} = emqx_ds:make_iterator(Stream, TopicFilter, StartTime),
    %% Restart the application:
    ?tp(warning, emqx_ds_SUITE_restart_app, #{}),
    ok = application:stop(emqx_durable_storage),
    {ok, _} = application:ensure_all_started(emqx_durable_storage),
    ok = emqx_ds:open_db(DB, #{}),
    %% The old iterator should be still operational:
    {ok, Iter, Batch} = iterate(Iter0, 1),
    ?assertEqual(Msgs, Batch, {Iter0, Iter}).

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

iterate(It, BatchSize) ->
    iterate(It, BatchSize, []).

iterate(It0, BatchSize, Acc) ->
    case emqx_ds:next(It0, BatchSize) of
        {ok, It, []} ->
            {ok, It, Acc};
        {ok, It, Msgs} ->
            iterate(It, BatchSize, Acc ++ Msgs);
        Ret ->
            Ret
    end.

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [mria, emqx_durable_storage],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    %% snabbkaffe:fix_ct_logging(),
    application:ensure_all_started(emqx_durable_storage),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = application:stop(emqx_durable_storage).
