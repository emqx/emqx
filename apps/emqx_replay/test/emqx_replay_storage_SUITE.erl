%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_replay_storage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

-define(DB_FILE, ?MODULE_STRING).

%% Smoke test of store function
t_store(Config) ->
    DB = ?config(handle, Config),
    MessageID = emqx_guid:gen(),
    PublishedAt = 1000,
    Topic = [<<"foo">>, <<"bar">>],
    Payload = <<"message">>,
    ?assertMatch(ok, emqx_replay_message_storage:store(DB, MessageID, PublishedAt, Topic, Payload)).

%% Smoke test for iteration through a concrete topic
t_iterate(Config) ->
    DB = ?config(handle, Config),
    %% Prepare data:
    Topics = [[<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>, <<"baz">>], [<<"a">>]],
    Timestamps = lists:seq(1, 10),
    [
        emqx_replay_message_storage:store(
            DB,
            emqx_guid:gen(),
            PublishedAt,
            Topic,
            integer_to_binary(PublishedAt)
        )
     || Topic <- Topics, PublishedAt <- Timestamps
    ],
    %% Iterate through individual topics:
    [
        begin
            {ok, It} = emqx_replay_message_storage:make_iterator(DB, Topic, 0),
            Values = iterate(It),
            ?assertEqual(Values, lists:map(fun integer_to_binary/1, Timestamps))
        end
     || Topic <- Topics
    ],
    ok.

iterate(It) ->
    case emqx_replay_message_storage:next(It) of
        {value, Val} ->
            [Val | iterate(It)];
        none ->
            []
    end.

%% CT callbacks

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TC, Config) ->
    Filename = filename:join(?MODULE_STRING, atom_to_list(TC)),
    ok = filelib:ensure_dir(Filename),
    {ok, DB} = emqx_replay_message_storage:open(Filename, []),
    [{handle, DB} | Config].

end_per_testcase(_TC, Config) ->
    DB = ?config(handle, Config),
    catch emqx_replay_message_storage:close(DB).
