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

-module(emqx_ft_storage_exporter_s3_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(assertS3Data(Data, Url),
    case httpc:request(Url) of
        {ok, {{_StatusLine, 200, "OK"}, _Headers, Body}} ->
            ?assertEqual(Data, list_to_binary(Body), "S3 data mismatch");
        OtherResponse ->
            ct:fail("Unexpected response: ~p", [OtherResponse])
    end
).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.
end_per_suite(_Config) ->
    ok.

set_special_configs(Config) ->
    fun
        (emqx_ft) ->
            Storage = emqx_ft_test_helpers:local_storage(Config, #{
                exporter => s3, bucket_name => ?config(bucket_name, Config)
            }),
            emqx_ft_test_helpers:load_config(#{<<"enable">> => true, <<"storage">> => Storage});
        (_) ->
            ok
    end.

init_per_testcase(Case, Config0) ->
    ClientId = atom_to_binary(Case),
    BucketName = create_bucket(),
    Config1 = [{bucket_name, BucketName}, {clientid, ClientId} | Config0],
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_ft], set_special_configs(Config1)),
    Config1.
end_per_testcase(_Case, _Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%-------------------------------------------------------------------

t_happy_path(Config) ->
    ClientId = ?config(clientid, Config),

    FileId = <<"🌚"/utf8>>,
    Name = "cool_name",
    Data = <<"data"/utf8>>,

    ?assertEqual(
        ok,
        emqx_ft_test_helpers:upload_file(ClientId, FileId, Name, Data)
    ),

    {ok, #{items := [#{uri := Uri}]}} = emqx_ft_storage:files(),

    ?assertS3Data(Data, Uri),

    Key = binary_to_list(ClientId) ++ "/" ++ binary_to_list(FileId) ++ "/" ++ Name,
    Meta = erlcloud_s3:get_object_metadata(
        ?config(bucket_name, Config), Key, emqx_ft_test_helpers:aws_config()
    ),

    ?assertEqual(
        ClientId,
        metadata_field("clientid", Meta)
    ),

    ?assertEqual(
        FileId,
        metadata_field("fileid", Meta)
    ),

    NameBin = list_to_binary(Name),
    ?assertMatch(
        #{
            <<"name">> := NameBin,
            <<"size">> := 4
        },
        emqx_utils_json:decode(metadata_field("filemeta", Meta), [return_maps])
    ).

t_upload_error(Config) ->
    ClientId = ?config(clientid, Config),

    FileId = <<"🌚"/utf8>>,
    Name = "cool_name",
    Data = <<"data"/utf8>>,

    {ok, _} = emqx_conf:update(
        [file_transfer, storage, exporter, bucket], <<"invalid-bucket">>, #{}
    ),

    ?assertEqual(
        {error, unspecified_error},
        emqx_ft_test_helpers:upload_file(ClientId, FileId, Name, Data)
    ).

t_paging(Config) ->
    ClientId = ?config(clientid, Config),
    N = 1050,

    FileId = fun integer_to_binary/1,
    Name = "cool_name",
    Data = fun integer_to_binary/1,

    ok = lists:foreach(
        fun(I) ->
            ok = emqx_ft_test_helpers:upload_file(ClientId, FileId(I), Name, Data(I))
        end,
        lists:seq(1, N)
    ),

    {ok, #{items := [#{uri := Uri}]}} = emqx_ft_storage:files(#{transfer => {ClientId, FileId(123)}}),

    ?assertS3Data(Data(123), Uri),

    lists:foreach(
        fun(PageSize) ->
            Pages = file_pages(#{limit => PageSize}),
            ?assertEqual(
                expected_page_count(PageSize, N),
                length(Pages)
            ),
            FileIds = [
                FId
             || #{transfer := {_, FId}} <- lists:concat(Pages)
            ],
            ?assertEqual(
                lists:sort([FileId(I) || I <- lists:seq(1, N)]),
                lists:sort(FileIds)
            )
        end,
        %% less than S3 limit, greater than S3 limit
        [20, 550]
    ).

t_invalid_cursor(_Config) ->
    InvalidUtf8 = <<16#80>>,
    ?assertError(
        {badarg, cursor},
        emqx_ft_storage:files(#{following => InvalidUtf8})
    ).

%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

expected_page_count(PageSize, Total) ->
    case Total rem PageSize of
        0 -> Total div PageSize;
        _ -> Total div PageSize + 1
    end.

file_pages(Query) ->
    case emqx_ft_storage:files(Query) of
        {ok, #{items := Items, cursor := NewCursor}} ->
            [Items] ++ file_pages(Query#{following => NewCursor});
        {ok, #{items := Items}} ->
            [Items];
        {error, Error} ->
            ct:fail("Failed to download files: ~p", [Error])
    end.

metadata_field(Field, Meta) ->
    Key = "x-amz-meta-" ++ Field,
    case lists:keyfind(Key, 1, Meta) of
        {Key, Value} -> list_to_binary(Value);
        false -> false
    end.

create_bucket() ->
    BucketName = emqx_s3_test_helpers:unique_bucket(),
    _ = application:ensure_all_started(lhttpc),
    ok = erlcloud_s3:create_bucket(BucketName, emqx_ft_test_helpers:aws_config()),
    BucketName.
