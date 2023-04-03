%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PROFILE_ID, atom_to_binary(?MODULE)).

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    AllCases = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, [], AllCases},
        {tls, [], AllCases}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(emqx_s3),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(emqx_s3).

init_per_group(ConnType, Config) ->
    [{conn_type, ConnType} | Config].
end_per_group(_ConnType, _Config) ->
    ok.

init_per_testcase(_TestCase, Config0) ->
    ConnType = ?config(conn_type, Config0),

    Bucket = emqx_s3_test_helpers:unique_bucket(),
    TestAwsConfig = emqx_s3_test_helpers:aws_config(ConnType),
    ok = erlcloud_s3:create_bucket(Bucket, TestAwsConfig),
    Config1 = [
        {key, emqx_s3_test_helpers:unique_key()},
        {bucket, Bucket}
        | Config0
    ],
    {ok, PoolName} = emqx_s3_profile_conf:start_http_pool(?PROFILE_ID, profile_config(Config1)),
    [{ehttpc_pool_name, PoolName} | Config1].

end_per_testcase(_TestCase, Config) ->
    ok = ehttpc_sup:stop_pool(?config(ehttpc_pool_name, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_multipart_upload(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),

    {ok, UploadId} = emqx_s3_client:start_multipart(Client, Key),

    Data = data(6_000_000),

    {ok, Etag1} = emqx_s3_client:upload_part(Client, Key, UploadId, 1, Data),
    {ok, Etag2} = emqx_s3_client:upload_part(Client, Key, UploadId, 2, Data),

    ok = emqx_s3_client:complete_multipart(
        Client, Key, UploadId, [{1, Etag1}, {2, Etag2}]
    ).

t_simple_put(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),

    Data = data(6_000_000),

    ok = emqx_s3_client:put_object(Client, Key, Data).

t_list(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),

    ok = emqx_s3_client:put_object(Client, Key, <<"data">>),

    {ok, List} = emqx_s3_client:list(Client, Key),

    [KeyInfo] = proplists:get_value(contents, List),
    ?assertMatch(
        #{
            key := Key,
            size := 4,
            etag := _,
            last_modified := _
        },
        maps:from_list(KeyInfo)
    ).

t_url(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),
    ok = emqx_s3_client:put_object(Client, Key, <<"data">>),

    Url = emqx_s3_client:url(Client, Key),

    ?assertMatch(
        {ok, {{_StatusLine, 200, "OK"}, _Headers, "data"}},
        httpc:request(Url)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client(Config) ->
    ClientConfig = emqx_s3_profile_conf:client_config(
        profile_config(Config), ?config(ehttpc_pool_name, Config)
    ),
    emqx_s3_client:create(ClientConfig).

profile_config(Config) ->
    maps:put(
        bucket,
        ?config(bucket, Config),
        emqx_s3_test_helpers:base_config(?config(conn_type, Config))
    ).

data(Size) ->
    iolist_to_binary([$a || _ <- lists:seq(1, Size)]).
