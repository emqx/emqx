%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_group(ConnTypeGroup, Config) when ConnTypeGroup =:= tcp; ConnTypeGroup =:= tls ->
    [{conn_type, ConnTypeGroup} | Config].

end_per_group(_ConnType, _Config) ->
    ok.

init_per_testcase(_TestCase, Config0) ->
    ConnType = ?config(conn_type, Config0),
    Bucket = emqx_s3_test_helpers:unique_bucket(),
    TestAwsConfig = emqx_s3_test_helpers:aws_config(ConnType),
    ok = erlcloud_s3:create_bucket(Bucket, TestAwsConfig),
    Config1 = [
        {key, emqx_s3_test_helpers:unique_key()},
        {bucket, Bucket},
        {aws_config, TestAwsConfig}
        | Config0
    ],
    {ok, PoolName} = emqx_s3_profile_conf:start_http_pool(?PROFILE_ID, profile_config(Config1)),
    [{pool_name, PoolName} | Config1].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_s3_client_http:stop_pool(?config(pool_name, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_multipart_upload(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),

    {ok, UploadId} = emqx_s3_client:start_multipart(Client, Key, #{}),

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

    ok = emqx_s3_client:put_object(Client, Key, #{acl => private}, Data).

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
    ok = emqx_s3_client:put_object(Client, Key, #{acl => public_read}, <<"data">>),

    Url = emqx_s3_client:uri(Client, Key),

    ?assertMatch(
        {ok, {{_StatusLine, 200, "OK"}, _Headers, "data"}},
        httpc:request(get, {Url, []}, [{ssl, [{verify, verify_none}]}], [])
    ).

t_no_acl(Config) ->
    Key = ?config(key, Config),

    Client = client(Config),

    ok = emqx_s3_client:put_object(Client, Key, #{}, <<"data">>).

t_extra_headers(Config0) ->
    Config = [{extra_headers, #{'Content-Type' => <<"application/json">>}} | Config0],
    Key = ?config(key, Config),

    Client = client(Config),
    Opts = #{acl => public_read},
    Data = #{<<"foo">> => <<"bar">>},
    ok = emqx_s3_client:put_object(Client, Key, Opts, emqx_utils_json:encode(Data)),

    Url = emqx_s3_client:uri(Client, Key),

    {ok, {{_StatusLine, 200, "OK"}, _Headers, Content}} =
        httpc:request(get, {Url, []}, [{ssl, [{verify, verify_none}]}], []),
    ?assertEqual(
        Data,
        emqx_utils_json:decode(Content)
    ).

t_no_credentials(Config) ->
    Bucket = ?config(bucket, Config),
    ProfileConfig = maps:merge(
        profile_config(Config),
        #{
            access_key_id => undefined,
            secret_access_key => undefined
        }
    ),
    ClientConfig = emqx_s3_profile_conf:client_config(
        ProfileConfig,
        ?config(pool_name, Config)
    ),
    Client = emqx_s3_client:create(Bucket, ClientConfig),
    ?assertMatch(
        {error, {config_error, {failed_to_obtain_credentials, _}}},
        emqx_s3_client:put_object(Client, ?config(key, Config), <<>>)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client(Config) ->
    Bucket = ?config(bucket, Config),
    ClientConfig = emqx_s3_profile_conf:client_config(
        profile_config(Config), ?config(pool_name, Config)
    ),
    emqx_s3_client:create(Bucket, ClientConfig).

profile_config(Config) ->
    ProfileConfig0 = emqx_s3_test_helpers:base_config(?config(conn_type, Config)),
    maps:fold(
        fun inject_config/3,
        ProfileConfig0,
        #{
            [transport_options, headers] => ?config(extra_headers, Config)
        }
    ).

inject_config(_Key, undefined, ProfileConfig) ->
    ProfileConfig;
inject_config(KeyPath, Value, ProfileConfig) when is_list(KeyPath) ->
    emqx_utils_maps:deep_put(KeyPath, ProfileConfig, Value);
inject_config(Key, Value, ProfileConfig) ->
    maps:put(Key, Value, ProfileConfig).

data(Size) ->
    iolist_to_binary([$a || _ <- lists:seq(1, Size)]).
