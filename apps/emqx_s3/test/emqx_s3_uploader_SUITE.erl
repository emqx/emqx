%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_uploader_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(assertProcessExited(Reason, Pid),
    receive
        {'DOWN', _, _, Pid, Reason} ->
            ok
    after 3000 ->
        ct:fail("uploader process did not exit")
    end
).

-define(assertObjectEqual(Value, AwsConfig, Bucket, Key),
    ?assertEqual(
        Value,
        proplists:get_value(
            content,
            erlcloud_s3:get_object(
                Bucket,
                Key,
                AwsConfig
            )
        )
    )
).

-define(VHOST_BUCKET, "test1").

all() ->
    [
        {group, tcp},
        {group, tls},
        {group, vhost}
    ].

groups() ->
    [
        {tcp, [
            {group, common_cases},
            {group, tcp_cases}
        ]},
        {tls, [
            {group, common_cases},
            {group, tls_cases}
        ]},
        {vhost, [], [
            {group, happy_cases}
        ]},

        {common_cases, [], [
            {group, happy_cases},
            {group, noconn_errors},
            {group, timeout_errors},
            {group, http_errors}
        ]},

        {happy_cases, [], [
            t_happy_path_simple_put,
            t_happy_path_multi,
            t_abort_multi,
            t_abort_simple_put,
            t_signed_url_download,
            t_signed_nonascii_url_download
        ]},

        {tcp_cases, [
            t_config_switch,
            t_too_large,
            t_no_profile
        ]},

        {tls_cases, [
            t_tls_error,
            t_config_switch_http_settings
        ]},

        {noconn_errors, [{group, transport_errors}]},
        {timeout_errors, [{group, transport_errors}]},
        {http_errors, [{group, transport_errors}]},

        {transport_errors, [
            t_start_multipart_error,
            t_upload_part_error,
            t_complete_multipart_error,
            t_abort_multipart_error,
            t_put_object_error
        ]}
    ].

suite() -> [{timetrap, {minutes, 1}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(emqx_s3),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(emqx_s3).

init_per_group(Group, Config) when Group =:= tcp orelse Group =:= tls ->
    [{conn_type, Group} | Config];
init_per_group(vhost, Config) ->
    [{conn_type, {tcp, direct}}, {access_method, vhost} | Config];
init_per_group(noconn_errors, Config) ->
    [{failure, down} | Config];
init_per_group(timeout_errors, Config) ->
    [{failure, timeout} | Config];
init_per_group(http_errors, Config) ->
    [{failure, httpc_500} | Config];
init_per_group(_ConnType, Config) ->
    Config.

end_per_group(_ConnType, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    ConnType = ?config(conn_type, Config),
    TestAwsConfig = emqx_s3_test_helpers:aws_config(ConnType),
    ProfileBaseConfig = emqx_s3_test_helpers:base_config(ConnType),
    AccessMethod = proplists:get_value(access_method, Config, path),
    case AccessMethod of
        path ->
            Bucket = emqx_s3_test_helpers:unique_bucket();
        vhost ->
            Bucket = ?VHOST_BUCKET
    end,
    ProfileConfig = ProfileBaseConfig#{
        bucket => Bucket,
        access_method => AccessMethod
    },
    ok = emqx_s3_test_helpers:recreate_bucket(Bucket, TestAwsConfig),
    ok = emqx_s3:start_profile(profile_id(), ProfileConfig),
    [{bucket, Bucket}, {profile_config, ProfileConfig}, {test_aws_config, TestAwsConfig} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    _ = emqx_s3:stop_profile(profile_id()).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_happy_path_simple_put(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    Data = data($a, 1024, 10),

    lists:foreach(
        fun(Chunk) ->
            ?assertEqual(
                ok,
                emqx_s3_uploader:write(Pid, Chunk)
            )
        end,
        Data
    ),

    ok = emqx_s3_uploader:complete(Pid),

    ?assertProcessExited(
        normal,
        Pid
    ),

    ?assertObjectEqual(
        iolist_to_binary(Data),
        ?config(test_aws_config, Config),
        ?config(bucket, Config),
        Key
    ).

t_happy_path_multi(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    Data = data($a, 1024 * 1024, 10),

    lists:foreach(
        fun(Chunk) ->
            ?assertEqual(
                ok,
                emqx_s3_uploader:write(Pid, Chunk)
            )
        end,
        Data
    ),

    ok = emqx_s3_uploader:complete(Pid),

    ?assertProcessExited(
        normal,
        Pid
    ),

    ?assertObjectEqual(
        iolist_to_binary(Data),
        ?config(test_aws_config, Config),
        ?config(bucket, Config),
        Key
    ).

t_signed_url_download(_Config) ->
    Prefix = emqx_s3_test_helpers:unique_key(),
    Key = Prefix ++ "/ascii.txt",

    {ok, Data} = upload(Key, 1024, 5),

    SignedUrl = emqx_s3:with_client(profile_id(), fun(Client) ->
        emqx_s3_client:uri(Client, Key)
    end),

    HttpOpts = [{ssl, [{verify, verify_none}]}, {timeout, 5000}],
    {ok, {_, _, Body}} = httpc:request(get, {SignedUrl, []}, HttpOpts, []),

    ?assertEqual(
        iolist_to_binary(Data),
        iolist_to_binary(Body)
    ).

t_signed_nonascii_url_download(_Config) ->
    Prefix = emqx_s3_test_helpers:unique_key(),
    Key = Prefix ++ "/unicode-🫠.txt",

    {ok, Data} = upload(Key, 1024 * 1024, 8),

    SignedUrl = emqx_s3:with_client(profile_id(), fun(Client) ->
        emqx_s3_client:uri(Client, Key)
    end),

    HttpOpts = [{ssl, [{verify, verify_none}]}, {timeout, 5000}],
    {ok, {_, _, Body}} = httpc:request(get, {SignedUrl, []}, HttpOpts, []),

    ?assertEqual(
        iolist_to_binary(Data),
        iolist_to_binary(Body)
    ).

t_abort_multi(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 6 * 1024 * 1024, 1),

    ok = emqx_s3_uploader:write(Pid, Data),

    ?assertMatch(
        [],
        list_objects(Config)
    ),

    ok = emqx_s3_uploader:abort(Pid),

    ?assertMatch(
        [],
        list_objects(Config)
    ),

    ?assertProcessExited(
        normal,
        Pid
    ).

t_abort_simple_put(_Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 10 * 1024, 1),

    ok = emqx_s3_uploader:write(Pid, Data),

    ok = emqx_s3_uploader:abort(Pid),

    ?assertProcessExited(
        normal,
        Pid
    ).

t_config_switch(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    OldBucket = ?config(bucket, Config),
    {ok, Pid0} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    [Data0, Data1] = data($a, 6 * 1024 * 1024, 2),

    ok = emqx_s3_uploader:write(Pid0, Data0),

    %% Switch to the new config, but without changing HTTP settings
    ProfileConfig = ?config(profile_config, Config),
    NewBucket = emqx_s3_test_helpers:unique_bucket(),
    ok = erlcloud_s3:create_bucket(NewBucket, ?config(test_aws_config, Config)),
    NewProfileConfig = ProfileConfig#{bucket => NewBucket},

    ok = emqx_s3:update_profile(profile_id(), NewProfileConfig),

    %% Already started uploader should be OK and use previous config
    ok = emqx_s3_uploader:write(Pid0, Data1),
    ok = emqx_s3_uploader:complete(Pid0),

    ?assertObjectEqual(
        iolist_to_binary([Data0, Data1]),
        ?config(test_aws_config, Config),
        OldBucket,
        Key
    ),

    %% Now check that new uploader uses new config
    {ok, Pid1} = emqx_s3:start_uploader(profile_id(), Key, #{}),
    ok = emqx_s3_uploader:write(Pid1, Data0),
    ok = emqx_s3_uploader:complete(Pid1),

    ?assertObjectEqual(
        iolist_to_binary(Data0),
        ?config(test_aws_config, Config),
        NewBucket,
        Key
    ).

t_config_switch_http_settings(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    OldBucket = ?config(bucket, Config),
    {ok, Pid0} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    [Data0, Data1] = data($a, 6 * 1024 * 1024, 2),

    ok = emqx_s3_uploader:write(Pid0, Data0),

    %% Switch to the new config, completely changing HTTP settings (tcp -> tls)
    NewBucket = emqx_s3_test_helpers:unique_bucket(),
    NewTestAwsConfig = emqx_s3_test_helpers:aws_config(tls),
    ok = erlcloud_s3:create_bucket(NewBucket, NewTestAwsConfig),
    NewProfileConfig0 = emqx_s3_test_helpers:base_config(tls),
    NewProfileConfig1 = NewProfileConfig0#{bucket => NewBucket},

    ok = emqx_s3:update_profile(profile_id(), NewProfileConfig1),

    %% Already started uploader should be OK and use previous config
    ok = emqx_s3_uploader:write(Pid0, Data1),
    ok = emqx_s3_uploader:complete(Pid0),

    ?assertObjectEqual(
        iolist_to_binary([Data0, Data1]),
        ?config(test_aws_config, Config),
        OldBucket,
        Key
    ),

    %% Now check that new uploader uses new config
    {ok, Pid1} = emqx_s3:start_uploader(profile_id(), Key, #{}),
    ok = emqx_s3_uploader:write(Pid1, Data0),
    ok = emqx_s3_uploader:complete(Pid1),

    ?assertObjectEqual(
        iolist_to_binary(Data0),
        NewTestAwsConfig,
        NewBucket,
        Key
    ).

t_start_multipart_error(Config) ->
    _ = process_flag(trap_exit, true),

    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 6 * 1024 * 1024, 1),

    emqx_s3_test_helpers:with_failure(
        ?config(conn_type, Config),
        ?config(failure, Config),
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_s3_uploader:write(Pid, Data)
            )
        end
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_upload_part_error(Config) ->
    _ = process_flag(trap_exit, true),

    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data0, Data1] = data($a, 6 * 1024 * 1024, 2),

    ok = emqx_s3_uploader:write(Pid, Data0),

    emqx_s3_test_helpers:with_failure(
        ?config(conn_type, Config),
        ?config(failure, Config),
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_s3_uploader:write(Pid, Data1)
            )
        end
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_abort_multipart_error(Config) ->
    _ = process_flag(trap_exit, true),

    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 6 * 1024 * 1024, 1),

    ok = emqx_s3_uploader:write(Pid, Data),

    emqx_s3_test_helpers:with_failure(
        ?config(conn_type, Config),
        ?config(failure, Config),
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_s3_uploader:abort(Pid)
            )
        end
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_complete_multipart_error(Config) ->
    _ = process_flag(trap_exit, true),

    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 6 * 1024 * 1024, 1),

    ok = emqx_s3_uploader:write(Pid, Data),

    emqx_s3_test_helpers:with_failure(
        ?config(conn_type, Config),
        ?config(failure, Config),
        fun() ->
            ?assertMatch(
                {error, _},
                emqx_s3_uploader:complete(Pid)
            )
        end
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_put_object_error(Config) ->
    _ = process_flag(trap_exit, true),

    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    %% Little data to avoid multipart upload
    [Data] = data($a, 1024, 1),

    emqx_s3_test_helpers:with_failure(
        ?config(conn_type, Config),
        ?config(failure, Config),
        fun() ->
            ok = emqx_s3_uploader:write(Pid, Data),
            ?assertMatch(
                {error, _},
                emqx_s3_uploader:complete(Pid)
            )
        end
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_too_large(Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 1024, 1),

    [DataLarge] = data($a, 20 * 1024 * 1024, 1),

    ?assertMatch(
        {error, {too_large, _}},
        emqx_s3_uploader:write(Pid, DataLarge)
    ),

    ok = emqx_s3_uploader:write(Pid, Data),
    ok = emqx_s3_uploader:complete(Pid),

    ?assertProcessExited(
        normal,
        Pid
    ),

    ?assertObjectEqual(
        iolist_to_binary(Data),
        ?config(test_aws_config, Config),
        ?config(bucket, Config),
        Key
    ).

t_tls_error(Config) ->
    _ = process_flag(trap_exit, true),

    ProfileBaseConfig = ?config(profile_config, Config),
    ProfileConfig = emqx_utils_maps:deep_put(
        [transport_options, ssl, server_name_indication], ProfileBaseConfig, "invalid-hostname"
    ),
    ok = emqx_s3:update_profile(profile_id(), ProfileConfig),
    Key = emqx_s3_test_helpers:unique_key(),
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    [Data] = data($a, 6 * 1024 * 1024, 1),

    ?assertMatch(
        {error, _},
        emqx_s3_uploader:write(Pid, Data)
    ),

    ?assertProcessExited(
        {error, _},
        Pid
    ).

t_no_profile(_Config) ->
    Key = emqx_s3_test_helpers:unique_key(),
    ?assertMatch(
        {error, profile_not_found},
        emqx_s3:start_uploader(<<"no-profile">>, Key, #{})
    ).

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

profile_id() ->
    <<"test">>.

data(Byte, ChunkSize, ChunkCount) ->
    Chunk = iolist_to_binary([Byte || _ <- lists:seq(1, ChunkSize)]),
    [Chunk || _ <- lists:seq(1, ChunkCount)].

list_objects(Config) ->
    Props = erlcloud_s3:list_objects(?config(bucket, Config), [], ?config(test_aws_config, Config)),
    proplists:get_value(contents, Props).

upload(Key, ChunkSize, ChunkCount) ->
    {ok, Pid} = emqx_s3:start_uploader(profile_id(), Key, #{}),

    _ = erlang:monitor(process, Pid),

    Data = data($a, ChunkSize, ChunkCount),

    ok = lists:foreach(
        fun(Chunk) -> ?assertEqual(ok, emqx_s3_uploader:write(Pid, Chunk)) end,
        Data
    ),

    ok = emqx_s3_uploader:complete(Pid),

    ok = ?assertProcessExited(
        normal,
        Pid
    ),

    {ok, Data}.
