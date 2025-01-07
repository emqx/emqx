%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_conf_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

suite() -> [{timetrap, {minutes, 1}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(emqx_s3),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(emqx_s3).

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    TestAwsConfig = emqx_s3_test_helpers:aws_config(tcp),

    Bucket = emqx_s3_test_helpers:unique_bucket(),
    ok = erlcloud_s3:create_bucket(Bucket, TestAwsConfig),

    ProfileBaseConfig = emqx_s3_test_helpers:base_config(tcp),
    ProfileConfig = ProfileBaseConfig#{bucket => Bucket},
    ok = emqx_s3:start_profile(profile_id(), ProfileConfig),

    [{profile_config, ProfileConfig} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    _ = emqx_s3:stop_profile(profile_id()).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_checkout_no_profile(_Config) ->
    ?assertEqual(
        {error, profile_not_found},
        emqx_s3_profile_conf:checkout_config(<<"no_such_profile">>)
    ).

t_httpc_pool_start_error(Config) ->
    %% `hackney_pool`s are lazy so it is difficult to trigger an error
    %% passing some bad connection options.
    %% So we emulate some unknown crash with `meck`.
    meck:new(hackney_pool, [passthrough]),
    meck:expect(hackney_pool, init, fun(_) -> meck:raise(error, badarg) end),

    ?assertMatch(
        {error, _},
        emqx_s3:start_profile(<<"profile">>, ?config(profile_config, Config))
    ).

t_httpc_pool_update_error(Config) ->
    %% `hackney_pool`s are lazy so it is difficult to trigger an error
    %% passing some bad connection options.
    %% So we emulate some unknown crash with `meck`.
    meck:new(hackney_pool, [passthrough]),
    meck:expect(hackney_pool, init, fun(_) -> meck:raise(error, badarg) end),

    ProfileBaseConfig = ?config(profile_config, Config),
    NewProfileConfig = emqx_utils_maps:deep_put(
        [transport_options, pool_size], ProfileBaseConfig, 16
    ),

    ?assertMatch(
        {error, _},
        emqx_s3:start_profile(<<"profile">>, NewProfileConfig)
    ).

t_orphaned_pools_cleanup(_Config) ->
    ProfileId = profile_id(),
    Pid = gproc:where({n, l, emqx_s3_profile_conf:id(ProfileId)}),

    %% We kill conf and wait for it to restart
    %% and create a new pool
    ?assertWaitEvent(
        exit(Pid, kill),
        #{?snk_kind := "s3_start_http_pool", profile_id := ProfileId},
        1000
    ),

    %% We should still have only one pool
    ?assertEqual(
        1,
        length(emqx_s3_profile_http_pools:all(ProfileId))
    ).

t_orphaned_pools_cleanup_non_graceful(_Config) ->
    ProfileId = profile_id(),
    Pid = gproc:where({n, l, emqx_s3_profile_conf:id(ProfileId)}),

    %% We stop pool, conf server should not fail when attempting to stop it once more
    [PoolName] = emqx_s3_profile_http_pools:all(ProfileId),
    ok = emqx_s3_client_http:stop_pool(PoolName),

    %% We kill conf and wait for it to restart
    %% and create a new pool
    ?assertWaitEvent(
        exit(Pid, kill),
        #{?snk_kind := "s3_start_http_pool", profile_id := ProfileId},
        1000
    ),

    %% We should still have only one pool
    ?assertEqual(
        1,
        length(emqx_s3_profile_http_pools:all(ProfileId))
    ).

t_checkout_client(Config) ->
    ProfileId = profile_id(),
    Key = emqx_s3_test_helpers:unique_key(),
    Caller = self(),
    Pid = spawn_link(fun() ->
        emqx_s3:with_client(
            ProfileId,
            fun(Client) ->
                receive
                    put_object ->
                        Caller ! {put_object, emqx_s3_client:put_object(Client, Key, <<"data">>)}
                end,
                receive
                    list_objects ->
                        Caller ! {list_objects, emqx_s3_client:list(Client, [])}
                end
            end
        ),
        Caller ! client_released,
        receive
            stop -> ok
        end
    end),

    %% Ask spawned process to put object
    Pid ! put_object,
    receive
        {put_object, ok} -> ok
    after 1000 ->
        ct:fail("put_object fail")
    end,

    %% Now change config for the profile
    ProfileBaseConfig = ?config(profile_config, Config),
    NewProfileConfig0 = ProfileBaseConfig#{bucket => <<"new_bucket">>},
    NewProfileConfig1 = emqx_utils_maps:deep_put(
        [transport_options, pool_size], NewProfileConfig0, 16
    ),
    ok = emqx_s3:update_profile(profile_id(), NewProfileConfig1),

    %% Ask spawned process to list objects
    Pid ! list_objects,
    receive
        {list_objects, Result} ->
            {ok, OkResult} = Result,
            Contents = proplists:get_value(contents, OkResult),
            ?assertEqual(1, length(Contents)),
            ?assertEqual(Key, proplists:get_value(key, hd(Contents)))
    after 1000 ->
        ct:fail("list_objects fail")
    end,

    %% Wait till spawned process releases client
    receive
        client_released -> ok
    after 1000 ->
        ct:fail("client not released")
    end,

    %% We should have only one pool now, because the old one is released
    ?assertEqual(
        1,
        length(emqx_s3_profile_http_pools:all(ProfileId))
    ).

t_unknown_messages(_Config) ->
    Pid = gproc:where({n, l, emqx_s3_profile_conf:id(profile_id())}),

    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),

    ?assertEqual(
        {error, not_implemented},
        gen_server:call(Pid, unknown)
    ).

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

profile_id() ->
    <<"test">>.
