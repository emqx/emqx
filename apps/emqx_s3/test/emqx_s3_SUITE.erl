%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_s3], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start_stop_update(_Config) ->
    ProfileId = <<"test">>,
    ProfileConfig = profile_config(),

    ?assertMatch(
        ok,
        emqx_s3:start_profile(ProfileId, ProfileConfig)
    ),

    ?assertMatch(
        {error, _},
        emqx_s3:start_profile(ProfileId, ProfileConfig)
    ),

    ?assertEqual(
        ok,
        emqx_s3:update_profile(ProfileId, ProfileConfig)
    ),

    ?assertMatch(
        {error, _},
        emqx_s3:update_profile(<<"unknown">>, ProfileConfig)
    ),

    ?assertEqual(
        ok,
        emqx_s3:stop_profile(ProfileId)
    ),

    ?assertMatch(
        {error, _},
        emqx_s3:stop_profile(ProfileId)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

profile_config() ->
    emqx_s3_test_helpers:base_config(tcp).
