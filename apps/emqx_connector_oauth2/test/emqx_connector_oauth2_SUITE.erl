%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_oauth2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [emqx_connector_oauth2],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    ok = emqx_cth_suite:stop(?config(apps, TCConfig)),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    emqx_connector_oauth2:clear_cache(),
    meck:new(emqx_connector_oauth2, [passthrough, no_link]),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    meck:unload(emqx_connector_oauth2),
    emqx_connector_oauth2:clear_cache(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_get_token_fetches_and_caches(_TCConfig) ->
    ResourceId = <<"res:1">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config()),
    ok = expect_token(<<"access-1">>, 3600),
    ?assertEqual({ok, <<"access-1">>}, emqx_connector_oauth2:get_token(ResourceId)),
    %% Second call must hit the ETS cache, not the token endpoint.
    ?assertEqual({ok, <<"access-1">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ?assertEqual(1, meck:num_calls(emqx_connector_oauth2, do_request, 1)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_refresh_renews_token_before_expiry(_TCConfig) ->
    ResourceId = <<"res:2">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config()),
    %% Short lifetime so the refresh timer fires quickly (75% of 2s = 1.5s).
    ok = expect_token(<<"access-2a">>, 2),
    ?assertEqual({ok, <<"access-2a">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = expect_token(<<"access-2b">>, 3600),
    ok = wait_for(
        fun() -> emqx_connector_oauth2:get_token(ResourceId) =:= {ok, <<"access-2b">>} end,
        10_000
    ),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_fetch_failure_is_cached_briefly(_TCConfig) ->
    ResourceId = <<"res:3">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config()),
    meck:expect(emqx_connector_oauth2, do_request, 1, {error, mocked}),
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)),
    %% A second call within the short failure-cache window must not hit the
    %% endpoint again.
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)),
    ?assertEqual(1, meck:num_calls(emqx_connector_oauth2, do_request, 1)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_unregister_clears_cache(_TCConfig) ->
    ResourceId = <<"res:4">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config()),
    ok = expect_token(<<"access-4">>, 3600),
    ?assertEqual({ok, <<"access-4">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = emqx_connector_oauth2:unregister(ResourceId),
    %% After unregister the RefreshFn is gone, so get_token returns an error
    %% without hitting the token endpoint.
    ?assertEqual(1, meck:num_calls(emqx_connector_oauth2, do_request, 1)),
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)).

t_fetch_token_derives_expiry_from_jwt_exp(_TCConfig) ->
    %% A token response whose `access_token' is a JWT carrying an `exp' claim,
    %% but with no `expires_in'.  This exercises `get_expiry_ms', which must
    %% peek the JWT `exp' instead of falling back to the short default.  This
    %% is a regression guard for the `jose_jwt:peek' pattern: the result is a
    %% `#jose_jwt{}' record, not a bare map.
    ExpSec = os:system_time(second) + 3600,
    JWT = make_jwt(#{<<"exp">> => ExpSec}),
    Body = emqx_utils_json:encode(#{
        <<"access_token">> => JWT,
        <<"token_type">> => <<"Bearer">>
    }),
    meck:expect(
        emqx_connector_oauth2,
        do_request,
        1,
        {ok, {{<<"HTTP/1.1">>, 200, <<"OK">>}, [], Body}}
    ),
    {ok, ExpiryMS, JWT} = emqx_connector_oauth2:fetch_token(fetch_params()),
    %% Derived from the JWT `exp' (~1h), not the 15s default.
    ?assert(ExpiryMS > 3_500_000),
    ?assert(ExpiryMS =< 3_600_000),
    ok.

t_fetch_token_returns_error_on_bad_status(_TCConfig) ->
    meck:expect(
        emqx_connector_oauth2,
        do_request,
        1,
        {ok, {{<<"HTTP/1.1">>, 401, <<"Unauthorized">>}, [], <<"nope">>}}
    ),
    ?assertMatch(
        {error, {bad_token_response, _}},
        emqx_connector_oauth2:fetch_token(fetch_params())
    ),
    ok.

t_validate_rejects_missing_fields(_TCConfig) ->
    Oauth2 = #{enable => true, token_endpoint => <<"https://a/token">>, client_id => <<"id">>},
    ?assertMatch(
        {error, #{
            reason := oauth2_missing_fields,
            message := _,
            missing := [client_secret]
        }},
        emqx_connector_oauth2_schema:validate(#{}, Oauth2)
    ).

t_validate_rejects_auth_header_conflict(_TCConfig) ->
    Oauth2 = #{
        enable => true,
        token_endpoint => <<"https://a/token">>,
        client_id => <<"id">>,
        client_secret => emqx_secret:wrap(<<"s">>)
    },
    ?assertMatch(
        {error, #{reason := oauth2_auth_header_conflict, message := _, headers := [_ | _]}},
        emqx_connector_oauth2_schema:validate(#{<<"Authorization">> => <<"Basic xx">>}, Oauth2)
    ).

t_validate_accepts_when_disabled_or_absent(_TCConfig) ->
    FullOauth2 = #{
        enable => true,
        token_endpoint => <<"https://a/token">>,
        client_id => <<"id">>,
        client_secret => emqx_secret:wrap(<<"s">>)
    },
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, undefined)),
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, #{enable => false})),
    ?assertEqual(
        ok,
        emqx_connector_oauth2_schema:validate(#{<<"authorization">> => <<"x">>}, undefined)
    ),
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, FullOauth2)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

oauth2_config() ->
    #{
        enable => true,
        grant_type => client_credentials,
        token_endpoint => <<"https://auth.example.com/oauth/token">>,
        client_id => <<"client-id">>,
        client_secret => emqx_secret:wrap(<<"client-secret">>),
        scope => <<"read">>,
        timeout => 5_000
    }.

expect_token(Token, ExpiresInSec) ->
    Body = emqx_utils_json:encode(#{
        <<"access_token">> => Token,
        <<"expires_in">> => ExpiresInSec,
        <<"token_type">> => <<"Bearer">>
    }),
    meck:expect(
        emqx_connector_oauth2, do_request, 1, {ok, {{<<"HTTP/1.1">>, 200, <<"OK">>}, [], Body}}
    ).

%% Plain fetch parameters (the shape `make_fetch_params/1' produces), used to
%% exercise `fetch_token/1' directly without going through the GenServer.
fetch_params() ->
    #{
        token_endpoint => <<"https://auth.example.com/oauth/token">>,
        client_id => <<"client-id">>,
        client_secret => emqx_secret:wrap(<<"client-secret">>),
        scope => <<"read">>,
        timeout => 5_000
    }.

%% Builds an unsigned (`alg=none') compact JWT carrying the given claims, just
%% enough for `jose_jwt:peek/1' to decode the payload.
make_jwt(Claims) ->
    Header = b64url(<<"{\"alg\":\"none\"}">>),
    Payload = b64url(emqx_utils_json:encode(Claims)),
    <<Header/binary, ".", Payload/binary, ".">>.

b64url(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

wait_for(_Fun, 0) ->
    error(wait_timeout);
wait_for(Fun, Timeout) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_for(Fun, Timeout - 100)
    end.
