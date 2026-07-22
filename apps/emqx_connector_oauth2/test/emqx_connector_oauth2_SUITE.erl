%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_oauth2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

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
    ok = snabbkaffe:start_trace(),
    emqx_connector_oauth2:clear_cache(),
    Table = ets:new(?MODULE, [set, public]),
    true = ets:insert(Table, [{request_count, 0}, {handler, fun unexpected_request/1}]),
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(random, "/[...]", false),
    ok = emqx_utils_http_test_server:set_handler(fun(Req, State) ->
        token_server_handler(Table, Req, State)
    end),
    BaseURL = iolist_to_binary(["http://127.0.0.1:", integer_to_list(Port)]),
    [{token_server, #{table => Table, base_url => BaseURL}} | TCConfig].

end_per_testcase(_TestCase, TCConfig) ->
    ok = emqx_utils_http_test_server:stop(),
    true = ets:delete(token_server_table(TCConfig)),
    emqx_connector_oauth2:clear_cache(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_get_token_fetches_and_caches(TCConfig) ->
    ResourceId = <<"res:1">>,
    ok = set_token_response(TCConfig, <<"access-1">>, 3600),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual({ok, <<"access-1">>}, emqx_connector_oauth2:get_token(ResourceId)),
    %% Second call must hit the ETS cache, not the token endpoint.
    ?assertEqual({ok, <<"access-1">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ?assertEqual(1, token_request_count(TCConfig)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_refresh_renews_token_before_expiry(TCConfig) ->
    ResourceId = <<"res:2">>,
    ok = set_token_response(TCConfig, <<"access-2a">>, 2),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    %% Short lifetime so the refresh timer fires quickly (75% of 2s = 1.5s).
    ?assertEqual({ok, <<"access-2a">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = set_token_response(TCConfig, <<"access-2b">>, 3600),
    ok = wait_for(
        fun() -> emqx_connector_oauth2:get_token(ResourceId) =:= {ok, <<"access-2b">>} end,
        10_000
    ),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_refresh_failure_keeps_valid_token(TCConfig) ->
    ResourceId = <<"res:refresh-failure">>,
    ok = set_token_response(TCConfig, <<"old-token">>, 8),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual({ok, <<"old-token">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = set_token_handler(TCConfig, fun(_Request) ->
        {503, #{}, <<"unavailable">>}
    end),
    {ok, _} = ?block_until(
        #{?snk_kind := "oauth2_token_refresh_failed", resource_id := ResourceId},
        8_000
    ),
    %% A failed proactive refresh must not replace the still-valid token with
    %% an error entry.
    ?assertEqual({ok, <<"old-token">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = set_token_response(TCConfig, <<"new-token">>, 3600),
    ok = wait_for(
        fun() -> emqx_connector_oauth2:get_token(ResourceId) =:= {ok, <<"new-token">>} end,
        5_000
    ),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_fetch_failure_is_cached_briefly(TCConfig) ->
    ResourceId = <<"res:3">>,
    ok = set_token_handler(TCConfig, fun(_Request) ->
        {503, #{}, <<"unavailable">>}
    end),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)),
    %% A second call within the short failure-cache window reuses the error.
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)),
    ?assertEqual(1, token_request_count(TCConfig)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_unregister_clears_cache(TCConfig) ->
    ResourceId = <<"res:4">>,
    ok = set_token_response(TCConfig, <<"access-4">>, 3600),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual({ok, <<"access-4">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = emqx_connector_oauth2:unregister(ResourceId),
    ?assertEqual(1, token_request_count(TCConfig)),
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ResourceId)).

t_register_invalidates_cached_token(TCConfig) ->
    ResourceId = <<"res:config-update">>,
    ok = set_token_response(TCConfig, <<"old-token">>, 3600),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual({ok, <<"old-token">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = set_token_response(TCConfig, <<"new-token">>, 3600),
    UpdatedConfig = (oauth2_config(TCConfig))#{client_id := <<"updated-client-id">>},
    ok = emqx_connector_oauth2:register(ResourceId, UpdatedConfig),
    ?assertEqual({ok, <<"new-token">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ?assertEqual(2, token_request_count(TCConfig)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_manager_restart_recovers_registration(TCConfig) ->
    ResourceId = <<"res:manager-restart">>,
    ok = set_token_response(TCConfig, <<"after-restart">>, 3600),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    OldPid = whereis(emqx_connector_oauth2),
    exit(OldPid, kill),
    ok = wait_for(
        fun() ->
            case whereis(emqx_connector_oauth2) of
                Pid when is_pid(Pid), Pid =/= OldPid -> true;
                _ -> false
            end
        end,
        5_000
    ),
    ?assertEqual({ok, <<"after-restart">>}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_get_token_handles_manager_unavailable(TCConfig) ->
    ResourceId = <<"res:manager-unavailable">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ok = supervisor:terminate_child(emqx_connector_oauth2_sup, emqx_connector_oauth2),
    try
        ?assertMatch(
            {error, {oauth2_manager_unavailable, _}},
            emqx_connector_oauth2:get_token(ResourceId)
        )
    after
        {ok, _} = supervisor:restart_child(emqx_connector_oauth2_sup, emqx_connector_oauth2)
    end,
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_unregister_handles_manager_unavailable(TCConfig) ->
    ResourceId = <<"res:unregister-manager-unavailable">>,
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ok = supervisor:terminate_child(emqx_connector_oauth2_sup, emqx_connector_oauth2),
    try
        ?assertEqual(ok, emqx_connector_oauth2:unregister(ResourceId))
    after
        {ok, _} = supervisor:restart_child(emqx_connector_oauth2_sup, emqx_connector_oauth2)
    end,
    ?assertEqual(
        {error, oauth2_not_registered},
        emqx_connector_oauth2:get_token(ResourceId)
    ).

t_different_resources_fetch_concurrently(TCConfig) ->
    SlowId = <<"res:slow">>,
    FastId = <<"res:fast">>,
    SlowEndpoint = token_endpoint(TCConfig, <<"/slow">>),
    FastEndpoint = token_endpoint(TCConfig, <<"/fast">>),
    ok = emqx_connector_oauth2:register(
        SlowId, (oauth2_config(TCConfig))#{token_endpoint := SlowEndpoint}
    ),
    ok = emqx_connector_oauth2:register(
        FastId, (oauth2_config(TCConfig))#{token_endpoint := FastEndpoint}
    ),
    TestPid = self(),
    ok = set_token_handler(TCConfig, fun(#{path := Path}) ->
        TestPid ! {fetch_started, Path, self()},
        receive
            continue -> token_response(Path, 3600)
        end
    end),
    spawn(fun() -> TestPid ! {fetch_done, SlowId, emqx_connector_oauth2:get_token(SlowId)} end),
    SlowFetch = ?assertReceive({fetch_started, <<"/slow">>, _}, 1_000),
    {fetch_started, <<"/slow">>, SlowWorker} = SlowFetch,
    spawn(fun() -> TestPid ! {fetch_done, FastId, emqx_connector_oauth2:get_token(FastId)} end),
    FastFetch = ?assertReceive({fetch_started, <<"/fast">>, _}, 1_000),
    {fetch_started, <<"/fast">>, FastWorker} = FastFetch,
    SlowWorker ! continue,
    FastWorker ! continue,
    ?assertReceive({fetch_done, SlowId, {ok, <<"/slow">>}}, 1_000),
    ?assertReceive({fetch_done, FastId, {ok, <<"/fast">>}}, 1_000),
    ok = emqx_connector_oauth2:unregister(SlowId),
    ok = emqx_connector_oauth2:unregister(FastId).

t_concurrent_get_token_fetches_once(TCConfig) ->
    ResourceId = <<"res:conc">>,
    TestPid = self(),
    ok = set_token_handler(TCConfig, fun(_Request) ->
        TestPid ! {fetch_started, self()},
        receive
            continue -> token_response(<<"access-conc">>, 3600)
        end
    end),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    spawn(fun() ->
        Responses0 =
            emqx_utils:pmap(
                fun(_) -> emqx_connector_oauth2:get_token(ResourceId) end,
                lists:seq(1, 10)
            ),
        TestPid ! {responses, Responses0}
    end),
    {fetch_started, FetchWorker} = ?assertReceive({fetch_started, _}, 1_000),
    FetchWorker ! continue,
    {responses, Responses} = ?assertReceive({responses, _}, 2_000),
    ?assertMatch([_ | _], Responses),
    ?assert(lists:all(fun(R) -> R =:= {ok, <<"access-conc">>} end, Responses)),
    ?assertEqual(1, token_request_count(TCConfig)),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_token_expiry_is_derived_from_jwt(TCConfig) ->
    ResourceId = <<"res:jwt-expiry">>,
    JWT = make_jwt(#{<<"exp">> => os:system_time(second) + 3}),
    ok = set_token_handler(TCConfig, fun(_Request) ->
        {200, json_headers(),
            emqx_utils_json:encode(#{
                <<"access_token">> => JWT,
                <<"token_type">> => <<"Bearer">>
            })}
    end),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual({ok, JWT}, emqx_connector_oauth2:get_token(ResourceId)),
    ok = set_token_response(TCConfig, <<"refreshed-after-jwt">>, 3600),
    ok = wait_for(
        fun() ->
            emqx_connector_oauth2:get_token(ResourceId) =:= {ok, <<"refreshed-after-jwt">>}
        end,
        6_000
    ),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_token_endpoint_error_is_sanitized(TCConfig) ->
    ResourceId = <<"res:bad-status">>,
    SensitiveBody = emqx_utils_json:encode(#{
        <<"error">> => <<"invalid_client">>,
        <<"error_description">> => <<"sensitive provider details">>
    }),
    ok = set_token_handler(TCConfig, fun(_Request) ->
        {401, #{<<"x-sensitive-header">> => <<"secret-value">>}, SensitiveBody}
    end),
    ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
    ?assertEqual(
        {error, {token_endpoint_error, 401, <<"invalid_client">>}},
        emqx_connector_oauth2:get_token(ResourceId)
    ),
    ok = emqx_connector_oauth2:unregister(ResourceId).

t_rejects_malformed_success_response(TCConfig) ->
    Responses = [
        #{<<"access_token">> => 42, <<"expires_in">> => 3600},
        #{<<"access_token">> => <<"token">>, <<"expires_in">> => <<"3600">>},
        #{<<"access_token">> => <<"token">>, <<"token_type">> => <<"MAC">>}
    ],
    lists:foreach(
        fun({I, Response}) ->
            ResourceId = <<"res:malformed:", (integer_to_binary(I))/binary>>,
            ok = set_token_handler(TCConfig, fun(_Request) ->
                {200, json_headers(), emqx_utils_json:encode(Response)}
            end),
            ok = emqx_connector_oauth2:register(ResourceId, oauth2_config(TCConfig)),
            ?assertMatch(
                {error, {bad_token_response, _}},
                emqx_connector_oauth2:get_token(ResourceId)
            ),
            ok = emqx_connector_oauth2:unregister(ResourceId)
        end,
        lists:enumerate(Responses)
    ).

t_validate_rejects_missing_fields(_TCConfig) ->
    Required = [token_endpoint, client_id, client_secret],
    Oauth2 = raw_oauth2_config(),
    lists:foreach(
        fun(Field) ->
            ?assertMatch({error, _}, check_oauth2(maps:remove(Field, Oauth2)), #{field => Field})
        end,
        Required
    ).

t_validate_rejects_unknown_grant_type(_TCConfig) ->
    ?assertMatch(
        {error, _},
        check_oauth2((raw_oauth2_config())#{grant_type => authorization_code})
    ).

t_validate_rejects_bad_token_endpoint(_TCConfig) ->
    ?assertMatch(
        {error, _},
        check_oauth2((raw_oauth2_config())#{token_endpoint => <<"ftp://example.com/token">>})
    ),
    ?assertMatch(
        {error, _},
        check_oauth2((raw_oauth2_config())#{token_endpoint => <<"not-a-url">>})
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
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, #{})),
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, #{enable => false})),
    ?assertMatch({ok, #{oauth2 := #{}}}, check_oauth2(#{})),
    ?assertMatch({ok, #{oauth2 := #{enable := false}}}, check_oauth2(#{enable => false})),
    ?assertMatch(
        {ok, #{oauth2 := #{enable := true, ssl := #{enable := true}}}},
        check_oauth2(raw_oauth2_config())
    ),
    ?assertEqual(
        ok,
        emqx_connector_oauth2_schema:validate(#{<<"authorization">> => <<"x">>}, undefined)
    ),
    ?assertEqual(ok, emqx_connector_oauth2_schema:validate(#{}, FullOauth2)).

t_omitted_oauth2_is_not_materialized(_TCConfig) ->
    Schema = #{roots => [emqx_connector_oauth2_schema:oauth2_field()]},
    Checked = hocon_tconf:check_plain(
        Schema,
        #{},
        #{atom_key => true, required => false}
    ),
    ?assertEqual(#{}, Checked).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

oauth2_config(TCConfig) ->
    #{
        enable => true,
        grant_type => client_credentials,
        token_endpoint => token_endpoint(TCConfig, <<"/oauth/token">>),
        client_id => <<"client-id">>,
        client_secret => emqx_secret:wrap(<<"client-secret">>),
        scope => <<"read">>,
        timeout => 5_000
    }.

raw_oauth2_config() ->
    #{
        enable => true,
        grant_type => client_credentials,
        token_endpoint => <<"https://auth.example.com/oauth/token">>,
        client_id => <<"client-id">>,
        client_secret => <<"client-secret">>
    }.

check_oauth2(Oauth2) ->
    Schema = #{roots => [emqx_connector_oauth2_schema:oauth2_field()]},
    try
        Checked = hocon_tconf:check_plain(
            Schema,
            #{<<"oauth2">> => emqx_utils_maps:binary_key_map(Oauth2)},
            #{atom_key => true, required => false}
        ),
        {ok, Checked}
    catch
        throw:Reason ->
            {error, Reason}
    end.

set_token_response(TCConfig, Token, ExpiresInSec) ->
    set_token_handler(TCConfig, fun(_Request) -> token_response(Token, ExpiresInSec) end).

token_response(Token, ExpiresInSec) ->
    {200, json_headers(),
        emqx_utils_json:encode(#{
            <<"access_token">> => Token,
            <<"expires_in">> => ExpiresInSec,
            <<"token_type">> => <<"Bearer">>
        })}.

json_headers() ->
    #{<<"content-type">> => <<"application/json">>}.

set_token_handler(TCConfig, Handler) ->
    true = ets:insert(token_server_table(TCConfig), {handler, Handler}),
    ok.

token_request_count(TCConfig) ->
    ets:lookup_element(token_server_table(TCConfig), request_count, 2).

token_endpoint(TCConfig, Path) ->
    #{base_url := BaseURL} = ?config(token_server, TCConfig),
    <<BaseURL/binary, Path/binary>>.

token_server_table(TCConfig) ->
    #{table := Table} = ?config(token_server, TCConfig),
    Table.

token_server_handler(Table, Req0, State) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    Count = ets:update_counter(Table, request_count, 1),
    Handler = ets:lookup_element(Table, handler, 2),
    Request = #{
        body => Body,
        count => Count,
        headers => cowboy_req:headers(Req1),
        path => cowboy_req:path(Req1)
    },
    {Status, Headers, ResponseBody} = Handler(Request),
    Req = cowboy_req:reply(Status, Headers, ResponseBody, Req1),
    {ok, Req, State}.

unexpected_request(Request) ->
    error({unexpected_token_request, Request}).

%% Builds an unsigned (`alg=none') compact JWT carrying the given claims, just
%% enough for `jose_jwt:peek/1' to decode the payload.
make_jwt(Claims) ->
    Header = b64url(<<"{\"alg\":\"none\"}">>),
    Payload = b64url(emqx_utils_json:encode(Claims)),
    <<Header/binary, ".", Payload/binary, ".">>.

b64url(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

wait_for(_Fun, Timeout) when Timeout =< 0 ->
    error(wait_timeout);
wait_for(Fun, Timeout) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_for(Fun, Timeout - 100)
    end.
