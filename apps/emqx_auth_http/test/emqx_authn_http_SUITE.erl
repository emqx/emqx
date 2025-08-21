%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(PATH, [?CONF_NS_ATOM]).

-define(HTTP_PORT, 32333).
-define(HTTP_PATH, "/auth/[...]").
-define(CREDENTIALS, #{
    clientid => <<"clienta">>,
    username => <<"plain">>,
    password => <<"plain">>,
    peerhost => {127, 0, 0, 1},
    peerport => 12345,
    listener => 'tcp:default',
    protocol => mqtt,
    cert_subject => <<"cert_subject_data">>,
    cert_common_name => <<"cert_common_name_data">>,
    cert_pem => <<"fake_raw_cert_to_be_base64_encoded">>,
    client_attrs => #{<<"group">> => <<"g1">>}
}).

-define(SERVER_RESPONSE_JSON(Result), ?SERVER_RESPONSE_JSON(Result, false)).
-define(SERVER_RESPONSE_JSON(Result, IsSuperuser),
    emqx_utils_json:encode(#{
        result => Result,
        is_superuser => IsSuperuser
    })
).

-define(SERVER_RESPONSE_WITH_ACL_JSON(ACL),
    emqx_utils_json:encode(#{
        result => allow,
        acl => ACL
    })
).

-define(SERVER_RESPONSE_WITH_ACL_JSON(ACL, Expire),
    emqx_utils_json:encode(#{
        result => allow,
        acl => ACL,
        expire_at => Expire
    })
).

-define(SERVER_RESPONSE_URLENCODE(Result, IsSuperuser),
    list_to_binary(
        "result=" ++
            uri_encode(Result) ++ "&" ++
            "is_superuser=" ++
            uri_encode(IsSuperuser)
    )
).

-define(EXCEPTION_ALLOW, ?EXCEPTION_ALLOW(false)).
-define(EXCEPTION_ALLOW(IsSuperuser), {ok, #{is_superuser := IsSuperuser}}).
-define(EXCEPTION_DENY, {error, not_authorized}).
-define(EXCEPTION_IGNORE, ignore).
-define(CLIENTID(N), iolist_to_binary([atom_to_list(?FUNCTION_NAME), "-", integer_to_binary(N)])).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_utils:interactive_load(emqx_variform_bif),
    Apps = emqx_cth_suite:start([cowboy, emqx, emqx_conf, emqx_auth, emqx_auth_http], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    {ok, _} = emqx_utils_http_test_server:start_link(?HTTP_PORT, ?HTTP_PATH),
    try
        ?MODULE:Case(init, Config)
    catch
        error:undef ->
            Config
    end.

end_per_testcase(Case, Config) ->
    try
        ?MODULE:Case('end', Config)
    catch
        error:undef ->
            ok
    end,
    _ = emqx_auth_cache:reset(?AUTHN_CACHE),
    ok = emqx_authn_test_lib:enable_node_cache(false),
    ok = emqx_utils_http_test_server:stop().

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_http_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_http}]} = emqx_authn_chains:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    AuthConfig = raw_http_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"headers">> => []},
            AuthConfig#{<<"method">> => <<"delete">>},
            AuthConfig#{<<"url">> => <<"localhost">>},
            AuthConfig#{<<"url">> => <<"http://foo.com/xxx#fragment">>},
            AuthConfig#{<<"url">> => <<"http://${foo}.com/xxx">>},
            AuthConfig#{<<"url">> => <<"//foo.com/xxx">>},
            AuthConfig#{<<"precondition">> => <<"not a valid precondition">>}
        ],

    lists:foreach(
        fun(Config) ->
            ct:pal("creating authenticator with invalid config: ~p", [Config]),
            {error, _} =
                try
                    emqx:update_config(
                        ?PATH,
                        {create_authenticator, ?GLOBAL, Config}
                    )
                catch
                    throw:Error ->
                        {error, Error}
                end,
            ?assertEqual(
                {error, {not_found, {chain, ?GLOBAL}}},
                emqx_authn_chains:list_authenticators(?GLOBAL)
            )
        end,
        InvalidConfigs
    ).

t_authenticate(_Config) ->
    ok = emqx_logger:set_primary_log_level(debug),
    ok = lists:foreach(
        fun(Sample) ->
            ct:pal("test_user_auth sample: ~p", [Sample]),
            test_user_auth(Sample)
        end,
        samples()
    ).

test_user_auth(
    #{
        handler := Handler,
        config_params := SpecificConfgParams,
        result := Expect
    } = Sample
) ->
    Credentials = maps:merge(?CREDENTIALS, maps:get(credentials, Sample, #{})),
    Result = perform_user_auth(SpecificConfgParams, Handler, Credentials),
    ?assertEqual(Expect, Result).

perform_user_auth(SpecificConfgParams, Handler, Credentials) ->
    AuthConfig = maps:merge(raw_http_auth_config(), SpecificConfgParams),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    ok = emqx_utils_http_test_server:set_handler(Handler),

    Result = emqx_access_control:authenticate(Credentials),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    Result.

t_authenticate_path_placeholders(_Config) ->
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req =
                case cowboy_req:path(Req0) of
                    <<"/auth/p%20ath//us+er/auth//">> ->
                        cowboy_req:reply(
                            200,
                            #{<<"content-type">> => <<"application/json">>},
                            emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                            Req0
                        );
                    Path ->
                        ct:pal("Unexpected path: ~p", [Path]),
                        cowboy_req:reply(403, Req0)
                end,
            {ok, Req, State}
        end
    ),

    Credentials = maps:merge(?CREDENTIALS, #{
        username => <<"us er">>
    }),

    AuthConfig = maps:merge(
        raw_http_auth_config(),
        #{
            <<"url">> => <<"http://127.0.0.1:32333/auth/p%20ath//${username}/auth//">>,
            <<"body">> => #{}
        }
    ),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    ?assertMatch(
        {ok, #{is_superuser := false}},
        emqx_access_control:authenticate(Credentials)
    ),

    _ = emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_no_value_for_placeholder(_Config) ->
    Handler = fun(Req0, State) ->
        {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
        #{
            <<"cert_subject">> := <<"">>,
            <<"cert_common_name">> := <<"">>,
            <<"cert_pem">> := <<"">>
        } = emqx_utils_json:decode(RawBody),
        Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(#{result => allow, is_superuser => false}),
            Req1
        ),
        {ok, Req, State}
    end,

    SpecificConfgParams = #{
        <<"method">> => <<"post">>,
        <<"headers">> => #{<<"content-type">> => <<"application/json">>},
        <<"body">> => #{
            <<"cert_subject">> => ?PH_CERT_SUBJECT,
            <<"cert_common_name">> => ?PH_CERT_CN_NAME,
            <<"cert_pem">> => ?PH_CERT_PEM
        }
    },

    AuthConfig = maps:merge(raw_http_auth_config(), SpecificConfgParams),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    ok = emqx_utils_http_test_server:set_handler(Handler),

    Credentials = maps:without([cert_subject, cert_common_name, cert_pem], ?CREDENTIALS),

    ?assertMatch({ok, _}, emqx_access_control:authenticate(Credentials)),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_disallowed_placeholders_preserved(_Config) ->
    Config = #{
        <<"method">> => <<"post">>,
        <<"headers">> => #{<<"content-type">> => <<"application/json">>},
        <<"body">> => #{
            <<"username">> => ?PH_USERNAME,
            <<"password">> => ?PH_PASSWORD,
            <<"this">> => <<"${whatisthis}">>
        }
    },
    Handler = fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        #{
            <<"username">> := <<"plain">>,
            <<"password">> := <<"plain">>,
            <<"this">> := <<"${whatisthis}">>
        } = emqx_utils_json:decode(Body),
        Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(#{result => allow, is_superuser => false}),
            Req1
        ),
        {ok, Req, State}
    end,
    ?assertMatch({ok, _}, perform_user_auth(Config, Handler, ?CREDENTIALS)),

    % NOTE: disallowed placeholder left intact, which makes the URL invalid
    ConfigUrl = Config#{
        <<"url">> => <<"http://127.0.0.1:32333/auth/${whatisthis}">>
    },
    ?assertMatch({error, _}, perform_user_auth(ConfigUrl, Handler, ?CREDENTIALS)).

t_destroy(_Config) ->
    AuthConfig = raw_http_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    Headers = #{<<"content-type">> => <<"application/json">>},
    Response = ?SERVER_RESPONSE_JSON(allow),

    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Headers, Response, Req0),
            {ok, Req, State}
        end
    ),

    {ok, [#{provider := emqx_authn_http, state := State}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL),

    Credentials = maps:with([username, password], ?CREDENTIALS),

    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_authn_http:authenticate(
            Credentials,
            State
        )
    ),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    % Authenticator should not be usable anymore
    ?assertMatch(
        ?EXCEPTION_IGNORE,
        emqx_authn_http:authenticate(
            Credentials,
            State
        )
    ).

t_update(_Config) ->
    CorrectConfig = raw_http_auth_config(),
    IncorrectConfig =
        CorrectConfig#{<<"url">> => <<"http://127.0.0.1:32333/invalid">>},

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, IncorrectConfig}
    ),

    Headers = #{<<"content-type">> => <<"application/json">>},
    Response = ?SERVER_RESPONSE_JSON(allow),

    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Headers, Response, Req0),
            {ok, Req, State}
        end
    ),

    ?assertMatch(
        ?EXCEPTION_DENY,
        emqx_access_control:authenticate(?CREDENTIALS)
    ),

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, CorrectConfig}
    ),

    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_resource_status(_Config) ->
    EnabledConfig = raw_http_auth_config(),
    DisabledConfig =
        EnabledConfig#{<<"enable">> => false},

    %% Create enabled, update to disabled
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, EnabledConfig}
    ),
    {ok, #{state := #{resource_id := ResourceId0}}} = emqx_authn_chains:lookup_authenticator(
        ?GLOBAL,
        <<"password_based:http">>
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId0)),
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, DisabledConfig}
    ),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId0)),

    %% Cleanup
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    %% Now, create disabled, update to enabled
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, DisabledConfig}
    ),
    {ok, #{state := #{resource_id := ResourceId1}}} = emqx_authn_chains:lookup_authenticator(
        ?GLOBAL,
        <<"password_based:http">>
    ),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId1)),
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, EnabledConfig}
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId1)),

    %% Cleanup
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_update_precondition(_Config) ->
    %% always allow
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ?SERVER_RESPONSE_JSON(allow),
                Req0
            ),
            {ok, Req, State}
        end
    ),

    CorrectConfig = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, CorrectConfig}
    ),
    InvalidPreconditionConfig =
        CorrectConfig#{<<"precondition">> => <<"not a valid precondition">>},

    ?assertMatch(
        {error, {post_config_update, emqx_authn_config, #{cause := "bad_precondition_expression"}}},
        emqx:update_config(
            ?PATH,
            {update_authenticator, ?GLOBAL, <<"password_based:http">>, InvalidPreconditionConfig}
        )
    ),

    Connect = fun(ClientId) ->
        {ok, Pid} = emqtt:start_link([
            {host, "127.0.0.1"},
            {port, 1883},
            {clean_start, true},
            {clientid, ClientId},
            {username, <<"plain">>},
            {password, <<"plain">>}
        ]),
        unlink(Pid),
        _ = monitor(process, Pid),
        R =
            case emqtt:connect(Pid) of
                {ok, _} ->
                    ok = emqtt:disconnect(Pid);
                {error, _} = Err ->
                    Err
            end,
        receive
            {'DOWN', _Ref, process, Pid, _Reason} ->
                ok
        after 1000 ->
            error(timeout)
        end,
        R
    end,

    %% allowed without precondition
    ?assertMatch(ok, Connect(<<"c1-123">>)),
    ?assertMatch(ok, Connect(<<"c2-123">>)),

    %% allow only clientid starting with c1-
    ValidPreconditionConfig =
        CorrectConfig#{<<"precondition">> => <<"regex_match(clientid, 'c1-.*')">>},

    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, ValidPreconditionConfig}
    ),
    ?assertMatch(ok, Connect(<<"c1-123">>)),
    ?assertMatch({error, {unauthorized_client, _}}, Connect(<<"c2-123">>)),
    ok.

t_node_cache(_Config) ->
    Config = maps:merge(
        raw_http_auth_config(),
        #{
            <<"method">> => <<"get">>,
            <<"url">> => <<"http://127.0.0.1:32333/auth/${clientid}?username=${username}">>,
            <<"body">> => #{<<"password">> => <<"${password}">>}
        }
    ),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok = emqx_authn_test_lib:enable_node_cache(true),
    Handler = fun(#{path := Path} = Req0, State) ->
        Req =
            case {Path, cowboy_req:match_qs([username, password], Req0)} of
                {<<"/auth/clientid">>, #{username := <<"username">>, password := <<"password">>}} ->
                    cowboy_req:reply(204, Req0);
                _ ->
                    cowboy_req:reply(403, Req0)
            end,
        {ok, Req, State}
    end,
    ok = emqx_utils_http_test_server:set_handler(Handler),

    %% We authenticate twice, the second time should be cached
    Credentials = maps:merge(?CREDENTIALS, #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        password => <<"password">>
    }),
    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ),

    %% Now change a var in each interpolated part, the cache should NOT be hit
    ?assertMatch(
        ?EXCEPTION_DENY,
        emqx_access_control:authenticate(Credentials#{username => <<"username2">>})
    ),
    ?assertMatch(
        ?EXCEPTION_DENY,
        emqx_access_control:authenticate(Credentials#{password => <<"password2">>})
    ),
    ?assertMatch(
        ?EXCEPTION_DENY,
        emqx_access_control:authenticate(Credentials#{clientid => <<"clientid2">>})
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 4}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ).

t_is_superuser(_Config) ->
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        %% {ContentType, ExpectedIsSuperuser, ResponseIsSuperuser}
        %% Is Superuser
        {json, true, <<"1">>},
        {json, true, 1},
        {json, true, 123},
        {json, true, <<"true">>},
        {json, true, true},

        %% Not Superuser
        {json, false, <<"">>},
        {json, false, <<"0">>},
        {json, false, 0},
        {json, false, null},
        {json, false, undefined},
        {json, false, <<"false">>},
        {json, false, false},

        {json, false, <<"val">>},

        %% Is Superuser
        {form, true, <<"1">>},
        {form, true, 1},
        {form, true, 123},
        {form, true, <<"true">>},
        {form, true, true},

        %% Not Superuser
        {form, false, <<"">>},
        {form, false, <<"0">>},
        {form, false, 0},

        {form, false, null},
        {form, false, undefined},
        {form, false, <<"false">>},
        {form, false, false},

        {form, false, <<"val">>}
    ],

    lists:foreach(fun test_is_superuser/1, Checks).

test_is_superuser({Kind, ExpectedValue, ServerResponse}) ->
    {ContentType, Res} =
        case Kind of
            json ->
                {<<"application/json; charset=utf-8">>,
                    ?SERVER_RESPONSE_JSON(allow, ServerResponse)};
            form ->
                {<<"application/x-www-form-urlencoded; charset=utf-8">>,
                    ?SERVER_RESPONSE_URLENCODE(allow, ServerResponse)}
        end,

    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => ContentType},
                Res,
                Req0
            ),
            {ok, Req, State}
        end
    ),

    ?assertMatch(
        ?EXCEPTION_ALLOW(ExpectedValue),
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_ignore_allow_deny(_Config) ->
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        %% only one chain, ignore by authn http and deny by default
        {deny, ?SERVER_RESPONSE_JSON(ignore)},

        {{allow, true}, ?SERVER_RESPONSE_JSON(allow, true)},
        {{allow, false}, ?SERVER_RESPONSE_JSON(allow)},
        {{allow, false}, ?SERVER_RESPONSE_JSON(allow, false)},

        {deny, ?SERVER_RESPONSE_JSON(deny)},
        {deny, ?SERVER_RESPONSE_JSON(deny, true)},
        {deny, ?SERVER_RESPONSE_JSON(deny, false)}
    ],

    lists:foreach(fun test_ignore_allow_deny/1, Checks).

test_ignore_allow_deny({ExpectedValue, ServerResponse}) ->
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ServerResponse,
                Req0
            ),
            {ok, Req, State}
        end
    ),

    case ExpectedValue of
        {allow, IsSuperuser} ->
            ?assertMatch(
                ?EXCEPTION_ALLOW(IsSuperuser),
                emqx_access_control:authenticate(?CREDENTIALS)
            );
        deny ->
            ?assertMatch(
                ?EXCEPTION_DENY,
                emqx_access_control:authenticate(?CREDENTIALS)
            )
    end.

t_acl(_Config) ->
    ACL = acl_rules(),
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ?SERVER_RESPONSE_WITH_ACL_JSON(ACL),
                Req0
            ),
            {ok, Req, State}
        end
    ),
    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, <<"password">>}
        ]
    ),
    {ok, _} = emqtt:connect(C),
    Cases = [
        {allow, <<"http-authn-acl/#">>},
        {deny, <<"http-authn-acl/1">>},
        {deny, <<"t/#">>}
    ],
    try
        lists:foreach(
            fun(Case) ->
                test_acl(Case, C)
            end,
            Cases
        )
    after
        ok = emqtt:disconnect(C)
    end.

t_auth_expire(_Config) ->
    ACL = acl_rules(),
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ExpireSec = 3,
    ExpireMSec = timer:seconds(ExpireSec),
    Tests = [
        {<<"ok-to-connect-but-expire-on-pub">>, erlang:system_time(second) + ExpireSec, fun(C) ->
            {ok, _} = emqtt:connect(C),
            receive
                {'DOWN', _Ref, process, C, Reason} ->
                    ?assertMatch({shutdown, {disconnected, ?RC_NOT_AUTHORIZED, _}}, Reason)
            after round(ExpireMSec * 1.5) ->
                error(timeout)
            end
        end},
        {<<"past">>, erlang:system_time(second) - 1, fun(C) ->
            ?assertMatch({error, {bad_username_or_password, _}}, emqtt:connect(C)),
            receive
                {'DOWN', _Ref, process, C, Reason} ->
                    ?assertMatch({shutdown, bad_username_or_password}, Reason)
            end
        end},
        {<<"invalid">>, erlang:system_time(millisecond), fun(C) ->
            ?assertMatch({error, {bad_username_or_password, _}}, emqtt:connect(C)),
            receive
                {'DOWN', _Ref, process, C, Reason} ->
                    ?assertMatch({shutdown, bad_username_or_password}, Reason)
            end
        end}
    ],
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            QS = cowboy_req:parse_qs(Req0),
            {_, Username} = lists:keyfind(<<"username">>, 1, QS),
            {_, ExpireTime, _} = lists:keyfind(Username, 1, Tests),
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ?SERVER_RESPONSE_WITH_ACL_JSON(ACL, ExpireTime),
                Req0
            ),
            {ok, Req, State}
        end
    ),
    lists:foreach(fun test_auth_expire/1, Tests).

test_auth_expire({Username, _ExpireTime, TestFn}) ->
    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, Username},
            {password, <<"password">>}
        ]
    ),
    _ = monitor(process, C),
    unlink(C),
    try
        TestFn(C)
    after
        catch emqtt:stop(C)
    end.

test_acl({allow, Topic}, C) ->
    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:subscribe(C, Topic)
    );
test_acl({deny, Topic}, C) ->
    ?assertMatch(
        {ok, #{}, [?RC_NOT_AUTHORIZED]},
        emqtt:subscribe(C, Topic)
    ).

t_precondition_check_listener_id(_Config) ->
    Config = raw_http_auth_config(),
    Config1 = Config#{
        <<"precondition">> => <<"str_eq(listener, 'tcp:default')">>
    },

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config1}
    ),

    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ?SERVER_RESPONSE_JSON(allow),
                Req0
            ),
            {ok, Req, State}
        end
    ),

    % Test TCP listener - should succeed
    {ok, C1} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, 1883},
        {clean_start, true},
        {clientid, ?CLIENTID(1)},
        {username, <<"plain">>},
        {password, <<"plain">>}
    ]),
    {ok, _} = emqtt:connect(C1),
    ok = emqtt:disconnect(C1),

    % Test SSL listener - should fail
    {ok, C2} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, 8883},
        {clean_start, true},
        {clientid, ?CLIENTID(2)},
        {username, <<"plain">>},
        {password, <<"plain">>},
        {ssl, true},
        {ssl_opts, [{verify, verify_none}]}
    ]),
    unlink(C2),
    _ = monitor(process, C2),
    ?assertMatch({error, {unauthorized_client, _}}, emqtt:connect(C2)),
    receive
        {'DOWN', _Ref, process, C2, Reason} ->
            ?assertMatch({shutdown, unauthorized_client}, Reason)
    after 1000 ->
        error(timeout)
    end,
    ok.

t_precondition_check_cert_cn(init, Config) ->
    %% Change the listener SSL configuration: require peer certificate.
    {ok, _} = emqx:update_config(
        [listeners, ssl, default],
        {update, #{
            <<"ssl_options">> => #{
                <<"verify">> => verify_peer,
                <<"fail_if_no_peer_cert">> => true
            }
        }}
    ),
    Config;
t_precondition_check_cert_cn('end', _Config) ->
    %% Restore the listener SSL configuration to default.
    {ok, _} = emqx:update_config(
        [listeners, ssl, default],
        {update, #{
            <<"ssl_options">> => #{
                <<"verify">> => verify_none,
                <<"fail_if_no_peer_cert">> => false
            }
        }}
    ),
    ok.

t_precondition_check_cert_cn(_Config) ->
    Config = raw_http_auth_config(),
    %% if cert_common_name is empty, the client will be denied
    Config1 = Config#{
        <<"precondition">> => <<"not(is_empty_val(cert_common_name))">>
    },

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config1}
    ),

    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ?SERVER_RESPONSE_JSON(allow),
                Req0
            ),
            {ok, Req, State}
        end
    ),

    % Test TCP listener - no cert_common_name, should be denied
    {ok, C1} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, 1883},
        {clean_start, true},
        {clientid, ?CLIENTID(1)},
        {username, <<"plain">>},
        {password, <<"plain">>}
    ]),
    unlink(C1),
    _ = monitor(process, C1),
    ?assertMatch({error, {unauthorized_client, _}}, emqtt:connect(C1)),
    receive
        {'DOWN', _Ref, process, C1, Reason} ->
            ?assertMatch({shutdown, unauthorized_client}, Reason)
    after 1000 ->
        error(timeout)
    end,

    % Test SSL listener - with cert_common_name, should be allowed
    {ok, C2} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, 8883},
        {clean_start, true},
        {clientid, ?CLIENTID(2)},
        {username, <<"plain">>},
        {password, <<"plain">>},
        {ssl, true},
        {ssl_opts, [
            {verify, verify_none},
            {certfile, cert_path("client-cert.pem")},
            {keyfile, cert_path("client-key.pem")}
        ]}
    ]),
    {ok, _} = emqtt:connect(C2),
    ok = emqtt:disconnect(C2).

-doc """
Checks that, if an authentication backend returns the `clientid_override` attribute, it's
used to override.
""".
t_clientid_override(TCConfig) when is_list(TCConfig) ->
    OverriddenClientId = <<"overridden_clientid">>,
    MkConfigFn = fun raw_http_auth_config/0,
    PostConfigFn = fun() ->
        ok = emqx_utils_http_test_server:set_handler(
            fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{
                        result => allow,
                        clientid_override => <<"overridden_clientid">>
                    }),
                    Req0
                ),
                {ok, Req, State}
            end
        )
    end,
    Opts = #{
        mk_config_fn => MkConfigFn,
        post_config_fn => PostConfigFn,
        overridden_clientid => OverriddenClientId
    },
    emqx_authn_test_lib:t_clientid_override(TCConfig, Opts),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

cert_path(FileName) ->
    Dir = code:lib_dir(emqx),
    filename:join([Dir, <<"etc/certs">>, FileName]).

raw_http_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"max_inactive">> => <<"10s">>,
        <<"url">> => <<"http://127.0.0.1:32333/auth">>,
        <<"body">> => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

samples() ->
    [
        %% simple get request
        #{
            handler => fun(Req0, State) ->
                #{
                    username := <<"plain">>,
                    password := <<"plain">>
                } = cowboy_req:match_qs([username, password], Req0),

                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% get request with json body response
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{
                        result => allow,
                        is_superuser => true,
                        client_attrs => #{
                            fid => <<"n11">>,
                            <<"#_bad_key">> => <<"v">>
                        }
                    }),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => true, client_attrs => #{<<"fid">> => <<"n11">>}}}
        },

        %% get request with non-utf8 password
        #{
            handler => fun(Req0, State) ->
                #{
                    password := <<255, 255, 255>>
                } = cowboy_req:match_qs([password], Req0),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{
                        result => allow,
                        is_superuser => true,
                        client_attrs => #{}
                    }),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            credentials => #{
                password => <<255, 255, 255>>
            },
            result => {ok, #{is_superuser => true, client_attrs => #{}}}
        },

        %% get request with url-form-encoded body response
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{
                        <<"content-type">> =>
                            <<"application/x-www-form-urlencoded">>
                    },
                    <<"is_superuser=true&result=allow">>,
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => true, client_attrs => #{}}}
        },

        %% get request with response of unknown encoding
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{
                        <<"content-type">> =>
                            <<"test/plain">>
                    },
                    <<"is_superuser=true">>,
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% simple post request, application/json
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = emqx_utils_json:decode(RawBody),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{<<"content-type">> => <<"application/json">>}
            },
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% post request, no content-type header
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = emqx_utils_json:decode(RawBody),
                <<"application/json">> = cowboy_req:header(<<"content-type">>, Req0),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{}
            },
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% simple post request, application/x-www-form-urlencoded
        #{
            handler => fun(Req0, State) ->
                {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = maps:from_list(PostVars),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{
                    <<"content-type">> =>
                        <<"application/x-www-form-urlencoded">>
                }
            },
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% simple post request for placeholders, application/json
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>,
                    <<"clientid">> := <<"clienta">>,
                    <<"peerhost">> := <<"127.0.0.1">>,
                    <<"peerport">> := <<"12345">>,
                    <<"cert_subject">> := <<"cert_subject_data">>,
                    <<"cert_common_name">> := <<"cert_common_name_data">>,
                    <<"cert_pem">> := CertPem,
                    <<"the_group">> := <<"g1">>
                } = emqx_utils_json:decode(RawBody),
                <<"g1">> = cowboy_req:header(<<"the_group">>, Req0),
                <<"fake_raw_cert_to_be_base64_encoded">> = base64:decode(CertPem),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{
                    <<"content-type">> => <<"application/json">>,
                    <<"the_group">> => <<"${client_attrs.group}">>
                },
                <<"body">> => #{
                    <<"clientid">> => ?PH_CLIENTID,
                    <<"username">> => ?PH_USERNAME,
                    <<"password">> => ?PH_PASSWORD,
                    <<"peerhost">> => ?PH_PEERHOST,
                    <<"peerport">> => ?PH_PEERPORT,
                    <<"cert_subject">> => ?PH_CERT_SUBJECT,
                    <<"cert_common_name">> => ?PH_CERT_CN_NAME,
                    <<"cert_pem">> => ?PH_CERT_PEM,
                    <<"the_group">> => <<"${client_attrs.group}">>
                }
            },
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% post request with non-utf8 password, application/json
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{<<"content-type">> => <<"application/json">>},
                <<"body">> => #{
                    <<"password">> => ?PH_PASSWORD
                }
            },
            credentials => #{
                password => <<255, 255, 255>>
            },
            %% non-utf8 password cannot be encoded in json
            result => {error, not_authorized}
        },

        %% post request with non-utf8 password, form urlencoded
        #{
            handler => fun(Req0, State) ->
                {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),
                #{
                    <<"password">> := <<255, 255, 255>>
                } = maps:from_list(PostVars),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{
                    <<"content-type">> =>
                        <<"application/x-www-form-urlencoded">>
                },
                <<"body">> => #{
                    <<"password">> => ?PH_PASSWORD
                }
            },
            credentials => #{
                password => <<255, 255, 255>>
            },
            result => {ok, #{is_superuser => false, client_attrs => #{}}}
        },

        %% custom headers
        #{
            handler => fun(Req0, State) ->
                <<"Test Value">> = cowboy_req:header(<<"x-test-header">>, Req0),
                Req = cowboy_req:reply(200, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% 204 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(204, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false}}
        },

        %% 400 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(400, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% 500 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(500, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% Handling error
        #{
            handler => fun(Req0, State) ->
                error(woops),
                {ok, Req0, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        }
    ].

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(N) when is_integer(N) ->
    integer_to_list(N);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

acl_rules() ->
    [
        #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"pub">>,
            <<"topics">> => [
                <<"http-authn-acl/1">>
            ]
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"sub">>,
            <<"topics">> =>
                [
                    <<"eq http-authn-acl/#">>
                ]
        },
        #{
            <<"permission">> => <<"deny">>,
            <<"action">> => <<"all">>,
            <<"topics">> => [<<"#">>]
        }
    ].
