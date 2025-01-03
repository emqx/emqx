%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TCP_DEFAULT, 'tcp:default').

-define(assertAuthenticatorsMatch(Guard, Path),
    (fun() ->
        {ok, 200, Response} = request(get, uri(Path)),
        ?assertMatch(Guard, emqx_utils_json:decode(Response, [return_maps]))
    end)()
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(t_authenticator_fail, Config) ->
    meck:expect(emqx_authn_proto_v1, lookup_from_all_nodes, 3, [{error, {exception, badarg}}]),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [?CONF_NS_ATOM],
        ?GLOBAL
    ),

    emqx_authn_test_lib:delete_authenticators(
        [listeners, tcp, default, ?CONF_NS_ATOM],
        ?TCP_DEFAULT
    ),
    Config.

end_per_testcase(t_authenticator_fail, Config) ->
    meck:unload(emqx_authn_proto_v1),
    Config;
end_per_testcase(_, Config) ->
    Config.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_auth,
            %% to load schema
            {emqx_auth_mnesia, #{start => false}},
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),
    _ = emqx_common_test_http:create_default_app(),
    ok = emqx_authn_test_lib:register_fake_providers([
        {password_based, built_in_database},
        {password_based, redis},
        {password_based, http},
        jwt
    ]),
    ?AUTHN:delete_chain(?GLOBAL),
    {ok, Chains} = ?AUTHN:list_chains(),
    ?assertEqual(length(Chains), 0),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    _ = emqx_common_test_http:delete_default_app(),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_invalid_listener(_) ->
    {ok, 404, _} = request(get, uri(["listeners", "invalid", ?CONF_NS])),
    {ok, 404, _} = request(get, uri(["listeners", "in:valid", ?CONF_NS])).

t_authenticators(_) ->
    test_authenticators([]).

t_authenticator(_) ->
    test_authenticator([]).

t_authenticator_fail(_) ->
    ValidConfig0 = emqx_authn_test_lib:http_example(),
    {ok, 200, _} = request(
        post,
        uri([?CONF_NS]),
        ValidConfig0
    ),
    ?assertMatch(
        {ok, 500, _},
        request(
            get,
            uri([?CONF_NS, "password_based:http", "status"])
        )
    ).

t_authenticator_position(_) ->
    test_authenticator_position([]).

t_authenticators_reorder(_) ->
    AuthenticatorConfs = [
        emqx_authn_test_lib:http_example(),
        %% Disabling an authenticator must not affect the requested order
        (emqx_authn_test_lib:jwt_example())#{enable => false},
        emqx_authn_test_lib:built_in_database_example()
    ],
    lists:foreach(
        fun(Conf) ->
            {ok, 200, _} = request(
                post,
                uri([?CONF_NS]),
                Conf
            )
        end,
        AuthenticatorConfs
    ),
    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"jwt">>, <<"enable">> := false},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>}
        ],
        [?CONF_NS]
    ),

    OrderUri = uri([?CONF_NS, "order"]),

    %% Invalid moves

    %% Bad schema
    {ok, 400, _} = request(
        put,
        OrderUri,
        [
            #{<<"not-id">> => <<"password_based:http">>},
            #{<<"not-id">> => <<"jwt">>}
        ]
    ),

    %% Partial order
    {ok, 400, _} = request(
        put,
        OrderUri,
        [
            #{<<"id">> => <<"password_based:http">>},
            #{<<"id">> => <<"jwt">>}
        ]
    ),

    %% Not found authenticators
    {ok, 400, _} = request(
        put,
        OrderUri,
        [
            #{<<"id">> => <<"password_based:http">>},
            #{<<"id">> => <<"jwt">>},
            #{<<"id">> => <<"password_based:built_in_database">>},
            #{<<"id">> => <<"password_based:mongodb">>}
        ]
    ),

    %% Both partial and not found errors
    {ok, 400, _} = request(
        put,
        OrderUri,
        [
            #{<<"id">> => <<"password_based:http">>},
            #{<<"id">> => <<"password_based:built_in_database">>},
            #{<<"id">> => <<"password_based:mongodb">>}
        ]
    ),

    %% Duplicates
    {ok, 400, _} = request(
        put,
        OrderUri,
        [
            #{<<"id">> => <<"password_based:http">>},
            #{<<"id">> => <<"password_based:built_in_database">>},
            #{<<"id">> => <<"jwt">>},
            #{<<"id">> => <<"password_based:http">>}
        ]
    ),

    %% Valid moves
    {ok, 204, _} = request(
        put,
        OrderUri,
        [
            #{<<"id">> => <<"password_based:built_in_database">>},
            #{<<"id">> => <<"jwt">>},
            #{<<"id">> => <<"password_based:http">>}
        ]
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>},
            #{<<"mechanism">> := <<"jwt">>, <<"enable">> := false},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>}
        ],
        [?CONF_NS]
    ).

%t_listener_authenticators(_) ->
%    test_authenticators(["listeners", ?TCP_DEFAULT]).

%t_listener_authenticator(_) ->
%    test_authenticator(["listeners", ?TCP_DEFAULT]).

%t_listener_authenticator_position(_) ->
%    test_authenticator_position(["listeners", ?TCP_DEFAULT]).

t_aggregate_metrics(_) ->
    Metrics = #{
        'emqx@node1.emqx.io' => #{
            metrics =>
                #{
                    failed => 0,
                    total => 1,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.1,
                    success => 1,
                    nomatch => 1
                }
        },
        'emqx@node2.emqx.io' => #{
            metrics =>
                #{
                    failed => 0,
                    total => 1,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.1,
                    success => 1,
                    nomatch => 2
                }
        }
    },
    Res = emqx_authn_api:aggregate_metrics(maps:values(Metrics)),
    ?assertEqual(
        #{
            metrics =>
                #{
                    failed => 0,
                    total => 2,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.2,
                    success => 2,
                    nomatch => 3
                }
        },
        Res
    ).

test_authenticators(PathPrefix) ->
    ValidConfig = emqx_authn_test_lib:http_example(),
    {ok, 200, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        ValidConfig
    ),

    {ok, 409, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        ValidConfig
    ),

    InvalidConfig0 = ValidConfig#{method => <<"delete">>},
    {ok, 400, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        InvalidConfig0
    ),

    ValidConfig1 = ValidConfig#{
        method => <<"get">>,
        headers => #{<<"content-type">> => <<"application/json">>}
    },
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        ValidConfig1
    ),

    ?assertAuthenticatorsMatch(
        [#{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>}],
        PathPrefix ++ [?CONF_NS]
    ).

test_authenticator(PathPrefix) ->
    ValidConfig0 = emqx_authn_test_lib:http_example(),
    {ok, 200, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        ValidConfig0
    ),
    {ok, 200, _} = request(
        get,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"])
    ),

    {ok, 200, Res} = request(
        get,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http", "status"])
    ),
    {ok, RList} = emqx_utils_json:safe_decode(Res),
    Snd = fun({_, Val}) -> Val end,
    LookupVal = fun LookupV(List, RestJson) ->
        case List of
            [Name] -> Snd(lists:keyfind(Name, 1, RestJson));
            [Name | NS] -> LookupV(NS, Snd(lists:keyfind(Name, 1, RestJson)))
        end
    end,
    LookFun = fun(List) -> LookupVal(List, RList) end,
    MetricsList = [
        {<<"failed">>, 0},
        {<<"total">>, 0},
        {<<"rate">>, 0.0},
        {<<"rate_last5m">>, 0.0},
        {<<"rate_max">>, 0.0},
        {<<"success">>, 0}
    ],
    EqualFun = fun({M, V}) ->
        ?assertEqual(
            V,
            LookFun([
                <<"metrics">>,
                M
            ])
        )
    end,
    lists:map(EqualFun, MetricsList),
    ?assertEqual(
        <<"connected">>,
        LookFun([<<"status">>])
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            get,
            uri(PathPrefix ++ [?CONF_NS, "unknown_auth_chain", "status"])
        )
    ),

    {ok, 404, _} = request(
        get,
        uri(PathPrefix ++ [?CONF_NS, "password_based:redis"])
    ),

    {ok, 404, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:built_in_database"]),
        emqx_authn_test_lib:built_in_database_example()
    ),

    InvalidConfig0 = ValidConfig0#{method => <<"delete">>},
    {ok, 400, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        InvalidConfig0
    ),

    ValidConfig1 = ValidConfig0#{
        method => <<"get">>,
        headers => #{<<"content-type">> => <<"application/json">>}
    },
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        ValidConfig1
    ),

    ValidConfig2 = ValidConfig0#{pool_size => 9},
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        ValidConfig2
    ),

    %% allow deletion of unknown (not created) authenticator
    {ok, 204, _} = request(
        delete,
        uri(PathPrefix ++ [?CONF_NS, "password_based:redis"])
    ),

    {ok, 204, _} = request(
        delete,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"])
    ),

    ?assertAuthenticatorsMatch([], PathPrefix ++ [?CONF_NS]).

test_authenticator_position(PathPrefix) ->
    AuthenticatorConfs = [
        emqx_authn_test_lib:http_example(),
        emqx_authn_test_lib:jwt_example(),
        emqx_authn_test_lib:built_in_database_example()
    ],

    lists:foreach(
        fun(Conf) ->
            {ok, 200, _} = request(
                post,
                uri(PathPrefix ++ [?CONF_NS]),
                Conf
            )
        end,
        AuthenticatorConfs
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"jwt">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>}
        ],
        PathPrefix ++ [?CONF_NS]
    ),

    %% Invalid moves

    {ok, 400, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "up"])
    ),

    {ok, 404, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position"])
    ),

    {ok, 404, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "before:invalid"])
    ),

    {ok, 404, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "before:password_based:redis"])
    ),

    %% Valid moves

    %% test front
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "front"])
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"jwt">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>}
        ],
        PathPrefix ++ [?CONF_NS]
    ),

    %% test rear
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "rear"])
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>},
            #{<<"mechanism">> := <<"jwt">>}
        ],
        PathPrefix ++ [?CONF_NS]
    ),

    %% test before
    {ok, 204, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "jwt", "position", "before:password_based:built_in_database"])
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"jwt">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>}
        ],
        PathPrefix ++ [?CONF_NS]
    ),

    %% test after
    {ok, 204, _} = request(
        put,
        uri(
            PathPrefix ++
                [
                    ?CONF_NS,
                    "password_based%3Abuilt_in_database",
                    "position",
                    "after:password_based:http"
                ]
        )
    ),

    ?assertAuthenticatorsMatch(
        [
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"http">>},
            #{<<"mechanism">> := <<"password_based">>, <<"backend">> := <<"built_in_database">>},
            #{<<"mechanism">> := <<"jwt">>}
        ],
        PathPrefix ++ [?CONF_NS]
    ).

t_authenticator_users_not_found(_) ->
    GlobalUser = #{user_id => <<"global_user">>, password => <<"p1">>},
    {ok, 404, _} = request(
        get,
        uri([?CONF_NS, "password_based:built_in_database", "users"])
    ),
    {ok, 404, _} = request(
        post,
        uri([?CONF_NS, "password_based:built_in_database", "users"]),
        GlobalUser
    ),
    ok.

%% listener authn api is not supported since 5.1.0
%% Don't support listener switch to global chain.
ignore_switch_to_global_chain(_) ->
    {ok, 200, _} = request(
        post,
        uri([?CONF_NS]),
        emqx_authn_test_lib:built_in_database_example()
    ),

    {ok, 200, _} = request(
        post,
        uri([listeners, "tcp:default", ?CONF_NS]),
        emqx_authn_test_lib:built_in_database_example()
    ),
    {ok, 200, _} = request(
        post,
        uri([listeners, "tcp:default", ?CONF_NS]),
        maps:put(enable, false, emqx_authn_test_lib:http_example())
    ),

    GlobalUser = #{user_id => <<"global_user">>, password => <<"p1">>},

    {ok, 201, _} = request(
        post,
        uri([?CONF_NS, "password_based:built_in_database", "users"]),
        GlobalUser
    ),

    ListenerUser = #{user_id => <<"listener_user">>, password => <<"p1">>},

    {ok, 201, _} = request(
        post,
        uri([listeners, "tcp:default", ?CONF_NS, "password_based:built_in_database", "users"]),
        ListenerUser
    ),

    process_flag(trap_exit, true),

    %% Listener user should be OK
    {ok, Client0} = emqtt:start_link([
        {username, <<"listener_user">>},
        {password, <<"p1">>}
    ]),
    ?assertMatch(
        {ok, _},
        emqtt:connect(Client0)
    ),
    ok = emqtt:disconnect(Client0),

    %% Global user should not be OK
    {ok, Client1} = emqtt:start_link([
        {username, <<"global_user">>},
        {password, <<"p1">>}
    ]),
    ?assertMatch(
        {error, {unauthorized_client, _}},
        emqtt:connect(Client1)
    ),

    {ok, 204, _} = request(
        delete,
        uri([listeners, "tcp:default", ?CONF_NS, "password_based:built_in_database"])
    ),

    %% Now listener has only disabled authenticators, should allow anonymous access
    {ok, Client2} = emqtt:start_link([
        {username, <<"any_user">>},
        {password, <<"any_password">>}
    ]),
    ?assertMatch(
        {ok, _},
        emqtt:connect(Client2)
    ),
    ok = emqtt:disconnect(Client2),

    {ok, 204, _} = request(
        delete,
        uri([listeners, "tcp:default", ?CONF_NS, "password_based:http"])
    ),
    %% Delete unknown should retun 204
    %% There is not even a name validation for it.
    {ok, 204, _} = request(
        delete,
        uri([listeners, "tcp:default", ?CONF_NS, "password_based:httpx"])
    ),

    %% Local chain is empty now and should be removed
    %% Listener user should not be OK
    {ok, Client3} = emqtt:start_link([
        {username, <<"listener_user">>},
        {password, <<"p1">>}
    ]),
    ?assertMatch(
        {error, {unauthorized_client, _}},
        emqtt:connect(Client3)
    ),

    %% Global user should be now OK, switched back to the global chain
    {ok, Client4} = emqtt:start_link([
        {username, <<"global_user">>},
        {password, <<"p1">>}
    ]),
    ?assertMatch(
        {ok, _},
        emqtt:connect(Client4)
    ),
    ok = emqtt:disconnect(Client4).

t_bcrypt_validation(_Config) ->
    BaseConf = #{
        mechanism => <<"password_based">>,
        backend => <<"built_in_database">>,
        user_id_type => <<"username">>
    },
    BcryptValid = #{
        name => <<"bcrypt">>,
        salt_rounds => 10
    },
    BcryptInvalid = #{
        name => <<"bcrypt">>,
        salt_rounds => 15
    },

    ConfValid = BaseConf#{password_hash_algorithm => BcryptValid},
    ConfInvalid = BaseConf#{password_hash_algorithm => BcryptInvalid},

    {ok, 400, _} = request(
        post,
        uri([?CONF_NS]),
        ConfInvalid
    ),

    {ok, 200, _} = request(
        post,
        uri([?CONF_NS]),
        ConfValid
    ).

t_cache(_Config) ->
    {ok, 200, CacheData0} = request(
        get,
        uri(["authentication_cache"])
    ),
    ?assertMatch(
        #{<<"enable">> := false},
        emqx_utils_json:decode(CacheData0, [return_maps])
    ),
    {ok, 200, MetricsData0} = request(
        get,
        uri(["authentication_cache", "status"])
    ),
    ?assertMatch(
        #{<<"metrics">> := #{<<"count">> := 0}},
        emqx_utils_json:decode(MetricsData0, [return_maps])
    ),
    {ok, 204, _} = request(
        put,
        uri(["authentication_cache"]),
        #{
            <<"enable">> => true
        }
    ),
    {ok, 200, CacheData1} = request(
        get,
        uri(["authentication_cache"])
    ),
    ?assertMatch(
        #{<<"enable">> := true},
        emqx_utils_json:decode(CacheData1, [return_maps])
    ),

    %% We enabled authn cache, let's create
    %% * authenticator
    %% * connection to miss the cache
    {ok, 200, _} = request(
        post,
        uri([?CONF_NS]),
        emqx_authn_test_lib:http_example()
    ),

    process_flag(trap_exit, true),
    {ok, Client} = emqtt:start_link([
        {username, <<"user">>},
        {password, <<"pass">>}
    ]),
    _ = emqtt:connect(Client),

    %% Now check the metrics, the cache should have been populated
    {ok, 200, MetricsData2} = request(
        get,
        uri(["authentication_cache", "status"])
    ),
    ?assertMatch(
        #{<<"metrics">> := #{<<"misses">> := #{<<"value">> := 1}}},
        emqx_utils_json:decode(MetricsData2, [return_maps])
    ),
    ok.

t_cache_reset(_) ->
    {ok, 204, _} = request(
        post,
        uri(["authentication_cache", "reset"])
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).
