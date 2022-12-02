%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_dashboard_api_test_helpers, [request/3, uri/1, multipart_formdata_request/3]).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TCP_DEFAULT, 'tcp:default').

-define(assertAuthenticatorsMatch(Guard, Path),
    (fun() ->
        {ok, 200, Response} = request(get, uri(Path)),
        ?assertMatch(Guard, jiffy:decode(Response, [return_maps]))
    end)()
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(t_authenticator_fail, Config) ->
    meck:expect(emqx_authn_proto_v1, lookup_from_all_nodes, 3, [{error, {exception, badarg}}]),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authn_test_lib:delete_authenticators(
        [?CONF_NS_ATOM],
        ?GLOBAL
    ),

    emqx_authn_test_lib:delete_authenticators(
        [listeners, tcp, default, ?CONF_NS_ATOM],
        ?TCP_DEFAULT
    ),

    {atomic, ok} = mria:clear_table(emqx_authn_mnesia),
    Config.

end_per_testcase(t_authenticator_fail, Config) ->
    meck:unload(emqx_authn_proto_v1),
    Config;
end_per_testcase(_, Config) ->
    Config.

init_per_suite(Config) ->
    emqx_config:erase(?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY),
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(
        [emqx_authn, emqx_dashboard],
        fun set_special_configs/1
    ),

    ?AUTHN:delete_chain(?GLOBAL),
    {ok, Chains} = ?AUTHN:list_chains(),
    ?assertEqual(length(Chains), 0),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authn]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_App) ->
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

t_authenticator_users(_) ->
    test_authenticator_users([]).

t_authenticator_user(_) ->
    test_authenticator_user([]).

t_authenticator_position(_) ->
    test_authenticator_position([]).

t_authenticator_import_users(_) ->
    test_authenticator_import_users([]).

t_listener_authenticators(_) ->
    test_authenticators(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator(_) ->
    test_authenticator(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_users(_) ->
    test_authenticator_users(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_user(_) ->
    test_authenticator_user(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_position(_) ->
    test_authenticator_position(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_import_users(_) ->
    test_authenticator_import_users(["listeners", ?TCP_DEFAULT]).

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
    {ok, 200, _} = request(
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
    {ok, RList} = emqx_json:safe_decode(Res),
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
    {ok, 200, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        ValidConfig1
    ),

    ValidConfig2 = ValidConfig0#{pool_size => 9},
    {ok, 200, _} = request(
        put,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"]),
        ValidConfig2
    ),

    {ok, 404, _} = request(
        delete,
        uri(PathPrefix ++ [?CONF_NS, "password_based:redis"])
    ),

    {ok, 204, _} = request(
        delete,
        uri(PathPrefix ++ [?CONF_NS, "password_based:http"])
    ),

    ?assertAuthenticatorsMatch([], PathPrefix ++ [?CONF_NS]).

test_authenticator_users(PathPrefix) ->
    UsersUri = uri(PathPrefix ++ [?CONF_NS, "password_based:built_in_database", "users"]),

    {ok, 200, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        emqx_authn_test_lib:built_in_database_example()
    ),

    {ok, Client} = emqtt:start_link(
        [
            {username, <<"u_event">>},
            {clientid, <<"c_event">>},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 60}}
        ]
    ),

    process_flag(trap_exit, true),
    ?assertMatch({error, _}, emqtt:connect(Client)),
    timer:sleep(300),

    UsersUri0 = uri(PathPrefix ++ [?CONF_NS, "password_based:built_in_database", "status"]),
    {ok, 200, PageData0} = request(get, UsersUri0),
    case PathPrefix of
        [] ->
            #{
                <<"metrics">> := #{
                    <<"total">> := 1,
                    <<"success">> := 0,
                    <<"nomatch">> := 1
                }
            } = jiffy:decode(PageData0, [return_maps]);
        ["listeners", 'tcp:default'] ->
            #{
                <<"metrics">> := #{
                    <<"total">> := 1,
                    <<"success">> := 0,
                    <<"nomatch">> := 1
                }
            } = jiffy:decode(PageData0, [return_maps])
    end,

    InvalidUsers = [
        #{clientid => <<"u1">>, password => <<"p1">>},
        #{user_id => <<"u2">>},
        #{user_id => <<"u3">>, password => <<"p3">>, foobar => <<"foobar">>}
    ],

    lists:foreach(
        fun(User) -> {ok, 400, _} = request(post, UsersUri, User) end,
        InvalidUsers
    ),

    ValidUsers = [
        #{user_id => <<"u1">>, password => <<"p1">>},
        #{user_id => <<"u2">>, password => <<"p2">>, is_superuser => true},
        #{user_id => <<"u3">>, password => <<"p3">>}
    ],

    lists:foreach(
        fun(User) ->
            {ok, 201, UserData} = request(post, UsersUri, User),
            CreatedUser = jiffy:decode(UserData, [return_maps]),
            ?assertMatch(#{<<"user_id">> := _}, CreatedUser)
        end,
        ValidUsers
    ),

    {ok, Client1} = emqtt:start_link(
        [
            {username, <<"u1">>},
            {password, <<"p1">>},
            {clientid, <<"c_event">>},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 60}}
        ]
    ),
    {ok, _} = emqtt:connect(Client1),
    timer:sleep(300),
    UsersUri01 = uri(PathPrefix ++ [?CONF_NS, "password_based:built_in_database", "status"]),
    {ok, 200, PageData01} = request(get, UsersUri01),
    case PathPrefix of
        [] ->
            #{
                <<"metrics">> := #{
                    <<"total">> := 2,
                    <<"success">> := 1,
                    <<"nomatch">> := 1
                }
            } = jiffy:decode(PageData01, [return_maps]);
        ["listeners", 'tcp:default'] ->
            #{
                <<"metrics">> := #{
                    <<"total">> := 2,
                    <<"success">> := 1,
                    <<"nomatch">> := 1
                }
            } = jiffy:decode(PageData01, [return_maps])
    end,

    {ok, 200, Page1Data} = request(get, UsersUri ++ "?page=1&limit=2"),

    #{
        <<"data">> := Page1Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 2,
                <<"count">> := 3
            }
    } =
        jiffy:decode(Page1Data, [return_maps]),

    {ok, 200, Page2Data} = request(get, UsersUri ++ "?page=2&limit=2"),

    #{
        <<"data">> := Page2Users,
        <<"meta">> :=
            #{
                <<"page">> := 2,
                <<"limit">> := 2,
                <<"count">> := 3
            }
    } = jiffy:decode(Page2Data, [return_maps]),

    ?assertEqual(2, length(Page1Users)),
    ?assertEqual(1, length(Page2Users)),

    ?assertEqual(
        [<<"u1">>, <<"u2">>, <<"u3">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Page1Users ++ Page2Users])
    ),

    {ok, 200, Super1Data} = request(get, UsersUri ++ "?page=1&limit=3&is_superuser=true"),

    #{
        <<"data">> := Super1Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 3,
                <<"count">> := 1
            }
    } = jiffy:decode(Super1Data, [return_maps]),

    ?assertEqual(
        [<<"u2">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Super1Users])
    ),

    {ok, 200, Super2Data} = request(get, UsersUri ++ "?page=1&limit=3&is_superuser=false"),

    #{
        <<"data">> := Super2Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 3,
                <<"count">> := 2
            }
    } = jiffy:decode(Super2Data, [return_maps]),

    ?assertEqual(
        [<<"u1">>, <<"u3">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Super2Users])
    ),

    ok.

test_authenticator_user(PathPrefix) ->
    UsersUri = uri(PathPrefix ++ [?CONF_NS, "password_based:built_in_database", "users"]),

    {ok, 200, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        emqx_authn_test_lib:built_in_database_example()
    ),

    User = #{user_id => <<"u1">>, password => <<"p1">>},
    {ok, 201, _} = request(post, UsersUri, User),

    {ok, 404, _} = request(get, UsersUri ++ "/u123"),

    {ok, 409, _} = request(post, UsersUri, User),

    {ok, 200, UserData} = request(get, UsersUri ++ "/u1"),

    FetchedUser = jiffy:decode(UserData, [return_maps]),
    ?assertMatch(#{<<"user_id">> := <<"u1">>}, FetchedUser),
    ?assertNotMatch(#{<<"password">> := _}, FetchedUser),

    ValidUserUpdates = [
        #{password => <<"p1">>},
        #{password => <<"p1">>, is_superuser => true}
    ],

    lists:foreach(
        fun(UserUpdate) -> {ok, 200, _} = request(put, UsersUri ++ "/u1", UserUpdate) end,
        ValidUserUpdates
    ),

    InvalidUserUpdates = [#{user_id => <<"u1">>, password => <<"p1">>}],

    lists:foreach(
        fun(UserUpdate) -> {ok, 400, _} = request(put, UsersUri ++ "/u1", UserUpdate) end,
        InvalidUserUpdates
    ),

    {ok, 404, _} = request(delete, UsersUri ++ "/u123"),
    {ok, 204, _} = request(delete, UsersUri ++ "/u1").

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

test_authenticator_import_users(PathPrefix) ->
    ImportUri = uri(
        PathPrefix ++
            [?CONF_NS, "password_based:built_in_database", "import_users"]
    ),

    {ok, 200, _} = request(
        post,
        uri(PathPrefix ++ [?CONF_NS]),
        emqx_authn_test_lib:built_in_database_example()
    ),

    {ok, 400, _} = multipart_formdata_request(ImportUri, [], []),
    {ok, 400, _} = multipart_formdata_request(ImportUri, [], [
        {filenam, "user-credentials.json", <<>>}
    ]),

    Dir = code:lib_dir(emqx_authn, test),
    JSONFileName = filename:join([Dir, <<"data/user-credentials.json">>]),
    CSVFileName = filename:join([Dir, <<"data/user-credentials.csv">>]),

    {ok, JSONData} = file:read_file(JSONFileName),
    {ok, 204, _} = multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.json", JSONData}
    ]),

    {ok, CSVData} = file:read_file(CSVFileName),
    {ok, 204, _} = multipart_formdata_request(ImportUri, [], [
        {filename, "user-credentials.csv", CSVData}
    ]).

t_switch_to_global_chain(_) ->
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

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).
