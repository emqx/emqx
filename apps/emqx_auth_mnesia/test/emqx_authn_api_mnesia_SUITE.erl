%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_api_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_dashboard_api_test_helpers, [multipart_formdata_request/3]).
-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-define(TCP_DEFAULT, 'tcp:default').
-define(OTHER_NS, <<"some_other_ns">>).

-define(global, global).
-define(ns, ns).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_ctl,
            emqx,
            {emqx_conf,
                "mqtt.client_attrs_init = [{expression = \"user_property.ns\", set_as_attr = tns}]"},
            emqx_auth,
            emqx_auth_mnesia,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    ?AUTHN:delete_chain(?GLOBAL),
    {ok, Chains} = ?AUTHN:list_chains(),
    ?assertEqual(length(Chains), 0),
    ok = emqx_hooks:add(
        'namespace.resource_pre_create',
        {?MODULE, on_namespace_resource_pre_create, []},
        ?HP_HIGHEST
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_group(?global, TCConfig) ->
    AuthHeader = create_superuser(),
    [
        {auth_header, AuthHeader},
        {namespace, ?global_ns}
        | TCConfig
    ];
init_per_group(?ns, TCConfig) ->
    GlobalAuthHeader = create_superuser(),
    Namespace = <<"ns1">>,
    Username = <<"ns_admin">>,
    Password = <<"superSecureP@ss">>,
    AdminRole = <<"ns:", Namespace/binary, "::administrator">>,
    {200, _} = create_user_api(
        #{
            <<"username">> => Username,
            <<"password">> => Password,
            <<"role">> => AdminRole,
            <<"description">> => <<"namespaced person">>
        },
        GlobalAuthHeader
    ),
    {200, #{<<"token">> := Token}} = login(#{
        <<"username">> => Username,
        <<"password">> => Password
    }),
    AuthHeader = bearer_auth_header(Token),
    [
        {auth_header, AuthHeader},
        {namespace, Namespace}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

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

end_per_testcase(_, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

on_namespace_resource_pre_create(#{namespace := _Namespace}, ResCtx) ->
    {stop, ResCtx#{exists := true}}.

put_auth_header(Header) ->
    _ = put({?MODULE, authn}, Header),
    ok.

get_auth_header() ->
    get({?MODULE, authn}).

bearer_auth_header(Token) ->
    {"Authorization", iolist_to_binary(["Bearer ", Token])}.

create_superuser() ->
    emqx_common_test_http:create_default_app(),
    Username = <<"superuser">>,
    Password = <<"secretP@ss1">>,
    AdminRole = <<"administrator">>,
    case emqx_dashboard_admin:add_user(Username, Password, AdminRole, <<"desc">>) of
        {ok, _} ->
            ok;
        {error, <<"username_already_exists">>} ->
            ok
    end,
    {200, #{<<"token">> := Token}} = login(#{<<"username">> => Username, <<"password">> => Password}),
    {"Authorization", iolist_to_binary(["Bearer ", Token])}.

login(Params) ->
    URL = emqx_mgmt_api_test_util:api_path(["login"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => [{"no", "auth"}],
        method => post,
        url => URL,
        body => Params
    }).

create_user_api(Params, AuthHeader) ->
    URL = emqx_mgmt_api_test_util:api_path(["users"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Params
    }).

request(Method, Url) ->
    request(Method, Url, []).

ns_of(TCConfig) ->
    proplists:get_value(namespace, TCConfig, ?global_ns).

maybe_add_ns_qs(QueryParams, TCConfig) ->
    case ns_of(TCConfig) of
        ?global_ns -> QueryParams;
        Namespace when is_binary(Namespace) -> QueryParams#{<<"ns">> => Namespace}
    end.

maybe_add_ns_user_info(UserInfo, TCConfig) ->
    case ns_of(TCConfig) of
        ?global_ns -> UserInfo;
        Namespace when is_binary(Namespace) -> UserInfo#{<<"namespace">> => Namespace}
    end.

create_authenticator(Params) ->
    URL = uri([?CONF_NS]),
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => URL,
        body => Params
    }).

get_authenticator_status(QueryParams) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "status"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => get,
        url => URL,
        query_params => QueryParams
    }).

add_user(Params) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "users"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => post,
        url => URL,
        body => Params
    }).

get_user(UserId, QueryParams) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "users", UserId]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => get,
        url => URL,
        query_params => QueryParams
    }).

update_user(UserId, Params, QueryParams) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "users", UserId]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => put,
        url => URL,
        body => Params,
        query_params => QueryParams
    }).

delete_user(UserId, QueryParams) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "users", UserId]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => delete,
        url => URL,
        query_params => QueryParams
    }).

list_users(QueryParams) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "users"]),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => get,
        url => URL,
        query_params => QueryParams
    }).

import_users_multipart(Params) ->
    #{files := Files} = Params,
    QueryParams = maps:get(query_params, Params, #{}),
    URL = uri([?CONF_NS, "password_based:built_in_database", "import_users"]),
    Res = emqx_mgmt_api_test_util:upload_request(#{
        url => URL,
        files => Files,
        mime_type => <<"application/octet-stream">>,
        other_params => [],
        auth_token => get_auth_header(),
        method => post,
        query_params => QueryParams
    }),
    emqx_mgmt_api_test_util:simplify_decode_result(Res).

import_users(Params, QueryParams) ->
    import_users(Params, QueryParams, _Opts = #{}).

import_users(Params, QueryParams, Opts) ->
    URL = uri([?CONF_NS, "password_based:built_in_database", "import_users"]),
    ContentType = maps:get(content_type, Opts, "application/json"),
    emqx_mgmt_api_test_util:simple_request(#{
        auth_header => get_auth_header(),
        method => post,
        url => URL,
        body => Params,
        query_params => QueryParams,
        'content-type' => ContentType
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_authenticator_users() ->
    [{matrix, true}].
t_authenticator_users(matrix) ->
    [[?global], [?ns]];
t_authenticator_users(TCConfig) ->
    {200, _} = create_authenticator(emqx_authn_test_lib:built_in_database_example()),

    WithNsClientOpts = fun(Opts) ->
        case ns_of(TCConfig) of
            ?global_ns ->
                Opts;
            Namespace when is_binary(Namespace) ->
                emqx_utils_maps:deep_put(
                    [properties, 'User-Property'],
                    Opts,
                    [{<<"ns">>, Namespace}]
                )
        end
    end,

    {ok, Client} = emqtt:start_link(
        WithNsClientOpts(#{
            username => <<"u_event">>,
            clientid => <<"c_event">>,
            proto_ver => v5,
            properties => #{'Session-Expiry-Interval' => 60}
        })
    ),

    process_flag(trap_exit, true),
    ?assertMatch({error, _}, emqtt:connect(Client)),
    timer:sleep(300),

    %% Using either global or namespaced admin user.
    put_auth_header(?config(auth_header, TCConfig)),

    {200, PageData0} = get_authenticator_status(#{}),
    #{
        <<"metrics">> := #{
            <<"total">> := 1,
            <<"success">> := 0,
            <<"failed">> := 1
        }
    } = PageData0,

    InvalidUsers0 = [
        #{clientid => <<"u1">>, password => <<"p1">>},
        #{user_id => <<"u2">>},
        #{user_id => <<"u3">>, password => <<"p3">>, foobar => <<"foobar">>},
        %% Empty username
        #{user_id => <<"">>, password => <<"p3">>}
    ],
    InvalidUsers = lists:map(fun(U) -> maybe_add_ns_user_info(U, TCConfig) end, InvalidUsers0),

    lists:foreach(
        fun(User) -> {400, _} = add_user(User) end,
        InvalidUsers
    ),

    ValidUsers0 = [
        #{user_id => <<"u1">>, password => <<"p1">>},
        #{user_id => <<"u2">>, password => <<"p2">>, is_superuser => true},
        #{user_id => <<"u3">>, password => <<"p3">>}
    ],
    ValidUsers = lists:map(fun(U) -> maybe_add_ns_user_info(U, TCConfig) end, ValidUsers0),

    NsAPIOut =
        case ns_of(TCConfig) of
            ?global_ns -> null;
            Ns -> Ns
        end,
    lists:foreach(
        fun(User) ->
            {201, CreatedUser} = add_user(User),
            ?assertMatch(
                #{
                    <<"user_id">> := _,
                    <<"namespace">> := NsAPIOut
                },
                CreatedUser,
                #{expected_ns => NsAPIOut}
            )
        end,
        ValidUsers
    ),

    {ok, Client1} = emqtt:start_link(
        WithNsClientOpts(#{
            username => <<"u1">>,
            password => <<"p1">>,
            clientid => <<"c_event">>,
            proto_ver => v5,
            properties => #{'Session-Expiry-Interval' => 60}
        })
    ),
    {ok, _} = emqtt:connect(Client1),
    timer:sleep(300),
    {200, PageData01} = get_authenticator_status(#{}),
    #{
        <<"metrics">> := #{
            <<"total">> := 2,
            <<"success">> := 1,
            <<"failed">> := 1
        }
    } = PageData01,

    {200, Page1Data} = list_users(
        maybe_add_ns_qs(
            #{
                <<"page">> => <<"1">>,
                <<"limit">> => <<"2">>
            },
            TCConfig
        )
    ),

    #{
        <<"data">> := Page1Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 2,
                <<"count">> := 3
            }
    } = Page1Data,

    {200, Page2Data} = list_users(
        maybe_add_ns_qs(
            #{
                <<"page">> => <<"2">>,
                <<"limit">> => <<"2">>
            },
            TCConfig
        )
    ),

    #{
        <<"data">> := Page2Users,
        <<"meta">> :=
            #{
                <<"page">> := 2,
                <<"limit">> := 2,
                <<"count">> := 3
            }
    } = Page2Data,

    ?assertEqual(2, length(Page1Users)),
    ?assertEqual(1, length(Page2Users)),

    ?assertEqual(
        [<<"u1">>, <<"u2">>, <<"u3">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Page1Users ++ Page2Users])
    ),

    {200, Super1Data} = list_users(
        maybe_add_ns_qs(
            #{
                <<"page">> => <<"1">>,
                <<"limit">> => <<"3">>,
                <<"is_superuser">> => <<"true">>
            },
            TCConfig
        )
    ),

    #{
        <<"data">> := Super1Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 3,
                <<"count">> := 1
            }
    } = Super1Data,

    ?assertEqual(
        [<<"u2">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Super1Users])
    ),

    {200, Super2Data} = list_users(
        maybe_add_ns_qs(
            #{
                <<"page">> => <<"1">>,
                <<"limit">> => <<"3">>,
                <<"is_superuser">> => <<"false">>
            },
            TCConfig
        )
    ),

    #{
        <<"data">> := Super2Users,
        <<"meta">> :=
            #{
                <<"page">> := 1,
                <<"limit">> := 3,
                <<"count">> := 2
            }
    } = Super2Data,

    ?assertEqual(
        [<<"u1">>, <<"u3">>],
        lists:usort([UserId || #{<<"user_id">> := UserId} <- Super2Users])
    ),

    %% Global admin can manage other namespaces.

    %% N.B.: same username as existing user in different namespace
    OtherNsUser = #{
        <<"user_id">> => <<"u1">>, <<"password">> => <<"p1">>, <<"namespace">> => ?OTHER_NS
    },
    maybe
        true ?= ns_of(TCConfig) == ?global_ns,
        ?assertMatch({200, #{<<"data">> := []}}, list_users(#{<<"ns">> => ?OTHER_NS})),
        ?assertMatch({201, #{<<"namespace">> := ?OTHER_NS}}, add_user(OtherNsUser)),
        ?assertMatch({200, #{<<"data">> := [_]}}, list_users(#{<<"ns">> => ?OTHER_NS})),
        ok
    end,

    %% Namespaced admin can only manage their own namespace.
    maybe
        true ?= ns_of(TCConfig) /= ?global_ns,
        ?assertMatch({403, _}, list_users(#{<<"ns">> => ?OTHER_NS})),
        ?assertMatch({403, _}, add_user(OtherNsUser)),
        ok
    end,

    ok.

t_authenticator_user() ->
    [{matrix, true}].
t_authenticator_user(matrix) ->
    [[?global], [?ns]];
t_authenticator_user(TCConfig) ->
    {200, _} = create_authenticator(emqx_authn_test_lib:built_in_database_example()),
    put_auth_header(?config(auth_header, TCConfig)),

    User = maybe_add_ns_user_info(#{user_id => <<"u1">>, password => <<"p1">>}, TCConfig),
    {201, _} = add_user(User),

    {404, _} = get_user(<<"u123">>, maybe_add_ns_qs(#{}, TCConfig)),

    {409, _} = add_user(User),

    {200, FetchedUser} = get_user(<<"u1">>, maybe_add_ns_qs(#{}, TCConfig)),

    ?assertMatch(#{<<"user_id">> := <<"u1">>}, FetchedUser),
    ?assertNotMatch(#{<<"password">> := _}, FetchedUser),

    ValidUserUpdates = [
        #{password => <<"p1">>},
        #{password => <<"p1">>, is_superuser => true}
    ],

    lists:foreach(
        fun(UserUpdate) ->
            {200, _} = update_user(<<"u1">>, UserUpdate, maybe_add_ns_qs(#{}, TCConfig))
        end,
        ValidUserUpdates
    ),

    InvalidUserUpdates = [
        #{user_id => <<"u1">>, password => <<"p1">>},
        #{namespace => ?OTHER_NS, password => <<"p1">>}
    ],

    lists:foreach(
        fun(UserUpdate) ->
            {400, _} = update_user(<<"u1">>, UserUpdate, maybe_add_ns_qs(#{}, TCConfig))
        end,
        InvalidUserUpdates
    ),

    {404, _} = delete_user(<<"u123">>, maybe_add_ns_qs(#{}, TCConfig)),
    {204, _} = delete_user(<<"u1">>, maybe_add_ns_qs(#{}, TCConfig)),

    %% N.B.: same username as existing user in different namespace
    OtherNsUser = #{
        <<"user_id">> => <<"u1">>, <<"password">> => <<"p1">>, <<"namespace">> => ?OTHER_NS
    },
    UpdateParams = #{<<"password">> => <<"p2">>},
    maybe
        true ?= ns_of(TCConfig) == ?global_ns,
        ?assertMatch({201, #{<<"namespace">> := ?OTHER_NS}}, add_user(OtherNsUser)),
        ?assertMatch({200, _}, update_user(<<"u1">>, UpdateParams, #{<<"ns">> => ?OTHER_NS})),
        ?assertMatch({204, _}, delete_user(<<"u1">>, #{<<"ns">> => ?OTHER_NS})),
        ?assertMatch({200, #{<<"data">> := []}}, list_users(#{<<"ns">> => ?OTHER_NS})),
        ok
    end,

    %% Namespaced admin can only manage their own namespace.
    maybe
        true ?= ns_of(TCConfig) /= ?global_ns,
        ?assertMatch({403, _}, update_user(<<"u1">>, UpdateParams, #{<<"ns">> => ?OTHER_NS})),
        ?assertMatch({403, _}, delete_user(<<"u1">>, #{<<"ns">> => ?OTHER_NS})),
        ok
    end,

    ok.

t_authenticator_import_users_global() ->
    [{matrix, true}].
t_authenticator_import_users_global(matrix) ->
    [[?global]];
t_authenticator_import_users_global(TCConfig) ->
    {200, _} = create_authenticator(emqx_authn_test_lib:built_in_database_example()),

    put_auth_header(?config(auth_header, TCConfig)),

    {400, _} = import_users_multipart(#{files => []}),
    {400, _} = import_users_multipart(#{
        files => [
            %% problem is either bad parameter name or empty file?
            {<<"filenam">>, <<"user-credentials.json">>, <<>>}
        ]
    }),

    Dir = code:lib_dir(emqx_auth),
    JSONFileName = filename:join([Dir, <<"test/data/user-credentials.json">>]),
    CSVFileName = filename:join([Dir, <<"test/data/user-credentials.csv">>]),

    {ok, JSONData} = file:read_file(JSONFileName),
    {200, Result1} = import_users_multipart(#{
        files => [
            {<<"filename">>, <<"user-credentials.json">>, JSONData}
        ]
    }),
    ?assertMatch(#{<<"total">> := 2, <<"success">> := 2}, Result1),

    {ok, CSVData} = file:read_file(CSVFileName),
    {200, Result2} = import_users_multipart(#{
        files => [
            {<<"filename">>, <<"user-credentials.csv">>, CSVData}
        ]
    }),
    ?assertMatch(#{<<"total">> := 2, <<"success">> := 2}, Result2),

    %% test application/json
    {200, _} = import_users(emqx_utils_json:decode(JSONData), #{<<"type">> => <<"hash">>}),
    {ok, JSONData1} = file:read_file(
        filename:join([Dir, <<"test/data/user-credentials-plain.json">>])
    ),
    {200, _} = import_users(emqx_utils_json:decode(JSONData1), #{<<"type">> => <<"plain">>}),

    %% test application/json; charset=utf-8
    {200, _} = import_users({raw, JSONData1}, #{<<"type">> => <<"plain">>}, #{
        content_type => "application/json; charset=utf-8"
    }),
    ok.

%% Each imported user contains its namespace.  So, we leave importing multiple users to a
%% global admin only.
t_authenticator_import_users_ns() ->
    [{matrix, true}].
t_authenticator_import_users_ns(matrix) ->
    [[?ns]];
t_authenticator_import_users_ns(TCConfig) ->
    {200, _} = create_authenticator(emqx_authn_test_lib:built_in_database_example()),
    put_auth_header(?config(auth_header, TCConfig)),
    put_auth_header(?config(auth_header, TCConfig)),

    Dir = code:lib_dir(emqx_auth),
    JSONFileName = filename:join([Dir, <<"test/data/user-credentials.json">>]),
    {ok, JSONData} = file:read_file(JSONFileName),
    {403, _} = import_users_multipart(#{
        files => [
            {<<"filename">>, <<"user-credentials.json">>, JSONData}
        ]
    }),
    {403, _} = import_users(emqx_utils_json:decode(JSONData), #{<<"type">> => <<"hash">>}),

    ok.
