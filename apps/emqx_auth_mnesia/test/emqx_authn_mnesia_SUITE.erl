%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(AUTHN_ID, <<"mechanism:backend">>).
-define(NS, <<"some_ns">>).
-define(OTHER_NS, <<"some_other_ns">>).

-define(global, global).
-define(ns, ns).

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_mnesia], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_group(?global, Config) ->
    [{ns, ?global_ns} | Config];
init_per_group(?ns, Config) ->
    [{ns, ?NS} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    mria:clear_table(emqx_authn_mnesia),
    mria:clear_table(emqx_authn_mnesia_ns),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_) ->
    Config0 = config(),

    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config1),
    ok.

t_bootstrap_file(_) ->
    Config = config(),
    %% hash to hash
    HashConfig = Config#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser1">>, is_superuser := true},
            #{namespace := ?global_ns, user_id := <<"myuser2">>, is_superuser := false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials.json">>)
    ),
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser3">>, is_superuser := true},
            #{namespace := ?global_ns, user_id := <<"myuser4">>, is_superuser := false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials.csv">>)
    ),
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser5">>, is_superuser := true},
            #{namespace := <<"ns1">>, user_id := <<"myuser6">>, is_superuser := false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials-ns.csv">>)
    ),
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser7">>, is_superuser := true},
            #{namespace := <<"ns1">>, user_id := <<"myuser8">>, is_superuser := false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials-ns.json">>)
    ),

    %% plain to plain
    PlainConfig = Config#{
        password_hash_algorithm =>
            #{name => plain, salt_position => disable}
    },
    ?assertMatch(
        [
            #{
                namespace := ?global_ns,
                user_id := <<"myuser1">>,
                is_superuser := true,
                password_hash := <<"password1">>
            },
            #{
                namespace := ?global_ns,
                user_id := <<"myuser2">>,
                is_superuser := false,
                password_hash := <<"password2">>
            }
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain.json">>)
    ),
    ?assertMatch(
        [
            #{
                namespace := ?global_ns,
                user_id := <<"myuser1">>,
                is_superuser := true,
                password_hash := <<"password1">>
            },
            #{
                namespace := <<"ns1">>,
                user_id := <<"myuser2">>,
                is_superuser := false,
                password_hash := <<"password2">>
            }
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain-ns.json">>)
    ),
    ?assertMatch(
        [
            #{
                namespace := ?global_ns,
                user_id := <<"myuser3">>,
                is_superuser := true,
                password_hash := <<"password3">>
            },
            #{
                namespace := ?global_ns,
                user_id := <<"myuser4">>,
                is_superuser := false,
                password_hash := <<"password4">>
            }
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain.csv">>)
    ),
    ?assertMatch(
        [
            #{
                namespace := ?global_ns,
                user_id := <<"myuser3">>,
                is_superuser := true,
                password_hash := <<"password3">>
            },
            #{
                namespace := <<"ns1">>,
                user_id := <<"myuser4">>,
                is_superuser := false,
                password_hash := <<"password4">>
            }
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain-ns.csv">>)
    ),
    %% plain to hash
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser1">>, is_superuser := true},
            #{namespace := ?global_ns, user_id := <<"myuser2">>, is_superuser := false}
        ],
        test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain.json">>)
    ),
    Opts = #{clean => false},
    Result = test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain.csv">>, Opts),
    ?assertMatch(
        [
            #{namespace := ?global_ns, user_id := <<"myuser3">>, is_superuser := true},
            #{namespace := ?global_ns, user_id := <<"myuser4">>, is_superuser := false}
        ],
        Result
    ),
    %% Don't override the exist user id.
    ?assertMatch(
        Result, test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain_v2.csv">>)
    ),
    ok.

test_bootstrap_file(Config0, Type, File) ->
    test_bootstrap_file(Config0, Type, File, #{clean => true}).

test_bootstrap_file(Config0, Type, File, Opts) ->
    {Type, Filename, _FileData} = sample_filename_and_data(Type, File),
    Config2 = Config0#{
        bootstrap_file => Filename,
        bootstrap_type => Type
    },
    {ok, State0} = emqx_authn_mnesia:create(?AUTHN_ID, Config2),
    Result = read_tables(),
    case maps:get(clean, Opts) of
        true ->
            ok = emqx_authn_mnesia:destroy(State0),
            ?assertMatch([], ets:tab2list(emqx_authn_mnesia)),
            ?assertMatch([], ets:tab2list(emqx_authn_mnesia_ns));
        _ ->
            ok
    end,
    Result.

read_tables() ->
    Rows = ets:tab2list(emqx_authn_mnesia) ++ ets:tab2list(emqx_authn_mnesia_ns),
    lists:map(fun emqx_authn_mnesia:rec_to_map/1, Rows).

t_update(_) ->
    Config0 = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, _} = emqx_authn_mnesia:update(Config1, State).

t_destroy() ->
    [{matrix, true}].
t_destroy(matrix) ->
    [[?global], [?ns]];
t_destroy(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    OtherConfig = Config#{user_group => <<"stomp:global">>},
    {ok, State0} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {ok, StateOther} = emqx_authn_mnesia:create(?AUTHN_ID, OtherConfig),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    User2 = add_ns(#{user_id => <<"u">>, password => <<"p">>}, ?OTHER_NS),

    {ok, _} = emqx_authn_mnesia:add_user(User, State0),
    {ok, _} = emqx_authn_mnesia:add_user(User2, State0),
    {ok, _} = emqx_authn_mnesia:add_user(User, StateOther),
    {ok, _} = emqx_authn_mnesia:add_user(User2, StateOther),

    {ok, _} = lookup_user(Namespace, <<"u">>, State0),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, State0),
    {ok, _} = lookup_user(Namespace, <<"u">>, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, StateOther),

    ok = emqx_authn_mnesia:destroy(State0),

    {ok, State1} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {error, not_found} = lookup_user(Namespace, <<"u">>, State1),
    {error, not_found} = lookup_user(?OTHER_NS, <<"u">>, State1),
    {ok, _} = lookup_user(Namespace, <<"u">>, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, StateOther),

    ok.

t_authenticate() ->
    [{matrix, true}].
t_authenticate(matrix) ->
    [[?global], [?ns]];
t_authenticate(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    {ok, _} = emqx_authn_mnesia:authenticate(
        maybe_add_ns_clientinfo(#{username => <<"u">>, password => <<"p">>}, TCConfig),
        State
    ),
    {error, bad_username_or_password} = emqx_authn_mnesia:authenticate(
        maybe_add_ns_clientinfo(#{username => <<"u">>, password => <<"badpass">>}, TCConfig),
        State
    ),
    ignore = emqx_authn_mnesia:authenticate(
        maybe_add_ns_clientinfo(#{clientid => <<"u">>, password => <<"p">>}, TCConfig),
        State
    ),
    %% Namespace mismatch; user doesn't exist
    %% Since we fallback to global namespace when the credentials are namespaced, we don't
    %% assert this for the global test group.
    maybe
        true ?= is_binary(ns(TCConfig)),
        ignore = emqx_authn_mnesia:authenticate(
            add_ns_clientinfo(#{username => <<"u">>, password => <<"p">>}, ?OTHER_NS),
            State
        )
    end,
    ok.

t_add_user() ->
    [{matrix, true}].
t_add_user(matrix) ->
    [[?global], [?ns]];
t_add_user(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_mnesia:add_user(User, State),
    {error, already_exist} = emqx_authn_mnesia:add_user(User, State),

    OtherUser = add_ns(User, ?OTHER_NS),
    {ok, _} = emqx_authn_mnesia:add_user(OtherUser, State),

    ok.

t_delete_user() ->
    [{matrix, true}].
t_delete_user(matrix) ->
    [[?global], [?ns]];
t_delete_user(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    {error, not_found} = delete_user(Namespace, <<"u">>, State),
    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    {error, not_found} = delete_user(?OTHER_NS, <<"u">>, State),
    ok = delete_user(Namespace, <<"u">>, State),
    {error, not_found} = delete_user(Namespace, <<"u">>, State).

t_update_user() ->
    [{matrix, true}].
t_update_user(matrix) ->
    [[?global], [?ns]];
t_update_user(TCConfig) ->
    Namespace = ns(TCConfig),
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = maybe_add_ns(#{user_id => <<"u">>, password => <<"p">>}, TCConfig),
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    {error, not_found} = update_user(?OTHER_NS, <<"u">>, #{password => <<"p1">>}, State),
    {error, not_found} = update_user(Namespace, <<"u1">>, #{password => <<"p1">>}, State),

    {ok, #{
        user_id := <<"u">>,
        is_superuser := true
    }} = update_user(
        Namespace,
        <<"u">>,
        #{password => <<"p1">>, is_superuser => true},
        State
    ),

    {ok, _} = emqx_authn_mnesia:authenticate(
        maybe_add_ns_clientinfo(#{username => <<"u">>, password => <<"p1">>}, TCConfig),
        State
    ),

    {ok, #{is_superuser := true}} = lookup_user(Namespace, <<"u">>, State).

t_list_users() ->
    [{matrix, true}].
t_list_users(matrix) ->
    [[?global], [?ns]];
t_list_users(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    Users0 = [
        #{user_id => <<"u1">>, password => <<"p">>},
        #{user_id => <<"u2">>, password => <<"p">>},
        #{user_id => <<"u3">>, password => <<"p">>}
    ],
    Users = lists:map(fun(U) -> maybe_add_ns(U, TCConfig) end, Users0),

    lists:foreach(
        fun(U) -> {ok, _} = emqx_authn_mnesia:add_user(U, State) end,
        Users
    ),
    OtherUser = #{user_id => <<"u4">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(add_ns(OtherUser, ?OTHER_NS), State),

    Namespace = ns(TCConfig),
    NSQS = fun
        (QS) when Namespace == ?global_ns ->
            QS;
        (QS) ->
            QS#{<<"ns">> => Namespace}
    end,

    #{
        data := [
            #{is_superuser := false, user_id := _},
            #{is_superuser := false, user_id := _}
        ],
        meta := #{page := 1, limit := 2, count := 3, hasnext := true}
    } = emqx_authn_mnesia:list_users(
        NSQS(#{<<"page">> => 1, <<"limit">> => 2}),
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := _}],
        meta := #{page := 2, limit := 2, count := 3, hasnext := false}
    } = emqx_authn_mnesia:list_users(
        NSQS(#{<<"page">> => 2, <<"limit">> => 2}),
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := <<"u3">>}],
        meta := #{page := 1, limit := 20, hasnext := false}
    } = emqx_authn_mnesia:list_users(
        NSQS(#{
            <<"page">> => 1,
            <<"limit">> => 20,
            <<"like_user_id">> => <<"3">>
        }),
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := <<"u4">>}],
        meta := #{page := 1, limit := 20, hasnext := false}
    } = emqx_authn_mnesia:list_users(
        #{
            <<"page">> => 1,
            <<"limit">> => 20,
            <<"ns">> => ?OTHER_NS
        },
        State
    ),
    ok.

t_import_users(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.json">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-ns.json">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.csv">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-ns.csv">>),
            State
        )
    ),

    ?assertMatch(
        {error, {unsupported_file_format, _}},
        emqx_authn_mnesia:import_users(
            {hash, <<"/file/with/unknown.extension">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, unknown_file_format},
        emqx_authn_mnesia:import_users(
            {hash, <<"/file/with/no/extension">>, <<>>},
            State
        )
    ),
    %% import plain.json with hash method
    ?assertEqual(
        {error, "hash_import_requires_password_hash_field"},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(hash, <<"user-credentials-plain.json">>),
            State
        )
    ),

    ?assertEqual(
        {error, bad_format},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed-0.json">>),
            State
        )
    ),

    ?assertMatch(
        {error, {_, invalid_json}},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed-1.json">>),
            State
        )
    ),

    ?assertEqual(
        {error, bad_format},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed.csv">>),
            State
        )
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, <<"empty_users.json">>, <<"[]">>},
            State
        )
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, <<"empty_users.csv">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, prepared_user_list, []},
            State
        )
    ).

t_import_users_plain(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain.json">>),
            State
        )
    ),
    %% import hash.json with plain method
    ?assertEqual(
        {error, "plain_import_requires_password_field"},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials.json">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain.csv">>),
            State
        )
    ).

t_import_users_prepared_list(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    Users1 = [
        #{<<"user_id">> => <<"u1">>, <<"password">> => <<"p1">>, <<"is_superuser">> => true},
        #{<<"user_id">> => <<"u2">>, <<"password">> => <<"p2">>, <<"is_superuser">> => true},
        #{<<"user_id">> => <<"u5">>, <<"password">> => <<"p5">>, <<"is_superuser">> => true}
    ],
    Users2 = [
        #{
            <<"user_id">> => <<"u3">>,
            <<"password_hash">> =>
                <<"c5e46903df45e5dc096dc74657610dbee8deaacae656df88a1788f1847390242">>,
            <<"salt">> => <<"e378187547bf2d6f0545a3f441aa4d8a">>,
            <<"is_superuser">> => true
        },
        #{
            <<"user_id">> => <<"u4">>,
            <<"password_hash">> =>
                <<"f4d17f300b11e522fd33f497c11b126ef1ea5149c74d2220f9a16dc876d4567b">>,
            <<"salt">> => <<"6d3f9bd5b54d94b98adbcfe10b6d181f">>,
            <<"is_superuser">> => true
        },
        #{
            <<"user_id">> => <<"u6">>,
            <<"password_hash">> =>
                <<"f4d17f300b11e522fd33f497c11b126ef1ea5149c74d2220f9a16dc876d4567b">>,
            <<"salt">> => <<"6d3f9bd5b54d94b98adbcfe10b6d181f">>,
            <<"is_superuser">> => true,
            <<"namespace">> => <<"ns1">>
        }
    ],

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {plain, prepared_user_list, Users1},
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {hash, prepared_user_list, Users2},
            State
        )
    ).

t_import_users_duplicated_records(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => plain, salt_position => disable}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain-dup.json">>),
            State
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain-dup.csv">>),
            State
        )
    ),
    Users1 = [
        #{
            <<"user_id">> => <<"myuser5">>,
            <<"password">> => <<"password5">>,
            <<"is_superuser">> => true
        },
        #{
            <<"user_id">> => <<"myuser5">>,
            <<"password">> => <<"password6">>,
            <<"is_superuser">> => false
        }
    ],
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {plain, prepared_user_list, Users1},
            State
        )
    ),

    %% assert: the last record overwrites the previous one
    ?assertMatch(
        [
            #{
                namespace := ?global,
                user_id := <<"myuser1">>,
                password_hash := <<"password2">>,
                is_superuser := false
            },
            #{
                namespace := ?global,
                user_id := <<"myuser3">>,
                password_hash := <<"password4">>,
                is_superuser := false
            },
            #{
                namespace := ?global,
                user_id := <<"myuser5">>,
                password_hash := <<"password6">>,
                is_superuser := false
            }
        ],
        read_tables()
    ),

    %% Namespaced users on different namespaces are independent.
    ok = emqx_authn_mnesia:destroy(State),
    {ok, State2} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain-dup-ns.json">>),
            State2
        )
    ),
    ?assertMatch(
        [
            #{
                namespace := ?global,
                user_id := <<"myuser1">>,
                password_hash := <<"password5">>,
                is_superuser := false
            },
            #{
                namespace := <<"ns1">>,
                user_id := <<"myuser1">>,
                password_hash := <<"password4">>,
                is_superuser := false
            },
            #{
                namespace := <<"ns2">>,
                user_id := <<"myuser1">>,
                password_hash := <<"password3">>,
                is_superuser := false
            }
        ],
        read_tables()
    ),

    ok.

-doc """
Verifies that, if we don't find an username in the desired namespace, we fallback the
lookup to the global namespace.
""".
t_namespace_fallback_to_global(_TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    Namespace = ?NS,

    Username = <<"u">>,
    PasswordGlobal = <<"pglobal">>,
    UserGlobal = #{user_id => Username, password => PasswordGlobal},

    %% First, only global user exists, and we attempt to authenticate using its password,
    %% even though we're aiming at the namespace.  Should succeed.
    {ok, _} = emqx_authn_mnesia:add_user(UserGlobal, State),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(
            add_ns_clientinfo(#{username => Username, password => PasswordGlobal}, Namespace),
            State
        )
    ),

    %% Now, we create a namespaced user with the same username and different password.
    %% Authentication should now return this new user.
    PasswordNs = <<"pns">>,
    UserNs = add_ns(#{user_id => Username, password => PasswordNs}, Namespace),
    {ok, _} = emqx_authn_mnesia:add_user(UserNs, State),
    ?assertMatch(
        {error, bad_username_or_password},
        emqx_authn_mnesia:authenticate(
            add_ns_clientinfo(#{username => Username, password => PasswordGlobal}, Namespace),
            State
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(
            add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
            State
        )
    ),

    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

sample_filename(Name) ->
    Dir = code:lib_dir(emqx_auth),
    filename:join([Dir, <<"test">>, <<"data">>, Name]).

sample_filename_and_data(Name) ->
    sample_filename_and_data(hash, Name).

sample_filename_and_data(Type, Name) ->
    Filename = sample_filename(Name),
    {ok, Data} = file:read_file(Filename),
    {Type, Filename, Data}.

config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{
            name => bcrypt,
            salt_rounds => 8
        },
        user_group => <<"global:mqtt">>
    }.

lookup_user(Namespace, UserId, State) ->
    emqx_authn_mnesia:lookup_user(Namespace, UserId, State).

update_user(Namespace, UserId, UserInfo, State) ->
    emqx_authn_mnesia:update_user(Namespace, UserId, UserInfo, State).

delete_user(Namespace, UserId, State) ->
    emqx_authn_mnesia:delete_user(Namespace, UserId, State).

maybe_add_ns(UserInfo, TCConfig) ->
    case ns(TCConfig) of
        ?global_ns ->
            UserInfo;
        Namespace when is_binary(Namespace) ->
            add_ns(UserInfo, Namespace)
    end.

add_ns(UserInfo, Namespace) when is_binary(Namespace) ->
    UserInfo#{namespace => Namespace}.

maybe_add_ns_clientinfo(ClientInfo, TCConfig) ->
    case ns(TCConfig) of
        ?global_ns ->
            ClientInfo;
        Namespace when is_binary(Namespace) ->
            add_ns_clientinfo(ClientInfo, Namespace)
    end.

add_ns_clientinfo(ClientInfo, Namespace) when is_binary(Namespace) ->
    ClientInfo#{client_attrs => #{?CLIENT_ATTR_NAME_TNS => Namespace}}.

ns(TCConfig) ->
    ?config(ns, TCConfig).
