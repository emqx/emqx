%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("emqx_auth_mnesia_internal.hrl").

-define(AUTHN_ID, <<"mechanism:backend">>).
-define(NS, <<"some_ns">>).
-define(OTHER_NS, <<"some_other_ns">>).

-define(global, global).
-define(ns, ns).

all() ->
    [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all_with_matrix(?MODULE),
    [
        {legacy, [], Tests},
        {hardened, [], Tests}
        | emqx_common_test_helpers:groups_with_matrix(?MODULE)
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, emqx_authn_test_lib:emqx_appspec()},
            emqx_auth,
            emqx_auth_mnesia
        ],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{apps, Apps}, {security_profile, Profile} | Config];
init_per_group(?global, Config) ->
    [{ns, ?global_ns} | Config];
init_per_group(?ns, Config) ->
    [{ns, ?NS} | Config];
init_per_group(distinct_users, Config) ->
    [{user_ids, {<<"u">>, <<"v">>}} | Config];
init_per_group(same_user, Config) ->
    [{user_ids, {<<"u">>, <<"u">>}} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile();
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

    Config1 = Config0#{password_hash_algorithm => #{name => sha256, salt_position => prefix}},
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

t_default_bootstrap_file_missing(_) ->
    Config = config(#{
        bootstrap_file => emqx_authn_mnesia_schema:default_bootstrap_file_path(),
        bootstrap_type => hash
    }),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    ?assertMatch([], ets:tab2list(emqx_authn_mnesia)),
    ok = emqx_authn_mnesia:destroy(State).

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

reset_tables() ->
    mria:clear_table(emqx_authn_mnesia),
    mria:clear_table(emqx_authn_mnesia_ns),
    ok.

namespaced_superusers() ->
    [
        User
     || #{namespace := Namespace, is_superuser := true} = User <- read_tables(),
        Namespace =/= ?global_ns
    ].

t_update(_) ->
    Config0 = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256, salt_position => prefix}},
    {ok, _} = emqx_authn_mnesia:update(Config1, State).

t_destroy() ->
    [{matrix, true}].
t_destroy(matrix) ->
    [[?global, distinct_users], [?ns, same_user]];
t_destroy(TCConfig) ->
    Namespace = ns(TCConfig),
    {UserId, OtherUserId} = ?config(user_ids, TCConfig),
    Config = config(),
    OtherConfig = Config#{user_group => <<"stomp:global">>},
    {ok, State0} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {ok, StateOther} = emqx_authn_mnesia:create(?AUTHN_ID, OtherConfig),

    User = maybe_add_ns(#{user_id => UserId, password => <<"p">>}, TCConfig),
    User2 = add_ns(#{user_id => OtherUserId, password => <<"p">>}, ?OTHER_NS),

    {ok, _} = emqx_authn_mnesia:add_user(User, State0),
    {ok, _} = emqx_authn_mnesia:add_user(User2, State0),
    {ok, _} = emqx_authn_mnesia:add_user(User, StateOther),
    {ok, _} = emqx_authn_mnesia:add_user(User2, StateOther),

    {ok, _} = lookup_user(Namespace, UserId, State0),
    {ok, _} = lookup_user(?OTHER_NS, OtherUserId, State0),
    {ok, _} = lookup_user(Namespace, UserId, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, OtherUserId, StateOther),

    ok = emqx_authn_mnesia:destroy(State0),

    {ok, State1} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {error, not_found} = lookup_user(Namespace, UserId, State1),
    {error, not_found} = lookup_user(?OTHER_NS, OtherUserId, State1),
    {ok, _} = lookup_user(Namespace, UserId, StateOther),
    {ok, _} = lookup_user(?OTHER_NS, OtherUserId, StateOther),

    ok.

-doc """
Tests that `purge_namespace/1` deletes all users from the given namespace across all user
groups, while leaving global and other-namespace users intact.
""".
t_purge_namespace(_TCConfig) ->
    Config = config(),
    OtherGroupConfig = Config#{user_group => <<"stomp:global">>},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {ok, StateOtherGroup} = emqx_authn_mnesia:create(?AUTHN_ID, OtherGroupConfig),

    GlobalUser = #{user_id => <<"global">>, password => <<"p">>},
    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(GlobalUser, State),
    {ok, _} = emqx_authn_mnesia:add_user(add_ns(User, ?NS), State),
    {ok, _} = emqx_authn_mnesia:add_user(add_ns(User, ?NS), StateOtherGroup),
    {ok, _} = emqx_authn_mnesia:add_user(add_ns(User, ?OTHER_NS), State),

    ok = emqx_authn_mnesia:purge_namespace(?NS),

    {error, not_found} = lookup_user(?NS, <<"u">>, State),
    {error, not_found} = lookup_user(?NS, <<"u">>, StateOtherGroup),
    {ok, _} = lookup_user(?global_ns, <<"global">>, State),
    {ok, _} = lookup_user(?OTHER_NS, <<"u">>, State),
    ?assertEqual(0, emqx_authn_mnesia:record_count(?NS)),

    %% Idempotent
    ok = emqx_authn_mnesia:purge_namespace(?NS),
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
    %%
    %% 1) If there are no username for the whole namespace, we fall back the lookup to the
    %%    global table.
    %%
    %% 2) If there is even a single record for the namespace being inspected, we don't
    %%    check the global table.

    %% Since we only have a global entry for the user in the `?global` test group, we run
    %% these assertions in it.
    maybe
        false ?= is_binary(ns(TCConfig)),
        %% At first, this other namespace has no records, hence we fall back to global.
        ?assertMatch(
            {ok, _},
            emqx_authn_mnesia:authenticate(
                add_ns_clientinfo(#{username => <<"u">>, password => <<"p">>}, ?OTHER_NS),
                State
            )
        ),
        %% Once we create any record for this namespace, we no longer fall back, hence
        %% authentication will find no user.
        OtherUser = add_ns(#{user_id => <<"v">>, password => <<"p">>}, ?OTHER_NS),
        {ok, _} = emqx_authn_mnesia:add_user(OtherUser, State),
        ?assertMatch(
            ignore,
            emqx_authn_mnesia:authenticate(
                add_ns_clientinfo(#{username => <<"u">>, password => <<"p">>}, ?OTHER_NS),
                State
            )
        ),
        ok
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
    case {?config(security_profile, TCConfig), ns(TCConfig)} of
        {hardened, ?global_ns} ->
            {error, already_exist} = emqx_authn_mnesia:add_user(OtherUser, State);
        _ ->
            {ok, _} = emqx_authn_mnesia:add_user(OtherUser, State)
    end,

    ok.

%% Verify password generation and format
t_generated_password(_) ->
    Algorithm = #{name => sha256, salt_position => disable},
    {ok, State} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{autogenerate_password => true, password_hash_algorithm => Algorithm})
    ),
    ?assertEqual(
        {error, password_not_allowed},
        emqx_authn_mnesia:add_user(#{user_id => <<"supplied">>, password => <<"p">>}, State)
    ),
    {ok, #{password := Password}} = emqx_authn_mnesia:add_user(
        #{user_id => <<"generated">>}, State
    ),
    ?assertEqual(32, byte_size(emqx_base62:decode(Password))),
    [Record] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"generated">>)
    ),
    #{password_hash := PasswordHash, salt := Salt, extra := Extra} =
        emqx_authn_mnesia:rec_to_map(Record),
    ?assertEqual(<<>>, Salt),
    ?assertEqual(#{algo => #simple{name = sha256, salt_position = disable}}, Extra),
    ?assertEqual(match, re:run(PasswordHash, <<"^[0-9a-f]{64}$">>, [{capture, none}])),
    ?assertEqual(
        {PasswordHash, Salt},
        emqx_authn_password_hashing:hash(Algorithm, Password)
    ),
    ?assertEqual(nomatch, binary:match(term_to_binary(Record), Password)),
    ?assertEqual(
        {error, password_required},
        emqx_authn_mnesia:add_user(
            #{user_id => <<"missing">>}, emqx_authn_mnesia_create(?AUTHN_ID, config())
        )
    ).

%% Checks generated password rotation, check rotation availability.
t_rotate_generated_password(_) ->
    {ok, State} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{
            autogenerate_password => true,
            password_hash_algorithm => #{name => sha256, salt_position => disable}
        })
    ),
    {ok, #{password := OldPassword}} = emqx_authn_mnesia:add_user(
        #{user_id => <<"u">>}, State
    ),
    {ok, #{password := NewPassword}} = emqx_authn_mnesia:rotate_password(
        ?global_ns, <<"u">>, State
    ),
    ?assertNotEqual(OldPassword, NewPassword),
    ?assertEqual(
        {error, bad_username_or_password},
        emqx_authn_mnesia:authenticate(
            #{username => <<"u">>, password => OldPassword}, State
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(
            #{username => <<"u">>, password => NewPassword}, State
        )
    ),
    ?assertEqual(
        {error, password_rotation_disabled},
        emqx_authn_mnesia:rotate_password(?global_ns, <<"u">>, State#{
            autogenerate_password => false
        })
    ),
    ?assertEqual(
        {error, password_not_allowed},
        emqx_authn_mnesia:update_user(
            ?global_ns, <<"u">>, #{password => <<"caller-supplied">>}, State
        )
    ).

%% Checks that stored algorithm metadata remains valid after authenticator configuration changes.
t_stored_algorithm_survives_config_change(_) ->
    OldAlgorithm = #{name => sha256, salt_position => suffix},
    {ok, OldState} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{password_hash_algorithm => OldAlgorithm})
    ),
    {ok, _} = emqx_authn_mnesia:add_user(
        #{user_id => <<"u">>, password => <<"old">>}, OldState
    ),
    NewAlgorithm = #{name => plain, salt_position => disable},
    {ok, NewState} = emqx_authn_mnesia:update(
        config(#{password_hash_algorithm => NewAlgorithm}), OldState
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(#{username => <<"u">>, password => <<"old">>}, NewState)
    ),
    {ok, _} = emqx_authn_mnesia:update_user(
        ?global_ns, <<"u">>, #{is_superuser => true}, NewState
    ),
    [MetadataRecord] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"u">>)
    ),
    ?assertMatch(
        #{extra := #{algo := #simple{name = sha256, salt_position = suffix}}},
        emqx_authn_mnesia:rec_to_map(MetadataRecord)
    ),
    {ok, _} = emqx_authn_mnesia:update_user(
        ?global_ns, <<"u">>, #{password => <<"new">>}, NewState
    ),
    [PasswordRecord] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"u">>)
    ),
    ?assertMatch(
        #{extra := #{algo := #simple{name = plain, salt_position = disable}}},
        emqx_authn_mnesia:rec_to_map(PasswordRecord)
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(#{username => <<"u">>, password => <<"new">>}, NewState)
    ).

%% Checks legacy global lookup and migration into the namespaced table during update
t_legacy_global_fallback_and_update(_) ->
    Algorithm = #{name => sha256, salt_position => suffix},
    {Hash, Salt} = emqx_authn_password_hashing:hash(Algorithm, <<"p">>),
    Legacy = {user_info, {'global:mqtt', <<"legacy">>}, Hash, Salt, false},
    ok = mria:dirty_write_sync(emqx_authn_mnesia, Legacy),
    {ok, State} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{password_hash_algorithm => Algorithm})
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(
            #{username => <<"legacy">>, password => <<"p">>}, State
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(
            add_ns_clientinfo(#{username => <<"legacy">>, password => <<"p">>}, ?NS), State
        )
    ),
    {ok, _} = emqx_authn_mnesia:update_user(
        ?global_ns, <<"legacy">>, #{is_superuser => true}, State
    ),
    ?assertEqual([], ets:lookup(emqx_authn_mnesia, {'global:mqtt', <<"legacy">>})),
    [Record] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"legacy">>)
    ),
    ?assertMatch(
        #{
            password_hash := Hash,
            salt := Salt,
            extra := #{algo := #simple{name = sha256, salt_position = suffix}}
        },
        emqx_authn_mnesia:rec_to_map(Record)
    ).

%% Checks that a newer legacy write takes precedence while a global user exists in both tables.
t_global_dual_table_precedence_and_operations(_) ->
    Legacy = {user_info, {'global:mqtt', <<"u">>}, <<"legacy">>, <<>>, false},
    New = #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"u">>),
        password_hash = <<"new">>,
        salt = <<>>,
        is_superuser = true,
        extra = #{algo => #simple{name = plain, salt_position = disable}}
    },
    ok = mria:dirty_write_sync(emqx_authn_mnesia, Legacy),
    ok = mria:dirty_write_sync(emqx_authn_mnesia_ns, New),
    {ok, State} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{password_hash_algorithm => #{name => plain, salt_position => disable}})
    ),
    ?assertEqual(
        {error, already_exist},
        emqx_authn_mnesia:add_user(#{user_id => <<"u">>, password => <<"duplicate">>}, State)
    ),
    %% In case of duplicates, legacy wins because it is the later update by an old node.
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(#{username => <<"u">>, password => <<"legacy">>}, State)
    ),
    ?assertEqual(
        {error, bad_username_or_password},
        emqx_authn_mnesia:authenticate(#{username => <<"u">>, password => <<"new">>}, State)
    ),
    ?assertEqual(
        {ok, #{user_id => <<"u">>, is_superuser => false}},
        emqx_authn_mnesia:lookup_user(?global_ns, <<"u">>, State)
    ),
    %% Allow transient duplicates
    ?assertMatch(
        #{
            data := [
                #{user_id := <<"u">>, is_superuser := false},
                #{user_id := <<"u">>, is_superuser := true}
            ],
            meta := #{count := 2}
        },
        emqx_authn_mnesia:list_users(#{<<"page">> => 1, <<"limit">> => 10}, State)
    ),
    ok = emqx_auth_mnesia_bookkeeper:tally_authn_now(),
    ?assertEqual(2, emqx_authn_mnesia:record_count(?global_ns)),
    {ok, #{override := 1}} = emqx_authn_mnesia:import_users(
        {hash, prepared_user_list, [
            #{
                <<"user_id">> => <<"u">>,
                <<"password_hash">> => <<"new">>,
                <<"salt">> => <<>>,
                <<"is_superuser">> => true
            }
        ]},
        State
    ),
    ?assertEqual([], ets:lookup(emqx_authn_mnesia, {'global:mqtt', <<"u">>})),
    {ok, _} = emqx_authn_mnesia:update_user(
        ?global_ns, <<"u">>, #{is_superuser => false}, State
    ),
    ok = emqx_authn_mnesia:delete_user(?global_ns, <<"u">>, State),
    ?assertEqual(
        [], ets:lookup(emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"u">>))
    ).

%% Checks that imports store the configured generated-password algorithm with each record.
t_import_records_algorithm_in_generated_mode(_) ->
    Algorithm = #{name => sha256, salt_position => disable},
    {ok, State} = emqx_authn_mnesia:create(
        ?AUTHN_ID,
        config(#{autogenerate_password => true, password_hash_algorithm => Algorithm})
    ),
    Users = [#{<<"user_id">> => <<"plain">>, <<"password">> => <<"p">>}],
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users({plain, prepared_user_list, Users}, State)
    ),
    [Record] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"plain">>)
    ),
    ?assertMatch(
        #{extra := #{algo := #simple{name = sha256, salt_position = disable}}},
        emqx_authn_mnesia:rec_to_map(Record)
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:authenticate(#{username => <<"plain">>, password => <<"p">>}, State)
    ),
    {Hash, Salt} = emqx_authn_password_hashing:hash(Algorithm, <<"prehashed-password">>),
    PrehashedUsers = [
        #{<<"user_id">> => <<"prehashed">>, <<"password_hash">> => Hash, <<"salt">> => Salt}
    ],
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users({hash, prepared_user_list, PrehashedUsers}, State)
    ),
    [PrehashedRecord] = ets:lookup(
        emqx_authn_mnesia_ns, ?AUTHN_NS_KEY(?global_ns, 'global:mqtt', <<"prehashed">>)
    ),
    ?assertMatch(
        #{
            password_hash := Hash,
            salt := Salt,
            extra := #{algo := #simple{name = sha256, salt_position = disable}}
        },
        emqx_authn_mnesia:rec_to_map(PrehashedRecord)
    ).

t_global_user_conflict(TCConfig) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    case ?config(security_profile, TCConfig) of
        legacy -> assert_global_user_conflict_legacy(State);
        hardened -> assert_global_user_conflict_hardened(State)
    end,
    ok.

assert_global_user_conflict_hardened(State) ->
    Namespace = ?NS,
    Username = <<"u">>,
    PasswordNs = <<"pns">>,
    PasswordGlobal = <<"pglobal">>,
    UserNs = add_ns(#{user_id => Username, password => PasswordNs}, Namespace),
    UserGlobal = #{user_id => Username, password => PasswordGlobal},

    {ok, _} = emqx_authn_mnesia:add_user(UserNs, State),
    {ok, _} = emqx_authn_mnesia:add_user(UserGlobal, State),
    ignore = emqx_authn_mnesia:authenticate(
        add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
        State
    ),
    ignore = emqx_authn_mnesia:authenticate(
        add_ns_clientinfo(#{username => Username, password => PasswordGlobal}, Namespace),
        State
    ),
    {ok, _} = emqx_authn_mnesia:authenticate(
        #{username => Username, password => PasswordGlobal}, State
    ),

    ok = delete_user(?global_ns, Username, State),
    {ok, _} = emqx_authn_mnesia:authenticate(
        add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
        State
    ),

    {ok, _} = emqx_authn_mnesia:add_user(UserGlobal, State),
    {error, already_exist} = emqx_authn_mnesia:add_user(
        add_ns(#{user_id => Username, password => PasswordNs}, ?OTHER_NS), State
    ),
    ok.

assert_global_user_conflict_legacy(State) ->
    Namespace = ?NS,
    Username = <<"u">>,
    PasswordNs = <<"pns">>,
    PasswordGlobal = <<"pglobal">>,
    UserNs = add_ns(#{user_id => Username, password => PasswordNs}, Namespace),
    UserGlobal = #{user_id => Username, password => PasswordGlobal},

    {ok, _} = emqx_authn_mnesia:add_user(UserNs, State),
    {ok, _} = emqx_authn_mnesia:add_user(UserGlobal, State),
    {ok, _} = emqx_authn_mnesia:authenticate(
        add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
        State
    ),
    {error, bad_username_or_password} = emqx_authn_mnesia:authenticate(
        add_ns_clientinfo(#{username => Username, password => PasswordGlobal}, Namespace),
        State
    ),
    {ok, _} = emqx_authn_mnesia:add_user(
        add_ns(#{user_id => Username, password => PasswordNs}, ?OTHER_NS), State
    ),
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
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => prefix}},
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

t_import_users_duplicated_records(TCConfig) ->
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

    ok = emqx_authn_mnesia:destroy(State),
    {ok, State2} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    case ?config(security_profile, TCConfig) of
        legacy ->
            %% The legacy profile keeps users in different namespaces independent.
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
            );
        hardened ->
            %% The hardened profile rejects namespaced users that conflict with a global user.
            ?assertMatch(
                {ok, #{total := 5, success := 1, override := 1, failed := 3}},
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
                    }
                ],
                read_tables()
            )
    end,

    ok.

-doc """
A built-in-database user in a non-global namespace must never be a superuser.
Bulk import (prepared list, JSON file and CSV file) rejects such rows while
keeping global-namespace superusers and non-superuser namespaced users.
""".
t_import_users_superuser_in_namespace_rejected(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    %% prepared list: global superuser ok, namespaced superuser rejected,
    %% namespaced non-superuser ok.
    Users = [
        #{<<"user_id">> => <<"gsuper">>, <<"password">> => <<"p">>, <<"is_superuser">> => true},
        #{
            <<"user_id">> => <<"nssuper">>,
            <<"password">> => <<"p">>,
            <<"is_superuser">> => true,
            <<"namespace">> => <<"ns1">>
        },
        #{
            <<"user_id">> => <<"nsplain">>,
            <<"password">> => <<"p">>,
            <<"is_superuser">> => false,
            <<"namespace">> => <<"ns1">>
        }
    ],
    ?assertMatch(
        {ok, #{success := 2, failed := 1}},
        emqx_authn_mnesia:import_users({plain, prepared_user_list, Users}, State)
    ),
    ?assertMatch(
        [
            #{namespace := ?global, user_id := <<"gsuper">>, is_superuser := true},
            #{namespace := <<"ns1">>, user_id := <<"nsplain">>, is_superuser := false}
        ],
        read_tables()
    ),
    ?assertEqual([], namespaced_superusers()),

    %% JSON file: global superuser ok, namespaced superuser rejected.
    reset_tables(),
    ?assertMatch(
        {ok, #{success := 1, failed := 1}},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(hash, <<"user-credentials-ns-superuser.json">>),
            State
        )
    ),
    ?assertEqual([], namespaced_superusers()),
    ?assertMatch(
        [#{namespace := ?global, user_id := <<"globalsuper">>, is_superuser := true}],
        read_tables()
    ),

    %% CSV file: same expectation.
    reset_tables(),
    ?assertMatch(
        {ok, #{success := 1, failed := 1}},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(hash, <<"user-credentials-ns-superuser.csv">>),
            State
        )
    ),
    ?assertEqual([], namespaced_superusers()),
    ?assertMatch(
        [#{namespace := ?global, user_id := <<"globalsuper">>, is_superuser := true}],
        read_tables()
    ),

    ok = emqx_authn_mnesia:destroy(State).

-doc """
A bootstrap file that contains a superuser row in a non-global namespace does
not prevent the node from booting; the offending row is rejected and no
namespaced superuser is persisted, while the global-namespace superuser is
still imported.
""".
t_bootstrap_file_superuser_in_namespace_rejected(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {Type, Filename, _} = sample_filename_and_data(
        hash, <<"user-credentials-ns-superuser.json">>
    ),
    BootstrapConfig = Config#{
        bootstrap_file => Filename,
        bootstrap_type => Type
    },
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, BootstrapConfig),
    ?assertEqual([], namespaced_superusers()),
    ?assertMatch(
        [#{namespace := ?global, user_id := <<"globalsuper">>, is_superuser := true}],
        read_tables()
    ),
    ok = emqx_authn_mnesia:destroy(State).

-doc """
Verifies that, if we don't find an username in the desired namespace, we fallback the
lookup to the global namespace.
""".
t_namespace_fallback_to_global(TCConfig) ->
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

    PasswordNs = <<"pns">>,
    UserNs = add_ns(#{user_id => Username, password => PasswordNs}, Namespace),
    case ?config(security_profile, TCConfig) of
        legacy ->
            {ok, _} = emqx_authn_mnesia:add_user(UserNs, State),
            ?assertMatch(
                {ok, _},
                emqx_authn_mnesia:authenticate(
                    add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
                    State
                )
            );
        hardened ->
            {error, already_exist} = emqx_authn_mnesia:add_user(UserNs, State),
            ?assertMatch(
                {ok, _},
                emqx_authn_mnesia:authenticate(
                    add_ns_clientinfo(
                        #{username => Username, password => PasswordGlobal}, Namespace
                    ),
                    State
                )
            ),
            ?assertMatch(
                {error, bad_username_or_password},
                emqx_authn_mnesia:authenticate(
                    add_ns_clientinfo(#{username => Username, password => PasswordNs}, Namespace),
                    State
                )
            )
    end,

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
        user_group => 'global:mqtt',
        autogenerate_password => false
    }.

config(Overrides) ->
    maps:merge(config(), Overrides).

emqx_authn_mnesia_create(AuthenticatorID, Config) ->
    {ok, State} = emqx_authn_mnesia:create(AuthenticatorID, Config),
    State.

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
