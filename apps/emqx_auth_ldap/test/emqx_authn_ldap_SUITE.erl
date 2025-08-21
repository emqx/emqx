%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authn_ldap_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_DEFAULT_PORT, 389).

-define(PATH, [authentication]).
-define(AUTHN_ID, <<"password_based:ldap">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

end_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_authn_test_lib:enable_node_cache(false),
    Config.

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_ldap], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_ldap_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_ldap}]} = emqx_authn_chains:list_authenticators(?GLOBAL),
    emqx_authn_test_lib:delete_config(?AUTHN_ID).

t_create_invalid(_Config) ->
    AuthConfig = raw_ldap_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"base_dn">> => <<"outestdevice,dcemqx,dc=io">>},
            AuthConfig#{<<"filter">> => <<"(cn=${username}">>}
        ],

    lists:foreach(
        fun(Config) ->
            {error, _Error} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, Config}
            )
        end,
        InvalidConfigs
    ).

t_authenticate_timeout_cause_reconnect(_Config) ->
    TestPid = self(),
    meck:new(eldap, [non_strict, passthrough]),
    %% cause eldap process to be killed
    meck:expect(
        eldap,
        search,
        fun(Pid, SearchOptions) ->
            BaseDN = iolist_to_binary(proplists:get_value(base, SearchOptions, <<>>)),
            case BaseDN of
                <<"uid=mqttuser0007", _/binary>> ->
                    TestPid ! {eldap_pid, Pid},
                    {error, {gen_tcp_error, timeout}};
                _ ->
                    meck:passthrough([Pid, SearchOptions])
            end
        end
    ),
    Credentials = fun(Username) ->
        #{
            username => Username,
            password => Username,
            listener => 'tcp:default',
            protocol => mqtt
        }
    end,

    SpecificConfigParams = #{},
    Result = {ok, #{is_superuser => true}},

    Timeout = 1000,
    Config0 = raw_ldap_auth_config(),
    Config = Config0#{
        <<"pool_size">> => 1,
        <<"request_timeout">> => Timeout
    },
    AuthConfig = maps:merge(Config, SpecificConfigParams),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    %% 0006 is a disabled user
    ?assertEqual(
        {error, bad_username_or_password},
        emqx_access_control:authenticate(Credentials(<<"mqttuser0006">>))
    ),
    ?assertEqual(
        {error, not_authorized},
        emqx_access_control:authenticate(Credentials(<<"mqttuser0007">>))
    ),
    ok = wait_for_ldap_pid(1000),
    [#{id := ResourceID}] = emqx_resource_manager:list_all(),
    ?retry(1_000, 20, {ok, connected} = emqx_resource_manager:health_check(ResourceID)),
    %% turn back to normal
    meck:expect(
        eldap,
        search,
        2,
        fun(Pid2, Query) ->
            meck:passthrough([Pid2, Query])
        end
    ),
    %% expect eldap process to be restarted
    ?assertEqual(Result, emqx_access_control:authenticate(Credentials(<<"mqttuser0007">>))),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

wait_for_ldap_pid(After) ->
    receive
        {eldap_pid, Pid} ->
            ?assertNot(is_process_alive(Pid)),
            ok
    after After ->
        error(timeout)
    end.

t_authenticate(_Config) ->
    ok = lists:foreach(
        fun(Sample) ->
            test_user_auth(Sample)
        end,
        user_seeds()
    ).

test_user_auth(#{
    credentials := Credentials0,
    config_params := SpecificConfigParams,
    result := ExpectedResult
}) ->
    AuthConfig = maps:merge(raw_ldap_auth_config(), SpecificConfigParams),

    ct:pal("test_user_auth~ncredentials: ~p~nconfig: ~p~nresult: ~p", [
        Credentials0, AuthConfig, ExpectedResult
    ]),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    Credentials = Credentials0#{
        listener => 'tcp:default',
        protocol => mqtt
    },

    ActualAuthResult0 = emqx_access_control:authenticate(Credentials),
    ActualAuthResult = filter_expected_fields(ExpectedResult, ActualAuthResult0),
    ?assertEqual(ExpectedResult, ActualAuthResult),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_destroy(_Config) ->
    AuthConfig = raw_ldap_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_ldap, state := State}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL),

    {ok, _} = emqx_authn_ldap:authenticate(
        #{
            username => <<"mqttuser0001">>,
            password => <<"mqttuser0001">>
        },
        State
    ),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    % Authenticator should not be usable anymore
    ?assertMatch(
        ignore,
        emqx_authn_ldap:authenticate(
            #{
                username => <<"mqttuser0001">>,
                password => <<"mqttuser0001">>
            },
            State
        )
    ).

t_update(_Config) ->
    CorrectConfig = raw_ldap_auth_config(),
    IncorrectConfig =
        CorrectConfig#{
            <<"base_dn">> => <<"ou=testdevice,dc=emqx,dc=io">>
        },

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, IncorrectConfig}
    ),

    {error, _} = emqx_access_control:authenticate(
        #{
            username => <<"mqttuser0001">>,
            password => <<"mqttuser0001">>,
            listener => 'tcp:default',
            protocol => mqtt
        }
    ),

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:ldap">>, CorrectConfig}
    ),

    {ok, _} = emqx_access_control:authenticate(
        #{
            username => <<"mqttuser0001">>,
            password => <<"mqttuser0001">>,
            listener => 'tcp:default',
            protocol => mqtt
        }
    ).

t_node_cache(_Config) ->
    Config = maps:merge(raw_ldap_auth_config(), #{
        <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
        <<"filter">> => <<"(memberOf=cn=${cert_common_name},ou=Groups,dc=emqx,dc=io)">>
    }),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok = emqx_authn_test_lib:enable_node_cache(true),
    Credentials = #{
        listener => 'tcp:default',
        protocol => mqtt,
        username => <<"mqttuser0003">>,
        password => <<"mqttuser0003">>,
        cn => <<"test">>
    },

    %% First time should be a miss, second time should be a hit
    ?assertMatch(
        {ok, #{is_superuser := false}},
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        {ok, #{is_superuser := false}},
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ),

    %% Change a variable in the different parts of the query, should be 3 misses
    _ = emqx_access_control:authenticate(Credentials#{username => <<"user2">>}),
    _ = emqx_access_control:authenticate(Credentials#{cn => <<"testdevice2">>}),
    _ = emqx_access_control:authenticate(Credentials#{password => <<"password2">>}),
    %% Changing only password should be a hit because
    %% for hash method password is not a part of the key
    ?assertMatch(
        #{hits := #{value := 2}, misses := #{value := 3}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ).

-doc """
Checks that, if an authentication backend returns the `clientid_override` attribute, it's
used to override.
""".
t_clientid_override(TCConfig) when is_list(TCConfig) ->
    OverriddenClientId = <<"overridden_clientid">>,
    Username = <<"mqttuser0010">>,
    Password = <<"mqttuser0010">>,
    MkConfigFn = fun() ->
        maps:merge(raw_ldap_auth_config(), #{
            <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
            <<"method">> => #{
                <<"type">> => <<"hash">>,
                <<"clientid_override_attribute">> => <<"clientIdOverride">>
            }
        })
    end,
    Opts = #{
        client_opts => #{
            username => Username,
            password => Password
        },
        mk_config_fn => MkConfigFn,
        overridden_clientid => OverriddenClientId
    },
    emqx_authn_test_lib:t_clientid_override(TCConfig, Opts),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_ldap_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"server">> => ldap_server(),
        <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 8
    }.

user_seeds() ->
    New4 = fun(Username, Password, Result, Params) ->
        #{
            credentials => #{
                username => Username,
                password => Password
            },
            config_params => Params,
            result => Result
        }
    end,

    New = fun(Username, Password, Result) ->
        New4(Username, Password, Result, #{})
    end,

    Valid =
        lists:map(
            fun(Idx) ->
                Username = erlang:iolist_to_binary(io_lib:format("mqttuser000~b", [Idx])),
                New(Username, Username, {ok, #{is_superuser => false}})
            end,
            lists:seq(1, 5)
        ),
    [
        %% Not exists
        New(<<"notexists">>, <<"notexists">>, {error, not_authorized}),
        %% Wrong Password
        New(<<"mqttuser0001">>, <<"wrongpassword">>, {error, bad_username_or_password}),
        %% Disabled
        New(<<"mqttuser0006">>, <<"mqttuser0006">>, {error, bad_username_or_password}),
        %% IsSuperuser
        New(<<"mqttuser0007">>, <<"mqttuser0007">>, {ok, #{is_superuser => true}}),
        New(<<"mqttuser0008 (test)">>, <<"mqttuser0008 (test)">>, {ok, #{is_superuser => true}}),
        New(
            <<"mqttuser0009 \\test\\">>,
            <<"mqttuser0009 \\\\test\\\\">>,
            {ok, #{is_superuser => true}}
        ),
        %% not in group
        New4(
            <<"mqttuser0002">>,
            <<"mqttuser0002">>,
            {error, not_authorized},
            #{<<"filter">> => <<"(memberOf=cn=test,ou=Groups,dc=emqx,dc=io)">>}
        ),
        %% in group
        New4(
            <<"mqttuser0003">>,
            <<"mqttuser0003">>,
            {ok, #{is_superuser => false}},
            #{<<"filter">> => <<"(memberOf=cn=test,ou=Groups,dc=emqx,dc=io)">>}
        ),
        %% non exists group
        New4(
            <<"mqttuser0003">>,
            <<"mqttuser0003">>,
            {error, not_authorized},
            #{<<"filter">> => <<"(memberOf=cn=nonexists,ou=Groups,dc=emqx,dc=io)">>}
        ),
        New4(<<"mqttuser0007">>, <<"mqttuser0007">>, {ok, #{is_superuser => true}}, #{
            <<"filter">> =>
                %% evaluates to true
                <<"(& (!(uid=mqttuser000999)) (|(!(uid=mqttuser000999)) (uid=mqttuser000999)))">>
        })
        | Valid
    ].

ldap_server() ->
    iolist_to_binary(io_lib:format("~s:~B", [?LDAP_HOST, ?LDAP_DEFAULT_PORT])).

filter_expected_fields({ok, Expected}, {ok, Actual}) ->
    {ok, maps:with(maps:keys(Expected), Actual)};
filter_expected_fields(_Expected, Actual) ->
    Actual.
