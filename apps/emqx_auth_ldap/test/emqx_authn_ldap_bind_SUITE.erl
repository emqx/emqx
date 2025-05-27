%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authn_ldap_bind_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_auth_ldap.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_DEFAULT_PORT, 389).
-define(LDAP_RESOURCE, <<"emqx_authn_ldap_bind_SUITE">>).

-define(PATH, [authentication]).
-define(ResourceID, <<"password_based:ldap">>).

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
    case emqx_common_test_helpers:is_tcp_server_available(?LDAP_HOST, ?LDAP_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_ldap], #{
                work_dir => ?config(priv_dir, Config)
            }),
            {ok, _Data} = emqx_authn_utils:create_resource(
                ?LDAP_RESOURCE,
                emqx_ldap_connector,
                ldap_config(),
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
            [{apps, Apps} | Config];
        false ->
            {skip, no_ldap}
    end.

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_authn_ldap:destroy(#{resource_id => ?LDAP_RESOURCE}),
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
    emqx_authn_test_lib:delete_config(?ResourceID).

t_create_invalid(_Config) ->
    AuthConfig = raw_ldap_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"server">> => <<"unknownhost:3333">>},
            AuthConfig#{<<"password">> => <<"wrongpass">>}
        ],

    lists:foreach(
        fun(Config) ->
            {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, Config}
            ),
            emqx_authn_test_lib:delete_config(?ResourceID),
            ?assertEqual(
                {error, {not_found, {chain, ?GLOBAL}}},
                emqx_authn_chains:list_authenticators(?GLOBAL)
            )
        end,
        InvalidConfigs
    ).

t_authenticate(_Config) ->
    ok = lists:foreach(
        fun(Sample) ->
            ct:pal("test_user_auth sample: ~p", [Sample]),
            test_user_auth(Sample)
        end,
        user_seeds()
    ).

test_user_auth(#{
    credentials := Credentials0,
    config_params := SpecificConfigParams,
    result := Result
}) ->
    AuthConfig = maps:merge(raw_ldap_auth_config(), SpecificConfigParams),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    Credentials = Credentials0#{
        listener => 'tcp:default',
        protocol => mqtt
    },

    ?assertEqual(Result, emqx_access_control:authenticate(Credentials)),

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
            <<"base_dn">> => <<"ou=testdevice,dc=emqx,dc=io">>,
            <<"filter">> => <<"(objectClass=mqttUser)">>
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

t_update_with_zone_and_listener(_Config) ->
    Tester = self(),
    ZoneListener = "default-tcp:default",
    meck:new(eldap, [passthrough, no_link, no_history]),
    meck:expect(eldap, search, fun(_Pid, SearchOptions) ->
        {_, Base} = lists:keyfind(base, 1, SearchOptions),
        case iolist_to_binary(Base) of
            <<"cn=checkalive">> ->
                {ok, []};
            Base1 ->
                {_, Filter} = lists:keyfind(filter, 1, SearchOptions),
                %% Filter is a deep tuple, we do not want to
                %% assert its layout, just to check if ZoneListener is in it
                Filter1 = iolist_to_binary(io_lib:format("~0p", [Filter])),
                Tester ! {check, Base1, Filter1},
                {error, 'noSuchObject'}
        end
    end),
    try
        CorrectConfig = raw_ldap_auth_config(),
        IncorrectConfig =
            CorrectConfig#{
                <<"base_dn">> => <<"ou=${zone}-${listener},dc=emqx,dc=io">>,
                <<"filter">> => <<"(objectClass=${zone}-${listener})">>
            },

        {ok, _} = emqx:update_config(
            ?PATH,
            {create_authenticator, ?GLOBAL, IncorrectConfig}
        ),

        {error, not_authorized} = emqx_access_control:authenticate(
            #{
                username => <<"mqttuser0001">>,
                password => <<"mqttuser0001">>,
                listener => 'tcp:default',
                zone => default,
                protocol => mqtt
            }
        ),
        receive
            {check, Base, Filter} ->
                ?assertMatch({match, _}, re:run(Base, ZoneListener)),
                ?assertMatch({match, _}, re:run(Filter, ZoneListener))
        after 5000 ->
            ct:fail("the eldap:search function is not called")
        end
    after
        meck:unload(eldap)
    end.

t_node_cache(_Config) ->
    Config = maps:merge(raw_ldap_auth_config(), #{
        <<"base_dn">> => <<"ou=${cert_common_name},dc=emqx,dc=io">>
    }),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok = emqx_authn_test_lib:enable_node_cache(true),
    Credentials = #{
        listener => 'tcp:default',
        protocol => mqtt,
        username => <<"mqttuser0001">>,
        password => <<"mqttuser0001">>,
        cn => <<"testdevice">>
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
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 4}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_ldap_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"server">> => ldap_server(),
        <<"base_dn">> => <<"ou=testdevice,dc=emqx,dc=io">>,
        <<"filter">> => <<"(uid=${username})">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 8,
        <<"method">> => #{
            <<"type">> => <<"bind">>,
            <<"bind_password">> => <<"${password}">>
        }
    }.

user_seeds() ->
    New = fun(Username, Password, Result) ->
        #{
            credentials => #{
                username => Username,
                password => Password
            },
            config_params => #{},
            result => Result
        }
    end,

    Normal = lists:map(
        fun(Idx) ->
            {erlang:iolist_to_binary(io_lib:format("mqttuser000~b", [Idx])), false}
        end,
        lists:seq(1, 5)
    ),

    Valid =
        lists:map(
            fun({Username, IsSuperuser}) ->
                New(Username, Username, {ok, #{is_superuser => IsSuperuser}})
            end,
            Normal
        ),
    [
        New(<<"mqttuser0008 (test)">>, <<"mqttuser0008 (test)">>, {ok, #{is_superuser => false}}),
        New(
            <<"mqttuser0009 \\test\\">>,
            <<"mqttuser0009 \\\\test\\\\">>,
            {ok, #{is_superuser => false}}
        ),
        %% Not exists
        New(<<"notexists">>, <<"notexists">>, {error, not_authorized}),
        %% Wrong Password
        New(<<"mqttuser0001">>, <<"wrongpassword">>, {error, bad_username_or_password})
        | Valid
    ].

ldap_server() ->
    iolist_to_binary(io_lib:format("~s:~B", [?LDAP_HOST, ?LDAP_DEFAULT_PORT])).

ldap_config() ->
    emqx_ldap_SUITE:ldap_config([]).
