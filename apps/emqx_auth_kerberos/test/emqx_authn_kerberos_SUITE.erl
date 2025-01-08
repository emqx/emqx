%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_kerberos_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_auth_kerberos.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-define(PATH, [authentication]).

-define(INVALID_SVR_PRINCIPAL, <<"not-exists/erlang.emqx.nett@KDC.EMQX.NET">>).

-define(SVR_HOST, "erlang.emqx.net").
-define(SVR_PRINCIPAL, <<"mqtt/erlang.emqx.net@KDC.EMQX.NET">>).
-define(SVR_KEYTAB_FILE, <<"/var/lib/secret/erlang.keytab">>).

-define(CLI_PRINCIPAL, <<"krb_authn_cli@KDC.EMQX.NET">>).
-define(CLI_KEYTAB_FILE, <<"/var/lib/secret/krb_authn_cli.keytab">>).

-define(HOST, "127.0.0.1").
-define(PORT, 1883).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = sasl_auth:kinit(?CLI_KEYTAB_FILE, ?CLI_PRINCIPAL),
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_kerberos], #{
        work_dir => ?config(priv_dir, Config)
    }),
    IdleTimeout = emqx_config:get([mqtt, idle_timeout]),
    [{apps, Apps}, {idle_timeout, IdleTimeout} | Config].

end_per_suite(Config) ->
    ok = emqx_config:put([mqtt, idle_timeout], ?config(idle_timeout, Config)),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    ValidConfig = raw_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, ValidConfig}
    ),

    {ok, [#{provider := emqx_authn_kerberos}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL).

t_disable_without_kinit(_Config) ->
    ID = <<"gssapi:kerberos">>,
    Create = raw_config(),
    Disable = Create#{<<"enable">> => false},
    DisableBadPrincipal = Disable#{<<"principal">> => <<"mqtt/a.b@REALM">>},
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Create}
    ),
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, ID, Disable}
    ),
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, ID, DisableBadPrincipal}
    ),
    {ok, [#{provider := emqx_authn_kerberos}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    %% cover the case when keytab_file is not provided
    InvalidConfig0 = maps:remove(<<"keytab_file">>, raw_config()),
    InvalidConfig = InvalidConfig0#{<<"principal">> := ?INVALID_SVR_PRINCIPAL},

    {error, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, InvalidConfig}
    ),

    ?assertEqual(
        {error, {not_found, {chain, ?GLOBAL}}},
        emqx_authn_chains:list_authenticators(?GLOBAL)
    ).

t_authenticate(_Config) ->
    _ = init_auth(),
    Args = emqx_authn_kerberos_client:auth_args(?CLI_KEYTAB_FILE, ?CLI_PRINCIPAL),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            custom_auth_callbacks =>
                #{
                    init => {fun emqx_authn_kerberos_client:auth_init/1, Args},
                    handle_auth => fun emqx_authn_kerberos_client:auth_handle/3
                }
        }
    ),
    ?assertMatch({ok, _}, emqtt:connect(C)),
    ok.

t_authenticate_bad_method(_Config) ->
    _ = init_auth(),
    %% The method is GSSAPI-KERBEROS, sending just "GSSAPI" will fail
    Method = <<"GSSAPI">>,
    Args = emqx_authn_kerberos_client:auth_args(?CLI_KEYTAB_FILE, ?CLI_PRINCIPAL, Method),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            custom_auth_callbacks =>
                #{
                    init => {fun emqx_authn_kerberos_client:auth_init/1, Args},
                    handle_auth => fun emqx_authn_kerberos_client:auth_handle/3
                }
        }
    ),
    unlink(C),
    ?assertMatch({error, {not_authorized, _}}, emqtt:connect(C)),
    ok.

t_authenticate_bad_token(_Config) ->
    _ = init_auth(),
    %% Malform the first client token to test auth failure.
    Args = emqx_authn_kerberos_client:auth_args(
        ?CLI_KEYTAB_FILE, ?CLI_PRINCIPAL, ?AUTHN_METHOD, <<"badtoken">>
    ),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            custom_auth_callbacks =>
                #{
                    init => {fun emqx_authn_kerberos_client:auth_init/1, Args},
                    handle_auth => fun emqx_authn_kerberos_client:auth_handle/3
                }
        }
    ),
    unlink(C),
    ?assertMatch({error, {not_authorized, _}}, emqtt:connect(C)),
    ok.

t_destroy(_) ->
    State = init_auth(),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    ?assertMatch(
        ignore,
        emqx_authn_mongodb:authenticate(
            #{
                auth_method => ?AUTHN_METHOD,
                auth_data => <<"anydata">>,
                auth_cache => undefined
            },
            State
        )
    ),

    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_config() ->
    #{
        <<"mechanism">> => <<"gssapi">>,
        <<"backend">> => <<"kerberos">>,
        <<"principal">> => ?SVR_PRINCIPAL,
        <<"keytab_file">> => ?SVR_KEYTAB_FILE
    }.

init_auth() ->
    Config = raw_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    {ok, [#{state := State}]} = emqx_authn_chains:list_authenticators(?GLOBAL),

    State.
