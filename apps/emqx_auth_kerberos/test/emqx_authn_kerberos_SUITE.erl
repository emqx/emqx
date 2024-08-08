%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(CLI_NAME, "krb_authn_cli").
-define(CLI_PRINCIPAL, <<"krb_authn_cli@KDC.EMQX.NET">>).
-define(CLI_KEYTAB_FILE, <<"/var/lib/secret/krb_authn_cli.keytab">>).

-define(HOST, "127.0.0.1").
-define(PORT, 1883).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
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

t_create_invalid(_Config) ->
    InvalidConfig0 = raw_config(),
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
    init_auth(),

    {ok, Handler, CT1} = setup_cli(),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            properties =>
                #{
                    'Authentication-Method' => ?AUTHN_METHOD,
                    'Authentication-Data' => CT1
                },
            custom_auth_callbacks =>
                #{
                    init => auth_init(Handler),
                    handle_auth => fun auth_handle/3
                }
        }
    ),
    ?assertMatch({ok, _}, emqtt:connect(C)),
    stop_cli(Handler),
    ok.

t_authenticate_bad_props(_Config) ->
    erlang:process_flag(trap_exit, true),
    init_auth(),

    {ok, Handler, CT1} = setup_cli(),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            properties =>
                #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => CT1
                },
            custom_auth_callbacks =>
                #{
                    init => auth_init(Handler),
                    handle_auth => fun auth_handle/3
                }
        }
    ),

    ?assertMatch({error, {not_authorized, _}}, emqtt:connect(C)),
    stop_cli(Handler),
    ok.

t_authenticate_bad_token(_Config) ->
    erlang:process_flag(trap_exit, true),
    init_auth(),

    {ok, Handler, CT1} = setup_cli(),
    {ok, C} = emqtt:start_link(
        #{
            host => ?HOST,
            port => ?PORT,
            proto_ver => v5,
            properties =>
                #{
                    'Authentication-Method' => <<"SCRAM-SHA-512">>,
                    'Authentication-Data' => <<CT1/binary, "invalid">>
                },
            custom_auth_callbacks =>
                #{
                    init => auth_init(Handler),
                    handle_auth => fun auth_handle/3
                }
        }
    ),

    ?assertMatch({error, {not_authorized, _}}, emqtt:connect(C)),
    stop_cli(Handler),
    ok.

t_destroy(_) ->
    State = init_auth(),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    {ok, Handler, CT1} = setup_cli(),

    ?assertMatch(
        ignore,
        emqx_authn_mongodb:authenticate(
            #{
                auth_method => ?AUTHN_METHOD,
                auth_data => CT1,
                auth_cache => undefined
            },
            State
        )
    ),

    stop_cli(Handler),
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

%%------------------------------------------------------------------------------
%% Custom auth

auth_init(Handler) ->
    fun() -> #{handler => Handler, step => 1} end.

auth_handle(AuthState, Reason, Props) ->
    ct:pal(">>> auth packet received:\n  rc: ~p\n  props:\n  ~p", [Reason, Props]),
    do_auth_handle(AuthState, Reason, Props).

do_auth_handle(
    #{handler := Handler, step := Step} = AuthState0,
    continue_authentication,
    #{
        'Authentication-Method' := ?AUTHN_METHOD,
        'Authentication-Data' := ST
    }
) when Step =< 3 ->
    {ok, CT} = call_cli_agent(Handler, {step, ST}),
    AuthState = AuthState0#{step := Step + 1},
    OutProps = #{
        'Authentication-Method' => ?AUTHN_METHOD,
        'Authentication-Data' => CT
    },
    {continue, {?RC_CONTINUE_AUTHENTICATION, OutProps}, AuthState};
do_auth_handle(_AuthState, _Reason, _Props) ->
    {stop, protocol_error}.

%%------------------------------------------------------------------------------
%% Client Agent

setup_cli() ->
    Pid = erlang:spawn(fun() -> cli_agent_loop(#{}) end),
    {ok, CT1} = call_cli_agent(Pid, setup),
    {ok, Pid, CT1}.

call_cli_agent(Pid, Msg) ->
    Ref = erlang:make_ref(),
    erlang:send(Pid, {call, self(), Ref, Msg}),
    receive
        {Ref, Data} ->
            {ok, Data}
    after 3000 ->
        error("client agent timeout")
    end.

stop_cli(Pid) ->
    erlang:send(Pid, stop).

cli_agent_loop(State) ->
    receive
        stop ->
            ok;
        {call, From, Ref, Msg} ->
            {ok, Reply, State2} = cli_agent_handler(Msg, State),
            erlang:send(From, {Ref, Reply}),
            cli_agent_loop(State2)
    end.

cli_agent_handler(setup, State) ->
    ok = sasl_auth:kinit(?CLI_KEYTAB_FILE, ?CLI_PRINCIPAL),
    {ok, Client} = sasl_auth:client_new(?SERVICE, ?SVR_HOST, ?CLI_PRINCIPAL, ?CLI_NAME),
    {ok, {sasl_continue, CT1}} = sasl_auth:client_start(Client),
    {ok, CT1, State#{client => Client}};
cli_agent_handler({step, ST}, #{client := Client} = State) ->
    {ok, {_, CT}} = sasl_auth:client_step(Client, ST),
    {ok, CT, State}.
