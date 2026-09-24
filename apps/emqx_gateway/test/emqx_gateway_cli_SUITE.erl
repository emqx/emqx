%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% The config with json format for mqtt-sn gateway
-define(CONF_MQTTSN,
    """
    {"idle_timeout": "30s",
     "enable_stats": true,
     "mountpoint": "mqttsn/",
     "gateway_id": 1,
     "broadcast": true,
     "enable_qos3": true,
     "predefined": [{"id": 1001, "topic": "pred/a"}],
     "listeners":
        [{"type": "udp",
          "name": "ct",
          "enable_authn": false,
          "bind": "1884"
        }]
    }
    """
).

-define(AUTH_SECRET, <<"gateway-cli-auth-secret-sentinel">>).
-define(HTTP_AUTHORIZATION, <<"Bearer gateway-cli-http-sentinel">>).
-define(LDAP_PASSWORD, <<"gateway-cli-ldap-password-sentinel">>).
-define(LDAP_BIND_PASSWORD, <<"gateway-cli-ldap-bind-password-sentinel">>).
-define(OVERRIDE_PASSWORD, <<"gateway-cli-password-sentinel">>).

-import(emqx_gateway_test_utils, [sn_client_connect/1, sn_client_disconnect/1]).

%%--------------------------------------------------------------------
%% Setup
%%--------------------------------------------------------------------

all() -> [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_auth,
            emqx_auth_jwt,
            emqx_auth_http,
            emqx_auth_ldap,
            emqx_gateway
        ],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(_, Conf) ->
    Self = self(),
    ok = meck:new(emqx_ctl, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_ctl,
        usage,
        fun(L) -> emqx_ctl:format_usage(L) end
    ),
    ok = meck:expect(
        emqx_ctl,
        print,
        fun(Fmt) ->
            Self ! {fmt, emqx_ctl:format(Fmt, [])}
        end
    ),
    ok = meck:expect(
        emqx_ctl,
        print,
        fun(Fmt, Args) ->
            Self ! {fmt, emqx_ctl:format(Fmt, Args)}
        end
    ),
    Conf.

end_per_testcase(_, _) ->
    meck:unload([emqx_ctl]),
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

%% TODO:

t_load_unload(_) ->
    ok.

t_gateway_registry_usage(_) ->
    ?assertEqual(
        ["gateway-registry list # List all registered gateways\n"],
        emqx_gateway_cli:'gateway-registry'(usage)
    ).

t_gateway_registry_list(_) ->
    emqx_gateway_cli:'gateway-registry'(["list"]),
    %% TODO: assert it.
    _ = acc_print().

t_gateway_usage(_) ->
    ?assertEqual(
        [
            "gateway list                     # List all gateway\n",
            "gateway lookup <Name>            # Lookup a gateway detailed information\n",
            "gateway load   <Name> <JsonConf> # Load a gateway with config\n",
            "gateway unload <Name>            # Unload the gateway\n",
            "gateway stop   <Name>            # Stop the gateway\n",
            "gateway start  <Name>            # Start the gateway\n"
        ],
        emqx_gateway_cli:gateway(usage)
    ).

t_redact(_) ->
    JSON = binary_to_list(
        emqx_utils_json:encode(#{
            <<"mountpoint">> => <<"mqttsn/">>,
            <<"clientinfo_override">> => #{<<"password">> => <<"secret">>},
            <<"api_secret">> => <<"api-secret">>
        })
    ),
    ["load", "mqttsn", RedactedJSON] =
        emqx_gateway_cli:gateway_audit_args(["load", "mqttsn", JSON]),
    ?assertEqual(
        #{
            <<"mountpoint">> => <<"mqttsn/">>,
            <<"clientinfo_override">> => #{<<"password">> => <<"******">>},
            <<"api_secret">> => <<"******">>
        },
        emqx_utils_json:decode(RedactedJSON)
    ),
    ?assertEqual(
        ["load", "mqttsn", "******"],
        emqx_gateway_cli:gateway_audit_args(["load", "mqttsn", "invalid-json"])
    ),
    ?assertEqual(
        ["load", "mqttsn", "******"],
        emqx_gateway_cli:gateway_audit_args(["load", "mqttsn", "\"secret\""])
    ),
    ?assertEqual(
        ["lookup", "mqttsn"],
        emqx_gateway_cli:gateway_audit_args(["lookup", "mqttsn"])
    ),
    ?assertEqual(
        [],
        [
            Cmd
         || {Cmd, emqx_gateway_cli, _} <- emqx_ctl:get_commands(),
            lists:suffix("_audit_args", atom_to_list(Cmd))
        ]
    ).

t_gateway_lookup_redacts_credentials(Config) ->
    BindPasswordFile = filename:join(?config(priv_dir, Config), "ldap-bind-password"),
    ok = file:write_file(BindPasswordFile, ?LDAP_BIND_PASSWORD),
    BindPasswordFileURI = iolist_to_binary(["file://", BindPasswordFile]),
    Cases = [
        {
            jwt_authentication(),
            [?AUTH_SECRET, ?OVERRIDE_PASSWORD],
            [<<"hmac-based">>]
        },
        {
            http_authentication(),
            [?HTTP_AUTHORIZATION, ?OVERRIDE_PASSWORD],
            [<<"gateway-cli-visible-header">>]
        },
        {
            ldap_authentication(BindPasswordFileURI),
            [?LDAP_PASSWORD, ?LDAP_BIND_PASSWORD, ?OVERRIDE_PASSWORD],
            [<<"gateway-cli-ldap-base">>, <<"bind_password">>]
        }
    ],
    lists:foreach(
        fun({Authentication, Secrets, VisibleValues}) ->
            assert_gateway_lookup_redacts_credentials(Authentication, Secrets, VisibleValues)
        end,
        Cases
    ).

assert_gateway_lookup_redacts_credentials(Authentication, Secrets, VisibleValues) ->
    Conf = #{
        <<"idle_timeout">> => <<"30s">>,
        <<"mountpoint">> => <<"mqttsn/">>,
        <<"clientinfo_override">> => #{<<"password">> => ?OVERRIDE_PASSWORD},
        <<"authentication">> => Authentication,
        <<"listeners">> => [
            #{
                <<"type">> => <<"udp">>,
                <<"name">> => <<"ct">>,
                <<"bind">> => <<"1884">>
            }
        ]
    },
    JSON = binary_to_list(emqx_utils_json:encode(Conf)),
    emqx_gateway_cli:gateway(["load", "mqttsn", JSON]),
    ?assertEqual("ok\n", acc_print()),
    try
        emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
        assert_safe_gateway_output(acc_print(), Secrets, VisibleValues)
    after
        emqx_gateway_cli:gateway(["unload", "mqttsn"]),
        _ = acc_print(),
        ok = emqx_authn_chains:delete_chain(emqx_gateway_utils:global_chain(mqttsn))
    end.

assert_safe_gateway_output(Output0, Secrets, VisibleValues) ->
    Output = iolist_to_binary(Output0),
    ?assertNotEqual(nomatch, binary:match(Output, <<"mqttsn/">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"******">>)),
    lists:foreach(
        fun(Secret) -> ?assertEqual(nomatch, binary:match(Output, Secret)) end,
        Secrets
    ),
    lists:foreach(
        fun(Value) -> ?assertNotEqual(nomatch, binary:match(Output, Value)) end,
        VisibleValues
    ).

jwt_authentication() ->
    #{
        <<"mechanism">> => <<"jwt">>,
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"secret">> => ?AUTH_SECRET,
        <<"secret_base64_encoded">> => false
    }.

http_authentication() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"http">>,
        <<"enable">> => false,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:1/auth">>,
        <<"headers">> => #{
            <<"Authorization">> => ?HTTP_AUTHORIZATION,
            <<"X-Test-Header">> => <<"gateway-cli-visible-header">>
        }
    }.

ldap_authentication(BindPassword) ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"enable">> => false,
        <<"server">> => <<"127.0.0.1:1">>,
        <<"base_dn">> => <<"ou=gateway-cli-ldap-base,dc=emqx,dc=io">>,
        <<"filter">> => <<"(uid=${username})">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => ?LDAP_PASSWORD,
        <<"method">> => #{
            <<"type">> => <<"bind">>,
            <<"bind_password">> => BindPassword
        }
    }.

t_gateway_list(_) ->
    emqx_gateway_cli:gateway(["list"]),
    %% TODO: assert it.
    _ = acc_print(),

    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["list"]),
    %% TODO: assert it.
    _ = acc_print(),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()).

t_gateway_load_unload_lookup(_) ->
    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    ?assertEqual("undefined\n", acc_print()),

    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    %% TODO: bad config name, format???

    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    %% TODO: assert it. for example:
    %% name: mqttsn
    %% status: running
    %% created_at: 2022-01-05T14:40:20.039+08:00
    %% started_at: 2022-01-05T14:42:37.894+08:00
    %% config: #{broadcast => false,enable => true,enable_qos3 => true,
    %%           enable_stats => true,gateway_id => 1,idle_timeout => 30000,
    %%           mountpoint => <<>>,predefined => []}
    _ = acc_print(),

    emqx_gateway_cli:gateway(["load", "mqttsn", "{}"]),
    ?assertEqual(
        "Error: The mqttsn gateway already loaded\n",
        acc_print()
    ),

    emqx_gateway_cli:gateway(["load", "bad-gw-name", "{}"]),
    %% TODO: assert it. for example:
    %% Error: Illegal gateway name
    _ = acc_print(),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% Always return ok, even the gateway has unloaded
    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["lookup", "mqttsn"]),
    ?assertEqual("undefined\n", acc_print()).

t_gateway_start_stop(_) ->
    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["stop", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% duplicated stop gateway, return ok
    emqx_gateway_cli:gateway(["stop", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["start", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),
    %% duplicated start gateway, return ok
    emqx_gateway_cli:gateway(["start", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()).

t_gateway_clients_usage(_) ->
    ?assertEqual(
        [
            "gateway-clients list   <Name>            "
            "# List all clients for a gateway\n",
            "gateway-clients lookup <Name> <ClientId> "
            "# Lookup the Client Info for specified client\n",
            "gateway-clients kick   <Name> <ClientId> "
            "# Kick out a client\n"
        ],
        emqx_gateway_cli:'gateway-clients'(usage)
    ).

t_gateway_clients(_) ->
    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    Socket = sn_client_connect(<<"client1">>),

    _ = emqx_gateway_cli:'gateway-clients'(["list", "mqttsn"]),
    ClientDesc1 = acc_print(),

    _ = emqx_gateway_cli:'gateway-clients'(["lookup", "mqttsn", "client1"]),
    ClientDesc2 = acc_print(),
    ?assertEqual(ClientDesc1, ClientDesc2),

    sn_client_disconnect(Socket),
    timer:sleep(500),

    _ = emqx_gateway_cli:'gateway-clients'(["lookup", "mqttsn", "bad-client"]),
    ?assertEqual("Not Found.\n", acc_print()),

    _ = emqx_gateway_cli:'gateway-clients'(["lookup", "bad-gw", "bad-client"]),
    ?assertEqual("Bad Gateway Name.\n", acc_print()),

    _ = emqx_gateway_cli:'gateway-clients'(["list", "mqttsn"]),
    %% no print for empty client list

    _ = emqx_gateway_cli:'gateway-clients'(["list", "bad-gw"]),
    ?assertEqual("Bad Gateway Name.\n", acc_print()),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()).

t_gateway_clients_kick(_) ->
    emqx_gateway_cli:gateway(["load", "mqttsn", ?CONF_MQTTSN]),
    ?assertEqual("ok\n", acc_print()),

    Socket = sn_client_connect(<<"client1">>),

    _ = emqx_gateway_cli:'gateway-clients'(["list", "mqttsn"]),
    _ = acc_print(),

    _ = emqx_gateway_cli:'gateway-clients'(["kick", "mqttsn", "bad-client"]),
    ?assertEqual("Not Found.\n", acc_print()),

    _ = emqx_gateway_cli:'gateway-clients'(["kick", "mqttsn", "client1"]),
    ?assertEqual("ok\n", acc_print()),

    sn_client_disconnect(Socket),

    emqx_gateway_cli:gateway(["unload", "mqttsn"]),
    ?assertEqual("ok\n", acc_print()).

t_gateway_metrcis_usage(_) ->
    ?assertEqual(
        [
            "gateway-metrics <Name> "
            "# List all metrics for a gateway\n"
        ],
        emqx_gateway_cli:'gateway-metrics'(usage)
    ).

t_gateway_metrcis(_) ->
    ok.

acc_print() ->
    lists:concat(lists:reverse(acc_print([]))).

acc_print(Acc) ->
    receive
        {fmt, S} -> acc_print([S | Acc])
    after 200 ->
        Acc
    end.
