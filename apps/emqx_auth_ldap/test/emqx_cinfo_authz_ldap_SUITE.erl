%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cinfo_authz_ldap_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_DEFAULT_PORT, 389).

all() ->
    emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()).

groups() ->
    emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx_auth,
            emqx_auth_ldap
        ],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    emqx_authz_test_lib:reset_authorizers(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_authz_test_lib:restore_authorizers(),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(Group, Config) ->
    [{test_case, emqx_authz_test_lib:get_case(Group, cases())} | Config].
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    ok = emqx_authn_test_lib:enable_node_cache(false),
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

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_run_case(Config) ->
    Case0 = ?config(test_case, Config),
    ok = setup_authenticator(Case0),
    Case = create_client_info(Case0),
    ok = maybe_make_delay(Case),
    ok = emqx_authz_test_lib:run_checks(Case).

%% NOTE
%% In the table cases we cheat a bit, performing authentication and authorization
%% via internal API.
%% This testcase is to verify that the whole flow works.
t_integration(_Config) ->
    AuthnConfig = maps:merge(
        raw_ldap_authn_config(),
        #{
            <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>
        }
    ),
    {ok, _} = emqx:update_config(
        [authentication],
        {create_authenticator, ?GLOBAL, AuthnConfig}
    ),
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {client_id, <<"mqttuser0001">>},
        {username, <<"mqttuser0001">>},
        {password, <<"mqttuser0001">>},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),
    ?assertMatch(
        {ok, _, [1]},
        emqtt:subscribe(C, <<"mqttuser0001/sub/1">>, 1)
    ),
    ?assertMatch(
        {ok, _, [?RC_NOT_AUTHORIZED]},
        emqtt:subscribe(C, <<"mqttuser0001/subXX/1">>, 1)
    ),
    ok = emqtt:disconnect(C).

%%------------------------------------------------------------------------------
%% Cases
%%------------------------------------------------------------------------------

cases() ->
    [
        #{
            name => simpe_hash,
            credentials => #{
                username => <<"mqttuser0001">>,
                password => <<"mqttuser0001">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0001">>},
            setup => #{
                <<"publish_attribute">> => <<"mqttPublishTopic">>,
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"all_attribute">> => <<"mqttPubSubTopic">>,
                <<"acl_rule_attribute">> => <<"mqttAclRule">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/+">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/#">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/+">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/#">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/+">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/#">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/+">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/#">>}
            ]
        },

        #{
            name => simpe_unmatched_hash,
            credentials => #{
                username => <<"mqttuser0001">>,
                password => <<"mqttuser0001">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0001">>},
            setup => #{
                <<"publish_attribute">> => <<"mqttPublishTopic">>,
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"all_attribute">> => <<"mqttPubSubTopic">>,
                <<"acl_rule_attribute">> => <<"mqttAclRule">>
            },
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0001/req/mqttuser0001/x">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0001/req/mqttuser0002/+">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/req/+/mqttuser0002">>}
            ]
        },

        #{
            name => raw_rules_hash,
            credentials => #{
                username => <<"mqttuser0002">>,
                password => <<"mqttuser0002">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0002">>},
            setup => #{
                <<"publish_attribute">> => <<"mqttPublishTopic">>,
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"all_attribute">> => <<"mqttPubSubTopic">>,
                <<"acl_rule_attribute">> => <<"mqttAclRule">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule1/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule2/2">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule3/3">>}
            ]
        },

        #{
            name => simple_and_raw_bind,
            credentials => #{
                username => <<"mqttuser0002">>,
                password => <<"mqttuser0002">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0002">>},
            setup => #{
                <<"publish_attribute">> => <<"mqttPublishTopic">>,
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"all_attribute">> => <<"mqttPubSubTopic">>,
                <<"acl_rule_attribute">> => <<"mqttAclRule">>,
                <<"method">> => #{
                    <<"type">> => <<"bind">>,
                    <<"bind_password">> => <<"${password}">>
                }
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule1/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule2/2">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule3/3">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/pub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/sub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0002/pubsub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/pubsub/1">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/pubXXX/1">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/subXXX/1">>}
            ]
        },

        #{
            %% If we do not specify acl attribute names in LDAP authenticator,
            %% the rules should not be fetched.
            %% So all actions should be denied.
            name => empty_attribute_names,
            credentials => #{
                username => <<"mqttuser0002">>,
                password => <<"mqttuser0002">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0002">>},
            setup => #{},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule1/1">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule2/2">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/rawrule3/3">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/pub/1">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/sub/1">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/pubsub/1">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/pubsub/1">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0002/pubXXX/1">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/subXXX/1">>}
            ]
        },

        #{
            name => simpe_hash_with_ttl_expired,
            credentials => #{
                username => <<"mqttuser0002">>,
                password => <<"mqttuser0002">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0002">>},
            setup => #{
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"acl_ttl_attribute">> => <<"mqttAclTtl">>
            },
            checks => [
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/sub/1">>}
            ]
        },

        #{
            name => simpe_hash_with_ttl_not_expired,
            credentials => #{
                username => <<"mqttuser0002">>,
                password => <<"mqttuser0002">>,
                listener => 'tcp:default',
                protocol => mqtt
            },
            client_info => #{username => <<"mqttuser0002">>},
            setup => #{
                <<"subscribe_attribute">> => <<"mqttSubscriptionTopic">>,
                <<"acl_ttl_attribute">> => <<"mqttAclTtl">>
            },
            checks => [
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0002/sub/1">>}
            ]
        }
    ].

maybe_make_delay(#{name := simpe_hash_with_ttl_expired}) ->
    timer:sleep(2000);
maybe_make_delay(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_authenticator(#{setup := ExtraConfig} = _Case) ->
    Config = maps:merge(raw_ldap_authn_config(), ExtraConfig),
    {ok, _} = emqx:update_config(
        [authentication],
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok.

create_client_info(#{credentials := Credentials, client_info := ClientInfo0} = Case) ->
    {ok, Data} = emqx_access_control:authenticate(Credentials),
    ClientInfo = maps:merge(ClientInfo0, Data),
    Case#{client_info => ClientInfo}.

raw_ldap_authn_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"server">> => ldap_server(),
        <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 8
    }.

ldap_server() ->
    iolist_to_binary(io_lib:format("~s:~B", [?LDAP_HOST, ?LDAP_DEFAULT_PORT])).
