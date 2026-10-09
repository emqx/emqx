%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_conf_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_config_SUITE, [prepare_conf_file/3]).

-define(READONLY_ROOT_KEYS, [rpc, node]).

-define(REDACTED, <<"******">>).
-define(JWT_SECRET, <<"conf-cli-jwt-secret">>).
-define(HTTP_AUTHORIZATION, <<"Bearer conf-cli-http-authorization">>).
-define(LDAP_BIND_PASSWORD, <<"conf-cli-ldap-bind-password">>).
-define(LDAP_BASE_DN, <<"ou=conf-cli,dc=emqx,dc=io">>).
-define(VISIBLE_HEADER_VALUE, <<"conf-cli-visible-header">>).
-define(FILE_SECRET_CONTENT, <<"conf-cli-file-secret-content">>).
-define(NS_NAME, <<"conf_cli_ns">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                before_start => fun(App, AppOpts) ->
                    ok = emqx_config:add_allowed_namespaced_config_root(<<"mqtt">>),
                    emqx_cth_suite:inhibit_config_loader(App, AppOpts)
                end
            }},
            emqx_conf,
            emqx_auth_redis,
            emqx_schema_registry,
            emqx_connector,
            {emqx_auth, #{after_start => fun() -> ok end}},
            emqx_auth_jwt,
            emqx_auth_http,
            emqx_auth_ldap,
            emqx_management
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_load_config(Config) ->
    Authz = authorization,
    Conf = emqx_conf:get_raw([Authz]),
    ?assertEqual(
        [emqx_authz_schema:default_authz()],
        maps:get(<<"sources">>, Conf)
    ),
    %% set sources to []
    ConfBin = hocon_pp:do(#{<<"authorization">> => #{<<"sources">> => []}}, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile]),
    ?assertMatch(#{<<"sources">> := []}, emqx_conf:get_raw([Authz])),

    ConfBin0 = hocon_pp:do(#{<<"authorization">> => Conf#{<<"sources">> => []}}, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile0]),
    ?assertEqual(Conf#{<<"sources">> => []}, emqx_conf:get_raw([Authz])),

    %% remove sources, it will reset to default file source.
    ConfBin1 = hocon_pp:do(#{<<"authorization">> => maps:remove(<<"sources">>, Conf)}, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile1]),
    Default = [emqx_authz_schema:default_authz()],
    ?assertEqual(Conf#{<<"sources">> => Default}, emqx_conf:get_raw([Authz])),
    %% reset
    ConfBin2 = hocon_pp:do(#{<<"authorization">> => Conf}, #{}),
    ConfFile2 = prepare_conf_file(?FUNCTION_NAME, ConfBin2, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile2]),
    ?assertEqual(
        Conf#{<<"sources">> => [emqx_authz_schema:default_authz()]},
        emqx_conf:get_raw([Authz])
    ),
    ?assertMatch({error, #{cause := not_a_file}}, emqx_conf_cli:conf(["load", "non-exist-file"])),
    EmptyFile = "empty_file.conf",
    ok = file:write_file(EmptyFile, <<>>),
    ?assertMatch({error, #{cause := empty_hocon_file}}, emqx_conf_cli:conf(["load", EmptyFile])),
    ok = file:delete(EmptyFile),
    ok.

t_remove_config(Config) ->
    Conf0 = #{
        <<"zones">> => #{<<"my-zone">> => #{<<"mqtt">> => #{<<"keepalive_multiplier">> => 10}}}
    },
    ConfBin0 = hocon_pp:do(Conf0, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile0]),
    %% Sanity check
    ?assertMatch(
        #{<<"mqtt">> := #{<<"keepalive_multiplier">> := 10}},
        emqx_conf:get_raw([<<"zones">>, <<"my-zone">>])
    ),
    ?assertMatch(ok, emqx_conf_cli:conf(["remove", "zones.my-zone"])),
    ?assertMatch(not_found, emqx_conf:get_raw([<<"zones">>, <<"my-zone">>], not_found)),
    ok.

t_conflict_mix_conf(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    Redis = #{
        <<"backend">> => <<"redis">>,
        <<"database">> => 0,
        <<"password_hash_algorithm">> =>
            #{<<"name">> => <<"sha256">>, <<"salt_position">> => <<"prefix">>},
        <<"pool_size">> => 8,
        <<"cmd">> => <<"HMGET mqtt_user:${username} password_hash salt">>,
        <<"enable">> => false,
        <<"mechanism">> => <<"password_based">>,
        %% password_hash_algorithm {name = sha256, salt_position = suffix}
        <<"redis_type">> => <<"single">>,
        <<"server">> => <<"127.0.0.1:6379">>,
        <<"precondition">> => <<>>
    },
    AuthN = #{<<"authentication">> => [Redis]},
    ConfBin = hocon_pp:do(AuthN, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    %% init with redis sources
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile]),
    [RedisRaw] = emqx_conf:get_raw([authentication]),
    ?assertEqual(
        lists:sort(maps:to_list(Redis)),
        lists:sort(maps:to_list(maps:remove(<<"ssl">>, RedisRaw))),
        {Redis, RedisRaw}
    ),
    %% change redis type from single to cluster
    %% the server field will become servers field
    RedisCluster = maps:without([<<"server">>, <<"database">>], Redis#{
        <<"redis_type">> => cluster,
        <<"servers">> => [<<"127.0.0.1:6379">>]
    }),
    AuthN1 = AuthN#{<<"authentication">> => [RedisCluster]},
    ConfBin1 = hocon_pp:do(AuthN1, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
    {error, Reason} = emqx_conf_cli:conf(["load", "--merge", ConfFile1]),
    ?assertNotEqual(
        nomatch,
        binary:match(
            Reason,
            [<<"Tips: There may be some conflicts in the new configuration under">>]
        ),
        Reason
    ),
    %% use replace to change redis type from single to cluster
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile1])),
    %% clean up
    ConfBinInit = hocon_pp:do(#{<<"authentication">> => AuthNInit}, #{}),
    ConfFileInit = prepare_conf_file(?FUNCTION_NAME, ConfBinInit, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFileInit]),
    ok.

t_config_handler_hook_failed(Config) ->
    Listeners =
        #{
            <<"listeners">> => #{
                <<"ssl">> => #{
                    <<"default">> => #{
                        <<"ssl_options">> => #{
                            <<"keyfile">> => <<"">>
                        }
                    }
                }
            }
        },
    ConfBin = hocon_pp:do(Listeners, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    {error, Reason} = emqx_conf_cli:conf(["load", "--merge", ConfFile]),
    %% the hook failed with empty keyfile
    ?assertEqual(
        nomatch,
        binary:match(Reason, [
            <<"Tips: There may be some conflicts in the new configuration under">>
        ]),
        Reason
    ),
    ?assertNotEqual(
        nomatch,
        binary:match(Reason, [
            <<"{bad_ssl_config,#{reason => pem_file_path_or_string_is_required">>
        ]),
        Reason
    ),
    ok.

t_load_readonly(Config) ->
    Base0 = base_conf(),
    Mqtt = #{<<"mqtt">> => emqx_conf:get_raw([mqtt])},
    lists:foreach(
        fun(Key) ->
            KeyBin = atom_to_binary(Key),
            Conf = emqx_conf:get_raw([Key]),
            ConfBin0 = hocon_pp:do(maps:merge(Mqtt, #{KeyBin => Conf}), #{}),
            ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
            Msg = iolist_to_binary(
                io_lib:format(
                    "Cannot update read-only key '~s'.", [KeyBin]
                )
            ),
            ?assertEqual(
                {error, Msg},
                emqx_conf_cli:conf(["load", ConfFile0]),
                ConfFile0
            ),
            %% reload etc/emqx.conf changed readonly keys
            Base1 = maps:merge(Base0, Mqtt),
            ConfBin1 = hocon_pp:do(Base1#{KeyBin => changed(Key)}, #{}),
            ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
            application:set_env(emqx, config_files, [ConfFile1]),
            ?assertMatch(ok, emqx_conf_cli:conf(["reload"])),
            %% Don't update readonly key
            ?assertEqual(Conf, emqx_conf:get_raw([Key]))
        end,
        ?READONLY_ROOT_KEYS
    ),
    ok.

t_error_schema_check(Config) ->
    Base = #{
        %% bad multiplier
        <<"mqtt">> => #{<<"keepalive_multiplier">> => -1},
        <<"zones">> => #{<<"my-zone">> => #{<<"mqtt">> => #{<<"keepalive_multiplier">> => 10}}}
    },
    ConfBin0 = hocon_pp:do(Base, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ?assertMatch({error, _}, emqx_conf_cli:conf(["load", ConfFile0])),
    %% zones is not updated because of error
    ?assertEqual(#{}, emqx_config:get_raw([zones])),
    ok.

t_reload_etc_emqx_conf_not_persistent(Config) ->
    Mqtt = emqx_conf:get_raw([mqtt]),
    Base = base_conf(),
    Conf = Base#{<<"mqtt">> => Mqtt#{<<"keepalive_multiplier">> => 3}},
    ConfBin = hocon_pp:do(Conf, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    application:set_env(emqx, config_files, [ConfFile]),
    ok = emqx_conf_cli:conf(["reload"]),
    ?assertEqual(3, emqx:get_config([mqtt, keepalive_multiplier])),
    ?assertNotEqual(
        3,
        emqx_utils_maps:deep_get(
            [<<"mqtt">>, <<"keepalive_multiplier">>],
            emqx_config:read_override_conf(#{}),
            undefined
        )
    ),
    ok.

t_update_cluster_readonly(Config) ->
    ConfBin = hocon_pp:do(
        #{
            %% NOTE: Initially from `emqx_cluster_link_config_SUITE`, thus `links`.
            <<"cluster">> => #{
                <<"links">> => [],
                <<"autoclean">> => <<"12h">>
            }
        },
        #{}
    ),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    ?assertMatch(
        {error, <<"Cannot update read-only key 'cluster.autoclean'.">>},
        emqx_conf_cli:conf(["load", ConfFile])
    ).

-doc """
Loading one listener field with `--merge` keeps the other stored fields of
that listener, and applies the loaded field.
""".
t_merge_keeps_omitted_listener_fields(Config) ->
    Path = [listeners, tcp, merge_test],
    Listener = #{
        <<"enable">> => false,
        <<"bind">> => <<"127.0.0.1:31883">>,
        <<"parse_unit">> => <<"chunk">>,
        <<"acceptors">> => 4,
        <<"max_connections">> => 100
    },
    ok = load_conf(merge, listener_conf(Listener), Config),
    ok = load_conf(
        merge,
        listener_conf(#{<<"tcp_options">> => #{<<"active_n">> => 50}}),
        Config
    ),
    ?assertMatch(
        #{
            bind := {{127, 0, 0, 1}, 31883},
            parse_unit := chunk,
            acceptors := 4,
            max_connections := 100,
            tcp_options := #{active_n := 50}
        },
        emqx_conf:get(Path)
    ),
    {ok, _} = emqx_conf:remove(Path, #{override_to => cluster}),
    ok.

-doc """
Loading one `mqtt` field with `--merge` keeps the other stored `mqtt` fields.
Loading the same file with `--replace` resets the omitted fields to defaults.
""".
t_merge_keeps_omitted_mqtt_fields(Config) ->
    MqttInit = emqx_conf:get_raw([mqtt]),
    ok = load_conf(
        merge,
        #{<<"mqtt">> => #{<<"idle_timeout">> => <<"30s">>, <<"max_inflight">> => 64}},
        Config
    ),
    OneField = #{<<"mqtt">> => #{<<"max_packet_size">> => <<"2MB">>}},
    ok = load_conf(merge, OneField, Config),
    ?assertMatch(
        #{idle_timeout := 30_000, max_inflight := 64, max_packet_size := 2_097_152},
        emqx_conf:get([mqtt])
    ),
    ok = load_conf(replace, OneField, Config),
    ?assertMatch(
        #{idle_timeout := 15_000, max_inflight := 32, max_packet_size := 2_097_152},
        emqx_conf:get([mqtt])
    ),
    ok = load_conf(replace, #{<<"mqtt">> => MqttInit}, Config),
    ok.

-doc """
Loading one `authorization` field with `--merge` keeps the other stored
`authorization` fields and the stored sources.
""".
t_merge_keeps_omitted_authz_fields(Config) ->
    AuthzInit = emqx_conf:get_raw([authorization]),
    [FileSource] = maps:get(<<"sources">>, AuthzInit),
    Authz = AuthzInit#{
        <<"deny_action">> => <<"disconnect">>,
        <<"cache">> => #{<<"max_size">> => 64},
        <<"sources">> => [FileSource#{<<"enable">> => false}]
    },
    ok = load_conf(replace, #{<<"authorization">> => Authz}, Config),
    ok = load_conf(merge, #{<<"authorization">> => #{<<"no_match">> => <<"deny">>}}, Config),
    ?assertMatch(
        #{
            no_match := deny,
            deny_action := disconnect,
            cache := #{max_size := 64},
            sources := [#{type := file, enable := false}]
        },
        emqx_conf:get([authorization])
    ),
    ok = load_conf(replace, #{<<"authorization">> => AuthzInit}, Config),
    ok.

-doc """
Loading an authenticator with `--merge` keeps the stored fields that the
loaded authenticator omits, and applies the loaded fields.
""".
t_merge_keeps_omitted_authn_fields(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    Redis = #{
        <<"backend">> => <<"redis">>,
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => false,
        <<"redis_type">> => <<"single">>,
        <<"server">> => <<"127.0.0.1:6379">>,
        <<"cmd">> => <<"HMGET mqtt_user:${username} password_hash salt">>
    },
    ok = load_conf(replace, #{<<"authentication">> => [Redis#{<<"pool_size">> => 4}]}, Config),
    NewCmd = <<"HMGET mqtt_user:${clientid} password_hash salt">>,
    ok = load_conf(merge, #{<<"authentication">> => [Redis#{<<"cmd">> => NewCmd}]}, Config),
    ?assertMatch(
        [#{<<"pool_size">> := 4, <<"cmd">> := NewCmd}],
        emqx_conf:get_raw([authentication])
    ),
    ok = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config),
    ok.

-doc """
Loading a field under its alias name with `--merge` overrides the stored
value under the canonical name.
""".
t_merge_alias_over_stored_field(Config) ->
    Path = [listeners, tcp, merge_test],
    Listener = #{<<"enable">> => true, <<"bind">> => <<"127.0.0.1:31884">>},
    ok = load_conf(merge, listener_conf(Listener), Config),
    ?assertMatch(#{enable := true}, emqx_conf:get(Path)),
    ok = load_conf(merge, listener_conf(#{<<"enabled">> => false}), Config),
    ?assertMatch(#{enable := false}, emqx_conf:get(Path)),
    ?assertNot(maps:is_key(<<"enabled">>, emqx_conf:get_raw(Path))),
    {ok, _} = emqx_conf:remove(Path, #{override_to => cluster}),
    ok.

-doc """
Loading `authentication` as a single object with `--merge` merges it
into the stored authenticator list.
""".
t_merge_authn_object_form(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    Redis = #{
        <<"backend">> => <<"redis">>,
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => false,
        <<"redis_type">> => <<"single">>,
        <<"server">> => <<"127.0.0.1:6379">>,
        <<"cmd">> => <<"HMGET mqtt_user:${username} password_hash salt">>
    },
    ok = load_conf(replace, #{<<"authentication">> => [Redis#{<<"pool_size">> => 4}]}, Config),
    NewCmd = <<"HMGET mqtt_user:${clientid} password_hash salt">>,
    ok = load_conf(merge, #{<<"authentication">> => Redis#{<<"cmd">> => NewCmd}}, Config),
    ?assertMatch(
        [#{<<"pool_size">> := 4, <<"cmd">> := NewCmd}],
        emqx_conf:get_raw([authentication])
    ),
    ok = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config),
    ok.

-doc """
Loading one field of a union member with `--merge` keeps the stored
fields that select the member (issue #17552, `schema_registry` schemas).
""".
t_merge_union_member_without_selector(Config) ->
    Path = [schema_registry, schemas, <<"merge_test">>],
    Schema = #{<<"type">> => <<"json">>, <<"source">> => <<"{}">>},
    ok = load_conf(merge, schema_conf(Schema), Config),
    ok = load_conf(merge, schema_conf(#{<<"description">> => <<"updated">>}), Config),
    ?assertMatch(
        #{type := json, source := <<"{}">>, description := <<"updated">>},
        emqx_conf:get(Path)
    ),
    {ok, _} = emqx_conf:remove(Path, #{override_to => cluster}),
    ok.

schema_conf(Schema) ->
    #{<<"schema_registry">> => #{<<"schemas">> => #{<<"merge_test">> => Schema}}}.

-doc """
Loading with `--merge` into a namespace merges over that namespace's
stored config, not over the global config, and leaves the global config
unchanged.
""".
t_merge_namespaced_over_namespaced_config(Config) ->
    Ns = <<"merge_ns">>,
    MqttInit = emqx_conf:get_raw([mqtt]),
    ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(emqx_schema, Ns),
    ok = load_conf(merge, #{<<"mqtt">> => #{<<"max_inflight">> => 64}}, Config),
    ok = load_ns_conf(Ns, merge, #{<<"mqtt">> => #{<<"idle_timeout">> => <<"30s">>}}),
    ok = load_ns_conf(Ns, merge, #{<<"mqtt">> => #{<<"max_packet_size">> => <<"2MB">>}}),
    ?assertMatch(
        #{
            <<"idle_timeout">> := <<"30s">>,
            <<"max_packet_size">> := <<"2MB">>,
            <<"max_inflight">> := 32
        },
        emqx_config:get_raw_namespaced([mqtt], Ns)
    ),
    ?assertMatch(
        #{idle_timeout := 15_000, max_packet_size := 1_048_576, max_inflight := 64},
        emqx_conf:get([mqtt])
    ),
    ok = load_conf(replace, #{<<"mqtt">> => MqttInit}, Config),
    ok.

load_ns_conf(Ns, Mode, Conf) ->
    Bin = iolist_to_binary(hocon_pp:do(Conf, #{})),
    emqx_conf_cli:load_config(Ns, Bin, #{mode => Mode}).

load_conf(Mode, Conf, Config) ->
    ConfFile = prepare_conf_file(?FUNCTION_NAME, hocon_pp:do(Conf, #{}), Config),
    emqx_conf_cli:conf(["load", "--" ++ atom_to_list(Mode), ConfFile]).

listener_conf(Listener) ->
    #{<<"listeners">> => #{<<"tcp">> => #{<<"merge_test">> => Listener}}}.

base_conf() ->
    #{
        <<"cluster">> => emqx_conf:get_raw([cluster]),
        <<"node">> => emqx_conf:get_raw([node])
    }.

changed(cluster) ->
    #{<<"name">> => <<"emqx-test">>};
changed(node) ->
    #{
        <<"name">> => <<"emqx-test@127.0.0.1">>,
        <<"cookie">> => <<"gokdfkdkf1122">>,
        <<"data_dir">> => <<"data">>
    };
changed(rpc) ->
    #{<<"mode">> => <<"sync">>}.

%%------------------------------------------------------------------------------
%% `conf show' output modes
%%------------------------------------------------------------------------------

-doc """
`conf show` redacts sensitive values by default and prints them unchanged only
with `--no-secret-redaction`, for both the full config and a single root.
""".
t_show_redacts_sensitive_values(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    SecretFile = filename:join(?config(priv_dir, Config), "conf-cli-secret"),
    ok = file:write_file(SecretFile, ?FILE_SECRET_CONTENT),
    SecretURI = iolist_to_binary(["file://", SecretFile]),
    try
        ok = load_conf(replace, authn_conf(SecretURI), Config),
        Cookie = to_bin(emqx:get_config([node, cookie])),
        RedactedFull = capture_show(["show"]),
        RawFull = capture_show(["show", "--no-secret-redaction"]),
        ?assertMatch(<<"#", _/binary>>, RedactedFull),
        ?assertNotMatch(<<"#", _/binary>>, RawFull),
        assert_absent(RedactedFull, authn_secrets(SecretURI, Cookie)),
        assert_present(RawFull, authn_secrets(SecretURI, Cookie)),
        %% A `file://` source is never expanded into the output.
        assert_absent(RawFull, [?FILE_SECRET_CONTENT]),
        {ok, Redacted} = hocon:binary(RedactedFull),
        {ok, Raw} = hocon:binary(RawFull),
        assert_redacted_authn(maps:get(<<"authentication">>, Redacted)),
        assert_raw_authn(maps:get(<<"authentication">>, Raw), SecretURI),
        %% `node.cookie` is only covered by the schema-level redaction.
        ?assertEqual(?REDACTED, maps:get(<<"cookie">>, maps:get(<<"node">>, Redacted))),
        ?assertEqual(Cookie, maps:get(<<"cookie">>, maps:get(<<"node">>, Raw))),
        RedactedKeyed = capture_show(["show", "authentication"]),
        RawKeyed = capture_show(["show", "--no-secret-redaction", "authentication"]),
        ?assertMatch(<<"#", _/binary>>, RedactedKeyed),
        ?assertNotMatch(<<"#", _/binary>>, RawKeyed),
        {ok, RedactedKeyedConf} = hocon:binary(RedactedKeyed),
        {ok, RawKeyedConf} = hocon:binary(RawKeyed),
        assert_redacted_authn(maps:get(<<"authentication">>, RedactedKeyedConf)),
        assert_raw_authn(maps:get(<<"authentication">>, RawKeyedConf), SecretURI)
    after
        _ = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config),
        _ = file:delete(SecretFile)
    end.

-doc """
Sensitive schema defaults are redacted too: an LDAP authenticator that keeps the
default `${password}` bind password shows `******` by default and the template
only with `--no-secret-redaction`.
""".
t_show_redacts_sensitive_defaults(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    Ldap = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"ldap">>,
        <<"enable">> => false,
        <<"server">> => <<"127.0.0.1:1">>,
        <<"base_dn">> => ?LDAP_BASE_DN,
        <<"filter">> => <<"(uid=${username})">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"method">> => #{<<"type">> => <<"bind">>}
    },
    try
        ok = load_conf(replace, #{<<"authentication">> => [Ldap]}, Config),
        Redacted = capture_show(["show", "authentication"]),
        {ok, #{<<"authentication">> := [#{<<"method">> := RedactedMethod}]}} =
            hocon:binary(Redacted),
        ?assertEqual(?REDACTED, maps:get(<<"bind_password">>, RedactedMethod)),
        Raw = capture_show(["show", "--no-secret-redaction", "authentication"]),
        {ok, #{<<"authentication">> := [#{<<"method">> := RawMethod}]}} = hocon:binary(Raw),
        ?assertEqual(<<"${password}">>, maps:get(<<"bind_password">>, RawMethod))
    after
        _ = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config)
    end.

-doc """
Namespaced `conf show` redacts in the same way, only reports the selected
namespace, accepts the opt-out flag in either order, and keeps the public
readers raw.
""".
t_show_namespaced_redacts_sensitive_values(Config) ->
    Ns = ?NS_NAME,
    Globals0 = emqx_conf:get_raw([connectors], undefined),
    NsPassword = <<"conf-cli-ns-connector-password">>,
    GlobalPassword = <<"conf-cli-global-connector-password">>,
    ok = load_ns_conf(Ns, replace, connector_conf(<<"ns">>, NsPassword)),
    try
        ok = load_conf(replace, connector_conf(<<"global">>, GlobalPassword), Config),
        NsHeader = scope_value(<<"ns">>, <<"authorization">>),
        NsHeaderLower = scope_value(<<"ns">>, <<"authorization-lower">>),
        GlobalHeader = scope_value(<<"global">>, <<"authorization">>),
        NsFull = capture_show(["show", "--namespace", Ns]),
        NsFullRaw = capture_show(["show", "--namespace", Ns, "--no-secret-redaction"]),
        Redacted = capture_show(["show", "--namespace", Ns, "connectors"]),
        Raw = capture_show(["show", "--namespace", Ns, "--no-secret-redaction", "connectors"]),
        RawAlt = capture_show(["show", "--no-secret-redaction", "--namespace", Ns, "connectors"]),
        RawRepeated = capture_show([
            "show",
            "--no-secret-redaction",
            "--no-secret-redaction",
            "--namespace",
            Ns,
            "connectors"
        ]),
        ?assertMatch(<<"#", _/binary>>, NsFull),
        ?assertMatch(<<"#", _/binary>>, Redacted),
        ?assertEqual(Raw, RawAlt),
        ?assertEqual(Raw, RawRepeated),
        %% The full output is scoped to the namespace, and never leaks global values.
        assert_absent(NsFull, [NsHeader, NsHeaderLower, NsPassword, GlobalHeader, GlobalPassword]),
        assert_present(NsFullRaw, [NsHeader, NsHeaderLower, NsPassword]),
        assert_absent(NsFullRaw, [GlobalHeader, GlobalPassword]),
        assert_absent(Redacted, [NsHeader, NsHeaderLower, NsPassword, GlobalHeader, GlobalPassword]),
        assert_present(Raw, [
            NsHeader, NsHeaderLower, NsPassword, scope_value(<<"ns">>, <<"visible">>)
        ]),
        %% The namespace does not leak the same-named global connector.
        assert_absent(Raw, [GlobalHeader, GlobalPassword]),
        assert_absent(capture_show(["show"]), [NsHeader, NsHeaderLower, NsPassword]),
        {ok, NsFullConf} = hocon:binary(NsFull),
        assert_connector_redacted(maps:get(<<"connectors">>, NsFullConf)),
        {ok, NsFullRawConf} = hocon:binary(NsFullRaw),
        assert_connector_scope(
            maps:get(<<"connectors">>, NsFullRawConf), NsHeader, NsHeaderLower, NsPassword
        ),
        {ok, RawConf} = hocon:binary(Raw),
        #{<<"connectors">> := RawConnectors} = RawConf,
        assert_connector_scope(RawConnectors, NsHeader, NsHeaderLower, NsPassword),
        {ok, RedactedConf} = hocon:binary(Redacted),
        #{<<"connectors">> := RedactedConnectors} = RedactedConf,
        assert_connector_redacted(RedactedConnectors),
        %% `get_config_namespaced/1,2` stay raw for the RPC callers.
        #{<<"connectors">> := RawViaRpc} = emqx_conf_cli:get_config_namespaced(
            Ns, <<"connectors">>
        ),
        assert_connector_scope(RawViaRpc, NsHeader, NsHeaderLower, NsPassword)
    after
        _ = load_conf(replace, #{<<"connectors">> => restore_connectors(Globals0)}, Config),
        _ = load_ns_conf(Ns, replace, #{<<"connectors">> => #{}})
    end.

-doc """
A raw namespaced export can be loaded back with `conf load --namespace`,
restoring the exported values in that namespace only.
""".
t_show_namespaced_raw_round_trip(Config) ->
    Ns = ?NS_NAME,
    Globals0 = emqx_conf:get_raw([connectors], undefined),
    NsPassword = <<"conf-cli-ns-round-trip-password">>,
    ChainedPassword = <<"conf-cli-ns-changed-password">>,
    ok = load_ns_conf(Ns, replace, connector_conf(<<"ns">>, NsPassword)),
    try
        NsHeader = scope_value(<<"ns">>, <<"authorization">>),
        Export = capture_show(["show", "--namespace", Ns, "--no-secret-redaction", "connectors"]),
        assert_present(Export, [NsHeader, NsPassword]),
        ok = load_ns_conf(Ns, replace, connector_conf(<<"changed">>, ChainedPassword)),
        assert_present(
            capture_show(["show", "--namespace", Ns, "--no-secret-redaction", "connectors"]),
            [scope_value(<<"changed">>, <<"authorization">>)]
        ),
        ExportFile = prepare_conf_file(?FUNCTION_NAME, Export, Config),
        ok = emqx_conf_cli:conf(["load", "--namespace", Ns, "--replace", ExportFile]),
        Restored = capture_show([
            "show", "--namespace", Ns, "--no-secret-redaction", "connectors"
        ]),
        assert_present(Restored, [NsHeader, NsPassword]),
        assert_absent(Restored, [scope_value(<<"changed">>, <<"authorization">>), ChainedPassword])
    after
        _ = load_conf(replace, #{<<"connectors">> => restore_connectors(Globals0)}, Config),
        _ = load_ns_conf(Ns, replace, #{<<"connectors">> => #{}})
    end.

-doc """
A raw `conf show` export of a writable root can be loaded back, restoring the
literal secrets and the `file://` reference it contained.
""".
t_show_raw_round_trip(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    SecretFile = filename:join(?config(priv_dir, Config), "conf-cli-round-trip-secret"),
    ok = file:write_file(SecretFile, ?FILE_SECRET_CONTENT),
    SecretURI = iolist_to_binary(["file://", SecretFile]),
    try
        ok = load_conf(replace, authn_conf(SecretURI), Config),
        Export = capture_show(["show", "--no-secret-redaction", "authentication"]),
        %% Replace the secrets with different values before restoring the export.
        ok = load_conf(replace, changed_authn_conf(), Config),
        [JwtChanged | _] = emqx_conf:get_raw([authentication]),
        ?assertEqual(<<"changed-jwt-secret">>, maps:get(<<"secret">>, JwtChanged)),
        ExportFile = prepare_conf_file(?FUNCTION_NAME, Export, Config),
        ok = emqx_conf_cli:conf(["load", "--replace", ExportFile]),
        [Jwt, _Http, Ldap] = emqx_conf:get_raw([authentication]),
        ?assertEqual(?JWT_SECRET, maps:get(<<"secret">>, Jwt)),
        ?assertEqual(
            ?LDAP_BIND_PASSWORD, maps:get(<<"bind_password">>, maps:get(<<"method">>, Ldap))
        ),
        ?assertEqual(SecretURI, maps:get(<<"password">>, Ldap))
    after
        _ = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config),
        _ = file:delete(SecretFile)
    end.

-doc """
The opt-out flag is rejected by `conf load` and `conf remove` before they read
the path or touch the config, and the help text documents both output modes.
""".
t_show_flag_is_rejected_by_load_and_remove(_Config) ->
    Mqtt = emqx_conf:get_raw([mqtt]),
    {Result, Usage0} = capture_conf(["show", "mqtt", "--no-secret-redaction"]),
    Usage = iolist_to_binary(Usage0),
    ?assertMatch({error, _}, Result),
    ?assertNotEqual(nomatch, binary:match(Usage, <<"--no-secret-redaction">>)),
    ?assertNotEqual(nomatch, binary:match(Usage, <<"Sensitive values are redacted by default">>)),
    %% Rejected before reading a path: the error is not `not_a_file`.
    ?assertEqual(
        {error, "bad arguments: --no-secret-redaction is not supported by this command"},
        emqx_conf_cli:conf(["load", "--no-secret-redaction", "not-an-existing-file"])
    ),
    ?assertMatch(
        {error, _},
        emqx_conf_cli:conf(["remove", "--no-secret-redaction", "mqtt.keepalive_multiplier"])
    ),
    %% No positional argument either: the flag alone is rejected, not crashed on.
    ?assertMatch({error, _}, emqx_conf_cli:conf(["load", "--no-secret-redaction"])),
    ?assertMatch({error, _}, emqx_conf_cli:conf(["remove", "--no-secret-redaction"])),
    ?assertEqual(Mqtt, emqx_conf:get_raw([mqtt])),
    %% A missing namespace value and a flag after the key stay bad arguments.
    ?assertMatch({error, _}, emqx_conf_cli:conf(["show", "--namespace"])),
    ?assertMatch({error, _}, emqx_conf_cli:conf(["show", "--no-secret-redaction", "--namespace"])),
    ?assertMatch({error, _}, emqx_conf_cli:conf(["show", "mqtt", "--no-secret-redaction"])),
    ok.

-doc """
The public readers keep returning raw values, hidden roots and the cluster
strategy filter are unchanged, and a binary `"all"` is not the full config.
""".
t_get_config_namespaced_stays_raw(Config) ->
    AuthNInit = emqx_conf:get_raw([authentication]),
    try
        ok = load_conf(replace, authn_conf(<<"rpc-reader-password">>), Config),
        Before = emqx_conf_cli:get_config_namespaced(global, <<"authentication">>),
        _ = capture_show(["show", "authentication"]),
        ?assertEqual(Before, emqx_conf_cli:get_config_namespaced(global, <<"authentication">>)),
        [Jwt | _] = maps:get(<<"authentication">>, Before),
        ?assertEqual(?JWT_SECRET, maps:get(<<"secret">>, Jwt)),
        Full = emqx_conf_cli:get_config_namespaced(global),
        ?assertEqual([], [
            Root
         || Root <- [<<"stats">>, <<"broker">>, <<"plugins">>, <<"zones">>],
            maps:is_key(Root, Full)
        ]),
        Cluster = maps:get(<<"cluster">>, Full),
        Strategy = maps:get(<<"discovery_strategy">>, Cluster),
        %% `filter_cluster_conf/1` drops every discovery strategy but the selected one.
        ?assertEqual(
            [],
            [
                S
             || S <- [<<"manual">>, <<"static">>, <<"dns">>, <<"etcd">>, <<"k8s">>],
                S =/= Strategy,
                maps:is_key(S, Cluster)
            ]
        ),
        ?assertEqual(
            {error, "key_not_found"},
            emqx_conf_cli:get_config_namespaced(global, <<"no_such_root">>)
        ),
        %% A binary "all" selects the root named `all`, not the full config.
        ?assertEqual(
            {error, "key_not_found"}, emqx_conf_cli:get_config_namespaced(global, <<"all">>)
        )
    after
        _ = load_conf(replace, #{<<"authentication">> => AuthNInit}, Config)
    end.

-doc """
A keyed `conf show` still reports a hidden root, while the full config drops it.
""".
t_show_keyed_keeps_hidden_roots(Config) ->
    Zone = <<"conf_cli_hidden_root_zone">>,
    Conf = #{
        <<"zones">> => #{Zone => #{<<"mqtt">> => #{<<"keepalive_multiplier">> => 10}}}
    },
    ok = load_conf(replace, Conf, Config),
    try
        Redacted = capture_show(["show", "zones"]),
        ?assertMatch(<<"#", _/binary>>, Redacted),
        {ok, #{<<"zones">> := #{Zone := _}}} = hocon:binary(Redacted),
        ?assertEqual(nomatch, binary:match(capture_show(["show"]), Zone)),
        #{<<"zones">> := #{Zone := _}} = emqx_conf_cli:get_config_namespaced(global, <<"zones">>),
        ?assertNot(maps:is_key(<<"zones">>, emqx_conf_cli:get_config_namespaced(global)))
    after
        _ = emqx_conf_cli:conf(["remove", "zones." ++ binary_to_list(Zone)])
    end.

capture_show(Args) ->
    {Result, Prints} = capture_conf(Args),
    ?assertEqual(ok, Result),
    iolist_to_binary(Prints).

capture_conf(Args) ->
    emqx_common_test_helpers:capture_io_format(fun() -> emqx_conf_cli:conf(Args) end).

authn_conf(LdapPassword) ->
    #{
        <<"authentication">> => [
            #{
                <<"mechanism">> => <<"jwt">>,
                <<"use_jwks">> => false,
                <<"algorithm">> => <<"hmac-based">>,
                <<"secret">> => ?JWT_SECRET,
                <<"secret_base64_encoded">> => false
            },
            #{
                <<"mechanism">> => <<"password_based">>,
                <<"backend">> => <<"http">>,
                <<"enable">> => false,
                <<"method">> => <<"get">>,
                <<"url">> => <<"http://127.0.0.1:1/auth">>,
                <<"headers">> => #{
                    <<"Authorization">> => ?HTTP_AUTHORIZATION,
                    <<"X-Test-Header">> => ?VISIBLE_HEADER_VALUE
                }
            },
            #{
                <<"mechanism">> => <<"password_based">>,
                <<"backend">> => <<"ldap">>,
                <<"enable">> => false,
                <<"server">> => <<"127.0.0.1:1">>,
                <<"base_dn">> => ?LDAP_BASE_DN,
                <<"filter">> => <<"(uid=${username})">>,
                <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
                <<"password">> => LdapPassword,
                <<"method">> => #{
                    <<"type">> => <<"bind">>,
                    <<"bind_password">> => ?LDAP_BIND_PASSWORD
                }
            }
        ]
    }.

changed_authn_conf() ->
    AuthN = authn_conf(<<"changed-ldap-password">>),
    [Jwt, Http, Ldap] = maps:get(<<"authentication">>, AuthN),
    AuthN#{
        <<"authentication">> => [
            Jwt#{<<"secret">> => <<"changed-jwt-secret">>},
            Http,
            Ldap#{
                <<"method">> => #{
                    <<"type">> => <<"bind">>,
                    <<"bind_password">> => <<"changed-bind-password">>
                }
            }
        ]
    }.

authn_secrets(SecretURI, Cookie) ->
    [?JWT_SECRET, ?HTTP_AUTHORIZATION, ?LDAP_BIND_PASSWORD, SecretURI, Cookie].

assert_redacted_authn([Jwt, Http, Ldap]) ->
    ?assertEqual(?REDACTED, maps:get(<<"secret">>, Jwt)),
    ?assertEqual(<<"hmac-based">>, maps:get(<<"algorithm">>, Jwt)),
    Headers = maps:get(<<"headers">>, Http),
    ?assertNot(lists:member(?HTTP_AUTHORIZATION, maps:values(Headers))),
    ?assertEqual(?REDACTED, authz_header(Headers)),
    ?assert(lists:member(?VISIBLE_HEADER_VALUE, maps:values(Headers))),
    ?assertEqual(?REDACTED, maps:get(<<"password">>, Ldap)),
    ?assertEqual(?REDACTED, maps:get(<<"bind_password">>, maps:get(<<"method">>, Ldap))),
    ?assertEqual(?LDAP_BASE_DN, maps:get(<<"base_dn">>, Ldap)).

assert_raw_authn([Jwt, Http, Ldap], SecretURI) ->
    ?assertEqual(?JWT_SECRET, maps:get(<<"secret">>, Jwt)),
    Headers = maps:get(<<"headers">>, Http),
    ?assertEqual(?HTTP_AUTHORIZATION, authz_header(Headers)),
    ?assert(lists:member(?VISIBLE_HEADER_VALUE, maps:values(Headers))),
    ?assertEqual(SecretURI, maps:get(<<"password">>, Ldap)),
    ?assertEqual(?LDAP_BIND_PASSWORD, maps:get(<<"bind_password">>, maps:get(<<"method">>, Ldap))),
    ?assertEqual(?LDAP_BASE_DN, maps:get(<<"base_dn">>, Ldap)).

authz_header(Headers) ->
    Values = [
        V
     || {K, V} <- maps:to_list(Headers),
        string:lowercase(emqx_utils_conv:str(K)) =:= "authorization"
    ],
    case Values of
        [Value] -> Value;
        _ -> undefined
    end.

connector_conf(Scope, MqttPassword) ->
    #{
        <<"connectors">> => #{
            <<"http">> => #{
                <<"conf_cli_http">> => #{
                    <<"url">> => <<"http://127.0.0.1:1/">>,
                    <<"enable">> => false,
                    <<"headers">> => #{
                        <<"Authorization">> => scope_value(Scope, <<"authorization">>),
                        <<"authorization">> => scope_value(Scope, <<"authorization-lower">>),
                        <<"X-Test-Header">> => scope_value(Scope, <<"visible">>)
                    }
                }
            },
            <<"mqtt">> => #{
                <<"conf_cli_mqtt">> => #{
                    <<"server">> => <<"127.0.0.1:1">>,
                    <<"username">> => <<"conf-cli-user">>,
                    <<"enable">> => false,
                    <<"password">> => MqttPassword
                }
            }
        }
    }.

scope_value(Scope, Kind) ->
    case Kind of
        <<"authorization">> -> <<"Bearer conf-cli-", Scope/binary, "-authorization">>;
        <<"authorization-lower">> -> <<"Bearer conf-cli-", Scope/binary, "-authorization-lower">>;
        <<"visible">> -> <<"conf-cli-", Scope/binary, "-visible">>
    end.

assert_connector_scope(Connectors, HttpAuthz, HttpAuthzLower, MqttPassword) ->
    #{<<"http">> := #{<<"conf_cli_http">> := Http}, <<"mqtt">> := #{<<"conf_cli_mqtt">> := Mqtt}} =
        Connectors,
    Headers = maps:get(<<"headers">>, Http),
    ?assertEqual(HttpAuthz, maps:get(<<"Authorization">>, Headers)),
    ?assertEqual(HttpAuthzLower, maps:get(<<"authorization">>, Headers)),
    ?assertEqual(MqttPassword, maps:get(<<"password">>, Mqtt)).

assert_connector_redacted(Connectors) ->
    #{<<"http">> := #{<<"conf_cli_http">> := Http}, <<"mqtt">> := #{<<"conf_cli_mqtt">> := Mqtt}} =
        Connectors,
    Headers = maps:get(<<"headers">>, Http),
    ?assertEqual(?REDACTED, maps:get(<<"Authorization">>, Headers)),
    ?assertEqual(?REDACTED, maps:get(<<"authorization">>, Headers)),
    ?assertEqual(?REDACTED, maps:get(<<"password">>, Mqtt)).

restore_connectors(undefined) -> #{};
restore_connectors(Connectors) -> Connectors.

assert_absent(Output, Values) ->
    lists:foreach(
        fun(Value) -> ?assertEqual(nomatch, binary:match(Output, Value), Value) end,
        Values
    ).

assert_present(Output, Values) ->
    lists:foreach(
        fun(Value) -> ?assertNotEqual(nomatch, binary:match(Output, Value), Value) end,
        Values
    ).

to_bin(Value) -> iolist_to_binary(Value).
