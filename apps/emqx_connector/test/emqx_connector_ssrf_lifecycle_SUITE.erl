%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Lifecycle tests for the connector SSRF policy.
%%
%% The policy is enforced only for HTTP (`url') and MQTT (`server') connectors,
%% and only when a connector is created or updated. Enabling/disabling and
%% deleting a connector, and any other connector's re-validation, are never
%% blocked by it. Other connector types are not subject to the policy at all.
%%
%% Includes a regression for #17940: with SSRF enabled, deleting or otherwise
%% touching a connector must not fail because an unrelated connector's stored
%% address is now denied.
-module(emqx_connector_ssrf_lifecycle_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(CACHE_KEY, {emqx_utils_ssrf, cache}).
-define(CONNECTOR, emqx_connector_dummy_impl).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge_mqtt,
            emqx_bridge_greptimedb,
            emqx_bridge
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    mock_resource(),
    set_ssrf_disabled(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_connectors(),
    set_ssrf_disabled(),
    catch meck:unload(),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

mock_resource() ->
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    ok.

set_ssrf_disabled() ->
    persistent_term:put(?CACHE_KEY, #{
        enable => false,
        allow_cidrs => [],
        deny_cidrs => [],
        deny_hosts => []
    }).

set_ssrf_deny(DenyCidrs) ->
    persistent_term:put(?CACHE_KEY, #{
        enable => true,
        allow_cidrs => [],
        deny_cidrs => emqx_utils_ssrf:compile_cidrs(DenyCidrs),
        deny_hosts => []
    }).

clear_connectors() ->
    Items = emqx_connector:list(?global_ns),
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            catch emqx_connector:remove(?global_ns, Type, Name)
        end,
        Items
    ),
    ok.

http_config(URL) ->
    #{
        <<"url">> => URL,
        <<"headers">> => #{},
        <<"enable">> => true,
        <<"connect_timeout">> => <<"5s">>,
        <<"pool_size">> => 1,
        <<"pool_type">> => <<"random">>,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
    }.

mqtt_config(Server) ->
    #{
        <<"server">> => Server,
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"proto_ver">> => <<"v5">>,
        <<"ssl">> => #{<<"enable">> => false},
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
    }.

%% A connector type that is NOT http/mqtt, i.e. not subject to the SSRF policy.
greptimedb_config(Server) ->
    #{
        <<"server">> => Server,
        <<"dbname">> => <<"public">>,
        <<"enable">> => true,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
    }.

%%--------------------------------------------------------------------
%% HTTP cases
%%--------------------------------------------------------------------

-doc "Creating an HTTP connector targeting a denied URL is rejected.".
t_http_create_denied_rejected(_Config) ->
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_connector:create(
            ?global_ns,
            http,
            http_denied,
            http_config(<<"http://127.0.0.1:18083">>)
        )
    ),
    ok.

-doc """
Enabling a connector whose stored URL is now denied is allowed: the SSRF
policy is enforced only when creating or updating a connector, not on the
enable/disable toggle.
""".
t_http_enable_now_allowed(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        http,
        http_revalidate,
        http_config(<<"http://127.0.0.1:18083">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    {ok, _} = emqx_connector:disable_enable(?global_ns, disable, http, http_revalidate),
    ?assertMatch(
        {ok, _},
        emqx_connector:disable_enable(?global_ns, enable, http, http_revalidate)
    ),
    ok.

-doc "Disabling a now-denied HTTP connector is always allowed.".
t_http_disable_always_allowed(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        http,
        http_disable_ok,
        http_config(<<"http://127.0.0.1:18083">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {ok, _},
        emqx_connector:disable_enable(?global_ns, disable, http, http_disable_ok)
    ),
    ok.

%%--------------------------------------------------------------------
%% MQTT cases
%%--------------------------------------------------------------------

-doc "Creating an MQTT connector targeting a denied server is rejected.".
t_mqtt_create_denied_rejected(_Config) ->
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_connector:create(
            ?global_ns,
            mqtt,
            mqtt_denied,
            mqtt_config(<<"127.0.0.1:1883">>)
        )
    ),
    ok.

-doc """
Regression for #17484: an existing denied MQTT connector must not block
creating an unrelated MQTT connector. Before the fix, schema-level SSRF
validation ran on the whole `connectors.mqtt' subtree on every update
and rejected the new connector because of the existing sibling.
""".
t_mqtt_create_valid_with_existing_denied(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        mqtt,
        mqtt_denied_existing,
        mqtt_config(<<"10.0.0.1:1883">>)
    ),
    set_ssrf_deny([<<"10.0.0.0/8">>]),
    ?assertMatch(
        {ok, _},
        emqx_connector:create(
            ?global_ns,
            mqtt,
            mqtt_valid_new,
            mqtt_config(<<"8.8.8.8:1883">>)
        )
    ),
    %% The denied connector must still be there, unchanged.
    Names = [Name || #{name := Name} <- emqx_connector:list(?global_ns)],
    ?assert(lists:member(mqtt_denied_existing, Names)),
    ?assert(lists:member(mqtt_valid_new, Names)),
    ok.

-doc "Mirror for MQTT: enabling a now-denied connector is allowed.".
t_mqtt_enable_now_allowed(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        mqtt,
        mqtt_revalidate,
        mqtt_config(<<"127.0.0.1:1883">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    {ok, _} = emqx_connector:disable_enable(?global_ns, disable, mqtt, mqtt_revalidate),
    ?assertMatch(
        {ok, _},
        emqx_connector:disable_enable(?global_ns, enable, mqtt, mqtt_revalidate)
    ),
    ok.

-doc "Disabling a now-denied MQTT connector is always allowed.".
t_mqtt_disable_always_allowed(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        mqtt,
        mqtt_disable_ok,
        mqtt_config(<<"127.0.0.1:1883">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {ok, _},
        emqx_connector:disable_enable(?global_ns, disable, mqtt, mqtt_disable_ok)
    ),
    ok.

-doc "Updating an HTTP connector to a denied URL is rejected.".
t_http_update_denied_rejected(_Config) ->
    {ok, _} = emqx_connector:create(
        ?global_ns,
        http,
        http_upd,
        http_config(<<"http://8.8.8.8:18083">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_connector:create(
            ?global_ns,
            http,
            http_upd,
            http_config(<<"http://127.0.0.1:18083">>)
        )
    ),
    ok.

%%--------------------------------------------------------------------
%% Other connector types (not subject to the SSRF policy) and #17940
%%--------------------------------------------------------------------

-doc """
Connector types other than http/mqtt are not subject to the SSRF policy:
creating one targeting a denied address succeeds, and it remains deletable.
""".
t_schema_connector_not_ssrf_checked(_Config) ->
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertMatch(
        {ok, _},
        emqx_connector:create(
            ?global_ns,
            greptimedb,
            greptime_denied,
            greptimedb_config(<<"127.0.0.1:4001">>)
        )
    ),
    ?assertEqual(ok, emqx_connector:remove(?global_ns, greptimedb, greptime_denied)),
    ok.

-doc """
Regression for #17940: with SSRF enabled, deleting a connector must succeed
even when an unrelated, pre-existing connector's stored address is now denied.
The whole `connectors' subtree is re-validated on every change, and a denied
sibling used to abort the delete and leave a residual connector.
""".
t_delete_unrelated_with_denied_sibling(_Config) ->
    %% pre-existing connectors, created while the policy is permissive
    {ok, _} = emqx_connector:create(
        ?global_ns,
        greptimedb,
        denied_sibling,
        greptimedb_config(<<"127.0.0.1:4001">>)
    ),
    {ok, _} = emqx_connector:create(
        ?global_ns,
        mqtt,
        to_delete,
        mqtt_config(<<"8.8.8.8:1883">>)
    ),
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    ?assertEqual(ok, emqx_connector:remove(?global_ns, mqtt, to_delete)),
    Names = [Name || #{name := Name} <- emqx_connector:list(?global_ns)],
    ?assertNot(lists:member(to_delete, Names)),
    ?assert(lists:member(denied_sibling, Names)),
    ok.
