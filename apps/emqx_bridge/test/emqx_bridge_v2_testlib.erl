%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2_testlib).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ROOT_KEY_ACTIONS, actions).
-define(ROOT_KEY_SOURCES, sources).
-define(tpal(MSG), begin
    ct:pal(MSG),
    ?tp(notice, MSG, #{})
end).

%% ct setup helpers

%% Deprecated: better to inline the setup in the test suite.
init_per_suite(Config, Apps) ->
    [{start_apps, Apps} | Config].

%% Deprecated: better to inline the setup in the test suite.
end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_maybe:apply(fun emqx_cth_suite:stop/1, Apps),
    ok.

%% Deprecated: better to inline the setup in the test suite.
init_per_group(TestGroup, BridgeType, Config) ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    Apps = emqx_cth_suite:start(
        ?config(start_apps, Config),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    UniqueNum = integer_to_binary(erlang:unique_integer([positive])),
    MQTTTopic = <<"mqtt/topic/abc", UniqueNum/binary>>,
    [
        {apps, Apps},
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, MQTTTopic},
        {test_group, TestGroup},
        {bridge_type, BridgeType}
        | Config
    ].

%% Deprecated: better to inline the setup in the test suite.
end_per_group(Config) ->
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok.

%% Deprecated: better to inline the setup in the test suite.
init_per_testcase(TestCase, Config0, BridgeConfigCb) ->
    ct:timetrap(timer:seconds(60)),
    delete_all_bridges_and_connectors(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    BridgeTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    TestGroup = ?config(test_group, Config0),
    Config = [{bridge_topic, BridgeTopic} | Config0],
    {Name, ConfigString, BridgeConfig} = BridgeConfigCb(
        TestCase, TestGroup, Config
    ),
    ok = snabbkaffe:start_trace(),
    [
        {bridge_name, Name},
        {bridge_config_string, ConfigString},
        {bridge_config, BridgeConfig}
        | Config
    ].

%% Deprecated: better to inline the setup in the test suite.
end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            %% in CI, apparently this needs more time since the
            %% machines struggle with all the containers running...
            emqx_common_test_helpers:call_janitor(60_000),
            delete_all_bridges_and_connectors(),
            ok = snabbkaffe:stop(),
            ok
    end.

delete_all_bridges_and_connectors() ->
    delete_all_bridges(),
    delete_all_connectors().

delete_all_bridges() ->
    maybe
        true ?= has_config_table(),
        Namespaces = lists:usort(
            emqx_config:get_all_namespaces_containing(<<"actions">>) ++
                emqx_config:get_all_namespaces_containing(<<"sources">>)
        ),
        lists:foreach(
            fun delete_all_bridges/1,
            [?global_ns | Namespaces]
        )
    else
        _ -> ok
    end.

delete_all_bridges(Namespace) ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge_v2:remove(Namespace, actions, Type, Name)
        end,
        emqx_bridge_v2:list(Namespace, actions)
    ),
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge_v2:remove(Namespace, sources, Type, Name)
        end,
        emqx_bridge_v2:list(Namespace, sources)
    ).

delete_all_connectors() ->
    maybe
        true ?= has_config_table(),
        lists:foreach(
            fun delete_all_connectors/1,
            [?global_ns | emqx_config:get_all_namespaces_containing(<<"connectors">>)]
        )
    else
        _ -> ok
    end.

delete_all_connectors(Namespace) ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_connector:remove(Namespace, Type, Name)
        end,
        emqx_connector:list(Namespace)
    ).

delete_all_rules() ->
    maybe
        true ?= has_config_table(),
        lists:foreach(
            fun delete_all_rules/1,
            [?global_ns | emqx_config:get_all_namespaces_containing(<<"rule_engine">>)]
        )
    else
        _ -> ok
    end.

delete_all_rules(Namespace) ->
    lists:foreach(
        fun(#{id := Id}) ->
            emqx_rule_engine_config:delete_rule(Namespace, Id)
        end,
        emqx_rule_engine:get_rules(Namespace)
    ).

has_config_table() ->
    maybe
        yes ?= mnesia:system_info(is_running),
        true ?= lists:member(emqx_config, mnesia:table_info(schema, tables)),
        true
    else
        _ -> false
    end.

%% test helpers
parse_and_check(Type, Name, InnerConfigMap0) ->
    parse_and_check(action, Type, Name, InnerConfigMap0).

parse_and_check(Kind, Type, Name, InnerConfigMap0) ->
    RootBin =
        case Kind of
            action -> <<"actions">>;
            source -> <<"sources">>
        end,
    TypeBin = emqx_utils_conv:bin(Type),
    RawConf = #{RootBin => #{TypeBin => #{Name => InnerConfigMap0}}},
    do_parse_and_check(RootBin, TypeBin, Name, emqx_bridge_v2_schema, RawConf).

parse_and_check_connector(Type, Name, InnerConfigMap0) ->
    TypeBin = emqx_utils_conv:bin(Type),
    RawConf = #{<<"connectors">> => #{TypeBin => #{Name => InnerConfigMap0}}},
    do_parse_and_check(<<"connectors">>, TypeBin, Name, emqx_connector_schema, RawConf).

do_parse_and_check(RootBin, TypeBin, NameBin, SchemaMod, RawConf) ->
    #{RootBin := #{TypeBin := #{NameBin := _}}} = hocon_tconf:check_plain(
        SchemaMod,
        RawConf,
        #{
            required => false,
            atom_key => false,
            %% to trigger validators that otherwise aren't triggered
            make_serializable => false
        }
    ),
    #{RootBin := #{TypeBin := #{NameBin := InnerConfigMap}}} = hocon_tconf:check_plain(
        SchemaMod,
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => true
        }
    ),
    InnerConfigMap.

bridge_id(Config) ->
    BridgeType = get_ct_config_with_fallback(Config, [action_type, bridge_type]),
    BridgeName = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    ConnectorId = emqx_bridge_resource:resource_id(BridgeType, BridgeName),
    <<"action:", BridgeId/binary, ":", ConnectorId/binary>>.

source_hookpoint(Config) ->
    #{kind := source, type := Type, name := Name} = get_common_values(Config),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    emqx_bridge_v2:source_hookpoint(BridgeId).

bridge_hookpoint(TCConfig) ->
    action_hookpoint(TCConfig).

%% fixme: this should actually be called `bridge_hookpoint`...
action_hookpoint(Config) ->
    #{type := Type, name := Name} = get_common_values(Config),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    emqx_bridge_resource:bridge_hookpoint(BridgeId).

add_source_hookpoint(Config) ->
    Hookpoint = source_hookpoint(Config),
    ok = emqx_hooks:add(Hookpoint, {?MODULE, source_hookpoint_callback, [self()]}, 1000),
    on_exit(fun() -> emqx_hooks:del(Hookpoint, {?MODULE, source_hookpoint_callback}) end),
    ok.

%% action/source resource id
resource_id(Config) ->
    #{
        resource_namespace := Namespace,
        kind := Kind,
        type := Type,
        name := Name,
        connector_name := ConnectorName
    } = get_common_values(Config),
    ConfRootKey =
        case Kind of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:id_with_root_and_connector_names(
        Namespace, ConfRootKey, Type, Name, ConnectorName
    ).

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    ActionType = get_ct_config_with_fallback(Config, [action_type, bridge_type]),
    ActionName = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
    ActionConfig0 = get_ct_config_with_fallback(Config, [action_config, bridge_config]),
    ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, Overrides),
    ConnectorName = ?config(connector_name, Config),
    ConnectorType = ?config(connector_type, Config),
    ConnectorConfig = ?config(connector_config, Config),
    ct:pal("creating connector with config: ~p, ~p\n  ~p", [
        ConnectorType, ConnectorName, ConnectorConfig
    ]),
    {ok, _} =
        emqx_connector:create(?global_ns, ConnectorType, ConnectorName, ConnectorConfig),

    ct:pal("creating bridge with config:\n  ~p", [ActionConfig]),
    emqx_bridge_v2:create(?global_ns, ?ROOT_KEY_ACTIONS, ActionType, ActionName, ActionConfig).

get_ct_config_with_fallback(Config, [Key]) ->
    ?config(Key, Config);
get_ct_config_with_fallback(Config, [Key | Rest]) ->
    case ?config(Key, Config) of
        undefined ->
            get_ct_config_with_fallback(Config, Rest);
        X ->
            X
    end.

get_config_by_kind(Config, Overrides) ->
    Kind = ?config(bridge_kind, Config),
    get_config_by_kind(Kind, Config, Overrides).

get_config_by_kind(Kind, Config, Overrides) ->
    case Kind of
        action ->
            %% TODO: refactor tests to use action_type...
            ActionType = get_ct_config_with_fallback(Config, [action_type, bridge_type]),
            ActionName = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
            ActionConfig0 = get_ct_config_with_fallback(Config, [action_config, bridge_config]),
            ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, Overrides),
            #{type => ActionType, name => ActionName, config => ActionConfig};
        source ->
            SourceType = ?config(source_type, Config),
            SourceName = ?config(source_name, Config),
            SourceConfig0 = ?config(source_config, Config),
            SourceConfig = emqx_utils_maps:deep_merge(SourceConfig0, Overrides),
            #{type => SourceType, name => SourceName, config => SourceConfig}
    end.

api_path_root(Kind) ->
    case Kind of
        action -> "actions";
        source -> "sources"
    end.

conf_root_key(Kind) ->
    case Kind of
        action -> ?ROOT_KEY_ACTIONS;
        source -> ?ROOT_KEY_SOURCES
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    AuthHeader = auth_header(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

simplify_result(Res) ->
    case Res of
        {error, {{_, Status, _}, _, Body}} ->
            {Status, Body};
        {ok, {{_, Status, _}, _, Body}} ->
            {Status, Body}
    end.

login(Params) ->
    URL = emqx_mgmt_api_test_util:api_path(["login"]),
    simple_request(#{
        auth_header => [{"no", "auth"}],
        method => post,
        url => URL,
        body => Params
    }).

create_superuser() ->
    create_superuser(_Opts = #{}).
create_superuser(Opts) ->
    Defaults = #{
        params => #{
            <<"username">> => <<"superuser">>,
            <<"password">> => <<"secretP@ss1">>
        }
    },
    #{
        params := #{
            <<"username">> := Username,
            <<"password">> := Password
        } = Params
    } = emqx_utils_maps:deep_merge(Defaults, Opts),
    on_exit(fun() -> emqx_dashboard_admin:remove_user(Username) end),
    case emqx_dashboard_admin:add_user(Username, Password, <<"administrator">>, <<"desc">>) of
        {ok, _} -> ok;
        {error, <<"username_already_exists">>} -> ok
    end,
    {200, #{<<"token">> := Token}} = login(Params),
    {"Authorization", iolist_to_binary(["Bearer ", Token])}.

create_namespaced_user(Opts0) ->
    Defaults = #{
        params => #{
            <<"username">> => <<"nsuser">>,
            <<"password">> => <<"SuperP@ss!1">>,
            <<"role">> => <<"ns:ns1::administrator">>,
            <<"description">> => <<"...">>
        }
    },
    #{
        params := #{
            <<"username">> := Username,
            <<"password">> := Password
        } = Params
    } = Opts = emqx_utils_maps:deep_merge(Defaults, Opts0),
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun create_superuser/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["users"]),
    on_exit(fun() -> emqx_dashboard_admin:remove_user(Username) end),
    {200, _} = simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Params
    }),
    #{username => Username, password => Password}.

create_namespaced_user_and_token(Opts) ->
    #{username := Username, password := Password} = create_namespaced_user(Opts),
    {200, #{<<"token">> := Token} = Res} =
        login(#{<<"username">> => Username, <<"password">> => Password}),
    ct:pal("namespaced token:\n  ~p", [Res]),
    Token.

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).

ensure_namespaced_api_key(Opts) ->
    #{namespace := Namespace} = Opts,
    Enabled = true,
    Name = maps:get(name, Opts, <<"default_", Namespace/binary>>),
    APIKey = <<Name/binary, "_key_", Namespace/binary>>,
    APISecret = <<"apisecret_", Namespace/binary>>,
    ExpiresAt = to_rfc3339(erlang:system_time(second) + 1_000),
    Role =
        case maps:get(role, Opts, admin) of
            admin ->
                <<"ns:", Namespace/binary, "::administrator">>;
            viewer ->
                <<"ns:", Namespace/binary, "::viewer">>
        end,
    Res = emqx_mgmt_auth:create(
        Name,
        APIKey,
        APISecret,
        Enabled,
        ExpiresAt,
        <<"...">>,
        Role
    ),
    case Res of
        {ok, _} ->
            emqx_common_test_http:auth_header(
                binary_to_list(APIKey),
                binary_to_list(APISecret)
            );
        {error, name_already_exists} ->
            emqx_common_test_http:auth_header(
                binary_to_list(APIKey),
                binary_to_list(APISecret)
            )
    end.

list_bridges_api() ->
    Params = [],
    Path = emqx_mgmt_api_test_util:api_path(["actions"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("listing bridges (via http)"),
    Res =
        case emqx_mgmt_api_test_util:request_api(get, Path, "", AuthHeader, Params, Opts) of
            {ok, {Status, Headers, Body0}} ->
                {ok, {Status, Headers, emqx_utils_json:decode(Body0)}};
            Error ->
                Error
        end,
    ct:pal("list bridges result: ~p", [Res]),
    Res.

get_source_api(BridgeType, BridgeName) ->
    get_bridge_api(source, BridgeType, BridgeName).

get_source_metrics_api(Config) ->
    SourceName = ?config(source_name, Config),
    SourceType = ?config(source_type, Config),
    SourceId = emqx_bridge_resource:bridge_id(SourceType, SourceName),
    Path = emqx_mgmt_api_test_util:api_path(["sources", SourceId, "metrics"]),
    ct:pal("getting source metrics (http)"),
    Res = request(get, Path, []),
    ct:pal("get source metrics (http) result:\n  ~p", [Res]),
    simplify_result(Res).

get_bridge_api(BridgeType, BridgeName) ->
    get_bridge_api(action, BridgeType, BridgeName).

get_bridge_api(BridgeKind, BridgeType, BridgeName) ->
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    Params = [],
    Root =
        case BridgeKind of
            source -> "sources";
            action -> "actions"
        end,
    Path = emqx_mgmt_api_test_util:api_path([Root, BridgeId]),
    ct:pal("get bridge ~p (via http)", [{BridgeKind, BridgeType, BridgeName}]),
    Res = request(get, Path, Params),
    ct:pal("get bridge ~p result:\n  ~p", [{BridgeKind, BridgeType, BridgeName}, Res]),
    Res.

create_bridge_api(Config) ->
    create_bridge_api(Config, _Overrides = #{}).

create_bridge_api(Config, Overrides) ->
    {ok, {{_, 201, _}, _, _}} = create_connector_api(Config),
    create_kind_api(Config, Overrides).

create_kind_api(Config) ->
    create_kind_api(Config, _Overrides = #{}).

create_kind_api(Config, Overrides) ->
    Kind = proplists:get_value(bridge_kind, Config, action),
    #{
        type := Type,
        name := Name,
        config := BridgeConfig
    } = get_config_by_kind(Kind, Config, Overrides),
    Params = BridgeConfig#{<<"type">> => Type, <<"name">> => Name},
    PathRoot = api_path_root(Kind),
    Path = emqx_mgmt_api_test_util:api_path([PathRoot]),
    ct:pal("creating bridge (~s, http):\n  ~p", [Kind, Params]),
    Res = request(post, Path, Params),
    ct:pal("bridge create (~s, http) result:\n  ~p", [Kind, Res]),
    Res.

enable_kind_api(Kind, Type, Name) ->
    do_enable_disable_kind_api(Kind, Type, Name, enable).

disable_kind_api(Kind, Type, Name) ->
    do_enable_disable_kind_api(Kind, Type, Name, disable).

do_enable_disable_kind_api(Kind, Type, Name, Op) ->
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    RootBin = api_path_root(Kind),
    {OpPath, OpStr} =
        case Op of
            enable -> {"true", "enable"};
            disable -> {"false", "disable"}
        end,
    Path = emqx_mgmt_api_test_util:api_path([RootBin, BridgeId, "enable", OpPath]),
    ct:pal(OpStr ++ " ~s ~s (http)", [Kind, BridgeId]),
    Res = request(put, Path, []),
    ct:pal(OpStr ++ " ~s ~s (http) result:\n  ~p", [Kind, BridgeId, Res]),
    simplify_result(Res).

create_connector_api(Config) ->
    create_connector_api(Config, _Overrides = #{}).

create_connector_api(Config, Overrides) ->
    ConnectorConfig0 = get_value(connector_config, Config),
    ConnectorName = get_value(connector_name, Config),
    ConnectorType = get_value(connector_type, Config),
    ConnectorConfig = emqx_utils_maps:deep_merge(ConnectorConfig0, Overrides),
    create_connector_api(ConnectorName, ConnectorType, ConnectorConfig).

create_connector_api(ConnectorName, ConnectorType, ConnectorConfig) ->
    Path = emqx_mgmt_api_test_util:api_path(["connectors"]),
    Params = ConnectorConfig#{<<"type">> => ConnectorType, <<"name">> => ConnectorName},
    ct:pal("creating connector (http):\n  ~p", [Params]),
    Res = request(post, Path, Params),
    ct:pal("connector create (http) result:\n  ~p", [Res]),
    Res.

create_connector_api_params(TCConfig, Overrides) when is_list(TCConfig) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := ConnectorConfig
    } = get_common_values_with_configs(TCConfig),
    emqx_utils_maps:deep_merge(
        ConnectorConfig#{
            <<"type">> => Type,
            <<"name">> => Name
        },
        Overrides
    ).

create_connector_api2(TCConfig, #{} = Overrides) when is_list(TCConfig) ->
    Params = create_connector_api_params(TCConfig, Overrides),
    do_create_connector_api2(Params, common_api_opts(TCConfig)).

do_create_connector_api2(Params, Opts) ->
    simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["connectors"]),
        body => Params,
        auth_header => auth_header_lazy(Opts)
    }).

update_connector_api(ConnectorName, ConnectorType, ConnectorConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
    Path = emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
    ct:pal("updating connector ~s (http):\n  ~p", [ConnectorId, ConnectorConfig]),
    Res = request(put, Path, ConnectorConfig),
    ct:pal("connector update (http) result:\n  ~p", [Res]),
    Res.

start_connector_api(ConnectorName, ConnectorType) ->
    ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
    Path = emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId, "start"]),
    ct:pal("starting connector ~s (http)", [ConnectorId]),
    Res = request(post, Path, #{}),
    ct:pal("connector update (http) result:\n  ~p", [Res]),
    Res.

enable_connector_api(ConnectorType, ConnectorName) ->
    do_enable_disable_connector_api(ConnectorType, ConnectorName, enable).

disable_connector_api(ConnectorType, ConnectorName) ->
    do_enable_disable_connector_api(ConnectorType, ConnectorName, disable).

do_enable_disable_connector_api(ConnectorType, ConnectorName, Op) ->
    ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
    {OpPath, OpStr} =
        case Op of
            enable -> {"true", "enable"};
            disable -> {"false", "disable"}
        end,
    Path = emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId, "enable", OpPath]),
    ct:pal(OpStr ++ " connector ~s (http)", [ConnectorId]),
    Res = request(put, Path, []),
    ct:pal(OpStr ++ " connector ~s (http) result:\n  ~p", [ConnectorId, Res]),
    Res.

get_connector_api(ConnectorType, ConnectorName) ->
    ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
    Path = emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
    ct:pal("get connector ~s (http)", [ConnectorId]),
    Res = request(get, Path, _Params = []),
    ct:pal("get connector (http) result:\n  ~p", [Res]),
    Res.

delete_connector_api(TCConfig) ->
    Type = get_value(connector_type, TCConfig),
    Name = get_value(connector_name, TCConfig),
    Id = emqx_connector_resource:connector_id(Type, Name),
    URL = emqx_mgmt_api_test_util:api_path(["connectors", Id]),
    simple_request(#{
        method => delete,
        url => URL
    }).

create_action_api(Config) ->
    create_action_api(Config, _Overrides = #{}).

create_action_api(Config, Overrides) ->
    ActionName = ?config(action_name, Config),
    ActionType = ?config(action_type, Config),
    ActionConfig0 = ?config(action_config, Config),
    ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, Overrides),
    Params = ActionConfig#{<<"type">> => ActionType, <<"name">> => ActionName},
    Path = emqx_mgmt_api_test_util:api_path(["actions"]),
    ct:pal("creating action (http):\n  ~p", [Params]),
    Res = request(post, Path, Params),
    ct:pal("action create (http) result:\n  ~p", [Res]),
    Res.

create_action_api_params(TCConfig) ->
    create_action_api_params(TCConfig, _Overrides = #{}).

create_action_api_params(TCConfig, Overrides) when is_list(TCConfig) ->
    #{
        type := Type,
        name := Name,
        config := ActionConfig
    } = get_common_values_with_configs(TCConfig),
    emqx_utils_maps:deep_merge(
        ActionConfig#{
            <<"type">> => Type,
            <<"name">> => Name
        },
        Overrides
    ).

create_action_api2(TCConfig, #{} = Overrides) when is_list(TCConfig) ->
    Params = create_action_api_params(TCConfig, Overrides),
    do_create_action_api2(Params, common_api_opts(TCConfig)).

do_create_action_api2(Params, Opts) ->
    simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["actions"]),
        body => Params,
        auth_header => auth_header_lazy(Opts)
    }).

create_source_api(Config) ->
    create_source_api(Config, _Overrides = #{}).

create_source_api(Config, Overrides) ->
    #{
        kind := source,
        type := Type,
        name := Name
    } = get_common_values(Config),
    ActionConfig0 = get_value(source_config, Config),
    ActionConfig = emqx_utils_maps:deep_merge(ActionConfig0, Overrides),
    Params = ActionConfig#{<<"type">> => Type, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["sources"]),
    ct:pal("creating source (http):\n  ~p", [Params]),
    Res = request(post, Path, Params),
    ct:pal("source create (http) result:\n  ~p", [Res]),
    simplify_result(Res).

get_action_api(Config) ->
    ActionName = get_value(action_name, Config),
    ActionType = get_value(action_type, Config),
    ActionId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    Path = emqx_mgmt_api_test_util:api_path(["actions", ActionId]),
    ct:pal("getting action (http)"),
    Res = request(get, Path, []),
    ct:pal("get action (http) result:\n  ~p", [Res]),
    Res.

get_action_api2(Config) ->
    simplify_result(get_action_api(Config)).

get_action_metrics_api(Config) ->
    ActionName = ?config(action_name, Config),
    ActionType = ?config(action_type, Config),
    ActionId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    Path = emqx_mgmt_api_test_util:api_path(["actions", ActionId, "metrics"]),
    ct:pal("getting action (http)"),
    Res = request(get, Path, []),
    ct:pal("get action (http) result:\n  ~p", [Res]),
    simplify_result(Res).

update_bridge_api2(TCConfig, Overrides) ->
    simplify_result(update_bridge_api(TCConfig, Overrides)).

update_bridge_api(Config) ->
    update_bridge_api(Config, _Overrides = #{}).

update_bridge_api(Config, Overrides) ->
    Kind = proplists:get_value(bridge_kind, Config, action),
    #{
        type := Type,
        name := Name,
        config := Params
    } = get_config_by_kind(Kind, Config, Overrides),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    PathRoot = api_path_root(Kind),
    Path = emqx_mgmt_api_test_util:api_path([PathRoot, BridgeId]),
    ct:pal("updating bridge (~s, http):\n  ~p", [Kind, Params]),
    Res = request(put, Path, Params),
    ct:pal("update bridge (~s, http) result:\n  ~p", [Kind, Res]),
    Res.

delete_kind_api(Kind, Type, Name) ->
    delete_kind_api(Kind, Type, Name, _Opts = #{}).

delete_kind_api(Kind, Type, Name, Opts) ->
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    PathRoot = api_path_root(Kind),
    Path = emqx_mgmt_api_test_util:api_path([PathRoot, BridgeId]),
    ct:pal("deleting bridge (~s, http)", [Kind]),
    Res = simple_request(#{
        method => delete,
        url => Path,
        query_params => maps:get(query_params, Opts, #{})
    }),
    ct:pal("delete bridge (~s, http) result:\n  ~p", [Kind, Res]),
    Res.

op_bridge_api(Op, BridgeType, BridgeName) ->
    op_bridge_api(_Kind = action, Op, BridgeType, BridgeName).

op_bridge_api(Kind, Op, BridgeType, BridgeName) ->
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    PathRoot = api_path_root(Kind),
    Path = emqx_mgmt_api_test_util:api_path([PathRoot, BridgeId, Op]),
    ct:pal("calling bridge ~p (~s, http):\n  ~p", [BridgeId, Kind, Op]),
    Method = post,
    Params = [],
    Res = request(Method, Path, Params),
    ct:pal("bridge op result:\n  ~p", [Res]),
    Res.

probe_bridge_api(Config) ->
    probe_bridge_api(Config, _Overrides = #{}).

probe_bridge_api(TCConfig, Overrides) ->
    #{
        kind := Kind,
        type := Type,
        name := Name,
        config := KindConfig0
    } = get_common_values_with_configs(TCConfig),
    KindConfig = emqx_utils_maps:deep_merge(KindConfig0, Overrides),
    probe_bridge_api(Kind, Type, Name, KindConfig).

probe_bridge_api(BridgeType, BridgeName, BridgeConfig) ->
    probe_bridge_api(action, BridgeType, BridgeName, BridgeConfig).

probe_bridge_api(Kind, BridgeType, BridgeName, BridgeConfig) ->
    Params = BridgeConfig#{<<"type">> => BridgeType, <<"name">> => BridgeName},
    PathRoot = api_path_root(Kind),
    Path = emqx_mgmt_api_test_util:api_path([PathRoot ++ "_probe"]),
    ct:pal("probing bridge (~s, http):\n  ~p", [Kind, Params]),
    Method = post,
    Res = request(Method, Path, Params),
    ct:pal("bridge probe (~s, http) result:\n  ~p", [Kind, Res]),
    Res.

probe_bridge_api_simple(TCConfig, Overrides) ->
    simplify_result(probe_bridge_api(TCConfig, Overrides)).

probe_connector_api(Config) ->
    probe_connector_api(Config, _Overrides = #{}).

probe_connector_api(Config, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name
    } = get_common_values(Config),
    ConnectorConfig0 = get_value(connector_config, Config),
    ConnectorConfig1 = emqx_utils_maps:deep_merge(ConnectorConfig0, Overrides),
    Params = ConnectorConfig1#{<<"type">> => Type, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["connectors_probe"]),
    ct:pal("probing connector (~s, http):\n  ~p", [Type, Params]),
    Method = post,
    Res = request(Method, Path, Params),
    ct:pal("probing connector (~s, http) result:\n  ~p", [Type, Res]),
    Res.

probe_connector_api2(TCConfig, Overrides) ->
    simplify_result(probe_connector_api(TCConfig, Overrides)).

list_bridges_http_api_v1() ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    ct:pal("list bridges (http v1)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list bridges (http v1) result:\n  ~p", [Res]),
    Res.

list_actions_http_api() ->
    Path = emqx_mgmt_api_test_util:api_path(["actions"]),
    ct:pal("list actions (http v2)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list actions (http v2) result:\n  ~p", [Res]),
    Res.

list_sources_http_api() ->
    Path = emqx_mgmt_api_test_util:api_path(["sources"]),
    ct:pal("list sources (http v2)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list sources (http v2) result:\n  ~p", [Res]),
    Res.

list_connectors_http_api() ->
    Path = emqx_mgmt_api_test_util:api_path(["connectors"]),
    ct:pal("list connectors"),
    Res = request(get, Path, _Params = []),
    ct:pal("list connectors result:\n  ~p", [Res]),
    Res.

summarize_actions_api() ->
    Path = emqx_mgmt_api_test_util:api_path(["actions_summary"]),
    ct:pal("summarize actions"),
    Res = request(get, Path, _Params = []),
    ct:pal("summarize actions result:\n  ~p", [Res]),
    simplify_result(Res).

summarize_sources_api() ->
    Path = emqx_mgmt_api_test_util:api_path(["sources_summary"]),
    ct:pal("summarize sources"),
    Res = request(get, Path, _Params = []),
    ct:pal("summarize sources result:\n  ~p", [Res]),
    simplify_result(Res).

enable_kind_http_api(Config) ->
    do_enable_disable_kind_http_api(enable, Config).

disable_kind_http_api(Config) ->
    do_enable_disable_kind_http_api(disable, Config).

do_enable_disable_kind_http_api(Op, Config) ->
    #{
        kind := Kind,
        type := Type,
        name := Name
    } = get_common_values(Config),
    RootBin =
        case Kind of
            action -> <<"actions">>;
            source -> <<"sources">>
        end,
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    OpPath =
        case Op of
            enable -> "true";
            disable -> "false"
        end,
    Path = emqx_mgmt_api_test_util:api_path([RootBin, BridgeId, "enable", OpPath]),
    OpStr = emqx_utils_conv:str(Op),
    ct:pal(OpStr ++ " action ~p (http v2)", [{Type, Name}]),
    Res = request(put, Path, _Params = []),
    ct:pal(OpStr ++ " action ~p (http v2) result:\n  ~p", [{Type, Name}, Res]),
    Res.

update_rule_http(RuleId, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId]),
    ct:pal("update rule ~p:\n  ~p", [RuleId, Params]),
    Res = request(put, Path, Params),
    ct:pal("update rule ~p result:\n  ~p", [RuleId, Res]),
    Res.

enable_rule_http(RuleId) ->
    Params = #{<<"enable">> => true},
    update_rule_http(RuleId, Params).

get_stats_http() ->
    Path = emqx_mgmt_api_test_util:api_path(["stats"]),
    Res = request(get, Path, _Params = []),
    ct:pal("get stats result:\n  ~p", [Res]),
    simplify_result(Res).

kick_clients_http(ClientIds) ->
    Path = emqx_mgmt_api_test_util:api_path(["clients", "kickout", "bulk"]),
    Res = request(post, Path, ClientIds),
    ct:pal("bulk kick clients result:\n  ~p", [Res]),
    simplify_result(Res).

is_rule_enabled(RuleId) ->
    {ok, #{enable := Enable}} = emqx_rule_engine:get_rule(?global_ns, RuleId),
    Enable.

try_decode_error(Body0) ->
    case emqx_utils_json:safe_decode(Body0) of
        {ok, #{<<"message">> := Msg0} = Body1} ->
            case emqx_utils_json:safe_decode(Msg0) of
                {ok, Msg1} -> Body1#{<<"message">> := Msg1};
                {error, _} -> Body1
            end;
        {ok, Body1} ->
            Body1;
        {error, _} ->
            Body0
    end.

create_rule_api(Opts) ->
    #{
        sql := SQL,
        actions := RuleActions
    } = Opts,
    Params = #{
        enable => true,
        sql => SQL,
        actions => RuleActions
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    ct:pal("create rule:\n  ~p", [Params]),
    Method = post,
    Res = request(Method, Path, Params),
    ct:pal("create rule results:\n  ~p", [Res]),
    Res.

get_rule_metrics(RuleId) ->
    emqx_metrics_worker:get_metrics(rule_metrics, RuleId).

create_rule_and_action_http(BridgeType, RuleTopic, Config) ->
    create_rule_and_action_http(BridgeType, RuleTopic, Config, _Opts = #{}).

create_rule_and_action_http(BridgeType, RuleTopic, Config, Opts) ->
    BridgeName = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
    BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
    SQL = maps:get(sql, Opts, <<"SELECT * FROM \"", RuleTopic/binary, "\"">>),
    Params0 = #{
        enable => true,
        sql => SQL,
        actions => [BridgeId]
    },
    Overrides = maps:get(overrides, Opts, #{}),
    Params = emqx_utils_maps:deep_merge(Params0, Overrides),
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = auth_header(),
    ReqOpts = maps:get(req_opts, Opts, #{}),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, ReqOpts) of
        {ok, Res0} ->
            Res =
                #{<<"id">> := RuleId} =
                case Res0 of
                    {_, _, Body0} ->
                        emqx_utils_json:decode(Body0);
                    _ ->
                        emqx_utils_json:decode(Res0)
                end,
            AuthHeaderGetter = get_auth_header_getter(),
            on_exit(fun() ->
                set_auth_header_getter(AuthHeaderGetter),
                {204, _} = delete_rule_api(RuleId)
            end),
            {ok, Res};
        Error ->
            Error
    end.

create_rule_api2(Params) ->
    create_rule_api2(Params, _Opts = #{}).

create_rule_api2(Params, Opts) ->
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["rules"]),
    simple_request(#{
        method => post,
        url => URL,
        body => Params,
        auth_header => AuthHeader
    }).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(<<"select * from \"${t}\" ">>, TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    #{rule_action_id := ActionId} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    RuleTopic = <<"t">>,
    {201, #{<<"id">> := RuleId}} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> => emqx_bridge_v2_testlib:fmt(SQL, #{t => RuleTopic}),
            <<"actions">> => [ActionId],
            <<"description">> => <<"bridge_v2 test rule">>
        }
    ),
    #{topic => RuleTopic, id => RuleId}.

delete_rule_api(RuleId) ->
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId]),
    simplify_result(request(delete, Path, "")).

start_rule_test_trace(RuleId, Opts) ->
    Name0 = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    Name = <<"trace-", Name0/binary>>,
    Body = #{
        <<"name">> => Name,
        <<"type">> => <<"ruleid">>,
        <<"ruleid">> => RuleId,
        <<"formatter">> => <<"json">>,
        <<"payload_encode">> => <<"text">>
    },
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["trace"]),
    simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Body
    }).

stop_rule_test_trace(TraceName) ->
    URL = emqx_mgmt_api_test_util:api_path(["trace", TraceName]),
    simple_request(#{
        method => delete,
        url => URL
    }).

trace_log_stream_api(TraceName, Opts) ->
    QueryParams = maps:get(query_params, Opts, #{}),
    URL = emqx_mgmt_api_test_util:api_path(["trace", TraceName, "log"]),
    simple_request(#{
        query_params => QueryParams,
        method => get,
        url => URL
    }).

trigger_rule_test_trace_flow(Opts) ->
    #{
        context := Context,
        rule_id := RuleId
    } = Opts,
    Body = #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => false
    },
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["rules", RuleId, "test"]),
    simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Body
    }).

get_test_trace_log(TraceName) ->
    URL = emqx_mgmt_api_test_util:api_path(["trace", TraceName, "log"]),
    QueryParams = #{
        <<"bytes">> => integer_to_binary(trunc(math:pow(2, 30)))
    },
    maybe
        {200, #{<<"items">> := Bin}} ?=
            simple_request(#{
                method => get,
                url => URL,
                query_params => QueryParams
            }),
        %% ct:pal("get log resp:\n  ~p", [Resp]),
        {ok, emqx_connector_aggreg_json_lines_test_utils:decode(Bin)}
    end.

api_spec_schemas(Root) ->
    Method = get,
    Path = emqx_mgmt_api_test_util:api_path(["schemas", Root]),
    Params = [],
    AuthHeader = [],
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {{_, 200, _}, _, Res0}} ->
            #{<<"components">> := #{<<"schemas">> := Schemas}} =
                emqx_utils_json:decode(Res0),
            Schemas
    end.

actions_api_spec_schemas() ->
    api_spec_schemas("actions").

export_backup_api(Params, Opts) ->
    simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["data", "export"]),
        body => Params,
        auth_header => auth_header_lazy(Opts)
    }).

import_backup_api(Params, Opts) ->
    simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["data", "import"]),
        body => Params,
        auth_header => auth_header_lazy(Opts)
    }).

get_value(Key, Config) ->
    case proplists:get_value(Key, Config, undefined) of
        undefined ->
            error({missing_required_config, Key, Config});
        Value ->
            Value
    end.

get_common_values(Config) ->
    Kind = proplists:get_value(bridge_kind, Config, action),
    case Kind of
        action ->
            Type = get_ct_config_with_fallback(Config, [action_type, bridge_type]),
            Name = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
            #{
                resource_namespace => proplists:get_value(resource_namespace, Config, ?global_ns),
                conf_root_key => actions,
                kind => Kind,
                rule_action_id => emqx_bridge_resource:bridge_id(Type, Name),
                type => Type,
                name => Name,
                connector_type => get_value(connector_type, Config),
                connector_name => get_value(connector_name, Config)
            };
        source ->
            #{
                resource_namespace => proplists:get_value(resource_namespace, Config, ?global_ns),
                conf_root_key => sources,
                kind => Kind,
                type => get_value(source_type, Config),
                name => get_value(source_name, Config),
                connector_type => get_value(connector_type, Config),
                connector_name => get_value(connector_name, Config)
            }
    end.

get_common_values_with_configs(Config) ->
    Values = #{kind := Kind} = get_common_values(Config),
    ConnectorConfig = get_value(connector_config, Config),
    KindConfig =
        case Kind of
            action ->
                get_value(action_config, Config);
            source ->
                get_value(source_config, Config)
        end,
    Values#{config => KindConfig, connector_config => ConnectorConfig}.

connector_resource_id(Config) ->
    #{
        resource_namespace := Namespace,
        connector_type := Type,
        connector_name := Name
    } = get_common_values(Config),
    emqx_connector_resource:resource_id(Namespace, Type, Name).

health_check_connector(Config) ->
    ConnectorResId = connector_resource_id(Config),
    emqx_resource_manager:health_check(ConnectorResId).

health_check_channel(Config) ->
    Opts = get_common_values(Config),
    force_health_check(Opts).

lookup_chan_id_in_conf(Opts) ->
    #{
        type := Type,
        name := Name
    } = Opts,
    Namespace = maps:get(resource_namespace, Opts, ?global_ns),
    ConfRootKey =
        case maps:get(kind, Opts, action) of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:lookup_chan_id_in_conf(Namespace, ConfRootKey, Type, Name).

make_chan_id(Opts) ->
    #{
        type := Type,
        name := Name,
        connector_name := ConnName
    } = Opts,
    Namespace = maps:get(resource_namespace, Opts, ?global_ns),
    ConfRootKey =
        case maps:get(kind, Opts, action) of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:id_with_root_and_connector_names(Namespace, ConfRootKey, Type, Name, ConnName).

get_metrics(Opts) ->
    #{
        type := Type,
        name := Name
    } = Opts,
    Namespace = maps:get(resource_namespace, Opts, ?global_ns),
    ConfRootKey =
        case maps:get(kind, Opts, action) of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:get_metrics(Namespace, ConfRootKey, Type, Name).

%%------------------------------------------------------------------------------
%% Internal export
%%------------------------------------------------------------------------------

source_hookpoint_callback(Message, _Namespace, TestPid) ->
    TestPid ! {consumed_message, Message},
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query(Config, MakeMessageFun, IsSuccessCheck, TracePoint) ->
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ResourceId = connector_resource_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            BridgeId = bridge_id(Config),
            Message = {BridgeId, MakeMessageFun()},
            IsSuccessCheck(emqx_resource:simple_sync_query(ResourceId, Message)),
            ok
        end,
        fun(Trace) ->
            ResourceId = connector_resource_id(Config),
            ?assertMatch([#{instance_id := ResourceId}], ?of_kind(TracePoint, Trace))
        end
    ),
    ok.

t_async_query(Config, MakeMessageFun, IsSuccessCheck, TracePoint) ->
    ReplyFun =
        fun(Pid, #{result := Result}) ->
            Pid ! {result, Result}
        end,
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ResourceId = connector_resource_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ActionResId = bridge_id(Config),
            ActionType = get_ct_config_with_fallback(Config, [action_type, bridge_type]),
            ActionName = get_ct_config_with_fallback(Config, [action_name, bridge_name]),
            Message = {ActionResId, MakeMessageFun()},
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_bridge_v2:query(?global_ns, ActionType, ActionName, Message, #{
                        async_reply_fun => {ReplyFun, [self()]}
                    }),
                    #{?snk_kind := TracePoint, instance_id := ResourceId},
                    5_000
                )
            ),
            ok
        end,
        fun(Trace) ->
            ResourceId = connector_resource_id(Config),
            ?assertMatch([#{instance_id := ResourceId}], ?of_kind(TracePoint, Trace))
        end
    ),
    receive
        {result, Result} -> IsSuccessCheck(Result)
    after 8_000 ->
        throw(timeout)
    end,
    ok.

t_rule_action(TCConfig) ->
    t_rule_action(TCConfig, _Opts = #{}).

%% Similar to `t_sync_query', but using only API functions and rule actions to trigger the
%% bridge, instead of lower level functions.
t_rule_action(TCConfig, Opts) ->
    TraceCheckers = maps:get(trace_checkers, Opts, []),
    PostPublishFn = maps:get(post_publish_fn, Opts, fun(Context) -> Context end),
    PrePublishFn = maps:get(pre_publish_fn, Opts, fun(Context) -> Context end),
    PayloadFn = maps:get(payload_fn, Opts, fun() -> emqx_guid:to_hexstr(emqx_guid:gen()) end),
    StartClientOpts = maps:get(start_client_opts, Opts, #{clean_start => true}),
    PublishFn = maps:get(
        publish_fn,
        Opts,
        fun(#{rule_topic := RuleTopic, payload_fn := PayloadFnIn} = Context) ->
            Payload = PayloadFnIn(),
            {ok, C} = emqtt:start_link(StartClientOpts),
            {ok, _} = emqtt:connect(C),
            ?assertMatch({ok, _}, emqtt:publish(C, RuleTopic, Payload, [{qos, 2}])),
            ok = emqtt:stop(C),
            Context#{payload => Payload}
        end
    ),
    RuleCreationOpts = maps:with([sql, rule_topic], Opts),
    CreateBridgeFn = maps:get(create_bridge_fn, Opts, fun() ->
        ?assertMatch({ok, _}, create_bridge_api(TCConfig))
    end),
    ?check_trace(
        begin
            #{type := Type} = get_common_values(TCConfig),
            CreateBridgeFn(),
            RuleTopic = maps:get(
                rule_topic,
                RuleCreationOpts,
                emqx_topic:join([<<"test">>, emqx_utils_conv:bin(Type)])
            ),
            {ok, _} = create_rule_and_action_http(Type, RuleTopic, TCConfig, RuleCreationOpts),
            ResourceId = connector_resource_id(TCConfig),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            Context0 = #{rule_topic => RuleTopic, payload_fn => PayloadFn},
            Context1 = PrePublishFn(Context0),
            Context2 = PublishFn(Context1),
            PostPublishFn(Context2),
            ok
        end,
        TraceCheckers
    ),
    ok.

%% Like `t_sync_query', but we send the message while the connector is
%% `?status_disconnected' and test that, after recovery, the buffered message eventually
%% is sent.
%%   * `make_message_fn' is a function that receives the rule topic and returns a
%%   `#message{}' record.
%%   * `enter_tp_filter' is a function that selects a tracepoint that indicates the point
%%     inside the connector's `on_query' callback function before actually trying to push
%%     the data.
%%   * `error_tp_filter' is a function that selects a tracepoint that indicates the
%%     message was attempted to be sent at least once and failed.
%%   * `success_tp_filter' is a function that selects a tracepoint that indicates the
%%     point where the message was acknowledged as successfully written.
t_sync_query_down(Config, Opts) ->
    #{
        make_message_fn := MakeMessageFn,
        enter_tp_filter := EnterTPFilter,
        error_tp_filter := ErrorTPFilter,
        success_tp_filter := SuccessTPFilter
    } = Opts,
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    {CTTimetrap, _} = ct:get_timetrap_info(),
    ?check_trace(
        #{timetrap => max(0, CTTimetrap - 500)},
        begin
            #{type := Type} = get_common_values(Config),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            RuleTopic = emqx_topic:join([<<"test">>, emqx_utils_conv:bin(Type)]),
            {ok, _} = create_rule_and_action_http(Type, RuleTopic, Config),
            ResourceId = connector_resource_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),

            ?force_ordering(
                #{?snk_kind := SNKKind} when
                    SNKKind =:= call_query orelse SNKKind =:= simple_query_enter,
                #{?snk_kind := cut_connection, ?snk_span := start}
            ),
            %% Note: order of arguments here is reversed compared to `?force_ordering'.
            snabbkaffe_nemesis:force_ordering(
                EnterTPFilter,
                _NEvents = 1,
                fun(Event1, Event2) ->
                    EnterTPFilter(Event1) andalso
                        ?match_event(#{
                            ?snk_kind := cut_connection,
                            ?snk_span := {complete, _}
                        })(
                            Event2
                        )
                end
            ),
            spawn_link(fun() ->
                ?tp_span(
                    cut_connection,
                    #{},
                    emqx_common_test_helpers:enable_failure(down, ProxyName, ProxyHost, ProxyPort)
                )
            end),
            ?tp("publishing_message", #{}),
            try
                {_, {ok, _}} =
                    snabbkaffe:wait_async_action(
                        fun() -> spawn(fun() -> emqx:publish(MakeMessageFn(RuleTopic)) end) end,
                        ErrorTPFilter,
                        infinity
                    )
            after
                ?tp("healing_failure", #{}),
                emqx_common_test_helpers:heal_failure(down, ProxyName, ProxyHost, ProxyPort)
            end,
            {ok, _} = snabbkaffe:block_until(SuccessTPFilter, infinity),
            ok
        end,
        []
    ),
    ok.

%% - `ProduceFn': produces a message in the remote system that shall be consumed.  May be
%%    a `{function(), integer()}' tuple.
%% - `Tracepoint': marks the end of consumed message processing.
t_consume(Config, Opts) ->
    #{
        consumer_ready_tracepoint := ConsumerReadyTPFn,
        produce_fn := ProduceFn,
        check_fn := CheckFn,
        produce_tracepoint := TracePointFn
    } = Opts,
    TestTimeout = maps:get(test_timeout, Opts, 60_000),
    ?check_trace(
        #{timetrap => TestTimeout},
        begin
            ConsumerReadyTimeout = maps:get(consumer_ready_timeout, Opts, 15_000),
            case ConsumerReadyTPFn of
                {Predicate, NEvents} when is_function(Predicate) ->
                    {ok, SRef0} = snabbkaffe:subscribe(Predicate, NEvents, ConsumerReadyTimeout);
                Predicate when is_function(Predicate) ->
                    {ok, SRef0} = snabbkaffe:subscribe(
                        Predicate, _NEvents = 1, ConsumerReadyTimeout
                    )
            end,
            ?tpal("creating connector and source"),
            ?assertMatch({ok, _}, create_bridge_api(Config)),
            ?assertMatch({ok, _}, snabbkaffe:receive_events(SRef0)),
            ?tpal("adding hookpoint"),
            ok = add_source_hookpoint(Config),
            ?tpal("waiting until connected"),
            ?retry(
                _Sleep = 200,
                _Attempts = 20,
                ?assertMatch(
                    #{status := ?status_connected},
                    health_check_channel(Config)
                )
            ),
            ?tpal("producing message and waiting for it to be consumed"),
            ?assertMatch(
                {_, {ok, _}},
                snabbkaffe:wait_async_action(
                    ProduceFn,
                    TracePointFn,
                    infinity
                )
            ),
            ?tpal("waiting for consumed message"),
            receive
                {consumed_message, Message} ->
                    CheckFn(Message)
            after 5_000 ->
                error({timeout, process_info(self(), messages)})
            end,
            ok
        end,
        []
    ),
    ok.

t_create_via_http(Config) ->
    t_create_via_http(Config, false).
t_create_via_http(Config, IsOnlyV2) ->
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge_api(Config)),

            ?assertMatch(
                {ok, _},
                update_bridge_api(
                    Config
                )
            ),

            %% check that v1 list API is fine
            (not IsOnlyV2) andalso
                ?assertMatch(
                    {ok, {{_, 200, _}, _, _}},
                    list_bridges_http_api_v1()
                ),

            ok
        end,
        []
    ),
    ok.

t_start_stop(Config, StopTracePoint) ->
    Kind = proplists:get_value(bridge_kind, Config, action),
    ConnectorName = ?config(connector_name, Config),
    ConnectorType = ?config(connector_type, Config),
    #{
        type := Type,
        name := Name,
        config := BridgeConfig
    } = get_config_by_kind(Kind, Config, _Overrides = #{}),

    ?assertMatch(
        {ok, {{_, 201, _}, _, _}},
        create_connector_api(Config)
    ),

    ct:timetrap({seconds, 20}),
    ?check_trace(
        snk_timetrap(),
        begin
            ?assertMatch(
                {ok, {{_, 204, _}, _Headers, _Body}},
                probe_bridge_api(
                    Kind,
                    Type,
                    Name,
                    BridgeConfig
                )
            ),
            %% Check that the bridge probe API doesn't leak atoms.
            ?assertMatch(
                {ok, {{_, 204, _}, _Headers, _Body}},
                probe_bridge_api(
                    Kind,
                    Type,
                    Name,
                    BridgeConfig
                )
            ),
            AtomsBefore = all_atoms(),
            AtomCountBefore = erlang:system_info(atom_count),
            %% Probe again; shouldn't have created more atoms.
            ProbeRes1 = probe_bridge_api(
                Kind,
                Type,
                Name,
                BridgeConfig
            ),

            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes1),
            AtomsAfter = all_atoms(),
            AtomCountAfter = erlang:system_info(atom_count),
            ?assertEqual(AtomCountBefore, AtomCountAfter, #{new_atoms => AtomsAfter -- AtomsBefore}),

            ?assertMatch({ok, _}, create_kind_api(Config)),

            %% Since the connection process is async, we give it some time to
            %% stabilize and avoid flakiness.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, health_check_connector(Config))
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(#{status := connected}, health_check_channel(Config))
            ),

            %% `start` bridge to trigger `already_started`
            ?assertMatch(
                {ok, {{_, 204, _}, _Headers, []}},
                op_bridge_api(Kind, "start", Type, Name)
            ),

            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({ok, connected}, health_check_connector(Config))
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(#{status := connected}, health_check_channel(Config))
            ),

            %% Disable the connector, which will also stop it.
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    emqx_connector:disable_enable(
                        ?global_ns, disable, ConnectorType, ConnectorName
                    ),
                    #{?snk_kind := StopTracePoint}
                )
            ),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertEqual({error, resource_is_stopped}, health_check_connector(Config))
            ),

            ResourceId = emqx_bridge_resource:resource_id(conf_root_key(Kind), Type, Name),
            #{resource_id => ResourceId}
        end,
        fun(Res, Trace) ->
            #{resource_id := ResourceId} = Res,
            %% one for each probe, one for real
            ?assertMatch(
                [_, _, _, #{instance_id := ResourceId}],
                ?of_kind(StopTracePoint, Trace)
            ),
            ok
        end
    ),
    ok.

t_on_get_status(Config) ->
    t_on_get_status(Config, _Opts = #{}).

t_on_get_status(Config, Opts) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    FailureStatus = maps:get(failure_status, Opts, ?status_disconnected),
    NormalStatus = maps:get(normal_status, Opts, ?status_connected),
    ?assertMatch({ok, _}, create_bridge_api(Config)),
    ResourceId = connector_resource_id(Config),
    %% Since the connection process is async, we give it some time to
    %% stabilize and avoid flakiness.
    ?retry(
        _Sleep = 200,
        _Attempts = 100,
        ?assertEqual(
            {ok, NormalStatus},
            emqx_resource_manager:health_check(ResourceId),
            #{resource_id => ResourceId}
        )
    ),
    case ProxyHost of
        undefined ->
            ok;
        _ ->
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                case is_list(FailureStatus) of
                    true ->
                        ?retry(
                            _Interval0 = 100,
                            _Attempts0 = 20,
                            begin
                                BadHCRes = emqx_resource_manager:health_check(ResourceId),
                                Expected = lists:map(fun(S) -> {ok, S} end, FailureStatus),
                                ?assert(
                                    lists:member(BadHCRes, Expected),
                                    #{expected => FailureStatus, got => BadHCRes}
                                )
                            end
                        );
                    false ->
                        ?retry(
                            _Interval0 = 100,
                            _Attempts0 = 20,
                            ?assertEqual(
                                {ok, FailureStatus}, emqx_resource_manager:health_check(ResourceId)
                            )
                        )
                end
            end),
            %% Check that it recovers itself.
            ?retry(
                _Sleep = 200,
                _Attempts = 100,
                ?assertEqual({ok, NormalStatus}, emqx_resource_manager:health_check(ResourceId))
            )
    end,
    ok.

%% Verifies that attempting to start an action while its connnector is disabled does not
%% start the connector.
t_start_action_or_source_with_disabled_connector(Config) ->
    #{
        kind := Kind,
        type := Type,
        name := Name,
        connector_type := ConnectorType,
        connector_name := ConnectorName
    } = get_common_values(Config),
    ?check_trace(
        begin
            {ok, _} = create_bridge_api(Config),
            {ok, {{_, 204, _}, _, _}} = disable_connector_api(ConnectorType, ConnectorName),
            ?assertMatch(
                {error, {{_, 400, _}, _, _}},
                op_bridge_api(Kind, "start", Type, Name)
            ),
            ok
        end,
        []
    ),
    ok.

%% For bridges that use `emqx_connector_aggregator' for aggregated mode uploads, verifies
%% that the bridge can recover from a buffer file corruption, and does so while preserving
%% uncompromised data.  `TCConfig' should contain keys that satisfy the usual functions of
%% this module for creating connectors and actions.
%%
%%   * `aggreg_id' : identifier for the aggregator.  It's the name of the
%%     `emqx_connector_aggregator' process defined when starting
%%     `emqx_connector_aggreg_upload_sup'.
%%   * `batch_size' : size of batch of messages to be sent before and after corruption.
%%     Should be less than the configured maximum number of aggregated records.
%%   * `rule_sql' : SQL statement for the rule that will send data to the action.
%%   * `make_message_fn' : function taking `N', an integer, and producing a `#message{}'
%%     that should match the rule topic.
%%   * `prepare_fn' (optional) : function that is run before producing messages, but after
%%     the connector, action and rule are created.  Receives a context map and should
%%     return it.  Defaults to a no-op.
%%   * `message_check_fn' : function that is run after the first batch is corrupted and
%%     the second batch is uploaded.  Receives a context map containing the first batch
%%     (which gets half corrupted) of messages in `messages_before' and the second batch
%%     (after corruption) in `messages_after'.  Add any assertions and integrity checks
%%     here.
%%   * `trace_checkers' (optional) : either function that receives the snabbkaffe trace
%%     and performs its analysis, os a list of such functions.  Defaults to a no-op.
t_aggreg_upload_restart_corrupted(TCConfig, Opts) ->
    #{
        aggreg_id := AggregId,
        batch_size := BatchSize,
        rule_sql := RuleSQL,
        make_message_fn := MakeMessageFn,
        message_check_fn := MessageCheckFn
    } = Opts,
    PrepareFn = maps:get(prepare_fn, Opts, fun(Ctx) -> Ctx end),
    TraceCheckers = maps:get(trace_checkers, Opts, []),
    #{type := ActionType} = get_common_values(TCConfig),
    ?check_trace(
        snk_timetrap(),
        begin
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(TCConfig)),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ActionType, <<"">>, TCConfig, #{
                        sql => RuleSQL
                    }
                ),
            Context0 = #{},
            Context1 = PrepareFn(Context0),
            Messages1 = lists:map(MakeMessageFn, lists:seq(1, BatchSize)),
            Context2 = Context1#{messages_before => Messages1},
            %% Ensure that they span multiple batch queries.
            {ok, {ok, _}} =
                ?wait_async_action(
                    publish_messages_delayed(Messages1, 1),
                    #{?snk_kind := connector_aggreg_records_written, action := AggregId}
                ),
            ct:pal("first batch's records have been written"),

            %% Find out the buffer file.
            {ok, #{filename := Filename}} = ?block_until(
                #{?snk_kind := connector_aggreg_buffer_allocated, action := AggregId}
            ),
            ct:pal("new buffer allocated"),

            %% Stop the bridge, corrupt the buffer file, and restart the bridge.
            {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:disable_kind_http_api(TCConfig),
            BufferFileSize = filelib:file_size(Filename),
            ok = emqx_connector_aggregator_test_helpers:truncate_at(Filename, BufferFileSize div 2),
            {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:enable_kind_http_api(TCConfig),

            %% Send some more messages.
            Messages2 = lists:map(MakeMessageFn, lists:seq(1, BatchSize)),
            Context3 = Context2#{messages_after => Messages2},
            ok = publish_messages_delayed(Messages2, 1),
            ct:pal("published second batch"),

            %% Wait until the delivery is completed.
            {ok, _} = ?block_until(
                #{
                    ?snk_kind := connector_aggreg_delivery_completed,
                    action := AggregId,
                    transfer := T
                } when T /= empty
            ),
            ct:pal("delivery completed"),

            MessageCheckFn(Context3)
        end,
        TraceCheckers
    ),
    ok.

%% Simulates a sequence of requests from the frontend and checks that secrets are
%% deobfuscated correctly for a connector.  The sequence is simply:
%%
%%   1) Create a connector.
%%   2) Update the connector with the response config.
%%
%% This assumes that the response from (1) is already obfuscated.  That is, this doesn't
%% check that secret fields are correctly marked as such.
t_deobfuscate_connector(Config) ->
    ?check_trace(
        begin
            #{
                connector_type := ConnectorType,
                connector_name := ConnectorName
            } = get_common_values(Config),
            OriginalConnectorConfig = get_value(connector_config, Config),
            {201, Response} = simplify_result(create_connector_api(Config)),
            %% Sanity check
            ?assertEqual(
                OriginalConnectorConfig,
                emqx_config:get_raw([<<"connectors">>, bin(ConnectorType), bin(ConnectorName)])
            ),
            ConnectorConfig = maps:without(
                [
                    <<"name">>,
                    <<"actions">>,
                    <<"sources">>,
                    <<"node_status">>,
                    <<"status">>,
                    <<"type">>
                ],
                Response
            ),
            ?assertMatch(
                {200, _},
                simplify_result(
                    update_connector_api(ConnectorName, ConnectorType, ConnectorConfig)
                )
            ),
            %% Even if the request is accepted, shouldn't clobber secrets
            ?assertEqual(
                OriginalConnectorConfig,
                emqx_config:get_raw([<<"connectors">>, bin(ConnectorType), bin(ConnectorName)])
            ),
            ok
        end,
        []
    ),
    ok.

-doc """
Verifies that the action correctly handles rule test tracing correctly.

This basically entails checking that we call `emqx_trace:rendered_action_template/2` or
`emqx_trace:rendered_action_template_with_ctx/2` before actually sending any data to the
remote systems.

This is done by the frontend via:

1. Starting a trace with `POST /trace` with `type` `ruleid`.
2. Performing a `POST /rules/:rule-id/test`, which in turn runs the actual rule with
   special logger metadata set.  Currently, the frontend always calls the test endpoint
   with `stop_action_after_template_rendering = false`, which means that junk test data
   will actually be sent to the remote servers.
3. This rule triggers the real action and logs the data.
4. Frontend fetches logs via `GET /trace/:name/log_detail`.
""".
t_rule_test_trace(Config, Opts) ->
    #{
        resource_namespace := Namespace,
        type := ActionType0,
        name := ActionName0
    } = get_common_values(Config),
    AuthHeaderOpts =
        case proplists:get_value(auth_header, Config) of
            undefined -> #{};
            AuthHeader -> #{auth_header => AuthHeader}
        end,
    ActionName = bin(ActionName0),
    ActionType = bin(ActionType0),
    ct:pal(asciiart:visible($+, "testing primary action success", [])),
    ct:pal("namespace: ~p", [Namespace]),
    ?tpal("creating connector and actions"),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api2(Config, #{}),
    FallbackActionName = <<ActionName/binary, "_fallback">>,
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api2([{action_name, FallbackActionName} | Config], #{}),
    RepublishTopicFallback = <<"fallback/republish">>,
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api2(
            Config,
            #{
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => bin(ActionType),
                        <<"name">> => FallbackActionName
                    },
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => #{
                            <<"topic">> => RepublishTopicFallback,
                            <<"qos">> => 1,
                            <<"retain">> => false,
                            <<"payload">> => <<"${.}">>,
                            <<"mqtt_properties">> => #{},
                            <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
                            <<"direct_dispatch">> => false
                        }
                    }
                ]
            }
        ),
    ?tpal("creating rule"),
    RuleTopic = <<"rule/test/trace">>,
    BridgeId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    RepublishTopicPrimary = <<"primary/republish">>,
    {201, #{<<"id">> := RuleId}} = create_rule_api2(
        #{
            <<"enable">> => true,
            <<"sql">> => fmt(<<"select * from \"${t}\" ">>, #{t => RuleTopic}),
            <<"actions">> => [
                BridgeId,
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> => #{
                        <<"topic">> => RepublishTopicPrimary,
                        <<"qos">> => 1,
                        <<"retain">> => false,
                        <<"payload">> => <<"${.}">>,
                        <<"mqtt_properties">> => #{},
                        <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
                        <<"direct_dispatch">> => false
                    }
                }
            ]
        },
        AuthHeaderOpts
    ),

    Context0 = #{},
    PreTestFn = maps:get(pre_test_fn, Opts, fun(Context) -> Context end),
    ?tpal("calling pre-test fn"),
    Context1 = PreTestFn(Context0),

    ?tpal("start rule test trace"),
    {200, #{<<"name">> := TraceName}} = start_rule_test_trace(RuleId, AuthHeaderOpts),
    PayloadFn = maps:get(payload_fn, Opts, fun() ->
        emqx_utils_json:encode(#{<<"msg">> => <<"hello">>})
    end),

    Payload = PayloadFn(),
    TestOpts = AuthHeaderOpts#{
        context => #{
            <<"clientid">> => <<"c_emqx">>,
            <<"event_type">> => <<"message_publish">>,
            <<"payload">> => Payload,
            <<"qos">> => 1,
            <<"topic">> => RuleTopic,
            <<"username">> => <<"u_emqx">>
        },
        rule_id => RuleId
    },
    ?tpal("triggering rule test flow"),
    {200, TriggerTestResp} = trigger_rule_test_trace_flow(TestOpts),
    ct:pal("trigger test trace response:\n  ~p", [TriggerTestResp]),
    PostTestFn = maps:get(post_test_fn, Opts, fun(Context) -> Context end),
    ?tpal("calling post-test fn"),
    Context2 = PostTestFn(Context1#{payload => Payload}),

    %% Actions may be evaluated concurrently; so we sort them to have deterministic test
    %% results.
    EventSorter = fun(#{<<"msg">> := TypeA} = EventA, #{<<"msg">> := TypeB} = EventB) ->
        AIA0 = emqx_utils_maps:deep_get([<<"meta">>, <<"action_info">>], EventA, undefined),
        AIB0 = emqx_utils_maps:deep_get([<<"meta">>, <<"action_info">>], EventB, undefined),
        ActionInfo = fun(AI) ->
            case AI of
                #{<<"type">> := T, <<"name">> := N} ->
                    {1, T, N};
                #{<<"func">> := F, <<"args">> := #{<<"topic">> := T}} ->
                    {2, F, T};
                _ ->
                    {3, AI}
            end
        end,
        AIA = ActionInfo(AIA0),
        AIB = ActionInfo(AIB0),
        {TypeA, AIA} =< {TypeB, AIB}
    end,
    ToSequence = fun(#{<<"msg">> := Msg} = Event) ->
        case Event of
            #{<<"meta">> := #{<<"action_info">> := #{<<"name">> := N, <<"type">> := T}}} ->
                #{msg => Msg, name => N, type => T};
            #{<<"meta">> := #{<<"action_info">> := #{<<"args">> := #{<<"topic">> := T}}}} ->
                #{msg => Msg, topic => T};
            _ ->
                #{msg => Msg}
        end
    end,

    AssertLogFn = maps:get(assert_log_fn, Opts, fun(TraceNameIn) ->
        {ok, LogLines0} = get_test_trace_log(TraceNameIn),
        LogLines1 = lists:filter(
            fun(#{<<"msg">> := Msg}) ->
                lists:member(Msg, [
                    <<"rule_activated">>,
                    <<"bridge_action">>,
                    <<"action_template_rendered">>,
                    <<"republish_message">>,
                    <<"action_success">>
                ])
            end,
            LogLines0
        ),
        LogLines = lists:sort(EventSorter, LogLines1),
        Sequence = lists:map(ToSequence, LogLines),
        ?assertMatch(
            [
                %% One success event for Primary Action, one for republish
                #{<<"msg">> := <<"action_success">>},
                #{<<"msg">> := <<"action_success">>},
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    %% The action itself
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"name">> := ActionName,
                            <<"type">> := ActionType
                        }
                    }
                },
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    %% The republish "action"
                    <<"meta">> := #{<<"action_info">> := #{<<"func">> := <<"republish">>}}
                },
                #{<<"msg">> := <<"bridge_action">>},
                #{
                    <<"msg">> := <<"republish_message">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"func">> := <<"republish">>,
                            <<"args">> := #{
                                <<"topic">> := RepublishTopicPrimary
                            }
                        }
                    }
                },
                #{<<"msg">> := <<"rule_activated">>}
            ],
            LogLines,
            #{sequence => Sequence}
        )
    end),
    ?tpal("checking logs"),
    ?retry(1_000, 10, AssertLogFn(TraceName)),
    ?tpal("logs ok"),
    {204, _} = stop_rule_test_trace(TraceName),

    ?tpal("calling cleanup fn"),
    CleanupFn = maps:get(cleanup_fn, Opts, fun(Context) -> Context end),
    Context3 = CleanupFn(Context2),

    %% Now we test fallback action traces
    ct:pal(asciiart:visible($+, "testing primary action failure with fallbacks", [])),
    ?tpal("starting fallback action test trace"),
    {200, #{<<"name">> := TraceNameErr}} = start_rule_test_trace(RuleId, AuthHeaderOpts),
    AssertFallbackLogFn = maps:get(assert_fallback_log_fn, Opts, fun(TraceNameIn) ->
        {ok, LogLines0} = get_test_trace_log(TraceNameIn),
        LogLines1 = lists:filter(
            fun(#{<<"msg">> := Msg}) ->
                lists:member(Msg, [
                    <<"rule_activated">>,
                    <<"bridge_action">>,
                    <<"action_template_rendered">>,
                    <<"republish_message">>,
                    <<"action_success">>
                ])
            end,
            LogLines0
        ),
        LogLines = lists:sort(EventSorter, LogLines1),
        Sequence = lists:map(ToSequence, LogLines),
        ?assertMatch(
            [
                %% Only one `action_success`, which comes from rule runtime evaluating the
                %% republish action contained in the rule.
                #{<<"msg">> := <<"action_success">>},
                %% Both the primary and fallback render their payloads, and so does the
                %% primary (rule) and fallback action republish actions.
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"name">> := ActionName
                        }
                    }
                },
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"name">> := FallbackActionName
                        }
                    }
                },
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"func">> := <<"republish">>,
                            <<"args">> := #{<<"topic">> := RepublishTopicFallback}
                        }
                    }
                },
                #{
                    <<"msg">> := <<"action_template_rendered">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"func">> := <<"republish">>,
                            <<"args">> := #{<<"topic">> := RepublishTopicPrimary}
                        }
                    }
                },
                #{<<"msg">> := <<"bridge_action">>},
                #{
                    <<"msg">> := <<"republish_message">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"func">> := <<"republish">>,
                            <<"args">> := #{
                                <<"topic">> := RepublishTopicFallback
                            }
                        }
                    }
                },
                #{
                    <<"msg">> := <<"republish_message">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"func">> := <<"republish">>,
                            <<"args">> := #{
                                <<"topic">> := RepublishTopicPrimary
                            }
                        }
                    }
                },
                #{<<"msg">> := <<"rule_activated">>}
            ],
            LogLines,
            #{sequence => Sequence}
        )
    end),
    {ok, {_ConnResId, PrimaryActionResId}} =
        emqx_bridge_v2:get_resource_ids(Namespace, actions, ActionType, ActionName),
    emqx_common_test_helpers:with_mock(
        emqx_trace,
        rendered_action_template,
        fun(ActionId, Result) ->
            %% Call the trace to generate the genuine primary event
            Res = meck:passthrough([ActionId, Result]),
            %% Generate an unrecoverable error to trigger fallback action
            case ActionId == PrimaryActionResId of
                true ->
                    error({unrecoverable_error, <<"mocked error, check fallback!">>});
                false ->
                    Res
            end
        end,
        fun() ->
            {200, TriggerTestRespErr} = trigger_rule_test_trace_flow(TestOpts),
            ct:pal("trigger test trace response (with error):\n  ~p", [TriggerTestRespErr]),
            _Context4 = PostTestFn(Context3),
            ?tpal("waiting for fallback action test logs"),
            ?retry(1_000, 10, AssertFallbackLogFn(TraceNameErr))
        end
    ),
    {204, _} = stop_rule_test_trace(TraceNameErr),

    ok.

-doc """
For SQL-like bridges that use ecpool reconnect callbacks, checks that starting with a bad
SQL and then updating the config to the correct one updates the DB connection session
state properly.
""".
t_update_with_invalid_prepare(TCConfig, #{} = Opts) ->
    #{
        bad_sql := BadSQL,
        reconnect_cb := {Mod, ReconnectFn},
        get_sig_fn := GetSigFn,
        check_expected_error_fn := CheckExpectedErrorFn
    } = Opts,
    {201, _} = create_connector_api2(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api2(TCConfig, #{}),
    Overrides = #{<<"parameters">> => #{<<"sql">> => BadSQL}},
    {200, Body1} = update_bridge_api2(TCConfig, Overrides),
    ?assertMatch(#{<<"status">> := <<"disconnected">>}, Body1),
    CheckExpectedErrorFn(maps:get(<<"error">>, Body1)),
    %% assert that although there was an error returned, the invliad SQL is actually put\
    ?assertMatch(
        {200, #{<<"parameters">> := #{<<"sql">> := BadSQL}}},
        get_action_api2(TCConfig)
    ),

    %% update again with the original sql
    %% the error should be gone now, and status should be 'connected'
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        update_bridge_api2(TCConfig, #{})
    ),
    %% finally check if ecpool worker should have exactly one of reconnect callback
    ConnectorResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ActionResId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    Workers = ecpool:workers(ConnectorResId),
    [_ | _] = WorkerPids = lists:map(fun({_, Pid}) -> Pid end, Workers),
    lists:foreach(
        fun(Pid) ->
            [{Mod, ReconnectFn, Args}] =
                ecpool_worker:get_reconnect_callbacks(Pid),
            Sig = GetSigFn(Args),
            ?assertEqual(ActionResId, Sig)
        end,
        WorkerPids
    ),
    ok.

snk_timetrap() ->
    {CTTimetrap, _} = ct:get_timetrap_info(),
    #{timetrap => max(0, CTTimetrap - 1_000)}.

publish_messages_delayed(MessageEvents, Delay) ->
    lists:foreach(
        fun(Msg) ->
            emqx:publish(Msg),
            ct:sleep(Delay)
        end,
        MessageEvents
    ).

proplist_update(Proplist, K, Fn) ->
    {K, OldV} = lists:keyfind(K, 1, Proplist),
    NewV = Fn(OldV),
    lists:keystore(K, 1, Proplist, {K, NewV}).

kickoff_action_health_check(Type, Name) ->
    kickoff_kind_health_check(actions, Type, Name).

kickoff_source_health_check(Type, Name) ->
    kickoff_kind_health_check(sources, Type, Name).

kickoff_kind_health_check(ConfRootKey, Type, Name) ->
    Kind =
        case ConfRootKey of
            sources -> source;
            actions -> action
        end,
    kickoff_kind_health_check(#{
        kind => Kind,
        type => Type,
        name => Name
    }).

kickoff_kind_health_check(Opts) ->
    #{
        kind := Kind,
        type := Type,
        name := Name
    } = Opts,
    Namespace = maps:get(namespace, Opts, ?global_ns),
    ConfRootKey = conf_root_key(Kind),
    Res =
        try
            maybe
                {ok, {ConnResId, ChannelResId}} ?=
                    emqx_bridge_v2:get_resource_ids(Namespace, ConfRootKey, Type, Name),
                _ = emqx_resource_manager:channel_health_check(ConnResId, ChannelResId),
                ok
            end
        catch
            throw:Reason ->
                {error, Reason};
            exit:{timeout, _} ->
                {error, timeout};
            K:E:S ->
                {error, {K, E, S}}
        end,
    maybe
        {error, _} ?= Res,
        ct:pal("health check kickoff failed: ~p", [Res]),
        Res
    end.

-define(AUTH_HEADER_FN_PD_KEY, {?MODULE, auth_header_fn}).
get_auth_header_getter() ->
    get(?AUTH_HEADER_FN_PD_KEY).

%% Note: must be set in init_per_testcase, as this is stored in process dictionary.
set_auth_header_getter(Fun) ->
    _ = put(?AUTH_HEADER_FN_PD_KEY, Fun),
    ok.

clear_auth_header_getter() ->
    _ = erase(?AUTH_HEADER_FN_PD_KEY),
    ok.
-undef(AUTH_HEADER_FN_PT_KEY).

auth_header() ->
    case get_auth_header_getter() of
        Fun when is_function(Fun, 0) ->
            Fun();
        _ ->
            emqx_mgmt_api_test_util:auth_header_()
    end.

common_api_opts(TCConfig) ->
    Opts0 =
        case proplists:get_value(auth_header, TCConfig) of
            undefined -> #{};
            AuthHeader -> #{auth_header => AuthHeader}
        end,
    Opts0#{tc_config => TCConfig}.

auth_header_lazy(TCConfig) when is_list(TCConfig) ->
    auth_header_lazy(maps:from_list(TCConfig));
auth_header_lazy(#{} = Opts) ->
    emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun auth_header/0
    ).

bin(X) -> emqx_utils_conv:bin(X).

common_connector_resource_opts() ->
    #{
        <<"health_check_interval">> => <<"1s">>,
        <<"health_check_timeout">> => <<"30s">>,
        <<"start_after_created">> => true,
        <<"start_timeout">> => <<"5s">>
    }.

common_action_resource_opts() ->
    #{
        <<"batch_size">> => 1,
        <<"batch_time">> => <<"0ms">>,
        <<"buffer_mode">> => <<"memory_only">>,
        <<"buffer_seg_bytes">> => <<"10MB">>,
        <<"health_check_interval">> => <<"1s">>,
        <<"health_check_interval_jitter">> => <<"0s">>,
        <<"health_check_timeout">> => <<"30s">>,
        <<"inflight_window">> => 100,
        <<"max_buffer_bytes">> => <<"256MB">>,
        <<"metrics_flush_interval">> => <<"1s">>,
        <<"query_mode">> => <<"sync">>,
        <<"request_ttl">> => <<"15s">>,
        <<"resume_interval">> => <<"1s">>,
        <<"worker_pool_size">> => 1
    }.

common_source_resource_opts() ->
    #{
        <<"health_check_interval">> => <<"1s">>,
        <<"health_check_interval_jitter">> => <<"0s">>,
        <<"health_check_timeout">> => <<"1s">>,
        <<"resume_interval">> => <<"1s">>
    }.

clean_aggregated_upload_work_dir() ->
    Dir = filename:join([emqx:data_dir(), bridge]),
    _ = file:del_dir_r(Dir),
    ok.

%% https://stackoverflow.com/a/34883331/2708711
all_atoms() ->
    do_all_atoms(_N = 0, []).

idx_to_atom(N) ->
    binary_to_term(<<131, 75, N:24>>).

do_all_atoms(N, Acc) ->
    try idx_to_atom(N) of
        A ->
            do_all_atoms(N + 1, [A | Acc])
    catch
        error:badarg ->
            Acc
    end.

force_health_check(Opts) ->
    #{
        type := Type,
        name := Name
    } = Opts,
    Namespace = maps:get(resource_namespace, Opts, ?global_ns),
    ConfRootKey =
        case maps:get(kind, Opts, action) of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:health_check(Namespace, ConfRootKey, Type, Name).

-doc """
Please avoid using this function whenever possible.  Prefer to use helpers that use the
HTTP API.
""".
delete_rule_directly(#{<<"id">> := Id} = Opts) ->
    Namespace = maps:get(<<"namespace">>, Opts, ?global_ns),
    emqx_rule_engine_config:delete_rule(Namespace, Id).

-doc """
Please avoid using this function whenever possible.  Prefer to use helpers that use the
HTTP API.
""".
create_rule_directly(#{<<"id">> := Id} = Opts0) ->
    Namespace = maps:get(<<"namespace">>, Opts0, ?global_ns),
    Opts = maps:without([<<"id">>, <<"namespace">>], Opts0),
    emqx_rule_engine_config:create_or_update_rule(Namespace, Id, Opts).

fmt(FmtStr, Context) ->
    Template = emqx_template:parse(FmtStr),
    iolist_to_binary(emqx_template:render_strict(Template, Context)).
