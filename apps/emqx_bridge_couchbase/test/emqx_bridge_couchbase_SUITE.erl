%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_couchbase_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-elvis([{elvis_text_style, line_length, #{skip_comments => whole_line}}]).

%% -import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_bridge_couchbase.hrl").

-define(USERNAME, <<"admin">>).
-define(PASSWORD, <<"public">>).
-define(BUCKET, <<"mqtt">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Host = os:getenv("COUCHBASE_HOST", "toxiproxy"),
    DirectHost = os:getenv("COUCHBASE_DIRECT_HOST", "couchbase"),
    Port = list_to_integer(os:getenv("COUCHBASE_PORT", "8093")),
    AdminPort = list_to_integer(os:getenv("COUCHBASE_ADMIN_PORT", "8091")),
    Server = iolist_to_binary([Host, ":", integer_to_binary(Port)]),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "couchbase",
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_couchbase,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps},
                {proxy_name, ProxyName},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {server, Server},
                {host, Host},
                {direct_host, DirectHost},
                {admin_port, AdminPort}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_couchbase);
                _ ->
                    {skip, no_couchbase}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(16)),
    Server = ?config(server, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, Server),
    ActionConfig0 = action_config(Name, #{connector => Name}),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, ActionConfig0),
    Config = [
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig}
        | Config0
    ],
    start_admin_client(Config).

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    delete_scope(scope(), Config),
    stop_admin_client(Config),
    emqx_common_test_helpers:call_janitor(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

start_admin_client(Config) ->
    DirectHost = ?config(direct_host, Config),
    AdminPort = ?config(admin_port, Config),
    AdminPool = <<"couchbase_SUITE_admin">>,
    PoolOpts = [
        {host, DirectHost},
        {port, AdminPort},
        {transport, tcp}
    ],
    {ok, _} = ehttpc_sup:start_pool(AdminPool, PoolOpts),
    [{admin_pool, AdminPool} | Config].

stop_admin_client(Config) ->
    AdminPool = ?config(admin_pool, Config),
    ok = ehttpc_sup:stop_pool(AdminPool),
    ok.

connector_config(Name, Server) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"server">> => Server,
            <<"username">> => ?USERNAME,
            <<"password">> => ?PASSWORD,
            <<"ssl">> => #{<<"enable">> => false},
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap0).

action_config(Name, Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"sql">> => sql(Name),
                    <<"max_retries">> => 3
                },
            <<"resource_opts">> => #{
                %% Batch is not yet supported
                %% <<"batch_size">> => 1,
                %% <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

auth_header() ->
    BasicAuth = base64:encode(<<?USERNAME/binary, ":", ?PASSWORD/binary>>),
    {<<"Authorization">>, [<<"Basic ">>, BasicAuth]}.

ensure_scope(Scope, Config) ->
    case get_scope(Scope, Config) of
        {ok, _} ->
            ct:pal("scope ~s already exists", [Scope]),
            ok;
        undefined ->
            ct:pal("creating scope ~s", [Scope]),
            {200, _} = create_scope(Scope, Config),
            ok
    end.

ensure_collection(Scope, Collection, Opts, Config) ->
    case get_collection(Scope, Collection, Config) of
        {ok, _} ->
            ct:pal("collection ~s.~s already exists", [Scope, Collection]),
            ok;
        undefined ->
            ct:pal("creating collection ~s.~s", [Scope, Collection]),
            {200, _} = create_collection(Scope, Collection, Opts, Config),
            ok
    end.

create_scope(Scope, Config) ->
    AdminPool = ?config(admin_pool, Config),
    ReqBody = [<<"name=">>, Scope],
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes">>],
        [
            auth_header(),
            {<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}
        ],
        ReqBody
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, post, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("create scope response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

create_collection(Scope, Collection, _Opts, Config) ->
    AdminPool = ?config(admin_pool, Config),
    ReqBody = [<<"name=">>, Collection],
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes/">>, Scope, <<"/collections">>],
        [
            auth_header(),
            {<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}
        ],
        ReqBody
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, post, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("create collection response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

delete_scope(Scope, Config) ->
    AdminPool = ?config(admin_pool, Config),
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes/">>, Scope],
        [auth_header()]
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, StatusCode, _Headers, Body0} = ehttpc:request(
        AdminPool, delete, Request, RequestTTL, MaxRetries
    ),
    Body = maybe_decode_json(Body0),
    ct:pal("delete scope response:\n  ~p", [{StatusCode, Body}]),
    {StatusCode, Body}.

get_scopes(Config) ->
    AdminPool = ?config(admin_pool, Config),
    Request = {
        [<<"/pools/default/buckets/">>, ?BUCKET, <<"/scopes">>],
        [auth_header()]
    },
    RequestTTL = timer:seconds(5),
    MaxRetries = 3,
    {ok, 200, _Headers, Body0} = ehttpc:request(AdminPool, get, Request, RequestTTL, MaxRetries),
    Body = maybe_decode_json(Body0),
    ct:pal("get scopes response:\n  ~p", [Body]),
    Body.

get_scope(Scope, Config) ->
    #{<<"scopes">> := Scopes} = get_scopes(Config),
    fetch_with_name(Scopes, Scope).

get_collections(Scope, Config) ->
    maybe
        {ok, #{<<"collections">> := Cs}} = get_scope(Scope, Config),
        {ok, Cs}
    end.

get_collection(Scope, Collection, Config) ->
    maybe
        {ok, Cs} = get_collections(Scope, Config),
        fetch_with_name(Cs, Collection)
    end.

fetch_with_name(Xs, Name) ->
    case [X || X = #{<<"name">> := N} <- Xs, N =:= Name] of
        [] ->
            undefined;
        [X] ->
            {ok, X}
    end.

maybe_decode_json(Body) ->
    case emqx_utils_json:safe_decode(Body, [return_maps]) of
        {ok, JSON} ->
            JSON;
        {error, _} ->
            Body
    end.

%% Collection creation is async...  Trying to insert or select from a recently created
%% collection might result in error 12003, "Keyspace not found in CB datastore".
%% https://www.couchbase.com/forums/t/error-creating-primary-index-immediately-after-collection-creation-keyspace-not-found-in-cb-datastore/32479
wait_until_collection_is_ready(Scope, Collection, Config) ->
    wait_until_collection_is_ready(Scope, Collection, Config, _N = 5, _SleepMS = 200).

wait_until_collection_is_ready(Scope, Collection, _Config, N, _SleepMS) when N < 0 ->
    error({collection_not_ready_timeout, Scope, Collection});
wait_until_collection_is_ready(Scope, Collection, Config, N, SleepMS) ->
    case get_all_rows(Scope, Collection, Config) of
        {ok, _} ->
            ct:pal("collection ~s.~s ready", [Scope, Collection]),
            ok;
        Resp ->
            ct:pal("waiting for collection ~s.~s error response:\n  ~p", [Scope, Collection, Resp]),
            ct:sleep(SleepMS),
            wait_until_collection_is_ready(Scope, Collection, Config, N - 1, SleepMS)
    end.

scope() ->
    <<"some_scope">>.

sql(Name) ->
    Scope = scope(),
    iolist_to_binary([
        <<"insert into default:mqtt.">>,
        Scope,
        <<".">>,
        <<"`">>,
        Name,
        <<"`">>,
        <<" (key, value) values (${.id}, ${.})">>
    ]).

get_all_rows(Scope, Collection, Config) ->
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    SQL = iolist_to_binary([
        <<"select * from default:mqtt.">>,
        Scope,
        <<".">>,
        <<"`">>,
        Collection,
        <<"`">>
    ]),
    Opts = #{},
    Resp = emqx_bridge_couchbase_connector:query(ConnResId, SQL, Opts),
    ct:pal("get rows response:\n  ~p", [Resp]),
    case Resp of
        {ok, #{
            status_code := 200, body := #{<<"status">> := <<"success">>, <<"results">> := Rows0}
        }} ->
            Rows = lists:map(
                fun(#{Collection := Value}) ->
                    maybe_decode_json(Value)
                end,
                Rows0
            ),
            {ok, Rows};
        {error, _} ->
            Resp
    end.

pre_publish_fn(Scope, Collection, Config) ->
    fun(Context) ->
        ensure_scope(Scope, Config),
        ensure_collection(Scope, Collection, _Opts = #{}, Config),
        wait_until_collection_is_ready(Scope, Collection, Config),
        Context
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "couchbase_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

t_rule_action(Config) ->
    Scope = scope(),
    Collection = ?config(action_name, Config),
    PrePublishFn = pre_publish_fn(Scope, Collection, Config),
    PostPublishFn = fun(#{payload := Payload} = Context) ->
        %% need to retry because things are async in couchbase
        ?retry(
            100,
            10,
            ?assertMatch(
                {ok, [#{<<"payload">> := Payload}]},
                get_all_rows(Scope, Collection, Config)
            )
        ),
        Context
    end,
    Opts = #{pre_publish_fn => PrePublishFn, post_publish_fn => PostPublishFn},
    ok = emqx_bridge_v2_testlib:t_rule_action(Config, Opts),
    ok.

%% batch is not yet supported
skip_t_rule_action_batch(Config0) ->
    Config = emqx_bridge_v2_testlib:proplist_update(Config0, action_config, fun(ActionConfig) ->
        emqx_utils_maps:deep_merge(
            ActionConfig,
            #{
                <<"resource_opts">> => #{
                    <<"batch_size">> => 10,
                    <<"batch_time">> => <<"100ms">>
                }
            }
        )
    end),
    Scope = scope(),
    Collection = ?config(action_name, Config),
    PrePublishFn = pre_publish_fn(Scope, Collection, Config),
    PublishFn = fun(#{rule_topic := RuleTopic, payload_fn := PayloadFn} = Context) ->
        Payloads = emqx_utils:pmap(
            fun(_) ->
                Payload = PayloadFn(),
                ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
                {ok, C} = emqtt:start_link(#{clean_start => true, clientid => ClientId}),
                {ok, _} = emqtt:connect(C),
                ?assertMatch({ok, _}, emqtt:publish(C, RuleTopic, Payload, [{qos, 2}])),
                Payload
            end,
            lists:seq(1, 10)
        ),
        Context#{payloads => Payloads}
    end,
    PostPublishFn = fun(#{payloads := _Payloads} = Context) ->
        %% need to retry because things are async in couchbase
        ?retry(
            200,
            10,
            ?assertMatch(
                {ok, [#{<<"payload">> := todo}]},
                get_all_rows(Scope, Collection, Config)
            )
        ),
        Context
    end,
    Opts = #{
        pre_publish_fn => PrePublishFn,
        publish_fn => PublishFn,
        post_publish_fn => PostPublishFn
    },
    ok = emqx_bridge_v2_testlib:t_rule_action(Config, Opts),
    ok.

t_sync_query_down(Config) ->
    Scope = scope(),
    Collection = ?config(action_name, Config),
    MakeMsgFn = fun(RuleTopic) ->
        ensure_scope(Scope, Config),
        ensure_collection(Scope, Collection, _Opts = #{}, Config),
        wait_until_collection_is_ready(Scope, Collection, Config),
        emqx_message:make(RuleTopic, <<"hi">>)
    end,
    Opts = #{
        make_message_fn => MakeMsgFn,
        enter_tp_filter => ?match_event(#{?snk_kind := "couchbase_on_query_enter"}),
        error_tp_filter => ?match_event(#{?snk_kind := "couchbase_query_error"}),
        success_tp_filter => ?match_event(#{?snk_kind := "couchbase_query_success"})
    },
    emqx_bridge_v2_testlib:t_sync_query_down(Config, Opts),
    ok.
