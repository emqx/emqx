%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_bridge_zerobus.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("grpc/include/grpc.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(mock, mock).
-define(mock_only, mock_only).
-define(real_only, real_only).

-define(grpc, grpc).
-define(rest, rest).
-define(json, json).

-define(sync, sync).
-define(async, async).
-define(batching, batching).
-define(not_batching, not_batching).

-define(GRPC_SERVER, zerobus_grpc).
-define(REST_SERVER, zerobus_rest).
-define(AUTHN_SERVER, zerobus_authn).

-define(GRPC_PORT, 1234).
-define(REST_PORT, 1235).
-define(AUTHN_PORT, 1236).

-define(AUTHN_RESP_PT, {?MODULE, authn_resp}).
-define(REST_RESP_PT, {?MODULE, rest_resp}).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    {Mock, ConnConfig, ActionConfig} =
        case os:getenv("REAL_ZEROBUS_HOCON", "") of
            "" ->
                {
                    _Mock = true,
                    _ConnConfig = undefined,
                    _ActionConfig = undefined
                };
            HoconPath ->
                case hocon:files([HoconPath]) of
                    {error, Reason} ->
                        error({failed_to_load_real_zerobus_hocon, HoconPath, Reason});
                    {ok, #{
                        ~"connectors" := #{?CONNECTOR_TYPE_BIN := #{~"test" := ConnConfig0}},
                        ~"actions" := #{?ACTION_TYPE_BIN := #{~"test" := ActionConfig0}}
                    }} ->
                        {
                            _Mock = false,
                            ConnConfig0,
                            ActionConfig0
                        };
                    {ok, Conf} ->
                        error({invalid_real_zerobus_hocon, HoconPath, Conf})
                end
        end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_zerobus,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    case Mock of
        true ->
            ct:print(asciiart:visible($%, "running with MOCKED zerobus", []));
        false ->
            ct:print(asciiart:visible($%, "running with REAL zerobus", []))
    end,
    [
        {apps, Apps},
        {?mock, Mock},
        {real_connector_config, ConnConfig},
        {real_action_config, ActionConfig}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?batching, TCConfig) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig];
init_per_group(?not_batching, TCConfig) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    maybe
        ok ?= skip_invalid(TestCase, TCConfig0),
        TCConfig = pre_init_per_testcase(TestCase, TCConfig0),
        do_init_per_testcase(TestCase, TCConfig)
    end.

pre_init_per_testcase(TestCase, TCConfig0) when is_list(TCConfig0) ->
    case get_config(?mock, TCConfig0) of
        true ->
            WorkspaceURL = start_mocked_authn_server(TestCase, TCConfig0),
            Tab = ets:new(reqs, [public, ordered_set]),
            TCConfig = [{mocked_call_tab, Tab} | TCConfig0],
            case transport_of(TCConfig) of
                ?grpc ->
                    {ZerobusEndpoint, Agent} = start_mocked_grpc_server(TestCase, TCConfig),
                    mocked_grpc_server_agent_set(simple_spy_server(TCConfig)),
                    [
                        {mocked_workspace_url, WorkspaceURL},
                        {mocked_zerobus_endpoint, ZerobusEndpoint},
                        {mocked_grpc_server_agent, Agent}
                        | TCConfig
                    ];
                ?rest ->
                    ZerobusEndpoint = start_mocked_rest_server(TestCase, TCConfig),
                    [
                        {mocked_workspace_url, WorkspaceURL},
                        {mocked_zerobus_endpoint, ZerobusEndpoint}
                        | TCConfig
                    ]
            end;
        false ->
            TCConfig0
    end.

mocked_grpc_server_agent_set(St) ->
    emqx_bridge_zerobus_test_grpc_server:agent_set(St).

mocked_grpc_server_agent_update(Fn) ->
    emqx_bridge_zerobus_test_grpc_server:agent_update(Fn).

do_init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    Transport =
        case transport_of(TCConfig) of
            ?grpc ->
                grpc_transport_config();
            ?rest ->
                rest_transport_config()
        end,
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(
        merge_maps([
            #{
                ~"authentication" => authn_config(TCConfig),
                ~"transport" => Transport,
                ~"zerobus_endpoint" => zerobus_endpoint_config(TCConfig)
            },
            ssl_config_overrides(TCConfig)
        ])
    ),
    Record =
        case record_of(TCConfig) of
            ?json ->
                json_record_config();
            ?proto ->
                proto_record_config()
        end,
    ActionName = ConnectorName,
    ActionConfig = action_config(
        merge_maps([
            real_action_overrides(TCConfig),
            #{~"connector" => ConnectorName},
            #{~"parameters" => #{~"record" => Record}},
            #{
                ~"resource_opts" => #{
                    ~"query_mode" => get_config(query_mode, TCConfig, ~"sync"),
                    ~"batch_size" => get_config(batch_size, TCConfig, 1),
                    ~"batch_time" => get_config(batch_time, TCConfig, ~"0ms")
                }
            }
        ])
    ),
    snabbkaffe:start_trace(),
    TCConfig1 = [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ],
    ok = create_serde(TCConfig1),
    TCConfig1.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

now_ns() ->
    erlang:monotonic_time(nanosecond).

skip_invalid(TestCase, TCConfig) ->
    IsMockOnly = get_tc_prop(TestCase, ?mock_only, false),
    IsRealOnly = get_tc_prop(TestCase, ?real_only, false),
    IsMock = get_config(?mock, TCConfig),
    case 1 of
        _ when IsMock andalso IsRealOnly ->
            {skip, "test only run with real zerobus"};
        _ when not IsMock andalso IsMockOnly ->
            {skip, "test only run with mocked zerobus"};
        _ ->
            ok
    end.

maybe_with_forced_sync_query_mode(TCConfig, Fn) ->
    case query_mode(TCConfig) of
        ?sync ->
            emqx_bridge_v2_testlib:with_forced_sync_callback_mode(?CONNECTOR_TYPE, Fn);
        ?async ->
            Fn()
    end.

query_mode(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?sync, ?async], ?sync).

batching(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?batching, ?not_batching], ?not_batching).

authn_config(TCConfig) when is_list(TCConfig) ->
    authn_config(maps:from_list(TCConfig));
authn_config(#{mock := true} = TCConfig) ->
    #{mocked_workspace_url := WorkspaceURL} = TCConfig,
    #{
        ~"client_id" => ~"my-client-id",
        ~"client_secret" => ~"my-client-secret",
        ~"workspace_id" => ~"my-workspace-id",
        ~"workspace_url" => WorkspaceURL
    };
authn_config(#{mock := false} = TCConfig) ->
    #{real_connector_config := #{~"authentication" := Authn}} = TCConfig,
    Authn.

zerobus_endpoint_config(TCConfig) when is_list(TCConfig) ->
    zerobus_endpoint_config(maps:from_list(TCConfig));
zerobus_endpoint_config(#{mock := true} = TCConfig) ->
    #{mocked_zerobus_endpoint := ZerobusEndpoint} = TCConfig,
    ZerobusEndpoint;
zerobus_endpoint_config(#{mock := false} = TCConfig) ->
    #{real_connector_config := #{~"zerobus_endpoint" := ZerobusEndpoint}} = TCConfig,
    ZerobusEndpoint.

transport_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?grpc, ?rest], ?grpc).

record_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?json, ?proto], ?json).

grpc_transport_config() ->
    #{
        ~"type" => ~"grpc",
        ~"connect_timeout" => ~"2s",
        ~"pool_size" => 2
    }.

rest_transport_config() ->
    #{
        ~"type" => ~"rest",
        ~"connect_timeout" => ~"2s",
        ~"pool_size" => 2,
        ~"pipelining" => 100,
        ~"max_inactive" => ~"10s"
    }.

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        ~"zerobus_endpoint" => <<"please override">>,
        ~"authentication" => <<"please override">>,
        ~"transport" => <<"please override">>,
        ~"ssl" => #{~"enable" => false},
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            ~"catalog" => ~"mycatalog",
            ~"schema" => ~"myschema",
            ~"table" => ~"mytable"
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

ssl_config_overrides(TCConfig) when is_list(TCConfig) ->
    ssl_config_overrides(maps:from_list(TCConfig));
ssl_config_overrides(#{mock := true}) ->
    #{};
ssl_config_overrides(#{mock := false} = TCConfig) ->
    #{real_connector_config := #{~"ssl" := SSL}} = TCConfig,
    #{~"ssl" => SSL}.

real_action_overrides(TCConfig) when is_list(TCConfig) ->
    real_action_overrides(maps:from_list(TCConfig));
real_action_overrides(#{mock := true}) ->
    #{};
real_action_overrides(#{mock := false} = TCConfig) ->
    #{real_action_config := #{~"parameters" := Params0}} = TCConfig,
    Params = maps:with(
        [
            ~"catalog",
            ~"schema",
            ~"table"
        ],
        Params0
    ),
    #{~"parameters" => Params}.

json_record_config() ->
    #{
        ~"type" => ~"json"
    }.

proto_record_config() ->
    #{
        ~"type" => ~"proto",
        ~"schema_name" => ~"zerobus",
        ~"message_type" => ~"table_air_quality"
    }.

merge_maps(Maps) ->
    lists:foldl(fun(M, Acc) -> emqx_utils_maps:deep_merge(Acc, M) end, #{}, Maps).

fmt(Fmt, Ctx) -> emqx_bridge_v2_testlib:fmt(Fmt, Ctx).
get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, Default).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

delete_action_api(Config) ->
    Name = emqx_bridge_v2_testlib:get_value(action_name, Config),
    emqx_bridge_v2_testlib:delete_kind_api(action, ?ACTION_TYPE, Name).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            TCConfig
        )
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

start_mocked_authn_server(_TestCase, _TCConfig) ->
    on_exit(fun() -> ok = emqx_utils_http_test_server:stop(?AUTHN_SERVER) end),
    {ok, _} = emqx_utils_http_test_server:start_link(?AUTHN_PORT, "/[...]", false, ?AUTHN_SERVER),
    ok = emqx_utils_http_test_server:set_handler(?AUTHN_SERVER, fun token_server_handler/2),
    WorkspaceURL = fmt(~"http://127.0.0.1:${p}", #{p => ?AUTHN_PORT}),
    WorkspaceURL.

set_mocked_authn_response(Status, Body) ->
    on_exit(fun() -> persistent_term:erase(?AUTHN_RESP_PT) end),
    persistent_term:put(?AUTHN_RESP_PT, {fun(_) -> ok end, Status, Body}),
    ok.

set_mocked_authn_response(PreReplyFn, Status, Body) ->
    on_exit(fun() -> persistent_term:erase(?AUTHN_RESP_PT) end),
    persistent_term:put(?AUTHN_RESP_PT, {PreReplyFn, Status, Body}),
    ok.

token_server_handler(Req0, State) ->
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    ReqHeaders = cowboy_req:headers(Req1),
    Headers = #{<<"content-type">> => <<"application/json">>},
    {PreReplyFn, Status, RepBody} = persistent_term:get(
        ?AUTHN_RESP_PT,
        {fun(_) -> ok end, 200, default_authn_token_encoded()}
    ),
    PreReplyFn(#{headers => ReqHeaders, body => ReqBody}),
    Rep = cowboy_req:reply(Status, Headers, RepBody, Req0),
    {ok, Rep, State}.

default_authn_token() ->
    #{
        <<"access_token">> => <<"mocked.token.resp">>,
        <<"expires_in">> => 3600,
        <<"token_type">> => <<"Bearer">>
    }.

default_authn_token_encoded() ->
    emqx_utils_json:encode(default_authn_token()).

start_mocked_grpc_server(_TestCase, _TCConfig) ->
    on_exit(fun() -> grpc:stop_server(?GRPC_SERVER) end),
    Services = #{
        protos => [emqx_bridge_zerobus_gen_zerobus_service_pb],
        services => #{'databricks.zerobus.Zerobus' => emqx_bridge_zerobus_test_grpc_server}
    },
    Opts = [{ranch_opts, #{shutdown => brutal_kill}}],
    {ok, _} = grpc:start_server(?GRPC_SERVER, ?GRPC_PORT, Services, Opts),
    {ok, Agent} = emqx_utils_agent:start_link(#{}),
    emqx_bridge_zerobus_test_grpc_server:set_agent(Agent),
    ZerobusEndpoint = fmt(~"http://127.0.0.1:${p}", #{p => ?GRPC_PORT}),
    {ZerobusEndpoint, Agent}.

stop_mocked_grpc_server() ->
    grpc:stop_server(?GRPC_SERVER).

start_mocked_rest_server(_TestCase, TCConfig) ->
    Tab = get_config(mocked_call_tab, TCConfig),
    on_exit(fun() -> ok = emqx_utils_http_test_server:stop(?REST_SERVER) end),
    {ok, _} = emqx_utils_http_test_server:start_link(?REST_PORT, "/[...]", false, ?REST_SERVER),
    ok = emqx_utils_http_test_server:set_handler(?REST_SERVER, fun(Req, State) ->
        rest_server_handler(Req, State, #{tab => Tab})
    end),
    fmt(~"http://127.0.0.1:${p}", #{p => ?REST_PORT}).

set_mocked_rest_response(Status, Body) ->
    on_exit(fun() -> persistent_term:erase(?REST_RESP_PT) end),
    persistent_term:put(?REST_RESP_PT, {Status, Body}),
    ok.

rest_server_handler(Req0, State, #{tab := Tab}) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    ReqHeaders = cowboy_req:headers(Req1),
    URI = uri_string:parse(cowboy_req:uri(Req1)),
    Meta = #{headers => ReqHeaders, uri => URI},
    ets:insert(Tab, {now_ns(), #{type => ingest_record_batch, req => Body, meta => Meta}}),
    Headers = #{<<"content-type">> => <<"application/json">>},
    {Status, RepBody} = persistent_term:get(?REST_RESP_PT, {200, ~""}),
    Rep = cowboy_req:reply(Status, Headers, RepBody, Req1),
    {ok, Rep, State}.

stop_mocked_rest_server() ->
    emqx_utils_http_test_server:stop(?REST_SERVER).

wait_port_free(Port, Timeout) ->
    emqx_utils:nolink_apply(
        fun Recur() ->
            case gen_tcp:listen(Port, [{reuseaddr, true}, {active, false}]) of
                {ok, Sock} ->
                    gen_tcp:close(Sock),
                    ok;
                {error, eaddrinuse} ->
                    ct:sleep(200),
                    Recur()
            end
        end,
        Timeout
    ).

spy_create_stream(TCConfig) ->
    Tab = get_config(mocked_call_tab, TCConfig),
    fun(Msg, Req, Meta) ->
        ct:pal("msg: ~p;\n  req: ~p", [Msg, Req]),
        #{payload := {create_stream = Type, Req1}} = Msg,
        ets:insert(Tab, {now_ns(), #{type => Type, req => Req1, meta => Meta}}),
        grpc_stream:reply(Req, [#{payload => {create_stream_response, #{}}}]),
        continue
    end.

spy_ingest_record_batch(TCConfig) ->
    Tab = get_config(mocked_call_tab, TCConfig),
    fun(Msg, Req, Meta) ->
        #{payload := {ingest_record_batch = Type, #{offset_id := Id} = Req1}} = Msg,
        ets:insert(Tab, {now_ns(), #{type => Type, req => Req1, meta => Meta}}),
        grpc_stream:reply(Req, [
            #{
                payload =>
                    {ingest_record_response, #{
                        durability_ack_up_to_offset => Id
                    }}
            }
        ]),
        continue
    end.

simple_spy_server(TCConfig) ->
    #{
        create_stream => {run, spy_create_stream(TCConfig)},
        ingest_record_batch => {run, spy_ingest_record_batch(TCConfig)}
    }.

simple_protobuf_schema1() ->
    ~"""
        syntax = "proto2";

        package air_quality;

        message table_air_quality {
        	optional string device_name = 1;
        	optional int32 temp = 2;
        	optional int64 humidity = 3;
        }
    """.

create_serde(TCConfig) ->
    create_serde(TCConfig, _Opts = #{}).

create_serde(TCConfig, Opts) ->
    #{
        config := #{~"parameters" := #{~"record" := Record}}
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    case Record of
        #{~"type" := ~"json"} ->
            ok;
        #{~"type" := ~"proto"} ->
            Source = maps:get(source, Opts, simple_protobuf_schema1()),
            #{~"schema_name" := Name} = Record,
            on_exit(fun() -> do_delete_serde(Name) end),
            {201, _} = do_create_serde(Name, Source),
            ok
    end.

do_create_serde(Name, Source) ->
    emqx_schema_registry_http_api_SUITE:create_schema(#{
        ~"type" => ~"protobuf",
        ~"name" => Name,
        ~"source" => Source
    }).

delete_serde(TCConfig) ->
    #{
        config := #{~"parameters" := #{~"record" := #{~"schema_name" := Name}}}
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    {_, {ok, _}} =
        ?wait_async_action(
            do_delete_serde(Name),
            #{?snk_kind := schema_registry_serdes_deleted},
            5_000
        ),
    ok.

do_delete_serde(Name) ->
    emqx_schema_registry_http_api_SUITE:delete_schema(Name).

update_serde(TCConfig, Params) ->
    #{
        config := #{~"parameters" := #{~"record" := #{~"schema_name" := Name}}}
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    emqx_schema_registry_http_api_SUITE:update_schema(Name, Params).

full_matrix() ->
    [
        [Transport, Record, Sync, Batch]
     || Transport <- [?grpc, ?rest],
        Record <- [?json, ?proto],
        Sync <- [?sync, ?async],
        Batch <- [?not_batching, ?batching],
        %% unsupported by zerobus
        not (Transport == ?rest andalso Record == ?proto)
    ].

default_sql() ->
    ~"""
      select
        payload.device_name as device_name,
        payload.humidity as humidity,
        payload.temp as temp
      from '${t}'
    """.

decode_batch(Batch0, #{} = _TCConfig) when is_binary(Batch0) ->
    %% json rest body
    emqx_utils_json:decode(Batch0);
decode_batch(Batch0, #{} = TCConfig) ->
    #{
        config := #{~"parameters" := #{~"record" := Record}}
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(maps:to_list(TCConfig)),
    case Record of
        #{~"type" := ~"json"} ->
            {json_batch, #{records := Batch1}} = Batch0,
            lists:map(fun emqx_utils_json:decode/1, Batch1);
        #{~"type" := ~"proto"} ->
            #{
                ~"schema_name" := Schema,
                ~"message_type" := MessageType
            } = Record,
            {proto_encoded_batch, #{records := Batch1}} = Batch0,
            lists:map(
                fun(Data) ->
                    emqx_schema_registry_serde:decode(Schema, Data, [MessageType])
                end,
                Batch1
            )
    end.

read_table(TCConfig) when is_list(TCConfig) ->
    read_table(maps:from_list(TCConfig));
read_table(#{mock := true} = TCConfig) ->
    #{mocked_call_tab := Tab} = TCConfig,
    lists:flatmap(
        fun
            ({_, #{type := ingest_record_batch} = Call}) ->
                case Call of
                    #{req := #{batch := Batch0}} ->
                        %% grpc
                        ok;
                    #{req := Batch0} ->
                        %% rest
                        ok
                end,
                decode_batch(Batch0, TCConfig);
            (_) ->
                []
        end,
        ets:tab2list(Tab)
    );
read_table(#{mock := false}) ->
    %% todo: will require a test container that can read the table.
    ct:fail(unimplemented).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(default_sql(), TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

sample_payload() ->
    emqx_utils_json:encode(#{
        ~"device_name" => emqx_guid:to_hexstr(emqx_guid:gen()),
        ~"temp" => 24,
        ~"humidity" => 60
    }).

seq_payload(N) ->
    emqx_utils_json:encode(#{
        ~"device_name" => integer_to_binary(N),
        ~"temp" => N,
        ~"humidity" => N
    }).

proto_file_path(File) ->
    Base = code:lib_dir(emqx_bridge_zerobus),
    filename:join([Base, "test", "protobuf_data", File]).

proto_file(File) ->
    {ok, Contents} = file:read_file(proto_file_path(File)),
    Contents.

update_protobuf_to_bundle(TCConfig) ->
    #{
        config := #{~"parameters" := #{~"record" := #{~"schema_name" := Name}}}
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    PrivDir = ?config(priv_dir, TCConfig),
    BundleFilename1 = filename:join([PrivDir, "bundle.tar.gz"]),
    FileList1 = [
        {"a.proto", emqx_schema_registry_SUITE:proto_file(<<"a.proto">>)},
        {"nested/b.proto", emqx_schema_registry_SUITE:proto_file(<<"b.proto">>)},
        {"c.proto", emqx_schema_registry_SUITE:proto_file(<<"c.proto">>)},
        {"table1.proto", proto_file(<<"table1.proto">>)}
    ],
    on_exit(fun() -> file:delete(BundleFilename1) end),
    ok = erl_tar:create(BundleFilename1, FileList1, [compressed]),
    {ok, Data1} = file:read_file(BundleFilename1),
    emqx_schema_registry_http_api_SUITE:create_schema_protobuf_bundle(#{
        name => Name,
        files => [{<<"bundle">>, BundleFilename1, Data1}],
        description => ~"my bundle with many files and mesage types",
        root_proto_path => <<"a.proto">>
    }).

find_async_receivers(TCConfig) ->
    #{
        resource_namespace := Namespace,
        type := ActionType,
        name := ActionName,
        connector_type := ConnType,
        connector_name := ConnName
    } = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    PoolSize = emqx:get_namespaced_config(
        Namespace, [connectors, ConnType, ConnName, transport, pool_size]
    ),
    {ok, {ConnResId, ActionResId}} =
        emqx_bridge_v2:get_resource_ids(Namespace, actions, ActionType, ActionName),
    Pool = emqx_bridge_zerobus_action_sup:async_receiver_pool(ConnResId, ActionResId),
    [
        emqx_bridge_zerobus_async_receiver_worker:where(Pool, Idx)
     || Idx <- lists:seq(1, PoolSize)
    ].

find_stream_writers(TCConfig) ->
    #{
        resource_namespace := Namespace,
        type := ActionType,
        name := ActionName,
        connector_type := ConnType,
        connector_name := ConnName
    } = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    PoolSize = emqx:get_namespaced_config(
        Namespace, [connectors, ConnType, ConnName, transport, pool_size]
    ),
    {ok, {ConnResId, ActionResId}} =
        emqx_bridge_v2:get_resource_ids(Namespace, actions, ActionType, ActionName),
    Pool = emqx_bridge_zerobus_action_sup:stream_writer_pool(ConnResId, ActionResId),
    [
        emqx_bridge_zerobus_stream_writer_worker:where(Pool, Idx)
     || Idx <- lists:seq(1, PoolSize)
    ].

find_action_sup(TCConfig) ->
    #{
        resource_namespace := Namespace,
        type := ActionType,
        name := ActionName
    } = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    {ok, {ConnResId, ActionResId}} =
        emqx_bridge_v2:get_resource_ids(Namespace, actions, ActionType, ActionName),
    emqx_bridge_zerobus_action_sup:whereis_action_sup(ConnResId, ActionResId).

get_current_stream(Idx, TCConfig) ->
    Writers = find_stream_writers(TCConfig),
    Writer = lists:nth(Idx, Writers),
    #{stream := Stream} = sys:get_state(Writer),
    Stream.

with_capture_stream(Fn) ->
    Killer = spawn_link(fun() ->
        ?tp_span(
            "kill_client",
            #{},
            receive
                continue -> ok
            end
        )
    end),
    ?force_ordering(
        #{?snk_kind := "zerobus_will_send_batch0"},
        #{?snk_kind := "kill_client", ?snk_span := start}
    ),
    ?force_ordering(
        #{?snk_kind := "kill_client", ?snk_span := {complete, _}},
        #{?snk_kind := "zerobus_will_send_batch1"}
    ),
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "zerobus_will_send_batch0"}),
        10_000
    ),
    Fn(),
    ct:pal("waiting for stream..."),
    {ok, [#{stream := Stream, ?snk_meta := #{pid := Writer}}]} = snabbkaffe:receive_events(SRef0),
    ct:pal("stream: ~p", [Stream]),
    #{
        grpc_stream => Stream,
        killer => Killer,
        writer => Writer
    }.

kill_grpc_client(Stream, Killer) ->
    #{client_pid := GRPCClient} = Stream,
    MRef = monitor(process, GRPCClient),
    %% important to use `shutdown` here, as it serves to test that the grpc supervision
    %% tree is no longer using `transient` restarts.
    ct:pal("killing grpc client ~p", [GRPCClient]),
    exit(GRPCClient, shutdown),
    receive
        {'DOWN', MRef, process, _, _} ->
            ok
    after 5_000 ->
        ct:fail("client didn't die")
    end,
    ct:pal("grpc client ~p dead; resuming killer", [GRPCClient]),
    Killer ! continue,
    ok.

server_shutdown_action() ->
    {shutdown, ?GRPC_STATUS_UNAVAILABLE, <<
        "Stream timeout reached. Closing the stream (ID: ...)."
        " Error Code: 6002, Error State: 0."
    >>}.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [Transport, ?json, ?sync, ?not_batching]
     || Transport <- [?grpc, ?rest]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "zerobus_connector_stop").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [Transport, ?json, ?sync, ?not_batching]
     || Transport <- [?grpc, ?rest]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    TCConfigMap = maps:from_list(TCConfig),
    ?assertMatch(
        {201, #{~"status" := ~"connected"}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{~"status" := ~"connected"}},
        create_action_api(TCConfig, #{})
    ),
    maybe
        %% can't use toxiproxy for this...
        #{mock := true} ?= TCConfigMap,
        ct:pal("stopping servers"),
        stop_mocked_grpc_server(),
        stop_mocked_rest_server(),
        ?retry(
            500,
            10,
            ?assertMatch(
                {200, #{~"status" := ~"disconnected"}},
                get_connector_api(TCConfig)
            )
        ),
        ?retry(
            500,
            10,
            ?assertMatch(
                {200, #{~"status" := ~"disconnected"}},
                get_action_api(TCConfig)
            )
        ),
        %% recovery
        ct:pal("restarting servers"),
        %% n.b.: agent is replaced here without the spy handler, but no matter.
        start_mocked_grpc_server(?FUNCTION_NAME, TCConfig),
        start_mocked_rest_server(?FUNCTION_NAME, TCConfig),
        ?retry(
            500,
            10,
            ?assertMatch(
                {200, #{~"status" := ~"connected"}},
                get_connector_api(TCConfig)
            )
        ),
        ?retry(
            500,
            10,
            ?assertMatch(
                {200, #{~"status" := ~"connected"}},
                get_action_api(TCConfig)
            )
        )
    end.

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PayloadFn = fun sample_payload/0,
    PostPublishFn = fun(Context) ->
        #{payload := PayloadBin} = Context,
        Payload = emqx_utils_json:decode(PayloadBin),
        ?retry(
            200,
            10,
            ?assertMatch(
                [Payload],
                read_table(TCConfig),
                #{payload => Payload}
            )
        ),
        ok
    end,
    RuleTopic = ~"t/zb",
    SQL = fmt(default_sql(), #{t => RuleTopic}),
    Opts = #{
        payload_fn => PayloadFn,
        rule_topic => RuleTopic,
        sql => SQL,
        post_publish_fn => PostPublishFn
    },
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts),
        maybe
            true ?= get_config(?mock, TCConfig),
            Tab = get_config(mocked_call_tab, TCConfig),
            case transport_of(TCConfig) of
                ?grpc ->
                    BadMetas = lists:filter(
                        fun
                            (
                                {_, #{
                                    meta :=
                                        #{
                                            ~"authorization" := ~"Bearer mocked.token.resp",
                                            ~"x-databricks-zerobus-table-name" := <<_/binary>>
                                        }
                                }}
                            ) ->
                                false;
                            (_) ->
                                true
                        end,
                        ets:tab2list(Tab)
                    ),
                    ?assertEqual([], BadMetas),
                    ok;
                ?rest ->
                    BadMetas = lists:filter(
                        fun
                            (
                                {_, #{
                                    meta :=
                                        #{
                                            headers := #{
                                                ~"authorization" := ~"Bearer mocked.token.resp"
                                            }
                                        }
                                }}
                            ) ->
                                false;
                            (_) ->
                                true
                        end,
                        ets:tab2list(Tab)
                    ),
                    ?assertEqual([], BadMetas),
                    ok
            end
        end
    end).

t_rule_test_trace() ->
    [{mock_only, true}, {matrix, true}].
t_rule_test_trace(matrix) ->
    full_matrix();
t_rule_test_trace(TCConfig) when is_list(TCConfig) ->
    PayloadFn = fun sample_payload/0,
    Opts = #{
        payload_fn => PayloadFn,
        rule_sql => default_sql()
    },
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, Opts).

-doc """
Checks the situation when the oauth2 endpoint returns an error.

Both bad secrets and referencing inexistent tables return 401.
""".
t_bad_token_endpoint_responses() ->
    [{mock_only, true}].
t_bad_token_endpoint_responses(TCConfig) when is_list(TCConfig) ->
    %% we only have enough info for the token at the action
    ?assertMatch(
        {201, #{~"status" := ~"connected"}},
        create_connector_api(TCConfig, #{
            ~"authentication" => #{
                ~"client_secret" => ~"wrong secret",
                ~"timeout" => ~"1s"
            }
        })
    ),

    %% timeout
    set_mocked_authn_response(
        fun(_) -> ct:sleep(30_000) end,
        200,
        default_authn_token_encoded()
    ),
    ExpectedReason1 = iolist_to_binary(
        io_lib:format("~p", [{token_error, {failed_to_fetch_token, timeout}}])
    ),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason1
        }},
        create_action_api(TCConfig, #{})
    ),
    {204, _} = delete_action_api(TCConfig),

    set_mocked_authn_response(
        401,
        emqx_utils_json:encode(#{
            <<"error">> => <<"invalid_client">>,
            <<"error_description">> =>
                <<"Client authentication failed">>,
            <<"error_id">> =>
                <<"65d39992-5944-4c91-80fe-213ca49ff830">>
        })
    ),
    ExpectedReason2 = iolist_to_binary(
        io_lib:format("~p", [{unhealthy_target, ~"Invalid credentials: invalid_client"}])
    ),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason2
        }},
        create_action_api(TCConfig, #{})
    ),
    {204, _} = delete_action_api(TCConfig),

    %% misc errors
    set_mocked_authn_response(500, ~"?!?"),
    ExpectedReason3 = ~"Bad token status code 500",
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason3
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

-doc """
For coverage.

Issues with the token are likely to arise before the open stream request is sent.
""".
t_bad_credentials_open_stream() ->
    [{mock_only, true}].
t_bad_credentials_open_stream(TCConfig) when is_list(TCConfig) ->
    %% "unauthenticated"
    mocked_grpc_server_agent_set(#{
        create_stream => {shutdown, ?GRPC_STATUS_UNAUTHENTICATED, ~"bad auth"}
    }),
    ?assertMatch(
        {201, #{~"status" := ~"connected"}},
        create_connector_api(TCConfig, #{})
    ),
    ExpectedReason1 = iolist_to_binary(
        io_lib:format("~p", [{unhealthy_target, ~"Invalid credentials: bad auth"}])
    ),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason1
        }},
        create_action_api(TCConfig, #{})
    ),
    {204, _} = delete_action_api(TCConfig),
    %% "permission_denied"
    mocked_grpc_server_agent_set(#{
        create_stream => {shutdown, ?GRPC_STATUS_PERMISSION_DENIED, ~"permission denied"}
    }),
    ExpectedReason2 = iolist_to_binary(
        io_lib:format("~p", [{unhealthy_target, ~"Permission denied: permission denied"}])
    ),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason2
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

-doc """
For coverage.

Tries to cover unexpected errors when opening the stream and that we set the action state
to disconnected.
""".
t_generic_errors_open_stream() ->
    [{mock_only, true}].
t_generic_errors_open_stream(TCConfig) when is_list(TCConfig) ->
    mocked_grpc_server_agent_set(#{
        create_stream => {shutdown, ?GRPC_STATUS_RESOURCE_EXHAUSTED, ~"whew"}
    }),
    ?assertMatch(
        {201, #{~"status" := ~"connected"}},
        create_connector_api(TCConfig, #{})
    ),
    ExpectedReason1 = ~"resource_exhausted: whew",
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ExpectedReason1
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

-doc """
Verifies that we recover from the server periodically closing the stream while ingesting.
""".
t_recover_stream_close_during_ingestion() ->
    [{mock_only, true}].
t_recover_stream_close_during_ingestion(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    mocked_grpc_server_agent_set(#{
        ingest_record_batch =>
            [
                server_shutdown_action(),
                {run, spy_ingest_record_batch(TCConfig)}
            ]
    }),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 1,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies handling of `grpc_client:send` failing as we send a batch.

If the grpc client dies for unknown reasons, we should eventually retry and succeed.
""".
t_stream_gone_before_ingest() ->
    [{matrix, true}, {mock_only, true}].
t_stream_gone_before_ingest(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_stream_gone_before_ingest(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    TestPid = self(),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            batch_done => [{ask, TestPid}, continue]
        }
    end),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% make the stream disappear from the client state
    #{grpc_stream := Stream, killer := Killer} =
        with_capture_stream(fun() ->
            emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}])
        end),
    kill_grpc_client(Stream, Killer),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 1,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies handling of a schema serde disappearing as we need to serialize protobuf records.

There's nothing to do, so it's unrecoverable.
""".
t_serde_gone_before_ingest() ->
    [{matrix, true}].
t_serde_gone_before_ingest(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_serde_gone_before_ingest(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    delete_serde(TCConfig),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 0,
                    ~"failed" := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies handling of a schema serde missing as we create the action.
""".
t_serde_gone_before_create() ->
    [{matrix, true}].
t_serde_gone_before_create(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_serde_gone_before_create(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    delete_serde(TCConfig),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ~"Schema not found"
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

-doc """
Verifies handling of a schema serde changing to an invalid type as we need to serialize
protobuf records.

There's nothing to do, so it's unrecoverable.
""".
t_serde_changed_type_before_ingest() ->
    [{matrix, true}].
t_serde_changed_type_before_ingest(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_serde_changed_type_before_ingest(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    update_serde(TCConfig, #{
        ~"type" => ~"json",
        ~"source" => ~"{}"
    }),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 0,
                    ~"failed" := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies handling of a schema serde with the wrong type as we create the action.
""".
t_serde_wrong_type_before_create() ->
    [{matrix, true}].
t_serde_wrong_type_before_create(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_serde_wrong_type_before_create(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    update_serde(TCConfig, #{
        ~"type" => ~"json",
        ~"source" => ~"{}"
    }),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ~"Invalid schema type; must be protobuf"
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

-doc """
Verifies handling of a schema serde with the wrong type as we create the action.
""".
t_serde_bad_message_type_before_create() ->
    [{matrix, true}].
t_serde_bad_message_type_before_create(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_serde_bad_message_type_before_create(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{
            ~"status" := ~"disconnected",
            ~"status_reason" := ~"Message type not found in protobuf schema/bundle"
        }},
        create_action_api(TCConfig, #{
            ~"parameters" => #{~"record" => #{~"message_type" => ~"unknown_msg"}}
        })
    ),
    ok.

-doc """
Verifies handling of invalid protobuf data when serializing.

There's nothing to do, so it's unrecoverable.
""".
t_bad_protobuf_data() ->
    [{matrix, true}].
t_bad_protobuf_data(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_bad_protobuf_data(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% missing all fields, unknown field
    emqtt:publish(C, RuleTopic, ~|{"a": 1}|, [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 0,
                    ~"failed" := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies handling of invalid json data.

There's nothing to do, so it's unrecoverable.
""".
t_bad_json_data() ->
    [{matrix, true}].
t_bad_json_data(matrix) ->
    [
        [Transport, ?json, ?sync, ?not_batching]
     || Transport <- [?grpc, ?rest]
    ];
t_bad_json_data(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ErrorMsg = <<
        "Record decoder/encoder error: invalid digit found in string at"
        " line 1 column 19. Error Code: 4044, Error State: 3."
    >>,
    case transport_of(TCConfig) of
        ?rest ->
            set_mocked_rest_response(
                400,
                emqx_utils_json:encode(#{
                    ~"error_code" => ~"INVALID_PARAMETER_VALUE",
                    ~"message" => ErrorMsg
                })
            );
        ?grpc ->
            mocked_grpc_server_agent_update(fun(St) ->
                St#{
                    ingest_record_batch => [{shutdown, ?GRPC_STATUS_INVALID_ARGUMENT, ErrorMsg}]
                }
            end)
    end,
    %% missing all fields, unknown field
    emqtt:publish(C, RuleTopic, ~|{"a": 1}|, [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 0,
                    ~"failed" := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies that we support protobuf bundles.

Uses a bundle with multiple files and message types, so we must traverse them when finding
the descriptor proto.
""".
t_protobuf_bundle() ->
    [{matrix, true}].
t_protobuf_bundle(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_protobuf_bundle(TCConfig) when is_list(TCConfig) ->
    update_protobuf_to_bundle(TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 1,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
For coverage.

Makes a sync call that will timeout.  Even though it's recoverable, the timeout is the
request TTL, so in practice the request will expire.
""".
t_grpc_sync_ingest_timeout() ->
    [{matrix, true}].
t_grpc_sync_ingest_timeout(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_grpc_sync_ingest_timeout(TCConfig) when is_list(TCConfig) ->
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        {201, _} = create_connector_api(TCConfig, #{
            ~"transport" => #{~"pool_size" => 1}
        }),
        {201, _} = create_action_api(TCConfig, #{
            ~"resource_opts" => #{~"request_ttl" => ~"1s"}
        }),
        #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
        C = start_client(),
        IngestFn = spy_ingest_record_batch(TCConfig),
        mocked_grpc_server_agent_update(fun(St) ->
            St#{
                ingest_record_batch => [
                    {run, fun(Msg, Req, Meta) ->
                        ct:sleep(2_000),
                        IngestFn(Msg, Req, Meta)
                    end},
                    default
                ]
            }
        end),
        emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
        ?retry(
            700,
            10,
            ?assertMatch(
                {200, #{
                    ~"metrics" := #{
                        ~"matched" := 1,
                        ~"success" := 0,
                        ~"failed" := 0,
                        ~"dropped" := 1,
                        ~"dropped.expired" := 1
                    }
                }},
                get_action_metrics_api(TCConfig)
            )
        ),
        ok
    end).

-doc """
Verifies async receive recovery.  It should consult the stream registered by its
corresponding writer and start pulling again as it restarts.

  1) A couple messages as published, so we have some existing state.
  2) Async receiver dies (for whatever reason) and restarts.
  3) It picks up the stream and starts pulling.
  4) Another message is published via the same stream.  The receiver should get the ack.
""".
t_async_receiver_recover() ->
    [{matrix, true}, {mock_only, true}].
t_async_receiver_recover(matrix) ->
    [[?grpc, ?proto, ?sync, ?not_batching]];
t_async_receiver_recover(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}])
            end,
            #{?snk_kind := "zerobus_writer_acked", seq := 1},
            5_000
        ),
    [AsyncWorker] = find_async_receivers(TCConfig),
    MRef = monitor(process, AsyncWorker),
    exit(AsyncWorker, kill),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end,
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
            #{?snk_kind := "zerobus_writer_acked", seq := 2},
            5_000
        ),
    ok.

-doc """
Verifies retries when using REST transport.

This functionality is identical to the one in the HTTP connector.
""".
t_rest_retries() ->
    [{matrix, true}, {mock_only, true}].
t_rest_retries(matrix) ->
    [[?rest, ?json, ?async, ?not_batching]];
t_rest_retries(TCConfig) when is_list(TCConfig) ->
    Tab = get_config(mocked_call_tab, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    AuthnResp0 = default_authn_token(),
    AuthnResp = AuthnResp0#{~"expires_in" => 1},
    set_mocked_authn_response(200, emqx_utils_json:encode(AuthnResp)),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, Agent} = emqx_utils_agent:start_link(1),
    GetAndBump = fun() ->
        emqx_utils_agent:get_and_update(Agent, fun(N) -> {N, N + 1} end)
    end,
    FreeRetryError = {error, {shutdown, normal}},
    %% succeed after some tries
    %% also check that we refresh the token when retrying.
    TestPid = self(),
    NewToken = ~"new.mocked.token",
    NewHeader = <<"Bearer ", NewToken/binary>>,
    set_mocked_authn_response(
        fun(Ctx) -> TestPid ! {authn_req, Ctx} end,
        200,
        emqx_utils_json:encode(AuthnResp#{~"access_token" := NewToken})
    ),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_zerobus_impl,
        rest_reply_delegator,
        fun(Context, ReplyFunAndArgs, Result) ->
            Attempts = GetAndBump(),
            case Attempts > 2 of
                true ->
                    ct:pal("succeeding ~p : ~p", [FreeRetryError, Attempts]),
                    meck:passthrough([Context, ReplyFunAndArgs, Result]);
                false ->
                    ct:pal("failing ~p : ~p", [FreeRetryError, Attempts]),
                    %% ensure token is expired and refreshes
                    ct:sleep(1_001),
                    meck:passthrough([Context, ReplyFunAndArgs, FreeRetryError])
            end
        end,
        fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := handle_async_reply},
                    20_000
                ),
            ?retry(
                700,
                10,
                ?assertMatch(
                    {200, #{
                        ~"metrics" := #{
                            ~"matched" := 1,
                            ~"success" := 1,
                            ~"failed" := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ?assertReceive({authn_req, _}),
            ?assertMatch(
                [_ | _],
                [
                    Req
                 || {_, #{meta := #{headers := #{~"authorization" := NewHeader0}}}} = Req <-
                        ets:tab2list(Tab),
                    NewHeader0 == NewHeader
                ],
                #{tab => ets:tab2list(Tab)}
            ),
            ok
        end
    ),
    %% exhausts retries
    Error = {error, huh},
    emqx_common_test_helpers:with_mock(
        emqx_bridge_zerobus_impl,
        rest_reply_delegator,
        fun(Context, ReplyFunAndArgs, _Result) ->
            ct:pal("failing ~p", [Error]),
            meck:passthrough([Context, ReplyFunAndArgs, Error])
        end,
        fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := handle_async_reply},
                    20_000
                ),
            ?retry(
                700,
                10,
                ?assertMatch(
                    {200, #{
                        ~"metrics" := #{
                            ~"matched" := 2,
                            ~"success" := 1,
                            ~"failed" := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end
    ),
    ok.

-doc """
  1) A few messages have been published to stream 1.
  2) Concurrently:
     a) Receiver receives an ack.
     b) Writer decides to open new stream (e.g.: server closes it).
  3) Writer opens new stream and tells receiver to follow it.
  4) Writer receives the last acked seq before replying to callers.
""".
t_follow_new_stream_race() ->
    [{matrix, true}, {mock_only, true}].
t_follow_new_stream_race(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_follow_new_stream_race(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    {201, _} = create_action_api(TCConfig, #{
        ~"resource_opts" => #{~"request_ttl" => ~"15s"}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    TestPid = self(),
    IngestFn = spy_ingest_record_batch(TCConfig),
    Tab = get_config(mocked_call_tab, TCConfig),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            ingest_record_batch => [
                %% 1st will receive its ack right away
                {run, fun(_Msg, GRPCReq, _Meta) ->
                    TestPid ! {grpc_req, GRPCReq},
                    {run, IngestFn}
                end},
                %% 2nd ack will be delayed
                {run, fun(Msg, _GRPCReq, _Meta) ->
                    #{payload := {ingest_record_batch = Type, Req1}} = Msg,
                    ets:insert(Tab, {now_ns(), #{type => Type, req => Req1}}),
                    TestPid ! second_msg,
                    continue
                end},
                %% we'll ack the 2nd ack here, and close the stream
                {run, fun(Msg, _GRPCReq, _Meta) ->
                    TestPid ! {msg, Msg},
                    {run, IngestFn}
                end}
            ]
        }
    end),
    ct:pal("publishing 1st message"),
    {_, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
            #{?snk_kind := "zerobus_writer_acked", seq := 0},
            5_000
        ),
    GRPCReq =
        receive
            {grpc_req, GRPCReq0} ->
                GRPCReq0
        after 1_000 -> ct:fail("didn't get grpc req")
        end,
    ct:pal("publishing 2nd message"),
    emqtt:publish(C, RuleTopic, seq_payload(2), [{qos, 1}]),
    ?assertReceive(second_msg),
    ct:pal("publishing 3rd message; "),
    #{grpc_stream := Stream0, killer := Killer} =
        with_capture_stream(fun() ->
            emqtt:publish(C, RuleTopic, seq_payload(3), [{qos, 1}])
        end),
    ct:pal("injecting race"),
    %% now server replies ack as it closes the stream, and as writer attempts a write.
    %% ack 2nd message
    {_, {ok, _}} =
        ?wait_async_action(
            grpc_stream:reply(GRPCReq, [
                #{
                    payload =>
                        {ingest_record_response, #{
                            durability_ack_up_to_offset => 1
                        }}
                }
            ]),
            #{?snk_kind := "zerobus_receiver_notified_ack", seq := 1},
            5_000
        ),
    %% writer is still paused just before sending the 3rd message at this point, and the
    %% receiver updated its state with the last acked seq.
    kill_grpc_client(Stream0, Killer),
    %% should have retried only the 3rd message
    {msg, #{payload := {ingest_record_batch, #{batch := BatchRetried}}}} =
        ?assertReceive({msg, _}, 15_000),
    ?assertMatch(
        [#{~"temp" := 3}],
        decode_batch(BatchRetried, maps:from_list(TCConfig))
    ),
    %% Each was only received by the server once.
    ?assertMatch(
        [
            #{~"temp" := 1},
            #{~"temp" := 2},
            #{~"temp" := 3}
        ],
        read_table(TCConfig)
    ),
    ok.

-doc """
Covers the case where `grpc_client:send` returns a non-explosive error like `{error,
not_found}` while ingesting records.
""".
t_ingest_grpc_send_errors() ->
    [{matrix, true}, {mock_only, true}].
t_ingest_grpc_send_errors(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_ingest_grpc_send_errors(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% recoverable error
    {ok, Agent} = emqx_utils_agent:start_link(not_found),
    %% is there a more "organic" way to provoke this?
    emqx_common_test_helpers:with_mock(
        grpc_client,
        send,
        fun
            (Stream, #{payload := {ingest_record_batch, _}} = Req, Fin, Opts) ->
                case emqx_utils_agent:get(Agent) of
                    ok ->
                        meck:passthrough([Stream, Req, Fin, Opts]);
                    Reason ->
                        {error, Reason}
                end;
            (Stream, Req, Fin, Opts) ->
                meck:passthrough([Stream, Req, Fin, Opts])
        end,
        fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := "zerobus_writer_send_error"},
                    5_000
                ),
            %% recoverable error
            emqx_utils_agent:set(Agent, closed),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := "zerobus_writer_send_error"},
                    5_000
                ),
            %% recoverable error
            emqx_utils_agent:set(Agent, bad_stream),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := "zerobus_writer_send_error"},
                    5_000
                ),
            %% recoverable error (don't know all possible errors...  they come from gun,
            %% if the gun client is down at the time the `send` call arrives)
            emqx_utils_agent:set(Agent, unexpected_mocked_error),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                    #{?snk_kind := "zerobus_writer_send_error"},
                    5_000
                ),
            emqx_utils_agent:set(Agent, ok),
            ?retry(
                700,
                10,
                ?assertMatch(
                    {200, #{
                        ~"metrics" := #{
                            ~"matched" := 4,
                            ~"success" := 4,
                            ~"failed" := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end
    ),
    ok.

-doc """
Verify we handle ack subsumption: the server may reply a single ack offset, and we should
consider smaller, unacked offsets as acked.
""".
t_ack_subsumption() ->
    [{matrix, true}, {mock_only, true}].
t_ack_subsumption(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_ack_subsumption(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            ingest_record_batch => [
                continue,
                continue,
                default
            ]
        }
    end),
    ?check_trace(
        begin
            emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
            emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
                    #{?snk_kind := "zerobus_writer_acked"},
                    5_000
                ),
            ?retry(
                700,
                10,
                ?assertMatch(
                    {200, #{
                        ~"metrics" := #{
                            ~"matched" := 3,
                            ~"success" := 3,
                            ~"failed" := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind("zerobus_writer_acked", Trace)),
            %% seq is monotonic starting at 0
            ?assertEqual(
                [0, 1, 2],
                ?projection(seq, ?of_kind(["zerobus_will_send_batch1"], Trace))
            ),
            ok
        end
    ),
    ok.

-doc """
Verifies the case when an ingest request arrives when the writer didn't open a stream yet.

It should reply the caller with a recoverable error.
""".
t_ingest_without_first_stream() ->
    [{matrix, true}, {mock_only, true}].
t_ingest_without_first_stream(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_ingest_without_first_stream(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    TestPid = self(),
    Spy = spawn_link(fun() ->
        receive
            {create_stream, Alias, #{grpc_req := GRPCReq0}} ->
                %% don't reply anything, so we never have a stream when the request
                %% arrives
                Alias ! {Alias, continue},
                TestPid ! {grpc_req, GRPCReq0},
                ok
        after 5_000 ->
            ct:fail("didn't try to open the stream")
        end
    end),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{create_stream => [{ask, Spy}, continue]}
    end),
    {201, _} = create_action_api(TCConfig, #{}),
    GRPCReq =
        receive
            {grpc_req, GRPCReq0} -> GRPCReq0
        after 5_000 -> ct:fail("didn't try to open the stream")
        end,
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{create_stream => [default]}
    end),
    grpc_stream:reply(GRPCReq, [#{payload => {create_stream_response, #{}}}]),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 1,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies the case when an ingest request arrives when the writer is reopening a stream.

It should reply the caller with a recoverable error.
""".
t_ingest_with_lost_stream() ->
    [{matrix, true}, {mock_only, true}].
t_ingest_with_lost_stream(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_ingest_with_lost_stream(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    TestPid = self(),
    Spy = spawn_link(fun() ->
        receive
            {create_stream, Alias, #{grpc_req := GRPCReq0}} ->
                %% don't reply anything yet
                TestPid ! {grpc_req, GRPCReq0, Alias},
                ok
        after 5_000 ->
            ct:fail("didn't try to open the stream")
        end
    end),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            create_stream => [
                default,
                %% when it tries to reopen it below, we stall the response
                {ask, Spy},
                default
            ],
            ingest_record_batch =>
                [
                    %% provoke an error so that the stream needs to be reopened
                    server_shutdown_action(),
                    %% succeed afterwards
                    default
                ]
        }
    end),
    {201, _} = create_action_api(TCConfig, #{
        ~"parameters" => #{~"grpc_open_stream_retry_interval" => ~"500ms"},
        ~"resource_opts" => #{~"request_ttl" => ~"15s"}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% make the stream close due to an error; should be retried once without the stream
    %% reopening.
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "zerobus_will_send_batch1"}),
        10_000
    ),
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "zerobus_ingest_without_stream"}),
        10_000
    ),
    emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    %% should change the action status if it's long gone
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                ~"status" := ~"connecting",
                ~"status_reason" := ~"reopening stream"
            }},
            get_action_api(TCConfig)
        )
    ),
    receive
        {grpc_req, _GRPCReq0, Alias} ->
            %% now we let the stream re-creation continue
            Alias ! {Alias, default},
            ok
    after 5_000 -> ct:fail("didn't try to reopen the stream")
    end,
    %% it should now open the stream and recover
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 1,
                    ~"success" := 1,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                ~"status" := ~"connected"
            }},
            get_action_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies the receiver ignores server messages other than ingest batch response.
""".
t_receiver_ignores_unknown_server_msgs() ->
    [{matrix, true}, {mock_only, true}].
t_receiver_ignores_unknown_server_msgs(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_receiver_ignores_unknown_server_msgs(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    TestPid = self(),
    Spy = spawn_link(fun() ->
        receive
            {create_stream, Alias, #{grpc_req := GRPCReq0}} ->
                Alias ! {Alias, default},
                TestPid ! {grpc_req, GRPCReq0},
                ok
        after 5_000 ->
            ct:fail("didn't try to open the stream")
        end
    end),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{create_stream => [{ask, Spy}, continue]}
    end),
    {201, _} = create_action_api(TCConfig, #{}),
    GRPCReq =
        receive
            {grpc_req, GRPCReq0} -> GRPCReq0
        after 5_000 -> ct:fail("didn't try to open the stream")
        end,
    ?check_trace(
        begin
            grpc_stream:reply(GRPCReq, [
                %% sent by the server once in a while
                #{payload => {close_stream_signal, #{}}},
                %% unexpected once stream is created
                #{payload => {create_stream_response, #{}}},
                %% we currently don't send individual records
                #{payload => {ingest_record_response, #{}}}
            ]),
            ct:sleep(200),
            %% call each receiver to sync
            lists:foreach(
                fun(Pid) ->
                    gen_server:call(Pid, xxx)
                end,
                find_async_receivers(TCConfig)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_, _, _],
                ?of_kind(
                    [
                        "zerobus_server_will_close_stream",
                        "zerobus_unexpected_ingest_response"
                    ],
                    Trace
                )
            ),
            ok
        end
    ),
    ok.

-doc """
Checks that we quote table identifiers in REST paths.

> Valid names cannot contain spaces, periods, forward slashes, or control characters.
""".
t_rest_escape_table_identifier() ->
    [{matrix, true}, {mock_only, true}].
t_rest_escape_table_identifier(matrix) ->
    [[?rest, ?json, ?sync, ?batching]];
t_rest_escape_table_identifier(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        ~"parameters" => #{
            ~"catalog" => ~"a#b&c",
            ~"schema" => ~"d#e&f",
            ~"table" => ~"g#h&i"
        }
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, RuleTopic, seq_payload(1), [{qos, 1}]),
    Tab = get_config(mocked_call_tab, TCConfig),
    ?assertMatch(
        [
            {_, #{
                meta := #{
                    uri := #{
                        path := "/zerobus/v1/tables/a%23b%26c.d%23e%26f.g%23h%26i/insert"
                    }
                }
            }}
        ],
        ets:tab2list(Tab)
    ),
    ok.

-doc """
Verifies the behavior when the receiver process is dead just at the moment the writer
tries to call it with `follow_stream`.

It should not crash the writer, and the caller simply has no new information to update the
acked seqs and proceeds normally.  The receiver, upon restarting, will read the ETS table
and recover as if it was called with `follow_stream`.
""".
t_receiver_dead_before_follow_stream() ->
    [{matrix, true}, {mock_only, true}].
t_receiver_dead_before_follow_stream(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_receiver_dead_before_follow_stream(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    TestPid = self(),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            create_stream => [
                %% we'll stall the response until the receiver is dead
                {ask, TestPid},
                default
            ]
        }
    end),
    {201, _} = create_action_api(TCConfig, #{
        ~"resource_opts" => #{~"request_ttl" => ~"15s"}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),

    %% suspend supervisor so receiver stays down
    [Writer] = find_stream_writers(TCConfig),
    WriterMRef = monitor(process, Writer),
    ActionSup = find_action_sup(TCConfig),
    ct:pal("suspending supervisor"),
    sys:suspend(ActionSup),
    ct:pal("killing receiver"),
    lists:foreach(
        fun(Receiver) ->
            MRef = monitor(process, Receiver),
            exit(Receiver, kill),
            receive
                {'DOWN', MRef, _, _, _} -> ok
            end
        end,
        find_async_receivers(TCConfig)
    ),
    %% now let the creation continue.
    ct:pal("will resume open stream"),
    {_, {ok, _}} =
        ?wait_async_action(
            receive
                {create_stream, Alias, _Ctx} ->
                    ct:pal("resuming open stream"),
                    Alias ! {Alias, default},
                    ok
            after 5_000 -> ct:fail("wasn't asked what to do")
            end,
            #{?snk_kind := "zerobus_receiver_dead_follow_stream"},
            5_000
        ),
    %% publish a few messages while receiver is still down
    ct:pal("publishing stuff"),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
    %% writer shouldn't die.
    ?assertNotReceive({'DOWN', WriterMRef, _, _, _}),
    %% now unfreeze the supervisor; receiver should be restarted and resume.
    ct:pal("resuming supervisor"),
    sys:resume(ActionSup),
    ct:pal("resumed supervisor"),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                ~"status" := ~"connected"
            }},
            get_action_api(TCConfig)
        )
    ),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 3,
                    ~"success" := 3,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

-doc """
Verifies the behavior when the receiver process is restarted but the registered stream is
tied to a dead writer.

It should not crash the receiver, which awaits as if starting a fresh action.  The writer,
upon restarting, will reopen the stream and call the receiver with `follow_stream`.
""".
t_writer_dead_before_receiver_recover() ->
    [{matrix, true}, {mock_only, true}].
t_writer_dead_before_receiver_recover(matrix) ->
    [[?grpc, ?json, ?async, ?not_batching]];
t_writer_dead_before_receiver_recover(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"transport" => #{~"pool_size" => 1}
    }),
    {201, _} = create_action_api(TCConfig, #{
        ~"resource_opts" => #{~"request_ttl" => ~"15s"}
    }),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}])
            end,
            #{?snk_kind := "zerobus_writer_acked", seq := 1, n_restarts := 0},
            5_000
        ),

    %% now we kill both workers, and make writer hang before it can open the stream until
    %% the receiver tries to recover.
    ?force_ordering(
        #{?snk_kind := ~"zerobus_receiver_recv_no_stream"},
        #{?snk_kind := "zerobus_writer_init_will_clear_old_stream"}
    ),
    lists:foreach(
        fun(Receiver) ->
            MRef = monitor(process, Receiver),
            exit(Receiver, kill),
            receive
                {'DOWN', MRef, _, _, _} -> ok
            end
        end,
        find_async_receivers(TCConfig) ++ find_stream_writers(TCConfig)
    ),
    %% publish more stuff now
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}]),
                emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 1}])
            end,
            #{?snk_kind := "zerobus_writer_acked", seq := 1, n_restarts := 1},
            5_000
        ),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                ~"status" := ~"connected"
            }},
            get_action_api(TCConfig)
        )
    ),
    ?retry(
        700,
        10,
        ?assertMatch(
            {200, #{
                ~"metrics" := #{
                    ~"matched" := 4,
                    ~"success" := 4,
                    ~"failed" := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

t_create_with_no_explicit_port(TCConfig) when is_list(TCConfig) ->
    ?assertMatch(
        {400, #{
            ~"message" := #{
                ~"reason" := ~"missing_port_number"
            }
        }},
        create_connector_api(TCConfig, #{
            ~"zerobus_endpoint" => ~"https://localhost"
        })
    ),
    ok.

t_grpc_sync_ingest_writer_dies() ->
    [{matrix, true}].
t_grpc_sync_ingest_writer_dies(matrix) ->
    [[?grpc, ?json, ?sync, ?not_batching]];
t_grpc_sync_ingest_writer_dies(TCConfig) when is_list(TCConfig) ->
    maybe_with_forced_sync_query_mode(TCConfig, fun() ->
        {201, _} = create_connector_api(TCConfig, #{
            ~"transport" => #{~"pool_size" => 1}
        }),
        {201, _} = create_action_api(TCConfig, #{}),
        #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
        C = start_client(),

        #{killer := Killer, writer := Writer} = with_capture_stream(fun() ->
            emqtt:publish(C, RuleTopic, sample_payload(), [{qos, 0}])
        end),
        MRef = monitor(process, Writer),
        {_, {ok, _}} =
            ?wait_async_action(
                exit(Writer, kill),
                #{?snk_kind := ~"zerobus_sync_ingest_writer_died"},
                5_000
            ),
        receive
            {'DOWN', MRef, _, _, _} -> ok
        end,
        Killer ! continue,

        ?retry(
            700,
            10,
            ?assertMatch(
                {200, #{
                    ~"metrics" := #{
                        ~"matched" := 1,
                        ~"success" := 1,
                        ~"failed" := 0,
                        ~"retried" := 1
                    }
                }},
                get_action_metrics_api(TCConfig)
            )
        ),

        ok
    end).

t_close_stream() ->
    [{matrix, true}, {mock_only, true}].
t_close_stream(matrix) ->
    [[?grpc, ?json, ?async, ?batching]];
t_close_stream(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    TestPid = self(),
    mocked_grpc_server_agent_update(fun(St) ->
        St#{
            batch_done => [
                {run, fun(GRPCReq, _Meta) ->
                    TestPid ! {grpc_req, GRPCReq},
                    process_flag(trap_exit, true),
                    receive
                        {'EXIT', _, Reason} ->
                            TestPid ! {grpc_server_handler_exit, Reason},
                            exit(Reason)
                    end,
                    {shutdonw, ?GRPC_STATUS_INTERNAL, ~"shouldn't reach here"}
                end},
                continue
            ]
        }
    end),
    {201, _} = create_action_api(TCConfig, #{}),
    %% `terminate` handler from writers should cancel the stream with grpc_client.
    {204, _} = delete_action_api(TCConfig),
    %% reason from cowboy_http2:rst_stream_frame is shutdown
    ?assertReceive({grpc_server_handler_exit, _}),
    ok.

%% -doc """
%% When the schema used by an already-running channel changes, it needs to get the new
%% descriptor proto and re-open the stream.
%% """.
%% t_proto_schema_changed_while_running() ->
%%     [{matrix, true}, {mock_only, true}].
%% t_proto_schema_changed_while_running(matrix) ->
%%     [[?grpc, ?proto, ?async, ?batching]];
%% t_proto_schema_changed_while_running(TCConfig) when is_list(TCConfig) ->
%%     ct:fail(todo),
%%     ok.

%% todo
%%  - stale responses
%%  - connection refused when creating
