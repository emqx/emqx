%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(PROXY_NAME_TCP, "influxdb_tcp").
-define(PROXY_NAME_TLS, "influxdb_tls").

-define(CONNECTOR_TYPE, influxdb).
-define(CONNECTOR_TYPE_BIN, <<"influxdb">>).
-define(ACTION_TYPE, influxdb).
-define(ACTION_TYPE_BIN, <<"influxdb">>).

-define(HELPER_POOL, <<"influx_suite">>).

-define(api_v1, api_v1).
-define(api_v2, api_v2).
-define(async, async).
-define(sync, sync).
-define(tcp, tcp).
-define(tls, tls).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_influxdb,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?api_v1, TCConfig) ->
    [{api_type, ?api_v1} | TCConfig];
init_per_group(?api_v2, TCConfig) ->
    [{api_type, ?api_v2} | TCConfig];
init_per_group(?tcp, TCConfig) ->
    [
        {host, <<"toxiproxy">>},
        {port, 8086},
        {use_tls, false},
        {proxy_name, ?PROXY_NAME_TCP}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {host, <<"toxiproxy">>},
        {port, 8087},
        {use_tls, true},
        {proxy_name, ?PROXY_NAME_TLS}
        | TCConfig
    ];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(
        merge_maps([
            #{<<"server">> => server(TCConfig)},
            connector_config_auth_fields(TCConfig),
            #{<<"ssl">> => #{<<"enable">> => get_config(use_tls, TCConfig, false)}}
        ])
    ),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{<<"batch_size">> => get_config(batch_size, TCConfig, 1)}
    }),
    start_ehttpc_helper_pool(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    stop_ehttpc_helper_pool(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

merge_maps(Maps) ->
    lists:foldl(fun maps:merge/2, #{}, Maps).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"server">> => <<"toxiproxy:8086">>,
        <<"max_inactive">> => <<"10s">>,
        <<"pool_size">> => 2,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

connector_config_auth_fields(TCConfig) ->
    case get_config(api_type, TCConfig, ?api_v2) of
        ?api_v1 ->
            #{
                <<"parameters">> => #{
                    <<"influxdb_type">> => <<"influxdb_api_v1">>,
                    <<"database">> => <<"mqtt">>,
                    <<"username">> => <<"root">>,
                    <<"password">> => <<"emqx@123">>
                }
            };
        ?api_v2 ->
            #{
                <<"parameters">> => #{
                    <<"influxdb_type">> => <<"influxdb_api_v2">>,
                    <<"bucket">> => <<"mqtt">>,
                    <<"org">> => <<"emqx">>,
                    <<"token">> => <<"abcdefg">>
                }
            }
    end.

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"fallback_actions">> => [],
        <<"parameters">> => #{
            <<"precision">> => <<"ns">>,
            <<"write_syntax">> => example_write_syntax()
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

example_write_syntax() ->
    %% N.B.: this single space character is relevant
    <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "float_value=${payload.float_key},", "undef_value=${payload.undef},",
        "${undef_key}=\"hard-coded-value\",", "bool=${payload.bool}">>.

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig, ?PROXY_NAME_TCP),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

server(TCConfig) ->
    case get_config(use_tls, TCConfig, false) of
        false ->
            <<"toxiproxy:8086">>;
        true ->
            <<"toxiproxy:8087">>
    end.

full_matrix() ->
    [
        [APIType, Conn, Sync, Batch]
     || APIType <- [?api_v1, ?api_v2],
        Conn <- [?tcp, ?tls],
        Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ].

start_ehttpc_helper_pool(TCConfig) ->
    Host = get_config(host, TCConfig, <<"toxiproxy">>),
    Port = get_config(port, TCConfig, 8086),
    {Transport, TransportOpts} =
        case get_config(use_tls, TCConfig, false) of
            true -> {tls, [{verify, verify_none}]};
            false -> {tcp, []}
        end,
    PoolOpts = [
        {host, str(Host)},
        {port, Port},
        {pool_size, 1},
        {transport, Transport},
        {transport_opts, TransportOpts}
    ],
    {ok, _} = ehttpc_sup:start_pool(?HELPER_POOL, PoolOpts),
    ok.

stop_ehttpc_helper_pool() ->
    ehttpc_sup:stop_pool(?HELPER_POOL).

str(X) -> emqx_utils_conv:str(X).

query_by_clientid(ClientId, Config) ->
    InfluxDBHost = get_config(host, Config, <<"toxiproxy">>),
    InfluxDBPort = get_config(port, Config, 8086),
    UseTLS = get_config(use_tls, Config, false),
    Path = <<"/api/v2/query?org=emqx">>,
    Scheme =
        case UseTLS of
            true -> <<"https://">>;
            false -> <<"http://">>
        end,
    URI = iolist_to_binary([
        Scheme,
        InfluxDBHost,
        ":",
        integer_to_binary(InfluxDBPort),
        Path
    ]),
    Query =
        <<
            "from(bucket: \"mqtt\")\n"
            "  |> range(start: -12h)\n"
            "  |> filter(fn: (r) => r.clientid == \"",
            ClientId/binary,
            "\")"
        >>,
    Headers = [
        {"Authorization", "Token abcdefg"},
        {"Content-Type", "application/json"}
    ],
    Body =
        emqx_utils_json:encode(#{
            query => Query,
            dialect => #{
                header => true,
                annotations => [<<"datatype">>],
                delimiter => <<";">>
            }
        }),
    {ok, 200, _Headers, RawBody0} =
        ehttpc:request(
            ?HELPER_POOL,
            post,
            {URI, Headers, Body},
            _Timeout = 10_000,
            _Retry = 0
        ),
    RawBody1 = iolist_to_binary(string:replace(RawBody0, <<"\r\n">>, <<"\n">>, all)),
    {ok, DecodedCSV0} = erl_csv:decode(RawBody1, #{separator => <<$;>>}),
    DecodedCSV1 = [
        [Field || Field <- Line, Field =/= <<>>]
     || Line <- DecodedCSV0, Line =/= [<<>>]
    ],
    DecodedCSV2 = csv_lines_to_maps(DecodedCSV1),
    index_by_field(DecodedCSV2).

csv_lines_to_maps([[<<"#datatype">> | DataType], Title | Rest]) ->
    csv_lines_to_maps(Rest, Title, _Acc = [], DataType);
csv_lines_to_maps([]) ->
    [].

csv_lines_to_maps([[<<"_result">> | _] = Data | RestData], Title, Acc, DataType) ->
    Map = maps:from_list(lists:zip(Title, Data)),
    MapT = lists:zip(Title, DataType),
    [Type] = [T || {<<"_value">>, T} <- MapT],
    csv_lines_to_maps(RestData, Title, [Map#{'_value_type' => Type} | Acc], DataType);
%% ignore the csv title line
%% it's always like this:
%% [<<"result">>,<<"table">>,<<"_start">>,<<"_stop">>,
%% <<"_time">>,<<"_value">>,<<"_field">>,<<"_measurement">>, Measurement],
csv_lines_to_maps([[<<"result">> | _] = _Title | RestData], Title, Acc, DataType) ->
    csv_lines_to_maps(RestData, Title, Acc, DataType);
csv_lines_to_maps([[<<"#datatype">> | DataType] | RestData], Title, Acc, _) ->
    csv_lines_to_maps(RestData, Title, Acc, DataType);
csv_lines_to_maps([], _Title, Acc, _DataType) ->
    lists:reverse(Acc).

index_by_field(DecodedCSV) ->
    maps:from_list([{Field, Data} || Data = #{<<"_field">> := Field} <- DecodedCSV]).

%% influxdb returns timestamps without trailing zeros such as
%% "2023-02-28T17:21:51.63678163Z"
%% while the standard should be
%% "2023-02-28T17:21:51.636781630Z"
pad_zero(BinTs) ->
    StrTs = binary_to_list(BinTs),
    [Nano | Rest] = lists:reverse(string:tokens(StrTs, ".")),
    [$Z | NanoNum] = lists:reverse(Nano),
    Padding = lists:duplicate(10 - length(Nano), $0),
    NewNano = lists:reverse(NanoNum) ++ Padding ++ "Z",
    iolist_to_binary(string:join(lists:reverse([NewNano | Rest]), ".")).

assert_persisted_data(ClientId, Expected, PersistedData) ->
    ClientIdIntKey = <<ClientId/binary, "_int_value">>,
    maps:foreach(
        fun
            (int_value, ExpectedValue) ->
                ?assertMatch(
                    #{<<"_value">> := ExpectedValue},
                    maps:get(ClientIdIntKey, PersistedData)
                );
            (Key, {ExpectedValue, ExpectedType}) ->
                ?assertMatch(
                    #{<<"_value">> := ExpectedValue, '_value_type' := ExpectedType},
                    maps:get(atom_to_binary(Key), PersistedData),
                    #{
                        key => Key,
                        expected_value => ExpectedValue,
                        expected_data_type => ExpectedType
                    }
                );
            (Key, ExpectedValue) ->
                ?assertMatch(
                    #{<<"_value">> := ExpectedValue},
                    maps:get(atom_to_binary(Key), PersistedData),
                    #{key => Key, expected_value => ExpectedValue}
                )
        end,
        Expected
    ),
    ok.

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

get_connector_api(Config) ->
    ConnectorType = ?config(connector_type, Config),
    ConnectorName = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [APIType, Conn, ?sync, ?without_batch]
     || APIType <- [?api_v1, ?api_v2],
        Conn <- [?tcp, ?tls]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, influxdb_client_stopped).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [
        [APIType, Conn, ?sync, ?without_batch]
     || APIType <- [?api_v1, ?api_v2],
        Conn <- [?tcp, ?tls]
    ];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
t_rule_action(TCConfig) when is_list(TCConfig) ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    StartClientOpts = #{clean_start => true, clientid => ClientId},
    PayloadFn = fun() ->
        emqx_utils_json:encode(
            #{
                <<"int_key">> => -123,
                <<"bool">> => true,
                <<"float_key">> => 24.5,
                <<"uint_key">> => 123
            }
        )
    end,
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?retry(200, 10, begin
            PersistedData = query_by_clientid(ClientId, TCConfig),
            Expected = #{
                bool => <<"true">>,
                int_value => <<"-123">>,
                uint_value => <<"123">>,
                float_value => <<"24.5">>,
                payload => Payload
            },
            assert_persisted_data(ClientId, Expected, PersistedData)
        end)
    end,
    TraceChecker = fun(Trace0) ->
        Trace = ?of_kind(influxdb_connector_send_query, Trace0),
        ?assertMatch([#{points := [_]}], Trace),
        [#{points := [Point]}] = Trace,
        ct:pal("sent point: ~p", [Point]),
        ?assertMatch(
            #{
                fields := #{},
                measurement := <<_/binary>>,
                tags := #{},
                timestamp := TS
            } when is_integer(TS),
            Point
        ),
        #{fields := Fields} = Point,
        ?assert(lists:all(fun is_binary/1, maps:keys(Fields))),
        ?assertNot(maps:is_key(<<"undefined">>, Fields)),
        ?assertNot(maps:is_key(<<"undef_value">>, Fields)),
        ok
    end,
    Opts = #{
        start_client_opts => StartClientOpts,
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn,
        trace_checkers => [TraceChecker]
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_start_ok_timestamp_write_syntax(TCConfig) when is_list(TCConfig) ->
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
            "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        })
    ),
    ok.

t_start_ok_no_subject_tags_write_syntax(TCConfig) when is_list(TCConfig) ->
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic}", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        })
    ),
    ok.

t_const_timestamp(TCConfig) when is_list(TCConfig) ->
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    WriteSyntax = <<
        "mqtt,clientid=${clientid} "
        "foo=${payload.foo}i,"
        "foo1=${payload.foo},"
        "foo2=\"${payload.foo}\","
        "foo3=\"${payload.foo}somestr\","
        "bar=5i,baz0=1.1,baz1=\"a\",baz2=\"ai\",baz3=\"au\",baz4=\"1u\" ",
        ConstBin/binary
    >>,
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        })
    ),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    Payload = emqx_utils_json:encode(#{<<"foo">> => 123}),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
    ?retry(200, 10, begin
        PersistedData = query_by_clientid(ClientId, TCConfig),
        Expected = #{
            foo => {<<"123">>, <<"long">>},
            foo1 => {<<"123">>, <<"double">>},
            foo2 => {<<"123">>, <<"string">>},
            foo3 => {<<"123somestr">>, <<"string">>},
            bar => {<<"5">>, <<"long">>},
            baz0 => {<<"1.1">>, <<"double">>},
            baz1 => {<<"a">>, <<"string">>},
            baz2 => {<<"ai">>, <<"string">>},
            baz3 => {<<"au">>, <<"string">>},
            baz4 => {<<"1u">>, <<"string">>}
        },
        assert_persisted_data(ClientId, Expected, PersistedData),
        TimeReturned0 = maps:get(<<"_time">>, maps:get(<<"foo">>, PersistedData)),
        TimeReturned = pad_zero(TimeReturned0),
        ?assertEqual(TsStr, TimeReturned)
    end),
    ok.

t_empty_timestamp(TCConfig) when is_list(TCConfig) ->
    WriteSyntax = <<
        "mqtt,clientid=${clientid}"
        " "
        "foo=${payload.foo}i,"
        "foo1=${payload.foo},"
        "foo2=\"${payload.foo}\","
        "foo3=\"${payload.foo}somestr\","
        "bar=5i,baz0=1.1,baz1=\"a\",baz2=\"ai\",baz3=\"au\",baz4=\"1u\""
        " "
        "${timestamp}"
    >>,
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        })
    ),
    #{topic := RuleTopic} =
        emqx_bridge_v2_testlib:simple_create_rule_api(
            <<"select clientid, topic, payload from \"${t}\" ">>,
            TCConfig
        ),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{<<"foo">> => 123}),
    emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
    ?retry(200, 10, begin
        PersistedData = query_by_clientid(ClientId, TCConfig),
        Expected = #{
            foo => {<<"123">>, <<"long">>},
            foo1 => {<<"123">>, <<"double">>},
            foo2 => {<<"123">>, <<"string">>},
            foo3 => {<<"123somestr">>, <<"string">>},
            bar => {<<"5">>, <<"long">>},
            baz0 => {<<"1.1">>, <<"double">>},
            baz1 => {<<"a">>, <<"string">>},
            baz2 => {<<"ai">>, <<"string">>},
            baz3 => {<<"au">>, <<"string">>},
            baz4 => {<<"1u">>, <<"string">>}
        },
        assert_persisted_data(ClientId, Expected, PersistedData)
    end),
    ok.

t_boolean_variants(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    BoolVariants = #{
        true => true,
        false => false,
        <<"t">> => true,
        <<"f">> => false,
        <<"T">> => true,
        <<"F">> => false,
        <<"TRUE">> => true,
        <<"FALSE">> => false,
        <<"True">> => true,
        <<"False">> => false
    },
    maps:foreach(
        fun(BoolVariant, Translation) ->
            ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
            C = start_client(#{clientid => ClientId}),
            Payload = emqx_utils_json:encode(#{
                <<"int_key">> => -123,
                <<"bool">> => BoolVariant,
                <<"uint_key">> => 123
            }),
            emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
            ?retry(200, 10, begin
                PersistedData = query_by_clientid(ClientId, TCConfig),
                Expected = #{
                    bool => atom_to_binary(Translation),
                    int_value => <<"-123">>,
                    uint_value => <<"123">>,
                    payload => Payload
                },
                assert_persisted_data(ClientId, Expected, PersistedData)
            end),
            ok
        end,
        BoolVariants
    ),
    ok.

t_any_num_as_float(TCConfig) when is_list(TCConfig) ->
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    WriteSyntax =
        <<"mqtt,clientid=${clientid}", " ",
            "float_no_dp=${payload.float_no_dp},float_dp=${payload.float_dp},bar=5i ",
            ConstBin/binary>>,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        }),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{
        %% no decimal point
        <<"float_no_dp">> => 123,
        %% with decimal point
        <<"float_dp">> => 123.0
    }),
    emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
    ?retry(200, 10, begin
        PersistedData = query_by_clientid(ClientId, TCConfig),
        Expected = #{float_no_dp => <<"123">>, float_dp => <<"123">>},
        assert_persisted_data(ClientId, Expected, PersistedData),
        TimeReturned0 = maps:get(<<"_time">>, maps:get(<<"float_no_dp">>, PersistedData)),
        TimeReturned = pad_zero(TimeReturned0),
        ?assertEqual(TsStr, TimeReturned)
    end),
    ok.

t_tag_set_use_literal_value(TCConfig) when is_list(TCConfig) ->
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    WriteSyntax =
        <<"mqtt,clientid=${clientid},tag_key1=100,tag_key2=123.4,tag_key3=66i,tag_key4=${payload.float_dp}",
            " ", "field_key1=100.1,field_key2=100i,field_key3=${payload.float_dp},bar=5i", " ",
            ConstBin/binary>>,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        }),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{
        %% with decimal point
        <<"float_dp">> => 123.4
    }),
    emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
    ?retry(200, 10, begin
        PersistedData = query_by_clientid(ClientId, TCConfig),
        Expected = #{field_key1 => <<"100.1">>, field_key2 => <<"100">>, field_key3 => <<"123.4">>},
        assert_persisted_data(ClientId, Expected, PersistedData),
        TimeReturned0 = maps:get(<<"_time">>, maps:get(<<"field_key1">>, PersistedData)),
        TimeReturned = pad_zero(TimeReturned0),
        ?assertEqual(TsStr, TimeReturned)
    end),
    ok.

t_bad_timestamp1() ->
    [{matrix, true}].
t_bad_timestamp1(matrix) ->
    [
        [?api_v2, ?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_bad_timestamp1(TCConfig) when is_list(TCConfig) ->
    do_t_bad_timestamp(<<"bad_timestamp">>, non_integer_timestamp, TCConfig).

t_bad_timestamp2() ->
    [{matrix, true}].
t_bad_timestamp2(matrix) ->
    [
        [?api_v2, ?tcp, Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_bad_timestamp2(TCConfig) when is_list(TCConfig) ->
    do_t_bad_timestamp(
        <<"${timestamp}000">>, unsupported_placeholder_usage_for_timestamp, TCConfig
    ).

do_t_bad_timestamp(Timestamp, ErrTag, TCConfig) when is_list(TCConfig) ->
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic}", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", Timestamp/binary>>,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"write_syntax">> => WriteSyntax
            }
        }),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{
        <<"int_key">> => -123,
        <<"bool">> => false,
        <<"uint_key">> => 123
    }),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
                    #{?snk_kind := influxdb_connector_send_query_error}
                )
        end,
        fun(Trace) ->
            QueryMode = get_config(query_mode, TCConfig),
            BatchSize = get_config(batch_size, TCConfig),
            IsBatch = BatchSize > 1,
            case {QueryMode, IsBatch} of
                {async, true} ->
                    ?assertMatch(
                        [#{error := points_trans_failed}],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                {async, false} ->
                    ?assertMatch(
                        [
                            #{
                                error := [
                                    {error, {bad_timestamp, {ErrTag, _}}}
                                ]
                            }
                        ],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                _ ->
                    ok
            end
        end
    ),
    ok.

t_create_disconnected(TCConfig) when is_list(TCConfig) ->
    ?check_trace(
        with_failure(down, TCConfig, fun() ->
            {201, _} = create_connector_api(TCConfig, #{})
        end),
        fun(Trace) ->
            [#{error := influxdb_client_not_alive, reason := Reason}] =
                ?of_kind(influxdb_connector_start_failed, Trace),
            case Reason of
                econnrefused -> ok;
                closed -> ok;
                {closed, _} -> ok;
                {shutdown, closed} -> ok;
                _ -> ct:fail("influxdb_client_not_alive with wrong reason: ~p", [Reason])
            end,
            ok
        end
    ),
    ok.

t_start_error(TCConfig) ->
    ct:timetrap({seconds, 10}),
    %% simulate client start error
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        emqx_common_test_helpers:with_mock(
            influxdb,
            start_client,
            fun(_Config) -> {error, some_error} end,
            fun() ->
                ?wait_async_action(
                    {201, _} = create_connector_api(TCConfig, #{}),
                    #{?snk_kind := influxdb_connector_start_failed}
                )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := some_error}],
                ?of_kind(influxdb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_start_exception(TCConfig) ->
    ct:timetrap({seconds, 10}),
    %% simulate client start exception
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        emqx_common_test_helpers:with_mock(
            influxdb,
            start_client,
            fun(_Config) -> error(boom) end,
            fun() ->
                ?wait_async_action(
                    {201, _} = create_connector_api(TCConfig, #{}),
                    #{?snk_kind := influxdb_connector_start_exception}
                )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {error, boom}}],
                ?of_kind(influxdb_connector_start_exception, Trace)
            ),
            ok
        end
    ),
    ok.

t_write_failure(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{
        <<"int_key">> => -123,
        <<"bool">> => true,
        <<"float_key">> => 24.5,
        <<"uint_key">> => 123
    }),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        with_failure(down, TCConfig, fun() ->
            ?wait_async_action(
                emqtt:publish(C, RuleTopic, Payload, [{qos, 0}]),
                #{?snk_kind := handle_async_reply}
            )
        end),
        fun(Trace0) ->
            Trace = ?of_kind(handle_async_reply, Trace0),
            ?assertMatch([#{action := nack} | _], Trace),
            [#{result := Result} | _] = Trace,
            ?assert(
                not emqx_bridge_influxdb_connector:is_unrecoverable_error(Result),
                #{got => Result}
            ),
            ok
        end
    ),
    ok.

t_missing_field() ->
    [{matrix, true}].
t_missing_field(matrix) ->
    [
        [?api_v2, ?tcp, ?sync, Batch]
     || Batch <- [?without_batch, ?with_batch]
    ];
t_missing_field(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"write_syntax">> => <<"${clientid} foo=${foo}i">>},
        <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
    }),
    %% note: we don't select foo here, but we interpolate it in the
    %% fields, so it'll become undefined.
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId0 = emqx_guid:to_hexstr(emqx_guid:gen()),
    ClientId1 = emqx_guid:to_hexstr(emqx_guid:gen()),
    C0 = start_client(#{clientid => ClientId0}),
    C1 = start_client(#{clientid => ClientId1}),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        ?wait_async_action(
            begin
                %% Message with the field that we "forgot" to select in the rule
                emqtt:publish(C0, RuleTopic, emqx_utils_json:encode(#{foo => 123})),
                %% Message without any fields
                emqtt:publish(C1, RuleTopic, emqx_utils_json:encode(#{}))
            end,
            #{?snk_kind := influxdb_connector_send_query_error}
        ),
        fun(Trace) ->
            BatchSize = get_config(batch_size, TCConfig),
            IsBatch = BatchSize > 1,
            PersistedData0 = query_by_clientid(ClientId0, TCConfig),
            PersistedData1 = query_by_clientid(ClientId1, TCConfig),
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{error := points_trans_failed} | _],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                false ->
                    ?assertMatch(
                        [#{error := [{error, no_fields}]} | _],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    )
            end,
            %% nothing should have been persisted
            ?assertEqual(#{}, PersistedData0),
            ?assertEqual(#{}, PersistedData1),
            ok
        end
    ),
    ok.

t_authentication_error() ->
    [{matrix, true}].
t_authentication_error(matrix) ->
    [
        [APIType, ?tcp, ?sync, ?without_batch]
     || APIType <- [?api_v1, ?api_v2]
    ];
t_authentication_error(TCConfig) when is_list(TCConfig) ->
    Overrides =
        case get_config(api_type, TCConfig) of
            ?api_v1 -> #{<<"password">> => <<"wrong_password">>};
            ?api_v2 -> #{<<"token">> => <<"wrong_token">>}
        end,
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        ?wait_async_action(
            create_connector_api(TCConfig, #{<<"parameters">> => Overrides}),
            #{?snk_kind := influxdb_connector_start_failed}
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := auth_error} | _],
                ?of_kind(influxdb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_authentication_error_on_get_status() ->
    [{matrix, true}].
t_authentication_error_on_get_status(matrix) ->
    [
        [APIType, ?tcp, ?sync, ?without_batch]
     || APIType <- [?api_v1, ?api_v2]
    ];
t_authentication_error_on_get_status(TCConfig) when is_list(TCConfig) ->
    Overrides =
        case get_config(api_type, TCConfig) of
            ?api_v1 -> #{<<"password">> => <<"wrong_password">>};
            ?api_v2 -> #{<<"token">> => <<"wrong_token">>}
        end,
    % Fake initialization to simulate credential update after bridge was created.
    emqx_common_test_helpers:with_mock(
        influxdb,
        check_auth,
        fun(_) ->
            ok
        end,
        fun() ->
            {201, #{<<"status">> := <<"connected">>}} =
                create_connector_api(TCConfig, #{<<"parameters">> => Overrides})
        end
    ),
    %% Now back to wrong credentials
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"disconnected">>}},
            get_connector_api(TCConfig)
        )
    ),
    ok.

t_authentication_error_on_send_message() ->
    [{matrix, true}].
t_authentication_error_on_send_message(matrix) ->
    [
        [APIType, ?tcp, ?sync, ?without_batch]
     || APIType <- [?api_v1, ?api_v2]
    ];
t_authentication_error_on_send_message(TCConfig) ->
    Overrides =
        case get_config(api_type, TCConfig) of
            ?api_v1 -> #{<<"password">> => <<"wrong_password">>};
            ?api_v2 -> #{<<"token">> => <<"wrong_token">>}
        end,
    % Fake initialization to simulate credential update after bridge was created.
    emqx_common_test_helpers:with_mock(
        influxdb,
        check_auth,
        fun(_) ->
            ok
        end,
        fun() ->
            {201, #{<<"status">> := <<"connected">>}} =
                create_connector_api(TCConfig, #{<<"parameters">> => Overrides}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{})
        end
    ),
    %% Now back to wrong credentials
    #{topic := RuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    C = start_client(#{clientid => ClientId}),
    Payload = emqx_utils_json:encode(#{
        <<"int_key">> => -123,
        <<"bool">> => true,
        <<"float_key">> => 24.5,
        <<"uint_key">> => 123
    }),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            ?wait_async_action(
                emqtt:publish(C, RuleTopic, Payload, [{qos, 0}]),
                #{?snk_kind := handle_async_reply}
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := <<"authorization failure">>} | _],
                ?of_kind(influxdb_connector_do_query_failure, Trace)
            ),
            ok
        end
    ),
    ok.
