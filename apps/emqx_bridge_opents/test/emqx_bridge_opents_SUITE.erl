%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_opents_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% DB defaults
-define(BRIDGE_TYPE_BIN, <<"opents">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, default}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {default, AllTCs}
    ].

init_per_suite(Config) ->
    emqx_bridge_v2_testlib:init_per_suite(Config, [
        emqx,
        emqx_conf,
        emqx_bridge_opents,
        emqx_connector,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard()
    ]).

end_per_suite(Config) ->
    emqx_bridge_v2_testlib:end_per_suite(Config).

init_per_group(default, Config0) ->
    Host = os:getenv("OPENTS_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("OPENTS_PORT", "4242")),
    ProxyName = "opents",
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(default, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {proxy_name, ProxyName}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_opents);
                _ ->
                    {skip, no_opents}
            end
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(default, Config) ->
    emqx_bridge_v2_testlib:end_per_group(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) ->
    Type = ?config(bridge_type, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    {_ConfigString, ConnectorConfig} = connector_config(Name, Config0),
    {_, ActionConfig} = action_config(Name, Config0),
    Config = [
        {connector_type, Type},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, Type},
        {bridge_name, Name},
        {bridge_config, ActionConfig}
        | Config0
    ],
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_bridge_v2_testlib:end_per_testcase(TestCase, Config).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

action_config(Name, Config) ->
    Type = ?config(bridge_type, Config),
    ConfigString =
        io_lib:format(
            "actions.~s.~s {\n"
            "  enable = true\n"
            "  connector = \"~s\"\n"
            "  parameters = {\n"
            "     data = []\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                Name
            ]
        ),
    ct:pal("ActionConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_action_and_check(ConfigString, Type, Name)}.

connector_config(Name, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Type = ?config(bridge_type, Config),
    ServerURL = opents_server_url(Host, Port),
    ConfigString =
        io_lib:format(
            "connectors.~s.~s {\n"
            "  enable = true\n"
            "  server = \"~s\"\n"
            "}\n",
            [
                Type,
                Name,
                ServerURL
            ]
        ),
    ct:pal("ConnectorConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_connector_and_check(ConfigString, Type, Name)}.

parse_action_and_check(ConfigString, BridgeType, Name) ->
    parse_and_check(ConfigString, emqx_bridge_schema, <<"actions">>, BridgeType, Name).

parse_connector_and_check(ConfigString, ConnectorType, Name) ->
    parse_and_check(
        ConfigString, emqx_connector_schema, <<"connectors">>, ConnectorType, Name
    ).
%%    emqx_utils_maps:safe_atom_key_map(Config).

parse_and_check(ConfigString, SchemaMod, RootKey, Type0, Name) ->
    Type = to_bin(Type0),
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(SchemaMod, RawConf, #{required => false, atom_key => false}),
    #{RootKey := #{Type := #{Name := Config}}} = RawConf,
    Config.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom);
to_bin(Bin) when is_binary(Bin) ->
    Bin.

opents_server_url(Host, Port) ->
    iolist_to_binary([
        "http://",
        Host,
        ":",
        integer_to_binary(Port)
    ]).

is_success_check({ok, 200, #{failed := Failed}}) ->
    ?assertEqual(0, Failed);
is_success_check(Ret) ->
    ?assert(false, Ret).

is_error_check(Result) ->
    ?assertMatch({error, {400, #{failed := 1}}}, Result).

opentds_query(Config, Metric) ->
    Path = <<"/api/query">>,
    Opts = #{return_all => true},
    Body = #{
        start => <<"1h-ago">>,
        queries => [
            #{
                aggregator => <<"last">>,
                metric => Metric,
                tags => #{
                    host => <<"*">>
                }
            }
        ],
        showTSUID => false,
        showQuery => false,
        delete => false
    },
    opentsdb_request(Config, Path, Body, Opts).

opentsdb_request(Config, Path, Body) ->
    opentsdb_request(Config, Path, Body, #{}).

opentsdb_request(Config, Path, Body, Opts) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    ServerURL = opents_server_url(Host, Port),
    URL = <<ServerURL/binary, Path/binary>>,
    emqx_mgmt_api_test_util:request_api(post, URL, [], [], Body, Opts).

make_data(Metric, Value) ->
    #{
        metric => Metric,
        tags => #{
            <<"host">> => <<"serverA">>
        },
        value => Value
    }.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_query_simple(Config) ->
    Metric = <<"t_query_simple">>,
    Value = 12,
    MakeMessageFun = fun() -> make_data(Metric, Value) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, opents_bridge_on_query
    ),
    {ok, {{_, 200, _}, _, IoTDBResult}} = opentds_query(Config, Metric),
    QResult = emqx_utils_json:decode(IoTDBResult),
    ?assertMatch(
        [
            #{
                <<"metric">> := Metric,
                <<"dps">> := _
            }
        ],
        QResult
    ),
    [#{<<"dps">> := Dps}] = QResult,
    ?assertMatch([Value | _], maps:values(Dps)).

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, opents_bridge_stopped).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}).

t_query_invalid_data(Config) ->
    Metric = <<"t_query_invalid_data">>,
    Value = 12,
    MakeMessageFun = fun() -> maps:remove(value, make_data(Metric, Value)) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_error_check/1, opents_bridge_on_query
    ).

t_tags_validator(Config) ->
    %% Create without data  configured
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),

    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => <<"${tags}">>,
                        <<"value">> => <<"${payload.value}">>
                    }
                ]
            }
        })
    ),

    ?assertMatch(
        {error, _},
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => <<"text">>,
                        <<"value">> => <<"${payload.value}">>
                    }
                ]
            }
        })
    ).

t_raw_int_value(Config) ->
    raw_value_test(<<"t_raw_int_value">>, 42, Config).

t_raw_float_value(Config) ->
    raw_value_test(<<"t_raw_float_value">>, 42.5, Config).

t_list_tags(Config) ->
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 10,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),

    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => #{<<"host">> => <<"valueA">>},
                        value => <<"${value}">>
                    }
                ]
            }
        })
    ),

    Metric = <<"t_list_tags">>,
    Value = 12,
    MakeMessageFun = fun() -> make_data(Metric, Value) end,

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MakeMessageFun()})
    ),

    {ok, {{_, 200, _}, _, IoTDBResult}} = opentds_query(Config, Metric),
    QResult = emqx_utils_json:decode(IoTDBResult),
    ?assertMatch(
        [
            #{
                <<"metric">> := Metric,
                <<"tags">> := #{<<"host">> := <<"valueA">>}
            }
        ],
        QResult
    ).

t_list_tags_with_var(Config) ->
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 10,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),

    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => #{<<"host">> => <<"${value}">>},
                        value => <<"${value}">>
                    }
                ]
            }
        })
    ),

    Metric = <<"t_list_tags_with_var">>,
    Value = 12,
    MakeMessageFun = fun() -> make_data(Metric, Value) end,

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MakeMessageFun()})
    ),

    {ok, {{_, 200, _}, _, IoTDBResult}} = opentds_query(Config, Metric),
    QResult = emqx_utils_json:decode(IoTDBResult),
    ?assertMatch(
        [
            #{
                <<"metric">> := Metric,
                <<"tags">> := #{<<"host">> := <<"12">>}
            }
        ],
        QResult
    ).

raw_value_test(Metric, RawValue, Config) ->
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 10,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),

    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"data">> => [
                    #{
                        <<"metric">> => <<"${metric}">>,
                        <<"tags">> => <<"${tags}">>,
                        <<"value">> => RawValue
                    }
                ]
            }
        })
    ),

    Value = 12,
    MakeMessageFun = fun() -> make_data(Metric, Value) end,

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MakeMessageFun()})
    ),

    {ok, {{_, 200, _}, _, IoTDBResult}} = opentds_query(Config, Metric),
    QResult = emqx_utils_json:decode(IoTDBResult),
    ?assertMatch(
        [
            #{
                <<"metric">> := Metric,
                <<"dps">> := _
            }
        ],
        QResult
    ),
    [#{<<"dps">> := Dps}] = QResult,
    ?assertMatch([RawValue | _], maps:values(Dps)).
