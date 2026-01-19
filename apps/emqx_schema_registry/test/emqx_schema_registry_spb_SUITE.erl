%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

%% See `DataType` in `priv/sparkplug_b.proto`.
-define(uint32_type, 7).
-define(string_type, 12).

-define(assertReceivePublish(EXPR, GUARD, EXTRA, TIMEOUT),
    (fun() ->
        lists:foreach(
            fun(Pub0) ->
                Pub = maps:update_with(
                    payload,
                    fun emqx_utils_json:decode/1,
                    Pub0
                ),
                self() ! {decoded, Pub}
            end,
            drain_publishes([])
        ),
        {decoded, __X} = ?assertReceive({decoded, EXPR} when ((GUARD)), TIMEOUT, EXTRA),
        __X
    end)()
).
-define(assertReceivePublish(EXPR, EXTRA), ?assertReceivePublish(EXPR, true, EXTRA, 1_000)).
-define(assertReceivePublish(EXPR, GUARD, EXTRA), ?assertReceivePublish(EXPR, GUARD, EXTRA, 1_000)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_schema_registry_testlib:emqx_schema_registry_app_spec(),
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    ok = snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    ok = snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

drain_publishes(Acc) ->
    receive
        {publish, Msg} ->
            drain_publishes([Msg | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

fmt(FmtStr, Context) -> emqx_bridge_v2_testlib:fmt(FmtStr, Context).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(C),
    C.

spb_encode(Payload) ->
    emqx_schema_registry_serde:rsf_spb_encode([Payload]).

publish_nbirth(C, Payload) ->
    publish_nbirth(C, Payload, _Opts = #{}).

publish_nbirth(C, Payload0, Opts) ->
    publish_spb_msg(C, node, <<"NBIRTH">>, Payload0, Opts).

publish_ndata(C, Payload) ->
    publish_ndata(C, Payload, _Opts = #{}).

publish_ndata(C, Payload0, Opts) ->
    publish_spb_msg(C, node, <<"NDATA">>, Payload0, Opts).

publish_dbirth(C, Payload) ->
    publish_dbirth(C, Payload, _Opts = #{}).

publish_dbirth(C, Payload0, Opts) ->
    publish_spb_msg(C, device, <<"DBIRTH">>, Payload0, Opts).

publish_ddata(C, Payload) ->
    publish_ddata(C, Payload, _Opts = #{}).

publish_ddata(C, Payload0, Opts) ->
    publish_spb_msg(C, device, <<"DDATA">>, Payload0, Opts).

publish_spb_msg(C, NodeOrDevice, MsgType, Payload0, Opts) ->
    Topic = spb_topic(NodeOrDevice, MsgType, Opts),
    Payload = spb_encode(Payload0),
    emqtt:publish(C, Topic, Payload),
    ok.

spb_opts(Opts) ->
    Defaults = #{
        namespace => <<"spBv1.0">>,
        group_id => <<"group_id0">>,
        edge_node_id => <<"eon_id0">>,
        device_id => <<"dev_id0">>
    },
    maps:merge(Defaults, Opts).

nbirth_topic() ->
    nbirth_topic(_Opts = #{}).

nbirth_topic(Opts) ->
    spb_topic(node, <<"NBIRTH">>, Opts).

ndata_topic() ->
    ndata_topic(_Opts = #{}).

ndata_topic(Opts) ->
    spb_topic(node, <<"NDATA">>, Opts).

dbirth_topic() ->
    dbirth_topic(_Opts = #{}).

dbirth_topic(Opts) ->
    spb_topic(device, <<"DBIRTH">>, Opts).

ddata_topic() ->
    ddata_topic(_Opts = #{}).

ddata_topic(Opts) ->
    spb_topic(device, <<"DDATA">>, Opts).

spb_topic(NodeOrDevice, MsgType, Opts) ->
    #{
        namespace := Namespace,
        group_id := GroupId,
        edge_node_id := EdgeNodeId,
        device_id := DeviceId
    } = spb_opts(Opts),
    Fmt =
        case NodeOrDevice of
            node -> <<"${ns}/${gid}/${mt}/${enid}">>;
            device -> <<"${ns}/${gid}/${mt}/${enid}/${did}">>
        end,
    fmt(Fmt, #{
        mt => MsgType,
        ns => Namespace,
        gid => GroupId,
        enid => EdgeNodeId,
        did => DeviceId
    }).

create_rule(RuleTopic) ->
    create_rule(RuleTopic, _Opts = #{}).

create_rule(RuleTopic, Opts) ->
    DefaultSQL = <<"select topic, spb_decode(payload) as decoded from \"${t}\" ">>,
    SQL = maps:get(sql, Opts, DefaultSQL),
    UniqueNum = integer_to_binary(erlang:unique_integer([positive])),
    RepublishTopic = <<"repub/", UniqueNum/binary>>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(#{
        <<"sql">> => fmt(SQL, #{t => RuleTopic}),
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> => #{
                    <<"topic">> => RepublishTopic,
                    <<"payload">> => <<"${.}">>,
                    <<"qos">> => 2,
                    <<"retain">> => false
                }
            }
        ],
        <<"description">> => <<"bridge_v2 test rule">>
    }),
    #{republish_topic => RepublishTopic}.

sample_birth_payload1() ->
    #{
        <<"metrics">> =>
            [
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"name">> => <<"non_aliased_metric1">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 84,
                    <<"name">> => <<"aliased_metric1">>,
                    <<"alias">> => 1,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 42,
                    <<"name">> => <<"non_aliased_metric2">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 5,
                    <<"int_value">> => 1,
                    <<"name">> => <<"aliased_metric2">>,
                    <<"alias">> => 2,
                    <<"timestamp">> => 1678094561525
                }
            ],
        <<"seq">> => 88,
        <<"timestamp">> => 1678094561521
    }.

sample_data_payload1() ->
    #{
        <<"metrics">> =>
            [
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"name">> => <<"non_aliased_metric1">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 84,
                    <<"alias">> => 1,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 42,
                    <<"name">> => <<"non_aliased_metric2">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 5,
                    <<"int_value">> => 1,
                    <<"alias">> => 2,
                    <<"timestamp">> => 1678094561525
                }
            ],
        <<"seq">> => 88,
        <<"timestamp">> => 1678094561521
    }.

singleton_aliased_metric(Name, Alias) ->
    Point =
        case Name of
            no_name ->
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"alias">> => Alias,
                    <<"timestamp">> => 1678094561525
                };
            _ ->
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"alias">> => Alias,
                    <<"name">> => Name,
                    <<"timestamp">> => 1678094561525
                }
        end,
    #{
        <<"metrics">> => [Point],
        <<"seq">> => 88,
        <<"timestamp">> => 1678094561521
    }.

payload_with_nested_property_sets() ->
    #{
        <<"metrics">> => [
            %% Property sets in `properties`
            #{
                <<"properties">> => #{
                    <<"keys">> => [
                        <<"leaf">>,
                        <<"nested_prop">>,
                        <<"nested_prop_list">>
                    ],
                    <<"values">> => [
                        #{<<"int_value">> => 99},
                        #{
                            <<"propertyset_value">> =>
                                #{
                                    <<"keys">> => [<<"inner">>],
                                    <<"values">> => [#{<<"int_value">> => 999}]
                                }
                        },
                        #{
                            <<"propertysets_value">> =>
                                #{
                                    <<"propertyset">> => [
                                        #{
                                            <<"keys">> => [<<"inner1">>],
                                            <<"values">> => [#{<<"int_value">> => 1}]
                                        },
                                        #{
                                            <<"keys">> => [<<"inner2">>],
                                            <<"values">> => [#{<<"int_value">> => 2}]
                                        }
                                    ]
                                }
                        }
                    ]
                }
            },
            %% Property sets in `dataset_value`
            #{
                <<"dataset_value">> => #{
                    <<"columns">> => [<<"col1">>, <<"col2">>],
                    <<"types">> => [?uint32_type, ?string_type],
                    <<"rows">> => [
                        #{
                            <<"elements">> => [
                                #{<<"int_value">> => 3},
                                #{<<"string_value">> => <<"3">>}
                            ]
                        },
                        #{
                            <<"elements">> => [
                                #{<<"int_value">> => 4},
                                #{<<"string_value">> => <<"4">>}
                            ]
                        }
                    ]
                }
            }
        ]
    }.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Smoke test for alias mapping when dealing with edge of network (EoN) node sparkplugb
metrics.

After the NBIRTH message is published, the mapping is stored to an ETS table.

When handling the publish of a NDATA message, the loading is loaded from ETS as the
current mapping, and any `spb_decode` invocation in the Rule Engine uses that mapping to
add back the aliased name to the output.

When the client session disconnects, the mapping is deleted from ETS.
""".
t_eon_node_aliases(_TCConfig) ->
    C = start_client(),
    NDataTopic = ndata_topic(),
    #{republish_topic := RepublishTopic} = create_rule(NDataTopic),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, [{qos, 1}]),
    publish_nbirth(C, sample_birth_payload1()),
    publish_ndata(C, sample_data_payload1()),
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> :=
                        [
                            #{<<"name">> := <<"non_aliased_metric1">>},
                            #{<<"name">> := <<"aliased_metric1">>, <<"alias">> := 1},
                            #{<<"name">> := <<"non_aliased_metric2">>},
                            #{<<"name">> := <<"aliased_metric2">>, <<"alias">> := 2}
                        ]
                }
            }
        },
        #{data_topic => NDataTopic}
    ),
    ok.

-doc """
Smoke test for alias mapping when dealing with device sparkplugb metrics.

After the DBIRTH message is published, the mapping is stored to an ETS table.

When handling the publish of a DDATA message, the loading is loaded from ETS as the
current mapping, and any `spb_decode` invocation in the Rule Engine uses that mapping to
add back the aliased name to the output.

When the client session disconnects, the mapping is deleted from ETS.
""".
t_device_aliases(_TCConfig) ->
    C = start_client(),
    DDataTopic = ddata_topic(),
    #{republish_topic := RepublishTopic} = create_rule(DDataTopic),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, [{qos, 1}]),
    publish_dbirth(C, sample_birth_payload1()),
    publish_ddata(C, sample_data_payload1()),
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> :=
                        [
                            #{<<"name">> := <<"non_aliased_metric1">>},
                            #{<<"name">> := <<"aliased_metric1">>, <<"alias">> := 1},
                            #{<<"name">> := <<"non_aliased_metric2">>},
                            #{<<"name">> := <<"aliased_metric2">>, <<"alias">> := 2}
                        ]
                }
            }
        },
        #{data_topic => DDataTopic}
    ),
    ok.

-doc """
Verifies that device and EoN mappings are independent for the same client, i.e., the
aliases don't clash between them.
""".
t_independent_mappings_device_and_eon(_TCConfig) ->
    C = start_client(),
    NDataTopic = ndata_topic(),
    DDataTopic = ddata_topic(),
    #{republish_topic := RepublishTopicN} = create_rule(NDataTopic),
    #{republish_topic := RepublishTopicD} = create_rule(DDataTopic),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopicN, [{qos, 1}]),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopicD, [{qos, 1}]),

    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    Alias = 1,
    publish_nbirth(C, singleton_aliased_metric(Name1, Alias)),
    publish_dbirth(C, singleton_aliased_metric(Name2, Alias)),

    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),
    publish_ddata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name1}]
                }
            }
        },
        #{data_topic => DDataTopic}
    ),
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name2}]
                }
            }
        },
        #{data_topic => DDataTopic}
    ),

    ok.

-doc """
Verifies that mappings are independent for different clients.

Assumes that well behaving clients do not publish NBIRTH/DBIRTH messages with the exact
same namespaces, group ids, edge node ids and device ids.
""".
t_independent_mappings_clients(_TCConfig) ->
    Opts1 = #{group_id => <<"g1">>},
    NDataTopic1 = ndata_topic(Opts1),
    DDataTopic1 = ddata_topic(Opts1),
    Opts2 = #{group_id => <<"g2">>},
    NDataTopic2 = ndata_topic(Opts2),
    DDataTopic2 = ddata_topic(Opts2),

    #{republish_topic := RepublishTopicN} = create_rule(<<"+/+/NDATA/+">>),
    #{republish_topic := RepublishTopicD} = create_rule(<<"+/+/DDATA/+/+">>),

    Sub = start_client(),
    {ok, _, _} = emqtt:subscribe(Sub, RepublishTopicN, [{qos, 1}]),
    {ok, _, _} = emqtt:subscribe(Sub, RepublishTopicD, [{qos, 1}]),

    C1 = start_client(),
    C2 = start_client(),

    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    Alias = 1,

    publish_nbirth(C1, singleton_aliased_metric(Name1, Alias), Opts1),
    publish_dbirth(C1, singleton_aliased_metric(Name1, Alias), Opts1),

    publish_nbirth(C2, singleton_aliased_metric(Name2, Alias), Opts2),
    publish_dbirth(C2, singleton_aliased_metric(Name2, Alias), Opts2),

    publish_ndata(C1, singleton_aliased_metric(no_name, Alias), Opts1),
    publish_ddata(C1, singleton_aliased_metric(no_name, Alias), Opts1),

    publish_ndata(C2, singleton_aliased_metric(no_name, Alias), Opts2),
    publish_ddata(C2, singleton_aliased_metric(no_name, Alias), Opts2),

    %% `C1` messages use only `Name1`
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic1,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name1}]
                }
            }
        },
        #{data_topic => NDataTopic1}
    ),
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic1,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name1}]
                }
            }
        },
        #{data_topic => DDataTopic1}
    ),

    %% `C2` messages use only `Name2`
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic2,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name2}]
                }
            }
        },
        #{data_topic => NDataTopic2}
    ),
    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic2,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name2}]
                }
            }
        },
        #{data_topic => DDataTopic2}
    ),

    emqtt:stop(Sub),
    emqtt:stop(C1),
    emqtt:stop(C2),

    ok.

-doc """
Asserts that fallback actions do not have access to the mapping if they republish to a
NDATA/DDATA topic.
""".
t_fallback_actions_republish_to_data_topic(_TCConfig) ->
    NDataTopic = ndata_topic(),
    BridgeConfig = [
        {connector_type, mqtt},
        {connector_name, <<"a">>},
        {connector_config, emqx_bridge_schema_testlib:mqtt_connector_config(#{})},
        {bridge_kind, action},
        {action_type, mqtt},
        {action_name, <<"a">>},
        {action_config,
            emqx_bridge_schema_testlib:mqtt_action_config(#{
                <<"connector">> => <<"a">>,
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => #{
                            <<"topic">> => NDataTopic,
                            <<"qos">> => 1,
                            <<"retain">> => false,
                            <<"payload">> => <<"${.payload}">>
                        }
                    }
                ],
                %% Simple way to make the requests fail: make the buffer overflow
                <<"resource_opts">> => #{
                    <<"max_buffer_bytes">> => <<"0B">>,
                    <<"buffer_seg_bytes">> => <<"0B">>
                }
            })}
    ],
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    {201, _} = create_connector_api(BridgeConfig, #{}),
    {201, _} = create_action_api(BridgeConfig, #{}),

    %% First rule: only triggers action, that will fail and fallback will republish to
    %% different rule.
    SQL = <<"select spb_decode(payload) as decoded, * from \"${t}\" ">>,
    #{topic := ActionRuleTopic} = emqx_bridge_v2_testlib:simple_create_rule_api(SQL, BridgeConfig),

    %% Second rule: will be triggered by fallback action, and call `spb_decode`.  Note
    %% that this happens in a different process than the original client's, so it must
    %% load the mapping from ETS regardless.
    #{republish_topic := RepublishTopic} = create_rule(NDataTopic),

    C = start_client(),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, 1),

    Name = <<"name">>,
    Alias = 1,
    publish_nbirth(C, singleton_aliased_metric(Name, Alias)),

    DataPayload = spb_encode(singleton_aliased_metric(no_name, Alias)),
    emqtt:publish(C, ActionRuleTopic, DataPayload),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"decoded">> := #{
                    %% Note: we do not support alias mappings if a fallback action
                    %% publishes to a data topic.
                    <<"metrics">> := [#{} = M]
                }
            },
            topic := RepublishTopic
        },
        not is_map_key(<<"name">>, M),
        #{data_topic => NDataTopic}
    ),

    ok.

-doc """
Asserts that fallback actions do not need access to mapping if already decoded by original rule.
""".
t_fallback_actions_republish_already_decoded(_TCConfig) ->
    FBRepublishTopic = <<"fallback/republish">>,
    BridgeConfig = [
        {connector_type, mqtt},
        {connector_name, <<"a">>},
        {connector_config, emqx_bridge_schema_testlib:mqtt_connector_config(#{})},
        {bridge_kind, action},
        {action_type, mqtt},
        {action_name, <<"a">>},
        {action_config,
            emqx_bridge_schema_testlib:mqtt_action_config(#{
                <<"connector">> => <<"a">>,
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => #{
                            <<"topic">> => FBRepublishTopic,
                            <<"qos">> => 1,
                            <<"retain">> => false,
                            <<"payload">> => <<"${.}">>
                        }
                    }
                ],
                %% Simple way to make the requests fail: make the buffer overflow
                <<"resource_opts">> => #{
                    <<"max_buffer_bytes">> => <<"0B">>,
                    <<"buffer_seg_bytes">> => <<"0B">>
                }
            })}
    ],
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    {201, _} = create_connector_api(BridgeConfig, #{}),
    {201, _} = create_action_api(BridgeConfig, #{}),

    %% Original rule: decodes and triggers action with result, that will fail and fallback
    %% will republish it unmodified.
    NDataTopic = ndata_topic(),
    SQL = fmt(<<"select spb_decode(payload) as decoded, * from \"${t}\" ">>, #{t => NDataTopic}),
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, BridgeConfig),

    C = start_client(),
    {ok, _, _} = emqtt:subscribe(C, FBRepublishTopic, 1),

    Name = <<"name">>,
    Alias = 1,
    publish_nbirth(C, singleton_aliased_metric(Name, Alias)),

    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"decoded">> := #{
                    %% Note: we do not support alias mappings if a fallback action
                    %% publishes to a data topic.
                    <<"metrics">> := [#{<<"name">> := Name}]
                }
            },
            topic := FBRepublishTopic
        },
        #{data_topic => NDataTopic}
    ),

    ok.

-doc """
Checks that we don't keep track of mappings when alias mapping is disabled.
""".
t_disabled(_TCConfig) ->
    on_exit(fun() ->
        {ok, _} = emqx_conf:update([schema_registry, sparkplugb, enable_alias_mapping], true, #{
            override_to => cluster
        })
    end),
    {ok, _} = emqx_conf:update([schema_registry, sparkplugb, enable_alias_mapping], false, #{
        override_to => cluster
    }),

    NDataTopic = ndata_topic(),
    #{republish_topic := RepublishTopic} = create_rule(NDataTopic),
    C = start_client(),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, 1),

    Name = <<"name">>,
    Alias = 1,
    publish_nbirth(C, singleton_aliased_metric(Name, Alias)),
    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"decoded">> := #{
                    <<"metrics">> := [#{} = M]
                }
            },
            topic := RepublishTopic
        },
        not is_map_key(<<"name">>, M),
        #{data_topic => NDataTopic}
    ),
    ok.

-doc """
Tests the behavior of the `spb_zip_propsets` Rule SQL function.

- Expects a valid, decoded sparkplugb message.

- `properties` (and any nested `PropertySet` values) have their `keys` and `values` fields
  removed and have a `_kvs` key added, whose value is the values of the two former fields
  zipped together (_à la_ `maps:from_list(lists:zip(Keys, Values))`).  Values that have
  the `PropertySet` or `PropertySetList` types are recursively transformed like this.

- Values of `PropertySetList` type have their `propertyset` field removed, and a new
  `_kvs` field is added with the already zipped `PropertySets` from its value, following
  the above item's description.

- If present, `dataset_value` field is transformed in a similar fashion: its `columns` and
  `rows` fields are removed and their values zipped together in an object under a new
  `_kvs` field.

- Other values/fields are untouched.

""".
t_property_sets(_TCConfig) ->
    NDataTopic = ndata_topic(),
    SQL = <<"select spb_zip_propsets(spb_decode(payload)) as decoded from \"${t}\" ">>,
    #{republish_topic := RepublishTopic} = create_rule(NDataTopic, #{sql => SQL}),
    C = start_client(),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, 1),
    publish_ndata(C, payload_with_nested_property_sets()),

    #{
        payload := #{
            <<"decoded">> := #{
                <<"metrics">> := [
                    #{<<"properties">> := M1Props},
                    #{<<"dataset_value">> := M2DV}
                ]
            }
        }
    } =
        ?assertReceivePublish(
            #{
                payload := #{
                    <<"decoded">> := #{
                        <<"metrics">> := [
                            #{<<"properties">> := _},
                            #{<<"dataset_value">> := _}
                        ]
                    }
                }
            },
            #{data_topic => NDataTopic}
        ),
    ?assertMatch(
        #{
            <<"leaf">> := #{<<"int_value">> := 99},
            <<"nested_prop">> := #{
                <<"propertyset_value">> := #{<<"inner">> := #{<<"int_value">> := 999}}
            },
            <<"nested_prop_list">> :=
                #{
                    <<"propertysets_value">> := [
                        #{<<"inner1">> := #{<<"int_value">> := 1}},
                        #{<<"inner2">> := #{<<"int_value">> := 2}}
                    ]
                }
        },
        M1Props
    ),
    ?assertMatch(
        #{
            <<"col1">> :=
                #{
                    <<"elements">> :=
                        [
                            #{<<"int_value">> := 3},
                            #{<<"string_value">> := <<"3">>}
                        ]
                },
            <<"col2">> :=
                #{
                    <<"elements">> :=
                        [
                            #{<<"int_value">> := 4},
                            #{<<"string_value">> := <<"4">>}
                        ]
                },
            <<"types">> := [?uint32_type, ?string_type]
        },
        M2DV
    ),

    ok.
