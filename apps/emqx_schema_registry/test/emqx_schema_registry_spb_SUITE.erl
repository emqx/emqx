%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
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
    DefaultSQL = <<"select username, topic, spb_decode(payload) as decoded from \"${t}\" ">>,
    SQL = maps:get(sql, Opts, DefaultSQL),
    UniqueNum = integer_to_binary(erlang:unique_integer([positive])),
    RepublishTopic0 = <<"repub/", UniqueNum/binary>>,
    RepubTopicTransformFn = maps:get(republish_topic_transform_fn, Opts, fun(X) -> X end),
    RepublishTopic = RepubTopicTransformFn(RepublishTopic0),
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
                    <<"num_of_columns">> => 2,
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

%% Runs `Fun` in a spawned process, optionally labeled via `proc_lib:set_label/1`, and
%% returns its result.
run_in_process(Label, Fun) ->
    Parent = self(),
    {Pid, MRef} = spawn_monitor(fun() ->
        Label =/= undefined andalso proc_lib:set_label(Label),
        Parent ! {result, self(), Fun()}
    end),
    receive
        {result, Pid, Res} ->
            erlang:demonitor(MRef, [flush]),
            Res;
        {'DOWN', MRef, process, Pid, Reason} ->
            error({worker_died, Reason})
    after 5_000 ->
        error(timeout)
    end.

%% Drives the publish hook directly in the calling process, as the broker does for the
%% process that publishes the message.
hook_publish(NodeOrDevice, MsgType, Payload0) ->
    hook_publish_raw(NodeOrDevice, MsgType, spb_encode(Payload0)).

hook_publish_raw(NodeOrDevice, MsgType, Payload) ->
    Topic = spb_topic(NodeOrDevice, MsgType, _Opts = #{}),
    Message = emqx_message:make(<<"from">>, Topic, Payload),
    _ = emqx_schema_registry_spb_hookcb:on_message_publish_alias_mapping(Message),
    ok.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).

with_tcp_listener(Conf0, Fn) ->
    Type = tcp,
    Port = emqx_common_test_helpers:select_free_port(Type),
    Name = spb,
    Conf = Conf0#{
        ~"bind" => format_bind({"127.0.0.1", Port})
    },
    {ok, _} = emqx:update_config([listeners, Type, Name], {create, Conf}),
    try
        Fn(#{port => Port})
    after
        ok = emqx_listeners:stop_listener(emqx_listeners:listener_id(Type, Name)),
        emqx:remove_config([listeners, Type, Name])
    end.

drop_first_level(Topic) ->
    [_ | Rest] = emqx_topic:words(Topic),
    emqx_topic:join(Rest).

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
        #{data_topic => NDataTopic, name => Name1}
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
        #{data_topic => DDataTopic, name => Name2}
    ),

    ok.

-doc """
Verifies that device and EoN mappings are independent for the same client, i.e., the
aliases don't clash between them.

Almost identical to `t_independent_mappings_device_and_eon2`, but with a different
publishing order: here, we publish an NDATA message before DBIRTH, populating the current
mapping in the process dictionary.

Ref: https://github.com/emqx/emqx/issues/19695
""".
t_independent_mappings_device_and_eon2(_TCConfig) ->
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
    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),

    publish_dbirth(C, singleton_aliased_metric(Name2, Alias)),
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
        #{data_topic => NDataTopic, name => Name1}
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
        #{data_topic => DDataTopic, name => Name2}
    ),

    ok.

-doc """
Smoke test to verify that a "rebirth" (EoN) with a different metric name for the same
alias correctly updates the mapping.
""".
t_rebirth_eon(_TCConfig) ->
    C = start_client(),
    NDataTopic = ndata_topic(),
    #{republish_topic := RepublishTopicN} = create_rule(NDataTopic),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopicN, [{qos, 1}]),

    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    Alias = 1,
    publish_nbirth(C, singleton_aliased_metric(Name1, Alias)),
    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name1}]
                }
            }
        },
        #{data_topic => NDataTopic, name => Name1}
    ),

    %% rebirth with different metric name
    publish_nbirth(C, singleton_aliased_metric(Name2, Alias)),
    publish_ndata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := NDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name2}]
                }
            }
        },
        #{data_topic => NDataTopic, name => Name2}
    ),

    ok.

-doc """
Smoke test to verify that a "rebirth" (device) with a different metric name for the same
alias correctly updates the mapping.
""".
t_rebirth_device(_TCConfig) ->
    C = start_client(),
    DDataTopic = ddata_topic(),
    #{republish_topic := RepublishTopicD} = create_rule(DDataTopic),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopicD, [{qos, 1}]),

    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    Alias = 1,
    publish_dbirth(C, singleton_aliased_metric(Name1, Alias)),
    publish_ddata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name1}]
                }
            }
        },
        #{data_topic => DDataTopic, name => Name1}
    ),

    %% rebirth with different metric name
    publish_dbirth(C, singleton_aliased_metric(Name2, Alias)),
    publish_ddata(C, singleton_aliased_metric(no_name, Alias)),

    ?assertReceivePublish(
        #{
            payload := #{
                <<"topic">> := DDataTopic,
                <<"decoded">> := #{
                    <<"metrics">> := [#{<<"name">> := Name2}]
                }
            }
        },
        #{data_topic => DDataTopic, name => Name2}
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
Tests the behavior of the `spb_zip_kvs` Rule SQL function.

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
    SQL = <<"select spb_zip_kvs(spb_decode(payload)) as decoded from \"${t}\" ">>,
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
                }
        },
        M2DV
    ),
    ?assertEqual(#{}, maps:with([<<"num_of_columns">>, <<"types">>], M2DV)),

    ok.

-doc """
Checks that the publish hook maintains the alias mapping when it runs in a process
labeled as an MQTT client channel process (`{clientid, _}` proc label).
""".
t_alias_cache_in_client_process(_TCConfig) ->
    Name = <<"name">>,
    Alias = 1,
    Mapping = run_in_process({clientid, <<"c1">>}, fun() ->
        hook_publish(node, <<"NBIRTH">>, singleton_aliased_metric(Name, Alias)),
        hook_publish(node, <<"NDATA">>, singleton_aliased_metric(no_name, Alias)),
        emqx_schema_registry_spb_state:get_current_alias_mapping()
    end),
    ?assertEqual(#{Alias => Name}, Mapping),
    ok.

-doc """
Checks that the publish hook does not touch the alias cache when it runs in a process
without a `{clientid, _}` proc label, such as a bridge ingress or `emqx_pool` worker
publishing on behalf of many clients.
""".
t_no_alias_cache_in_non_client_process(_TCConfig) ->
    Name = <<"name">>,
    Alias = 1,
    Mapping = run_in_process(_Label = undefined, fun() ->
        hook_publish(node, <<"NBIRTH">>, singleton_aliased_metric(Name, Alias)),
        hook_publish(node, <<"NDATA">>, singleton_aliased_metric(no_name, Alias)),
        emqx_schema_registry_spb_state:get_current_alias_mapping()
    end),
    ?assertEqual(#{}, Mapping),
    ok.

-doc """
Checks that alias mappings registered in one client channel process are not visible in
another, even for the same namespace, group id and edge node id.
""".
t_alias_cache_isolated_per_process(_TCConfig) ->
    Name = <<"name">>,
    Alias = 1,
    #{} = run_in_process({clientid, <<"c1">>}, fun() ->
        hook_publish(node, <<"NBIRTH">>, singleton_aliased_metric(Name, Alias)),
        emqx_schema_registry_spb_state:get_current_alias_mapping()
    end),
    Mapping = run_in_process({clientid, <<"c2">>}, fun() ->
        hook_publish(node, <<"NDATA">>, singleton_aliased_metric(no_name, Alias)),
        emqx_schema_registry_spb_state:get_current_alias_mapping()
    end),
    ?assertEqual(#{}, Mapping),
    ok.

-doc """
Verifies that clients under mountpoints behave the same as those that are not.
""".
t_mountpoints(_TCConfig) ->
    RepubTopicTransformFn = fun(T0) -> <<"${username}/", T0/binary>> end,
    Opts = #{republish_topic_transform_fn => RepubTopicTransformFn},

    NDataTopic = ndata_topic(),
    NDataTopicWildcard = emqx_topic:join([~"+", NDataTopic]),
    #{republish_topic := RepublishTopicN} = create_rule(NDataTopicWildcard, Opts),
    DDataTopic = ddata_topic(),
    DDataTopicWildcard = emqx_topic:join([~"+", DDataTopic]),
    #{republish_topic := RepublishTopicD} = create_rule(DDataTopicWildcard, Opts),

    %% no mountpoint
    Subscriber = start_client(),
    RepublishTopicN0 = drop_first_level(RepublishTopicN),
    {ok, _, _} = emqtt:subscribe(Subscriber, <<"+/", RepublishTopicN0/binary>>, [{qos, 1}]),
    RepublishTopicD0 = drop_first_level(RepublishTopicD),
    {ok, _, _} = emqtt:subscribe(Subscriber, <<"+/", RepublishTopicD0/binary>>, [{qos, 1}]),

    Conf0 = #{~"mountpoint" => ~"${username}/"},
    with_tcp_listener(Conf0, fun(#{port := Port}) ->
        C1 = start_client(#{port => Port, username => ~"u1"}),
        C2 = start_client(#{port => Port, username => ~"u2"}),
        Ctx = #{
            sub => Subscriber,
            c1 => C1,
            c2 => C2,
            repub_topic_ndata => RepublishTopicN0,
            repub_topic_ddata => RepublishTopicD0
        },

        [
            {ok, _, _} = emqtt:subscribe(C, T, [{qos, 1}])
         || C <- [C1, C2],
            T <- [RepublishTopicN0, RepublishTopicD0]
        ],

        publish_nbirth(C2, sample_birth_payload1()),
        publish_ndata(C2, sample_data_payload1()),
        %% non-mountpointed subscriber should get the mapped name
        ?assertReceivePublish(
            #{
                client_pid := Subscriber,
                topic := <<"u2/", RepublishTopicN0/binary>>,
                payload := #{
                    <<"topic">> := <<"u2/", NDataTopic/binary>>,
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
            Ctx#{data_topic => NDataTopic}
        ),
        %% mountpointed subscriber should get the mapped name, and doesn't see the mountpoint
        ?assertReceivePublish(
            #{
                client_pid := C2,
                topic := RepublishTopicN0,
                payload := #{
                    <<"topic">> := <<"u2/", NDataTopic/binary>>,
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
            Ctx#{data_topic => NDataTopic}
        ),

        publish_dbirth(C2, sample_birth_payload1()),
        publish_ddata(C2, sample_data_payload1()),
        %% non-mountpointed subscriber should get the mapped name
        ?assertReceivePublish(
            #{
                client_pid := Subscriber,
                topic := <<"u2/", RepublishTopicD0/binary>>,
                payload := #{
                    <<"topic">> := <<"u2/", DDataTopic/binary>>,
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
            Ctx#{data_topic => DDataTopic}
        ),
        %% mountpointed subscriber should get the mapped name, and doesn't see the mountpoint
        ?assertReceivePublish(
            #{
                client_pid := C2,
                topic := RepublishTopicD0,
                payload := #{
                    <<"topic">> := <<"u2/", DDataTopic/binary>>,
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
            Ctx#{data_topic => DDataTopic}
        ),

        ?assertNotReceive({decoded, #{client_pid := C1}}),
        ?assertNotReceive({publish, #{client_pid := C1}}),

        emqtt:stop(C1),
        emqtt:stop(C2),
        emqtt:stop(Subscriber),
        ok
    end),
    ok.

-doc """
Checks that a large birth message does not kill the publisher's channel process.

The publish hook runs in a process that has the heap limit of a channel process with
the default `force_shutdown` policy. A full protobuf decode of this payload needs more
heap than that limit. The aliases of the payload must still be registered.
""".
t_large_birth_message(_TCConfig) ->
    #{enable := true, max_heap_size := MaxHeapSize} =
        ShutdownPolicy =
        emqx_config:get_zone_conf(default, [force_shutdown]),
    ?assert(MaxHeapSize > 0),
    NumMetrics = 20_000,
    AliasEvery = 1_000,
    Payload = spb_encode(large_birth_payload(NumMetrics, AliasEvery)),
    Mapping = run_in_process({clientid, <<"c1">>}, fun() ->
        _ = emqx_utils:tune_heap_size(ShutdownPolicy),
        hook_publish_raw(device, <<"DBIRTH">>, Payload),
        hook_publish(device, <<"DDATA">>, singleton_aliased_metric(no_name, AliasEvery)),
        emqx_schema_registry_spb_state:get_current_alias_mapping()
    end),
    Expected = maps:from_list([
        {I, large_metric_name(I)}
     || I <- lists:seq(1, NumMetrics), I rem AliasEvery =:= 0
    ]),
    ?assertEqual(Expected, Mapping),
    ok.

-doc """
Checks that the alias scan of a birth payload returns the same mapping as the full
decode, for metrics with and without names and aliases, repeated aliases, the largest
alias value, and property sets.
""".
t_scan_aliases_matches_full_decode(_TCConfig) ->
    MaxAlias = (1 bsl 64) - 1,
    Payloads = [
        sample_birth_payload1(),
        sample_data_payload1(),
        large_birth_payload(50, 7),
        #{
            <<"seq">> => 1,
            <<"metrics">> => [
                #{<<"name">> => <<"a">>, <<"alias">> => 1},
                #{<<"name">> => <<"b">>, <<"alias">> => 1},
                #{<<"alias">> => 2},
                #{<<"name">> => <<"c">>},
                #{<<"name">> => <<"d">>, <<"alias">> => MaxAlias, <<"double_value">> => 1.5},
                #{<<"name">> => <<>>, <<"alias">> => 0}
            ]
        },
        #{<<"seq">> => 1}
    ],
    lists:foreach(
        fun(Payload0) ->
            Payload = spb_encode(Payload0),
            ?assertEqual(
                full_decode_aliases(Payload),
                emqx_schema_registry_spb_state:scan_aliases(Payload),
                #{payload => Payload0}
            )
        end,
        Payloads
    ),
    ok.

-doc """
Checks that a birth payload that is not valid protobuf registers no aliases and does
not crash the publisher's channel process.
""".
t_bad_birth_message(_TCConfig) ->
    Good = spb_encode(singleton_aliased_metric(<<"name">>, 1)),
    BadPayloads = [
        %% Truncated metric.
        binary:part(Good, 0, byte_size(Good) - 1),
        %% Varint longer than 10 bytes.
        <<(binary:copy(<<16#ff>>, 10))/binary, 1>>,
        %% Group start (wire type 3), which the scan does not support.
        <<((3 bsl 3) bor 3), Good/binary>>,
        %% Length larger than the rest of the payload.
        <<((2 bsl 3) bor 2), 100, 0>>,
        %% Truncated fixed64 field.
        <<((1 bsl 3) bor 1), 0, 0>>,
        %% Metric length larger than 64 bits. Its low 64 bits are the real length of the
        %% metric, so a reader that truncates lengths to 64 bits accepts it.
        begin
            Metric = <<((1 bsl 3) bor 2), 4, "name", ((2 bsl 3) bor 0), 1>>,
            Len = encode_varint((1 bsl 64) + byte_size(Metric)),
            <<((2 bsl 3) bor 2), Len/binary, Metric/binary>>
        end
    ],
    lists:foreach(
        fun(Bad) ->
            ?assertError(
                {bad_protobuf, _},
                emqx_schema_registry_spb_state:scan_aliases(Bad),
                #{payload => Bad}
            ),
            Mapping = run_in_process({clientid, <<"c1">>}, fun() ->
                hook_publish_raw(node, <<"NBIRTH">>, Bad),
                hook_publish(node, <<"NDATA">>, singleton_aliased_metric(no_name, 1)),
                emqx_schema_registry_spb_state:get_current_alias_mapping()
            end),
            ?assertEqual(#{}, Mapping, #{payload => Bad})
        end,
        BadPayloads
    ),
    ok.

large_birth_payload(NumMetrics, AliasEvery) ->
    NumProps = 8,
    Props = #{
        <<"keys">> => [
            <<"property", (integer_to_binary(I))/binary>>
         || I <- lists:seq(1, NumProps)
        ],
        <<"values">> => [
            #{<<"type">> => ?string_type, <<"string_value">> => <<"property value">>}
         || _ <- lists:seq(1, NumProps)
        ]
    },
    Metrics = [
        large_metric(I, I rem AliasEvery =:= 0, Props)
     || I <- lists:seq(1, NumMetrics)
    ],
    #{<<"timestamp">> => 1678094561521, <<"seq">> => 0, <<"metrics">> => Metrics}.

large_metric(I, WithAlias, Props) ->
    Metric = #{
        <<"name">> => large_metric_name(I),
        <<"datatype">> => ?uint32_type,
        <<"int_value">> => I,
        <<"properties">> => Props
    },
    case WithAlias of
        true -> Metric#{<<"alias">> => I};
        false -> Metric
    end.

large_metric_name(I) ->
    <<"device/metric/", (integer_to_binary(I))/binary>>.

%% Alias mapping from a full decode. Call it only in a process that has no current alias
%% mapping, such as the test case process: the decode adds names from that mapping.
full_decode_aliases(Payload) ->
    ?assertEqual(#{}, emqx_schema_registry_spb_state:get_current_alias_mapping()),
    Decoded = emqx_schema_registry_serde:rsf_spb_decode([Payload]),
    lists:foldl(
        fun
            (#{<<"alias">> := Alias, <<"name">> := Name}, Acc) -> Acc#{Alias => Name};
            (_, Acc) -> Acc
        end,
        #{},
        maps:get(<<"metrics">>, Decoded, [])
    ).

%% Like `full_decode_aliases/1', but return `error' when the decode fails.
try_full_decode_aliases(Payload) ->
    try
        {ok, full_decode_aliases(Payload)}
    catch
        _:_ -> error
    end.

try_scan_aliases(Payload) ->
    try
        {ok, emqx_schema_registry_spb_state:scan_aliases(Payload)}
    catch
        error:{bad_protobuf, _} -> bad_protobuf
    end.

%%------------------------------------------------------------------------------
%% Property tests
%%------------------------------------------------------------------------------

%% The properties run with `noshrink'. Shrinking a nested payload tries a very large
%% number of candidates and can run for many minutes. The generated payloads are small,
%% and `?WHENFAIL' prints the failing one.

-doc """
Property: for any Sparkplug B payload, the alias scan returns the same mapping as the
full protobuf decode.

The generated payloads have metrics with and without names and aliases, small aliases
that repeat, the largest alias value, all metric value types except `float_value`,
nested property sets, and templates whose metrics have their own aliases.
""".
t_prop_scan_aliases_matches_full_decode(_TCConfig) ->
    ?assert(
        proper:quickcheck(
            prop_scan_aliases_matches_full_decode(),
            [{numtests, 500}, noshrink, {to_file, user}]
        )
    ).

prop_scan_aliases_matches_full_decode() ->
    ?FORALL(
        Payload0,
        spb_payload_gen(),
        begin
            Payload = spb_encode(Payload0),
            Expected = full_decode_aliases(Payload),
            Scanned = emqx_schema_registry_spb_state:scan_aliases(Payload),
            ?WHENFAIL(
                ct:pal("payload: ~p~nexpected: ~p~nscanned: ~p", [Payload0, Expected, Scanned]),
                aggregate([map_size(Expected) > 0], Expected =:= Scanned)
            )
        end
    ).

-doc """
Property: for a valid payload with a random change, the alias scan returns a mapping or
raises a `bad_protobuf` error, and raises no other exception. When both the scan and the
full decode accept the bytes, they return the same mapping.

The change is one of: truncate, change one byte, insert random bytes. It applies to the
whole payload, or to the bytes of one metric inside a valid length prefix. The second
form reaches the fields inside a metric: a change to the whole payload usually breaks
the length prefix of a metric first.

Both can disagree on whether the bytes are valid: the scan does not check the fields
that it skips, and it rejects protobuf groups.
""".
t_prop_scan_aliases_bad_input(_TCConfig) ->
    ?assert(
        proper:quickcheck(
            prop_scan_aliases_bad_input(),
            [{numtests, 1000}, noshrink, {to_file, user}]
        )
    ).

prop_scan_aliases_bad_input() ->
    ?FORALL(
        {Payload0, Target, Mutation},
        {spb_payload_gen(), oneof([payload, {metric, non_neg_integer()}]), mutation_gen()},
        begin
            Payload = mutated_payload(Target, Mutation, Payload0),
            Scanned = try_scan_aliases(Payload),
            Full = try_full_decode_aliases(Payload),
            ?WHENFAIL(
                ct:pal("payload: ~0p~nscanned: ~p~nfull decode: ~p", [Payload, Scanned, Full]),
                aggregate(
                    [{scan, outcome(Scanned), full, outcome(Full)}],
                    case {Scanned, Full} of
                        {{ok, Mapping1}, {ok, Mapping2}} -> Mapping1 =:= Mapping2;
                        _ -> true
                    end
                )
            )
        end
    ).

outcome({ok, _}) -> ok;
outcome(Other) -> Other.

mutated_payload({metric, Index0}, Mutation, #{<<"metrics">> := [_ | _] = Metrics} = Payload0) ->
    Index = Index0 rem length(Metrics),
    MetricFields = [
        metric_field(maybe_mutate(I =:= Index, Mutation, encode_metric(Metric)))
     || {I, Metric} <- lists:enumerate(0, Metrics)
    ],
    iolist_to_binary([spb_encode(maps:remove(<<"metrics">>, Payload0)) | MetricFields]);
mutated_payload(_Target, Mutation, Payload0) ->
    mutate(Mutation, spb_encode(Payload0)).

maybe_mutate(true, Mutation, Bin) -> mutate(Mutation, Bin);
maybe_mutate(false, _Mutation, Bin) -> Bin.

%% `Payload.metrics' is field 2 with wire type 2 (length-delimited).
metric_field(Bin) ->
    <<((2 bsl 3) bor 2), (encode_varint(byte_size(Bin)))/binary, Bin/binary>>.

%% Same as `spb_encode/1' for a single metric: `bytes_value' is base64 in the term.
encode_metric(Metric0) ->
    Metric =
        case Metric0 of
            #{<<"bytes_value">> := Bytes64} ->
                Metric0#{<<"bytes_value">> := base64:decode(Bytes64)};
            _ ->
                Metric0
        end,
    emqx_schema_registry_serde:rsf_sparkplug_encode([Metric, <<"Payload.Metric">>]).

encode_varint(N) when N < 128 -> <<N>>;
encode_varint(N) -> <<1:1, (N band 127):7, (encode_varint(N bsr 7))/binary>>.

mutation_gen() ->
    oneof([
        {truncate, non_neg_integer()},
        {change, non_neg_integer(), range(1, 255)},
        {insert, non_neg_integer(), binary()}
    ]).

mutate({truncate, Pos}, Bin) ->
    binary:part(Bin, 0, Pos rem (byte_size(Bin) + 1));
mutate({change, _Pos, _Xor}, <<>>) ->
    <<>>;
mutate({change, Pos0, Xor}, Bin) ->
    Pos = Pos0 rem byte_size(Bin),
    <<Before:Pos/binary, Byte, After/binary>> = Bin,
    <<Before/binary, (Byte bxor Xor), After/binary>>;
mutate({insert, Pos0, Extra}, Bin) ->
    Pos = Pos0 rem (byte_size(Bin) + 1),
    <<Before:Pos/binary, After/binary>> = Bin,
    <<Before/binary, Extra/binary, After/binary>>.

spb_payload_gen() ->
    ?LET(
        {Timestamp, Seq, Uuid, Body, Metrics},
        {
            option(uint64_gen()),
            option(uint64_gen()),
            option(utf8()),
            option(binary()),
            short_list(metric_gen(_Depth = 1))
        },
        compact(#{
            <<"timestamp">> => Timestamp,
            <<"seq">> => Seq,
            <<"uuid">> => Uuid,
            <<"body">> => Body,
            <<"metrics">> => Metrics
        })
    ).

metric_gen(Depth) ->
    ?LET(
        {Name, Alias, Timestamp, Datatype, IsNull, Properties, Value},
        {
            option(name_gen()),
            option(alias_gen()),
            option(uint64_gen()),
            option(range(0, 34)),
            option(boolean()),
            option(property_set_gen(Depth)),
            option(metric_value_gen(Depth))
        },
        maps:merge(
            compact(#{
                <<"name">> => Name,
                <<"alias">> => Alias,
                <<"timestamp">> => Timestamp,
                <<"datatype">> => Datatype,
                <<"is_null">> => IsNull,
                <<"properties">> => Properties
            }),
            value_map(Value)
        )
    ).

name_gen() ->
    oneof([
        utf8(),
        ?LET(I, range(0, 3), <<"metric", (integer_to_binary(I))/binary>>)
    ]).

%% Small aliases repeat often across the metrics of one payload.
alias_gen() ->
    frequency([
        {5, range(0, 5)},
        {2, uint64_gen()},
        {1, exactly((1 bsl 64) - 1)}
    ]).

metric_value_gen(Depth) ->
    oneof(
        [
            {<<"int_value">>, uint32_gen()},
            {<<"long_value">>, uint64_gen()},
            {<<"double_value">>, float()},
            {<<"boolean_value">>, boolean()},
            {<<"string_value">>, utf8()},
            {<<"bytes_value">>, ?LET(Bytes, binary(), base64:encode(Bytes))}
        ] ++
            [
                {<<"template_value">>, template_gen(Depth - 1)}
             || Depth > 0
            ]
    ).

%% The metrics of a template have names and aliases. They must not go into the
%% mapping: only top-level metrics do.
template_gen(Depth) ->
    ?LET(
        {Version, Metrics, IsDefinition},
        {option(utf8()), short_list(metric_gen(Depth)), option(boolean())},
        compact(#{
            <<"version">> => Version,
            <<"metrics">> => Metrics,
            <<"is_definition">> => IsDefinition
        })
    ).

property_set_gen(Depth) ->
    ?LET(
        KVs,
        short_list({utf8(), property_value_gen(Depth)}),
        #{
            <<"keys">> => [K || {K, _} <- KVs],
            <<"values">> => [V || {_, V} <- KVs]
        }
    ).

property_value_gen(Depth) ->
    ?LET(
        {Type, IsNull, Value},
        {option(range(0, 34)), option(boolean()), option(property_value_value_gen(Depth))},
        maps:merge(
            compact(#{<<"type">> => Type, <<"is_null">> => IsNull}),
            value_map(Value)
        )
    ).

property_value_value_gen(Depth) ->
    oneof(
        [
            {<<"int_value">>, uint32_gen()},
            {<<"long_value">>, uint64_gen()},
            {<<"double_value">>, float()},
            {<<"boolean_value">>, boolean()},
            {<<"string_value">>, utf8()}
        ] ++
            [
                {<<"propertyset_value">>, property_set_gen(Depth - 1)}
             || Depth > 0
            ] ++
            [
                {<<"propertysets_value">>,
                    ?LET(
                        Sets,
                        short_list(property_set_gen(Depth - 1)),
                        #{<<"propertyset">> => Sets}
                    )}
             || Depth > 0
            ]
    ).

option(Gen) ->
    oneof([undefined, Gen]).

short_list(Gen) ->
    ?LET(N, range(0, 6), vector(N, Gen)).

uint32_gen() ->
    range(0, (1 bsl 32) - 1).

uint64_gen() ->
    range(0, (1 bsl 64) - 1).

value_map(undefined) -> #{};
value_map({Key, Value}) -> #{Key => Value}.

%% Remove the keys whose value is `undefined'.
compact(Map) ->
    maps:filter(fun(_K, V) -> V =/= undefined end, Map).
