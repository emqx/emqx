%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_schema_registry.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, avro},
        {group, protobuf},
        {group, json}
    ] ++ sparkplug_tests() ++ only_once_tcs().

groups() ->
    OnlyOnceTCs = only_once_tcs(),
    AllTCsExceptSP = emqx_common_test_helpers:all(?MODULE) -- (sparkplug_tests() ++ OnlyOnceTCs),
    ProtobufOnlyTCs = protobuf_only_tcs(),
    TCs = AllTCsExceptSP -- ProtobufOnlyTCs,
    [{avro, TCs}, {json, TCs}, {protobuf, AllTCsExceptSP}].

only_once_tcs() ->
    [
        t_import_config,
        t_external_registry_load_config
    ].

protobuf_only_tcs() ->
    [
        t_protobuf_union_encode,
        t_protobuf_union_decode
    ].

sparkplug_tests() ->
    [
        t_sparkplug_decode,
        t_sparkplug_encode,
        t_sparkplug_decode_encode_with_message_name,
        t_sparkplug_encode_float_to_uint64_key,
        t_decode_fail
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_schema_registry,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(avro, Config) ->
    [{serde_type, avro} | Config];
init_per_group(json, Config) ->
    [{serde_type, json} | Config];
init_per_group(protobuf, Config) ->
    [{serde_type, protobuf} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    clear_schemas(),
    clear_external_registries(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

trace_rule(Data, Envs, _Args) ->
    Now = erlang:monotonic_time(),
    ets:insert(recorded_actions, {Now, #{data => Data, envs => Envs}}),
    TestPid = persistent_term:get({?MODULE, test_pid}),
    TestPid ! {action, #{data => Data, envs => Envs}},
    ok.

make_trace_fn_action() ->
    persistent_term:put({?MODULE, test_pid}, self()),
    Fn = <<(atom_to_binary(?MODULE))/binary, ":trace_rule">>,
    emqx_utils_ets:new(recorded_actions, [named_table, public, ordered_set]),
    #{function => Fn, args => #{}}.

create_rule_http(RuleParams) ->
    create_rule_http(RuleParams, _Overrides = #{}).

create_rule_http(RuleParams, Overrides) ->
    RepublishTopic = <<"republish/schema_registry">>,
    emqx:subscribe(RepublishTopic),
    PayloadTemplate = maps:get(payload_template, Overrides, <<>>),
    DefaultParams = #{
        enable => true,
        actions => [
            make_trace_fn_action(),
            #{
                <<"function">> => <<"republish">>,
                <<"args">> =>
                    #{
                        <<"topic">> => RepublishTopic,
                        <<"payload">> => PayloadTemplate,
                        <<"qos">> => 0,
                        <<"retain">> => false,
                        <<"user_properties">> => <<>>
                    }
            }
        ]
    },
    Params = maps:merge(DefaultParams, RuleParams),
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res0} ->
            Res = #{<<"id">> := RuleId} = emqx_utils_json:decode(Res0, [return_maps]),
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            {ok, Res};
        Error ->
            Error
    end.

schema_params(avro) ->
    Source = #{
        type => record,
        name => <<"test">>,
        namespace => <<"emqx.com">>,
        fields => [
            #{name => <<"i">>, type => <<"int">>},
            #{name => <<"s">>, type => <<"string">>}
        ]
    },
    SourceBin = emqx_utils_json:encode(Source),
    #{type => avro, source => SourceBin};
schema_params(json) ->
    Source =
        #{
            type => object,
            properties => #{
                i => #{type => integer},
                s => #{type => string}
            },
            required => [<<"i">>, <<"s">>]
        },
    SourceBin = emqx_utils_json:encode(Source),
    #{type => json, source => SourceBin};
schema_params(protobuf) ->
    SourceBin =
        <<
            "message Person {\n"
            "     required string name = 1;\n"
            "     required int32 id = 2;\n"
            "     optional string email = 3;\n"
            "  }\n"
            "message UnionValue {\n"
            "    oneof u {\n"
            "        int32  a = 1;\n"
            "        string b = 2;\n"
            "    }\n"
            "}\n"
        >>,
    #{type => protobuf, source => SourceBin}.

create_serde(SerdeType, SerdeName) ->
    Schema = schema_params(SerdeType),
    ok = emqx_schema_registry:add_schema(SerdeName, Schema),
    ok.

test_params_for(Type, encode_decode1) when Type =:= avro; Type =:= json ->
    SQL =
        <<
            "select\n"
            "  schema_decode('my_serde',\n"
            "    schema_encode('my_serde', json_decode(payload))) as decoded,\n\n"
            "  decoded.i as decoded_int,\n"
            "  decoded.s as decoded_string\n"
            "from t\n"
        >>,
    Payload = #{<<"i">> => 10, <<"s">> => <<"text">>},
    ExpectedRuleOutput =
        #{
            <<"decoded">> => #{<<"i">> => 10, <<"s">> => <<"text">>},
            <<"decoded_int">> => 10,
            <<"decoded_string">> => <<"text">>
        },
    ExtraArgs = [],
    #{
        sql => SQL,
        payload => Payload,
        expected_rule_output => ExpectedRuleOutput,
        extra_args => ExtraArgs
    };
test_params_for(Type, encode1) when Type =:= avro; Type =:= json ->
    SQL =
        <<
            "select\n"
            "  schema_encode('my_serde', json_decode(payload)) as encoded\n"
            "from t\n"
        >>,
    Payload = #{<<"i">> => 10, <<"s">> => <<"text">>},
    PayloadTemplate = <<"${.encoded}">>,
    ExtraArgs = [],
    #{
        sql => SQL,
        payload => Payload,
        payload_template => PayloadTemplate,
        extra_args => ExtraArgs
    };
test_params_for(Type, decode1) when Type =:= avro; Type =:= json ->
    SQL =
        <<
            "select\n"
            "  schema_decode('my_serde', payload) as decoded\n"
            "from t\n"
        >>,
    Payload = #{<<"i">> => 10, <<"s">> => <<"text">>},
    ExtraArgs = [],
    #{
        sql => SQL,
        payload => Payload,
        extra_args => ExtraArgs
    };
test_params_for(protobuf, encode_decode1) ->
    SQL =
        <<
            "select\n"
            "  schema_decode('my_serde',\n"
            "    schema_encode('my_serde', json_decode(payload), 'Person'),\n"
            "      'Person') as decoded,\n"
            "  decoded.name as decoded_name,\n"
            "  decoded.email as decoded_email,\n"
            "  decoded.id as decoded_id\n"
            "from t\n"
        >>,
    Payload = #{<<"name">> => <<"some name">>, <<"id">> => 10, <<"email">> => <<"emqx@emqx.io">>},
    ExpectedRuleOutput =
        #{
            <<"decoded">> =>
                #{
                    <<"email">> => <<"emqx@emqx.io">>,
                    <<"id">> => 10,
                    <<"name">> => <<"some name">>
                },
            <<"decoded_email">> => <<"emqx@emqx.io">>,
            <<"decoded_id">> => 10,
            <<"decoded_name">> => <<"some name">>
        },
    ExtraArgs = [<<"Person">>],
    #{
        sql => SQL,
        payload => Payload,
        extra_args => ExtraArgs,
        expected_rule_output => ExpectedRuleOutput
    };
test_params_for(protobuf, decode1) ->
    SQL =
        <<
            "select\n"
            "  schema_decode('my_serde', payload, 'Person') as decoded\n"
            "from t\n"
        >>,
    Payload = #{<<"name">> => <<"some name">>, <<"id">> => 10, <<"email">> => <<"emqx@emqx.io">>},
    ExtraArgs = [<<"Person">>],
    #{
        sql => SQL,
        payload => Payload,
        extra_args => ExtraArgs
    };
test_params_for(protobuf, encode1) ->
    SQL =
        <<
            "select\n"
            "  schema_encode('my_serde', json_decode(payload), 'Person') as encoded\n"
            "from t\n"
        >>,
    Payload = #{<<"name">> => <<"some name">>, <<"id">> => 10, <<"email">> => <<"emqx@emqx.io">>},
    PayloadTemplate = <<"${.encoded}">>,
    ExtraArgs = [<<"Person">>],
    #{
        sql => SQL,
        payload => Payload,
        payload_template => PayloadTemplate,
        extra_args => ExtraArgs
    };
test_params_for(protobuf, union1) ->
    SQL =
        <<
            "select\n"
            "  schema_decode('my_serde', payload, 'UnionValue') as decoded,\n"
            "  decoded.a as decoded_a,\n"
            "  decoded.b as decoded_b\n"
            "from t\n"
        >>,
    PayloadA = #{<<"a">> => 10},
    PayloadB = #{<<"b">> => <<"string">>},
    ExtraArgs = [<<"UnionValue">>],
    #{
        sql => SQL,
        payload => #{a => PayloadA, b => PayloadB},
        extra_args => ExtraArgs
    };
test_params_for(protobuf, union2) ->
    SQL =
        <<
            "select\n"
            "  schema_encode('my_serde', json_decode(payload), 'UnionValue') as encoded\n"
            "from t\n"
        >>,
    PayloadA = #{<<"a">> => 10},
    PayloadB = #{<<"b">> => <<"string">>},
    ExtraArgs = [<<"UnionValue">>],
    #{
        sql => SQL,
        payload => #{a => PayloadA, b => PayloadB},
        extra_args => ExtraArgs
    };
test_params_for(Type, Name) ->
    ct:fail("unimplemented: ~p", [{Type, Name}]).

clear_schemas() ->
    maps:foreach(
        fun(Name, _Schema) ->
            ok = emqx_schema_registry:delete_schema(Name)
        end,
        emqx_schema_registry:list_schemas()
    ).

clear_external_registries() ->
    maps:foreach(
        fun(Name, _Schema) ->
            ok = emqx_schema_registry_config:delete_external_registry(Name)
        end,
        emqx_schema_registry_config:list_external_registries()
    ).

receive_action_results() ->
    receive
        {action, #{data := _} = Res} ->
            Res
    after 1_000 ->
        ct:fail("action didn't run")
    end.

receive_published(Line) ->
    receive
        {deliver, _Topic, Msg} ->
            MsgMap = emqx_message:to_map(Msg),
            maps:update_with(
                payload,
                fun(Raw) ->
                    case emqx_utils_json:safe_decode(Raw, [return_maps]) of
                        {ok, Decoded} -> Decoded;
                        {error, _} -> Raw
                    end
                end,
                MsgMap
            )
    after 1_000 ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        ct:fail("publish not received, line ~b", [Line])
    end.

wait_for_cluster_rpc(Node) ->
    %% need to wait until the config handler is ready after
    %% restarting during the cluster join.
    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        true = is_pid(erpc:call(Node, erlang, whereis, [emqx_config_handler]))
    ).

protobuf_unique_cache_hit_spec(#{serde_type := protobuf} = Res, Trace) ->
    #{nodes := Nodes} = Res,
    CacheEvents0 = ?of_kind(
        [
            schema_registry_protobuf_cache_hit,
            schema_registry_protobuf_cache_miss
        ],
        Trace
    ),
    CacheEvents = [
        Event
     || Event <- CacheEvents0,
        maps:get(name, Event, no_name) =/= ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME
    ],
    ?assertMatch(
        [
            schema_registry_protobuf_cache_hit,
            schema_registry_protobuf_cache_miss
        ],
        lists:sort(?projection(?snk_kind, CacheEvents))
    ),
    ?assertEqual(
        lists:usort(Nodes),
        lists:usort([N || #{?snk_meta := #{node := N}} <- CacheEvents])
    ),
    ok;
protobuf_unique_cache_hit_spec(_Res, _Trace) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_unknown_calls(_Config) ->
    Ref = monitor(process, emqx_schema_registry),
    %% for coverage
    emqx_schema_registry ! unknown,
    gen_server:cast(emqx_schema_registry, unknown),
    ?assertEqual({error, unknown_call}, gen_server:call(emqx_schema_registry, unknown)),
    receive
        {'DOWN', Ref, process, _, _} ->
            ct:fail("registry shouldn't have died")
    after 500 ->
        ok
    end.

t_encode_decode(Config) ->
    SerdeType = ?config(serde_type, Config),
    SerdeName = my_serde,
    ok = create_serde(SerdeType, SerdeName),
    #{
        sql := SQL,
        payload := Payload,
        expected_rule_output := ExpectedRuleOutput
    } = test_params_for(SerdeType, encode_decode1),
    {ok, _} = create_rule_http(#{sql => SQL}),
    PayloadBin = emqx_utils_json:encode(Payload),
    emqx:publish(emqx_message:make(<<"t">>, PayloadBin)),
    Res = receive_action_results(),
    ?assertMatch(#{data := ExpectedRuleOutput}, Res),
    ok.

t_delete_serde(Config) ->
    SerdeType = ?config(serde_type, Config),
    SerdeName = my_serde,
    ?check_trace(
        begin
            ok = create_serde(SerdeType, SerdeName),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_schema_registry:delete_schema(SerdeName),
                    #{?snk_kind := schema_registry_serdes_deleted},
                    1_000
                ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(schema_registry_serdes_deleted, Trace)),
            ?assertMatch([#{type := SerdeType}], ?of_kind(serde_destroyed, Trace)),
            ok
        end
    ),
    ok.

t_encode(Config) ->
    SerdeType = ?config(serde_type, Config),
    SerdeName = my_serde,
    ok = create_serde(SerdeType, SerdeName),
    #{
        sql := SQL,
        payload := Payload,
        payload_template := PayloadTemplate,
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, encode1),
    {ok, _} = create_rule_http(#{sql => SQL}, #{payload_template => PayloadTemplate}),
    PayloadBin = emqx_utils_json:encode(Payload),
    emqx:publish(emqx_message:make(<<"t">>, PayloadBin)),
    Published = receive_published(?LINE),
    case SerdeType of
        json ->
            %% should have received binary
            %% but since it's valid json, so it got
            %% 'safe_decode' decoded in receive_published
            ?assertMatch(#{payload := #{<<"i">> := _, <<"s">> := _}}, Published);
        _ ->
            ?assertMatch(#{payload := B} when is_binary(B), Published),
            #{payload := Encoded} = Published,
            {ok, Serde} = emqx_schema_registry:get_serde(SerdeName),
            ?assertEqual(Payload, eval_decode(Serde, [Encoded | ExtraArgs]))
    end,
    ok.

t_decode_fail(_Config) ->
    SerdeName = my_serde,
    SerdeType = protobuf,
    ok = create_serde(SerdeType, SerdeName),
    Payload = <<"ss">>,
    ?assertThrow(
        {schema_decode_error, #{
            data := <<"ss">>,
            error_type := decoding_failure,
            explain := _,
            message_type := 'Person',
            schema_name := <<"my_serde">>
        }},
        emqx_rule_funcs:schema_decode(<<"my_serde">>, Payload, <<"Person">>)
    ),
    ok.

t_decode(Config) ->
    SerdeType = ?config(serde_type, Config),
    SerdeName = my_serde,
    ok = create_serde(SerdeType, SerdeName),
    #{
        sql := SQL,
        payload := Payload,
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, decode1),
    {ok, _} = create_rule_http(#{sql => SQL}),
    {ok, Serde} = emqx_schema_registry:get_serde(SerdeName),
    EncodedBin = eval_encode(Serde, [Payload | ExtraArgs]),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBin)),
    Published = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"decoded">> := _}},
        Published
    ),
    #{payload := #{<<"decoded">> := Decoded}} = Published,
    ?assertEqual(Payload, Decoded),
    ok.

t_protobuf_union_encode(Config) ->
    SerdeType = ?config(serde_type, Config),
    ?assertEqual(protobuf, SerdeType),
    SerdeName = my_serde,
    ok = create_serde(SerdeType, SerdeName),
    #{
        sql := SQL,
        payload := #{a := PayloadA, b := PayloadB},
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, union1),
    {ok, _} = create_rule_http(#{sql => SQL}),
    {ok, Serde} = emqx_schema_registry:get_serde(SerdeName),

    EncodedBinA = eval_encode(Serde, [PayloadA | ExtraArgs]),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinA)),
    PublishedA = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"decoded">> := _}},
        PublishedA
    ),
    #{payload := #{<<"decoded">> := DecodedA}} = PublishedA,
    ?assertEqual(PayloadA, DecodedA),

    EncodedBinB = eval_encode(Serde, [PayloadB | ExtraArgs]),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinB)),
    PublishedB = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"decoded">> := _}},
        PublishedB
    ),
    #{payload := #{<<"decoded">> := DecodedB}} = PublishedB,
    ?assertEqual(PayloadB, DecodedB),

    ok.

t_protobuf_union_decode(Config) ->
    SerdeType = ?config(serde_type, Config),
    ?assertEqual(protobuf, SerdeType),
    SerdeName = my_serde,
    ok = create_serde(SerdeType, SerdeName),
    #{
        sql := SQL,
        payload := #{a := PayloadA, b := PayloadB},
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, union2),
    {ok, _} = create_rule_http(#{sql => SQL}),
    {ok, Serde} = emqx_schema_registry:get_serde(SerdeName),

    EncodedBinA = emqx_utils_json:encode(PayloadA),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinA)),
    PublishedA = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"encoded">> := _}},
        PublishedA
    ),
    #{payload := #{<<"encoded">> := EncodedA}} = PublishedA,
    ?assertEqual(PayloadA, eval_decode(Serde, [EncodedA | ExtraArgs])),

    EncodedBinB = emqx_utils_json:encode(PayloadB),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinB)),
    PublishedB = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"encoded">> := _}},
        PublishedB
    ),
    #{payload := #{<<"encoded">> := EncodedB}} = PublishedB,
    ?assertEqual(PayloadB, eval_decode(Serde, [EncodedB | ExtraArgs])),

    ok.

t_fail_rollback(Config) ->
    SerdeType = ?config(serde_type, Config),
    OkSchema = emqx_utils_maps:binary_key_map(schema_params(SerdeType)),
    BrokenSchema =
        case SerdeType of
            json ->
                OkSchema#{<<"source">> := <<"not a json value">>};
            _ ->
                OkSchema#{<<"source">> := <<"{}">>}
        end,
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [?CONF_KEY_ROOT, schemas, <<"a">>],
            OkSchema,
            #{}
        )
    ),
    ?assertMatch(
        {error, _},
        emqx_conf:update(
            [?CONF_KEY_ROOT, schemas, <<"z">>],
            BrokenSchema,
            #{}
        )
    ),
    ?assertMatch({ok, #serde{name = <<"a">>}}, emqx_schema_registry:get_serde(<<"a">>)),
    %% no z serdes should be in the table
    ?assertEqual({error, not_found}, emqx_schema_registry:get_serde(<<"z">>)),
    ok.

t_cluster_serde_build(Config) ->
    SerdeType = ?config(serde_type, Config),
    AppSpecs = [
        emqx_conf,
        emqx_rule_engine,
        emqx_schema_registry
    ],
    ClusterSpec = [
        {cluster_serde_build1, #{apps => AppSpecs}},
        {cluster_serde_build2, #{apps => AppSpecs}}
    ],
    SerdeName = my_serde,
    Schema = schema_params(SerdeType),
    #{
        payload := Payload,
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, encode_decode1),
    ?check_trace(
        begin
            Nodes =
                [N1, N2 | _] = emqx_cth_cluster:start(
                    ClusterSpec,
                    #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
                ),
            on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
            NumNodes = length(Nodes),
            ?assertMatch(
                ok,
                erpc:call(N2, emqx_schema_registry, add_schema, [SerdeName, Schema])
            ),
            %% check that we can serialize/deserialize in all nodes
            lists:foreach(
                fun(N) ->
                    ok = erpc:call(N, fun() ->
                        Res0 = emqx_schema_registry:get_serde(SerdeName),
                        ?assertMatch({ok, #serde{}}, Res0, #{node => N}),
                        {ok, Serde} = Res0,
                        ?assertEqual(
                            Payload,
                            encode_then_decode(Serde, Payload, ExtraArgs),
                            #{node => N}
                        ),
                        ok
                    end)
                end,
                Nodes
            ),
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := schema_registry_serdes_deleted}),
                NumNodes,
                10_000
            ),
            ?assertEqual(
                ok,
                erpc:call(N1, emqx_schema_registry, delete_schema, [SerdeName])
            ),
            {ok, DeleteEvents} = snabbkaffe:receive_events(SRef1),
            %% expect all nodes to delete (local) serdes
            ?assertEqual(NumNodes, length(DeleteEvents)),
            lists:foreach(
                fun(N) ->
                    erpc:call(N, fun() ->
                        ?assertMatch(
                            {error, not_found},
                            emqx_schema_registry:get_serde(SerdeName),
                            #{node => N}
                        ),
                        ok
                    end)
                end,
                Nodes
            ),
            #{serde_type => SerdeType, nodes => Nodes}
        end,
        [
            {"protobuf is only built on one node", fun ?MODULE:protobuf_unique_cache_hit_spec/2}
        ]
    ),
    ok.

%% Verifies that importing in both `merge' and `replace' modes work with external
%% registries, when importing configurations from the CLI interface.
t_external_registry_load_config(_Config) ->
    %% Existing config
    Name0 = <<"preexisting">>,
    Config0 = emqx_schema_registry_http_api_SUITE:confluent_schema_registry_with_basic_auth(),
    {201, _} = emqx_schema_registry_http_api_SUITE:create_external_registry(Config0#{
        <<"name">> => Name0
    }),
    [_] = emqx_schema_registry_http_api_SUITE:find_external_registry_worker(Name0),

    %% Config to load
    %% Will update existing config
    Name1 = Name0,
    URL1 = <<"http://new_url:8081">>,
    Config1 = emqx_schema_registry_http_api_SUITE:confluent_schema_registry_with_basic_auth(#{
        <<"url">> => URL1
    }),
    %% New config
    Name2 = <<"new">>,
    URL2 = <<"http://yet_another_url:8081">>,
    Config2 = emqx_schema_registry_http_api_SUITE:confluent_schema_registry_with_basic_auth(#{
        <<"url">> => URL2
    }),
    ConfigToLoad1 = #{
        <<"schema_registry">> => #{
            <<"external">> => #{
                Name1 => Config1,
                Name2 => Config2
            }
        }
    },
    ConfigToLoad1Bin = iolist_to_binary(hocon_pp:do(ConfigToLoad1, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoad1Bin, #{mode => merge})),

    Path = [schema_registry, external],
    PathBin = [emqx_utils_conv:bin(PS) || PS <- Path],
    Name1Atom = binary_to_atom(Name1),
    Name2Atom = binary_to_atom(Name2),
    ?assertMatch(
        #{
            Name1 := #{<<"url">> := URL1},
            Name2 := #{<<"url">> := URL2}
        },
        emqx_config:get_raw(PathBin)
    ),
    ?assertMatch(
        #{
            Name1Atom := #{url := URL1},
            Name2Atom := #{url := URL2}
        },
        emqx_config:get(Path)
    ),
    %% Correctly starts new processes
    ?assertMatch([_], emqx_schema_registry_http_api_SUITE:find_external_registry_worker(Name1)),
    ?assertMatch([_], emqx_schema_registry_http_api_SUITE:find_external_registry_worker(Name2)),

    %% New config; will replace everything
    Name3 = Name0,
    URL3 = <<"http://final_url:8081">>,
    Config3 = emqx_schema_registry_http_api_SUITE:confluent_schema_registry_with_basic_auth(#{
        <<"url">> => URL3
    }),
    ConfigToLoad2 = #{
        <<"schema_registry">> => #{
            <<"external">> => #{
                Name3 => Config3
            }
        }
    },
    ConfigToLoad2Bin = iolist_to_binary(hocon_pp:do(ConfigToLoad2, #{})),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoad2Bin, #{mode => replace})),

    Name3Atom = binary_to_atom(Name3),
    ?assertMatch(
        #{Name3 := #{<<"url">> := URL3}},
        emqx_config:get_raw(PathBin)
    ),
    ?assertMatch(
        #{Name3Atom := #{url := URL3}},
        emqx_config:get(Path)
    ),
    %% Correctly stops old processes
    ?assertMatch([_], emqx_schema_registry_http_api_SUITE:find_external_registry_worker(Name3)),
    ?assertMatch([], emqx_schema_registry_http_api_SUITE:find_external_registry_worker(Name2)),

    ok.

t_import_config(_Config) ->
    RawConf = #{
        <<"schema_registry">> =>
            #{
                <<"schemas">> =>
                    #{
                        <<"my_avro_schema">> =>
                            #{
                                <<"description">> => <<"My Avro Schema">>,
                                <<"source">> =>
                                    emqx_utils_json:encode(
                                        #{
                                            type => <<"record">>,
                                            name => <<"ct">>,
                                            namespace => <<"emqx.com">>,
                                            fields => [
                                                #{type => <<"int">>, name => <<"i">>},
                                                #{type => <<"string">>, name => <<"s">>}
                                            ]
                                        }
                                    ),
                                <<"type">> => <<"avro">>
                            }
                    }
            }
    },
    RawConf1 = emqx_utils_maps:deep_put(
        [<<"schema_registry">>, <<"schemas">>, <<"my_avro_schema">>, <<"description">>],
        RawConf,
        <<"Updated description">>
    ),
    Path = [schema_registry, schemas, my_avro_schema],
    ?assertEqual(
        {ok, #{root_key => schema_registry, changed => [Path]}},
        emqx_schema_registry_config:import_config(RawConf)
    ),
    ?assertEqual(
        {ok, #{root_key => schema_registry, changed => [Path]}},
        emqx_schema_registry_config:import_config(RawConf1)
    ).

sparkplug_example_data_base64() ->
    <<
        "CPHh67HrMBIqChxjb3VudGVyX2dyb3VwMS9jb3VudGVyMV8xc2VjGPXh67HrMCACUKgD"
        "EikKHGNvdW50ZXJfZ3JvdXAxL2NvdW50ZXIxXzVzZWMY9eHrseswIAJQVBIqCh1jb3Vu"
        "dGVyX2dyb3VwMS9jb3VudGVyMV8xMHNlYxj14eux6zAgAlAqEigKG2NvdW50ZXJfZ3Jv"
        "dXAxL2NvdW50ZXIxX3J1bhj14eux6zAgBVABEioKHWNvdW50ZXJfZ3JvdXAxL2NvdW50"
        "ZXIxX3Jlc2V0GPXh67HrMCAFUAAYWA=="
    >>.

sparkplug_example_data() ->
    #{
        <<"metrics">> =>
            [
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"name">> => <<"counter_group1/counter1_1sec">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 84,
                    <<"name">> => <<"counter_group1/counter1_5sec">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 42,
                    <<"name">> => <<"counter_group1/counter1_10sec">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 5,
                    <<"int_value">> => 1,
                    <<"name">> => <<"counter_group1/counter1_run">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 5,
                    <<"int_value">> => 0,
                    <<"name">> => <<"counter_group1/counter1_reset">>,
                    <<"timestamp">> => 1678094561525
                }
            ],
        <<"seq">> => 88,
        <<"timestamp">> => 1678094561521
    }.

wait_for_sparkplug_schema_registered() ->
    ?retry(
        100,
        100,
        [_] = ets:lookup(?SERDE_TAB, ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME)
    ).

t_sparkplug_decode(_Config) ->
    SQL =
        <<
            "select\n"
            "  sparkplug_decode(payload) as decoded\n"
            "from t\n"
        >>,
    PayloadBase64 = sparkplug_example_data_base64(),
    {ok, _} = create_rule_http(#{sql => SQL}),
    PayloadBin = base64:decode(PayloadBase64),
    ExpectedRuleOutput =
        #{<<"decoded">> => sparkplug_example_data()},
    wait_for_sparkplug_schema_registered(),
    emqx:publish(emqx_message:make(<<"t">>, PayloadBin)),
    Res = receive_action_results(),
    ?assertMatch(#{data := ExpectedRuleOutput}, Res),
    ok.

t_sparkplug_encode(_Config) ->
    %% Default message name field is 'Payload'
    SQL =
        <<
            "select\n"
            "  sparkplug_encode(json_decode(payload)) as encoded\n"
            "from t\n"
        >>,
    PayloadJSONBin = emqx_utils_json:encode(sparkplug_example_data()),
    {ok, _} = create_rule_http(#{sql => SQL}),
    ExpectedRuleOutput =
        #{<<"encoded">> => base64:decode(sparkplug_example_data_base64())},
    wait_for_sparkplug_schema_registered(),
    emqx:publish(emqx_message:make(<<"t">>, PayloadJSONBin)),
    Res = receive_action_results(),
    ?assertMatch(#{data := ExpectedRuleOutput}, Res),
    ok.

t_sparkplug_encode_float_to_uint64_key(_Config) ->
    %% Test that the following bug is fixed:
    %% https://emqx.atlassian.net/browse/EMQX-10775
    %% When one assign a float value to a uint64 key, one should get a
    %% gpb_type_error and not a badarith error
    wait_for_sparkplug_schema_registered(),
    ?assertException(
        error,
        {gpb_type_error, _},
        emqx_rule_funcs:sparkplug_encode(#{<<"seq">> => 1.5})
    ).

t_sparkplug_decode_encode_with_message_name(_Config) ->
    SQL =
        <<
            "select\n"
            "  sparkplug_encode(sparkplug_decode(payload, 'Payload'), 'Payload') as encoded\n"
            "from t\n"
        >>,
    PayloadBase64 = sparkplug_example_data_base64(),
    PayloadBin = base64:decode(PayloadBase64),
    {ok, _} = create_rule_http(#{sql => SQL}),
    ExpectedRuleOutput =
        #{<<"encoded">> => PayloadBin},
    wait_for_sparkplug_schema_registered(),
    emqx:publish(emqx_message:make(<<"t">>, PayloadBin)),
    Res = receive_action_results(),
    ?assertMatch(#{data := ExpectedRuleOutput}, Res),
    ok.

eval_encode(Serde, Args) ->
    emqx_schema_registry_serde:eval_encode(Serde, Args).

eval_decode(Serde, Args) ->
    emqx_schema_registry_serde:eval_decode(Serde, Args).

encode_then_decode(Serde, Payload, ExtraArgs) ->
    Encoded = eval_encode(Serde, [Payload | ExtraArgs]),
    eval_decode(Serde, [Encoded | ExtraArgs]).
