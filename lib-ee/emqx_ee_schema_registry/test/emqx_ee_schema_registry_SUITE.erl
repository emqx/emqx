%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_schema_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ee_schema_registry.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(APPS, [emqx_conf, emqx_rule_engine, emqx_ee_schema_registry]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [{group, avro}, {group, protobuf}].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    ProtobufOnlyTCs = protobuf_only_tcs(),
    TCs = AllTCs -- ProtobufOnlyTCs,
    [{avro, TCs}, {protobuf, AllTCs}].

protobuf_only_tcs() ->
    [
        t_protobuf_union_encode,
        t_protobuf_union_decode
    ].

init_per_suite(Config) ->
    emqx_config:save_schema_mod_and_names(emqx_ee_schema_registry_schema),
    emqx_mgmt_api_test_util:init_suite(?APPS),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(lists:reverse(?APPS)),
    ok.

init_per_group(avro, Config) ->
    [{serde_type, avro} | Config];
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
    RepublishTopic = <<"republish/schema_registry">>,
    emqx:subscribe(RepublishTopic),
    DefaultParams = #{
        enable => true,
        actions => [
            make_trace_fn_action(),
            #{
                <<"function">> => <<"republish">>,
                <<"args">> =>
                    #{
                        <<"topic">> => RepublishTopic,
                        <<"payload">> => <<>>,
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
        fields => [
            #{name => <<"i">>, type => <<"int">>},
            #{name => <<"s">>, type => <<"string">>}
        ]
    },
    SourceBin = emqx_utils_json:encode(Source),
    #{type => avro, source => SourceBin};
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
    ok = emqx_ee_schema_registry:add_schema(SerdeName, Schema),
    ok.

test_params_for(avro, encode_decode1) ->
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
test_params_for(avro, encode1) ->
    SQL =
        <<
            "select\n"
            "  schema_encode('my_serde', json_decode(payload)) as encoded\n"
            "from t\n"
        >>,
    Payload = #{<<"i">> => 10, <<"s">> => <<"text">>},
    ExtraArgs = [],
    #{
        sql => SQL,
        payload => Payload,
        extra_args => ExtraArgs
    };
test_params_for(avro, decode1) ->
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
    ExtraArgs = [<<"Person">>],
    #{
        sql => SQL,
        payload => Payload,
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
            ok = emqx_ee_schema_registry:delete_schema(Name)
        end,
        emqx_ee_schema_registry:list_schemas()
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

cluster(Config) ->
    PrivDataDir = ?config(priv_dir, Config),
    PeerModule =
        case os:getenv("IS_CI") of
            false ->
                slave;
            _ ->
                ct_slave
        end,
    Cluster = emqx_common_test_helpers:emqx_cluster(
        [core, core],
        [
            {apps, ?APPS},
            {listener_ports, []},
            {peer_mod, PeerModule},
            {priv_data_dir, PrivDataDir},
            {load_schema, true},
            {start_autocluster, true},
            {schema_mod, emqx_ee_conf_schema},
            %% need to restart schema registry app in the tests so
            %% that it re-registers the config handler that is lost
            %% when emqx_conf restarts during join.
            {env, [{emqx_machine, applications, [emqx_ee_schema_registry]}]},
            {load_apps, [emqx_machine | ?APPS]},
            {env_handler, fun
                (emqx) ->
                    application:set_env(emqx, boot_modules, [broker, router]),
                    ok;
                (emqx_conf) ->
                    ok;
                (_) ->
                    ok
            end}
        ]
    ),
    ct:pal("cluster:\n  ~p", [Cluster]),
    Cluster.

start_cluster(Cluster) ->
    Nodes = [
        emqx_common_test_helpers:start_slave(Name, Opts)
     || {Name, Opts} <- Cluster
    ],
    on_exit(fun() ->
        emqx_utils:pmap(
            fun(N) ->
                ct:pal("stopping ~p", [N]),
                ok = emqx_common_test_helpers:stop_slave(N)
            end,
            Nodes
        )
    end),
    erpc:multicall(Nodes, mria_rlog, wait_for_shards, [[?SCHEMA_REGISTRY_SHARD], 30_000]),
    Nodes.

wait_for_cluster_rpc(Node) ->
    %% need to wait until the config handler is ready after
    %% restarting during the cluster join.
    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        true = is_pid(erpc:call(Node, erlang, whereis, [emqx_config_handler]))
    ).

serde_deletion_calls_destructor_spec(#{serde_type := SerdeType}, Trace) ->
    ?assert(
        ?strict_causality(
            #{?snk_kind := will_delete_schema},
            #{?snk_kind := serde_destroyed, type := SerdeType},
            Trace
        )
    ),
    ok.

protobuf_unique_cache_hit_spec(#{serde_type := protobuf} = Res, Trace) ->
    #{nodes := Nodes} = Res,
    CacheEvents = ?of_kind(
        [
            schema_registry_protobuf_cache_hit,
            schema_registry_protobuf_cache_miss
        ],
        Trace
    ),
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
    Ref = monitor(process, emqx_ee_schema_registry),
    %% for coverage
    emqx_ee_schema_registry ! unknown,
    gen_server:cast(emqx_ee_schema_registry, unknown),
    ?assertEqual({error, unknown_call}, gen_server:call(emqx_ee_schema_registry, unknown)),
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
                    emqx_ee_schema_registry:delete_schema(SerdeName),
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
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, encode1),
    {ok, _} = create_rule_http(#{sql => SQL}),
    PayloadBin = emqx_utils_json:encode(Payload),
    emqx:publish(emqx_message:make(<<"t">>, PayloadBin)),
    Published = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"encoded">> := _}},
        Published
    ),
    #{payload := #{<<"encoded">> := Encoded}} = Published,
    {ok, #{deserializer := Deserializer}} = emqx_ee_schema_registry:get_serde(SerdeName),
    ?assertEqual(Payload, apply(Deserializer, [Encoded | ExtraArgs])),
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
    {ok, #{serializer := Serializer}} = emqx_ee_schema_registry:get_serde(SerdeName),
    EncodedBin = apply(Serializer, [Payload | ExtraArgs]),
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
    {ok, #{serializer := Serializer}} = emqx_ee_schema_registry:get_serde(SerdeName),

    EncodedBinA = apply(Serializer, [PayloadA | ExtraArgs]),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinA)),
    PublishedA = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"decoded">> := _}},
        PublishedA
    ),
    #{payload := #{<<"decoded">> := DecodedA}} = PublishedA,
    ?assertEqual(PayloadA, DecodedA),

    EncodedBinB = apply(Serializer, [PayloadB | ExtraArgs]),
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
    {ok, #{deserializer := Deserializer}} = emqx_ee_schema_registry:get_serde(SerdeName),

    EncodedBinA = emqx_utils_json:encode(PayloadA),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinA)),
    PublishedA = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"encoded">> := _}},
        PublishedA
    ),
    #{payload := #{<<"encoded">> := EncodedA}} = PublishedA,
    ?assertEqual(PayloadA, apply(Deserializer, [EncodedA | ExtraArgs])),

    EncodedBinB = emqx_utils_json:encode(PayloadB),
    emqx:publish(emqx_message:make(<<"t">>, EncodedBinB)),
    PublishedB = receive_published(?LINE),
    ?assertMatch(
        #{payload := #{<<"encoded">> := _}},
        PublishedB
    ),
    #{payload := #{<<"encoded">> := EncodedB}} = PublishedB,
    ?assertEqual(PayloadB, apply(Deserializer, [EncodedB | ExtraArgs])),

    ok.

t_fail_rollback(Config) ->
    SerdeType = ?config(serde_type, Config),
    OkSchema = emqx_utils_maps:binary_key_map(schema_params(SerdeType)),
    BrokenSchema = OkSchema#{<<"source">> := <<"{}">>},
    %% hopefully, for this small map, the key order is used.
    Serdes = #{
        <<"a">> => OkSchema,
        <<"z">> => BrokenSchema
    },
    ?assertMatch(
        {error, _},
        emqx_conf:update(
            [?CONF_KEY_ROOT, schemas],
            Serdes,
            #{}
        )
    ),
    %% no serdes should be in the table
    ?assertEqual({error, not_found}, emqx_ee_schema_registry:get_serde(<<"a">>)),
    ?assertEqual({error, not_found}, emqx_ee_schema_registry:get_serde(<<"z">>)),
    ok.

t_cluster_serde_build(Config) ->
    SerdeType = ?config(serde_type, Config),
    Cluster = cluster(Config),
    SerdeName = my_serde,
    Schema = schema_params(SerdeType),
    #{
        payload := Payload,
        extra_args := ExtraArgs
    } = test_params_for(SerdeType, encode_decode1),
    ?check_trace(
        begin
            Nodes = [N1, N2 | _] = start_cluster(Cluster),
            NumNodes = length(Nodes),
            wait_for_cluster_rpc(N2),
            ?assertEqual(
                ok,
                erpc:call(N2, emqx_ee_schema_registry, add_schema, [SerdeName, Schema])
            ),
            %% check that we can serialize/deserialize in all nodes
            lists:foreach(
                fun(N) ->
                    erpc:call(N, fun() ->
                        Res0 = emqx_ee_schema_registry:get_serde(SerdeName),
                        ?assertMatch({ok, #{}}, Res0, #{node => N}),
                        {ok, #{serializer := Serializer, deserializer := Deserializer}} = Res0,
                        ?assertEqual(
                            Payload,
                            apply(
                                Deserializer,
                                [apply(Serializer, [Payload | ExtraArgs]) | ExtraArgs]
                            ),
                            #{node => N}
                        ),
                        ok
                    end)
                end,
                Nodes
            ),
            %% now we delete and check it's removed from the table
            ?tp(will_delete_schema, #{}),
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := schema_registry_serdes_deleted}),
                NumNodes,
                5_000
            ),
            ?assertEqual(
                ok,
                erpc:call(N1, emqx_ee_schema_registry, delete_schema, [SerdeName])
            ),
            {ok, _} = snabbkaffe:receive_events(SRef1),
            lists:foreach(
                fun(N) ->
                    erpc:call(N, fun() ->
                        ?assertMatch(
                            {error, not_found},
                            emqx_ee_schema_registry:get_serde(SerdeName),
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
            {"destructor is always called", fun ?MODULE:serde_deletion_calls_destructor_spec/2},
            {"protobuf is only built on one node", fun ?MODULE:protobuf_unique_cache_hit_spec/2}
        ]
    ),
    ok.
