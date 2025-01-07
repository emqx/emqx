%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_serde_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(INVALID_JSON(REASON), #{
    kind := validation_error,
    reason := REASON,
    matched_type := "schema_registry:" ++ _
}).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_schema_registry,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:start_trace(),
    clear_schemas(),
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

clear_schemas() ->
    maps:foreach(
        fun(Name, _Schema) ->
            NameBin = emqx_utils_conv:bin(Name),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_schema_registry:delete_schema(Name),
                    #{
                        ?snk_kind := "schema_registry_serde_deleted",
                        name := NameBin
                    }
                )
        end,
        emqx_schema_registry:list_schemas()
    ).

schema_params(avro) ->
    Source = #{
        type => record,
        name => <<"n1">>,
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
            "\n"
            "           message Person {\n"
            "                required string name = 1;\n"
            "                required int32 id = 2;\n"
            "                optional string email = 3;\n"
            "             }\n"
            "           message UnionValue {\n"
            "               oneof u {\n"
            "                   int32  a = 1;\n"
            "                   string b = 2;\n"
            "               }\n"
            "           }\n"
            "          "
        >>,
    #{type => protobuf, source => SourceBin};
schema_params(json) ->
    Source =
        #{
            <<"$schema">> => <<"http://json-schema.org/draft-06/schema#">>,
            <<"$id">> => <<"http://json-schema.org/draft-06/schema#">>,
            type => object,
            properties => #{
                foo => #{type => integer},
                bar => #{type => integer}
            },
            required => [<<"foo">>]
        },
    SourceBin = emqx_utils_json:encode(Source),
    #{type => json, source => SourceBin}.

assert_roundtrip(SerdeName, Original) ->
    Encoded = emqx_schema_registry_serde:encode(SerdeName, Original),
    Decoded = emqx_schema_registry_serde:decode(SerdeName, Encoded),
    ?assertEqual(Original, Decoded, #{original => Original}).

assert_roundtrip(SerdeName, Original, ArgsSerialize, ArgsDeserialize) ->
    Encoded = emqx_schema_registry_serde:encode(SerdeName, Original, ArgsSerialize),
    Decoded = emqx_schema_registry_serde:decode(SerdeName, Encoded, ArgsDeserialize),
    ?assertEqual(Original, Decoded, #{original => Original}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_roundtrip_avro(_Config) ->
    SerdeName = my_serde,
    Params = schema_params(avro),
    ok = emqx_schema_registry:add_schema(SerdeName, Params),
    Original = #{<<"i">> => 10, <<"s">> => <<"hi">>},
    %% for coverage
    assert_roundtrip(SerdeName, Original, _ArgsSerialize = [], _ArgsDeserialize = []),
    assert_roundtrip(SerdeName, Original),
    ok.

t_avro_invalid_json_schema(_Config) ->
    SerdeName = my_serde,
    Params = schema_params(avro),
    WrongParams = Params#{source := <<"{">>},
    ?assertMatch(
        {error, ?INVALID_JSON("Truncated JSON value")},
        emqx_schema_registry:add_schema(SerdeName, WrongParams)
    ),
    ok.

t_avro_invalid_schema(_Config) ->
    SerdeName = my_serde,
    Params = schema_params(avro),
    WrongParams = Params#{source := <<"{}">>},
    ?assertMatch(
        {error, {post_config_update, _, {not_found, <<"type">>}}},
        emqx_schema_registry:add_schema(SerdeName, WrongParams)
    ),
    ok.

t_serde_not_found(_Config) ->
    %% for coverage
    NonexistentSerde = <<"nonexistent">>,
    EncodeData = #{},
    DecodeData = <<"data">>,
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:encode(NonexistentSerde, EncodeData)
    ),
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:decode(NonexistentSerde, DecodeData)
    ),
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:handle_rule_function(schema_check, [
            NonexistentSerde, EncodeData
        ])
    ),
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:handle_rule_function(schema_check, [
            NonexistentSerde, DecodeData
        ])
    ),
    ok.

t_roundtrip_protobuf(_Config) ->
    SerdeName = my_serde,
    Params = schema_params(protobuf),
    ok = emqx_schema_registry:add_schema(SerdeName, Params),
    ExtraArgsPerson = [<<"Person">>],

    Original0 = #{<<"name">> => <<"some name">>, <<"id">> => 10, <<"email">> => <<"emqx@emqx.io">>},
    assert_roundtrip(SerdeName, Original0, ExtraArgsPerson, ExtraArgsPerson),

    %% removing optional field
    Original1 = #{<<"name">> => <<"some name">>, <<"id">> => 10},
    assert_roundtrip(SerdeName, Original1, ExtraArgsPerson, ExtraArgsPerson),

    %% `oneof' fields
    ExtraArgsUnion = [<<"UnionValue">>],
    Original2 = #{<<"a">> => 1},
    assert_roundtrip(SerdeName, Original2, ExtraArgsUnion, ExtraArgsUnion),

    Original3 = #{<<"b">> => <<"string">>},
    assert_roundtrip(SerdeName, Original3, ExtraArgsUnion, ExtraArgsUnion),

    ok.

t_protobuf_invalid_schema(_Config) ->
    SerdeName = my_serde,
    Params = schema_params(protobuf),
    WrongParams = Params#{source := <<"xxxx">>},
    ?assertMatch(
        {error, {post_config_update, _, {invalid_protobuf_schema, _}}},
        emqx_schema_registry:add_schema(SerdeName, WrongParams)
    ),
    ok.

%% Checks that we unload code and clear code generation cache after destroying a protobuf
%% serde.
t_destroy_protobuf(_Config) ->
    SerdeName = ?FUNCTION_NAME,
    SerdeNameBin = atom_to_binary(SerdeName),
    ?check_trace(
        #{timetrap => 5_000},
        begin
            Params = schema_params(protobuf),
            ok = emqx_schema_registry:add_schema(SerdeName, Params),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_schema_registry:delete_schema(SerdeName),
                    #{?snk_kind := serde_destroyed, name := SerdeNameBin}
                ),
            %% Create again to check we don't hit the cache.
            ok = emqx_schema_registry:add_schema(SerdeName, Params),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_schema_registry:delete_schema(SerdeName),
                    #{?snk_kind := serde_destroyed, name := SerdeNameBin}
                ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind(schema_registry_protobuf_cache_hit, Trace)),
            ?assertMatch([_ | _], ?of_kind("schema_registry_protobuf_cache_destroyed", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we don't leave entries lingering in the protobuf code cache table when
%% updating the source of a serde.
t_update_protobuf_cache(_Config) ->
    SerdeName = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 5_000},
        begin
            #{source := Source0} = Params0 = schema_params(protobuf),
            ok = emqx_schema_registry:add_schema(SerdeName, Params0),
            %% Now we touch the source so protobuf needs to be recompiled.
            Source1 = <<Source0/binary, "\n\n">>,
            Params1 = Params0#{source := Source1},
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_schema_registry:add_schema(SerdeName, Params1),
                    #{?snk_kind := "schema_registry_protobuf_cache_destroyed"}
                ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind(schema_registry_protobuf_cache_hit, Trace)),
            ?assertMatch([_, _ | _], ?of_kind(schema_registry_protobuf_cache_miss, Trace)),
            ?assertMatch([_ | _], ?of_kind("schema_registry_protobuf_cache_destroyed", Trace)),
            ok
        end
    ),
    ok.

t_json_invalid_schema(_Config) ->
    SerdeName = invalid_json,
    Params = schema_params(json),
    BadParams1 = Params#{source := <<"not valid json value">>},
    BadParams2 = Params#{source := <<"\"not an object\"">>},
    BadParams3 = Params#{source := <<"{\"foo\": 1}">>},
    ?assertMatch(
        {error, ?INVALID_JSON("Invalid JSON literal")},
        emqx_schema_registry:add_schema(SerdeName, BadParams1)
    ),
    ?assertMatch(
        {error, {post_config_update, _, {invalid_json_schema, bad_schema_object}}},
        emqx_schema_registry:add_schema(SerdeName, BadParams2)
    ),
    ?assertMatch(
        ok,
        emqx_schema_registry:add_schema(SerdeName, BadParams3)
    ),
    ok.

t_roundtrip_json(_Config) ->
    SerdeName = my_json_schema,
    Params = schema_params(json),
    ok = emqx_schema_registry:add_schema(SerdeName, Params),
    Original = #{<<"foo">> => 1, <<"bar">> => 2},
    assert_roundtrip(SerdeName, Original),
    ok.

t_json_validation(_Config) ->
    SerdeName = my_json_schema,
    Params = schema_params(json),
    ok = emqx_schema_registry:add_schema(SerdeName, Params),
    F = fun(Fn, Data) ->
        emqx_schema_registry_serde:handle_rule_function(Fn, [SerdeName, Data])
    end,
    OK = #{<<"foo">> => 1, <<"bar">> => 2},
    NotOk = #{<<"bar">> => 2},
    ?assert(F(schema_check, OK)),
    ?assert(F(schema_check, <<"{\"foo\": 1, \"bar\": 2}">>)),
    ?assertNot(F(schema_check, NotOk)),
    ?assertNot(F(schema_check, <<"{\"bar\": 2}">>)),
    ?assertNot(F(schema_check, <<"{\"foo\": \"notinteger\", \"bar\": 2}">>)),
    ok.

t_is_existing_type(_Config) ->
    JsonName = <<"myjson">>,
    ?assertNot(emqx_schema_registry:is_existing_type(JsonName)),
    ok = emqx_schema_registry:add_schema(JsonName, schema_params(json)),
    AvroName = <<"myavro">>,
    ?assertNot(emqx_schema_registry:is_existing_type(AvroName)),
    ok = emqx_schema_registry:add_schema(AvroName, schema_params(avro)),
    ProtobufName = <<"myprotobuf">>,
    MessageType = <<"Person">>,
    ?assertNot(emqx_schema_registry:is_existing_type(ProtobufName)),
    ok = emqx_schema_registry:add_schema(ProtobufName, schema_params(protobuf)),
    %% JSON Schema: no inner names
    ?assert(emqx_schema_registry:is_existing_type(JsonName)),
    ?assertNot(emqx_schema_registry:is_existing_type(JsonName, [JsonName])),
    %% Avro: no inner names
    ?assert(emqx_schema_registry:is_existing_type(AvroName)),
    ?assertNot(emqx_schema_registry:is_existing_type(AvroName, [AvroName])),
    %% Protobuf: one level of message types
    ?assert(emqx_schema_registry:is_existing_type(ProtobufName)),
    ?assertNot(emqx_schema_registry:is_existing_type(ProtobufName, [ProtobufName])),
    ?assert(emqx_schema_registry:is_existing_type(ProtobufName, [MessageType])),
    ?assertNot(emqx_schema_registry:is_existing_type(ProtobufName, [MessageType, MessageType])),
    ok.
