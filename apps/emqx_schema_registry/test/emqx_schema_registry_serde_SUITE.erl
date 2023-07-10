%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_serde_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_schema_registry.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(APPS, [emqx_conf, emqx_rule_engine, emqx_schema_registry]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_config:save_schema_mod_and_names(emqx_schema_registry_schema),
    emqx_mgmt_api_test_util:init_suite(?APPS),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(lists:reverse(?APPS)),
    ok.
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    clear_schemas(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

clear_schemas() ->
    maps:foreach(
        fun(Name, _Schema) ->
            ok = emqx_schema_registry:delete_schema(Name)
        end,
        emqx_schema_registry:list_schemas()
    ).

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
    #{type => protobuf, source => SourceBin}.

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
        {error, #{reason := #{expected := _}}},
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
    Original = #{},
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:encode(NonexistentSerde, Original)
    ),
    ?assertError(
        {serde_not_found, NonexistentSerde},
        emqx_schema_registry_serde:decode(NonexistentSerde, Original)
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
