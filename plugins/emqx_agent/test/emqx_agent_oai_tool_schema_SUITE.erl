%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_oai_tool_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_valid_empty_root(_Config) ->
    ?assertEqual(ok, emqx_agent_oai_tool_schema:validate_schema(empty_object())).

t_valid_nested_shapes(_Config) ->
    Schema = #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"name">> => #{<<"type">> => <<"string">>},
            <<"age">> => #{<<"type">> => [<<"integer">>, <<"null">>]},
            <<"tags">> => #{
                <<"type">> => <<"array">>,
                <<"items">> => #{<<"type">> => <<"string">>}
            },
            <<"profile">> => #{
                <<"type">> => [<<"object">>, <<"null">>],
                <<"properties">> => #{<<"email">> => #{<<"type">> => <<"string">>}},
                <<"required">> => [<<"email">>],
                <<"additionalProperties">> => false
            }
        },
        <<"required">> => [<<"name">>, <<"age">>, <<"tags">>, <<"profile">>],
        <<"additionalProperties">> => false
    },
    ?assertEqual(ok, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_valid_nested_anyof(_Config) ->
    Schema = #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"item">> => #{
                <<"anyOf">> => [
                    #{<<"type">> => <<"string">>},
                    #{<<"type">> => <<"integer">>}
                ]
            }
        },
        <<"required">> => [<<"item">>],
        <<"additionalProperties">> => false
    },
    ?assertEqual(ok, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_root_anyof(_Config) ->
    assert_error(
        root_any_of_not_allowed,
        emqx_agent_oai_tool_schema:validate_schema(#{
            <<"anyOf">> => [empty_object()]
        })
    ).

t_reject_unsupported_keywords(_Config) ->
    lists:foreach(
        fun(Keyword) ->
            Schema = (empty_object())#{Keyword => true},
            assert_error(unsupported_keyword, emqx_agent_oai_tool_schema:validate_schema(Schema))
        end,
        [<<"oneOf">>, <<"allOf">>, <<"$ref">>, <<"const">>, <<"nullable">>, <<"minimum">>]
    ).

t_reject_missing_additional_properties(_Config) ->
    Schema = maps:remove(<<"additionalProperties">>, one_field_schema()),
    assert_error(missing_additional_properties, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_additional_properties_true(_Config) ->
    Schema = (one_field_schema())#{<<"additionalProperties">> => true},
    assert_error(
        additional_properties_not_false, emqx_agent_oai_tool_schema:validate_schema(Schema)
    ).

t_reject_required_mismatch(_Config) ->
    Schema = (one_field_schema())#{<<"required">> => []},
    assert_error(required_mismatch, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_required_item_not_string(_Config) ->
    Schema = (one_field_schema())#{<<"required">> => [<<"field">>, 123]},
    assert_error(required_item_not_string, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_invalid_nullable_type(_Config) ->
    Schema = one_field_schema(#{<<"type">> => [<<"string">>, <<"integer">>, <<"null">>]}),
    assert_error(invalid_nullable_type, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_array_without_items(_Config) ->
    Schema = one_field_schema(#{<<"type">> => <<"array">>}),
    assert_error(missing_items, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_reject_enum_value_wrong_type(_Config) ->
    Schema = one_field_schema(#{<<"type">> => <<"string">>, <<"enum">> => [1]}),
    assert_error(invalid_enum_value, emqx_agent_oai_tool_schema:validate_schema(Schema)).

t_generated_create_skill_schema_valid(_Config) ->
    Schema = #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"definition">> => emqx_agent_schema_oai_tool_converter:to_json_schema([skills, items])
        },
        <<"required">> => [<<"definition">>],
        <<"additionalProperties">> => false
    },
    ?assertEqual(ok, emqx_agent_oai_tool_schema:validate_schema(Schema)),
    ?assertEqual(false, contains_any_key([<<"$ref">>, <<"oneOf">>, <<"const">>], Schema)).

t_generated_create_pipeline_schema_valid(_Config) ->
    Schema = emqx_agent_schema_oai_tool_converter:to_json_schema([pipelines, items]),
    ?assertEqual(ok, emqx_agent_oai_tool_schema:validate_schema(Schema)),
    ?assertEqual(false, contains_any_key([<<"$ref">>, <<"oneOf">>, <<"const">>], Schema)),
    ?assertEqual(true, contains_property(<<"active">>, Schema)).

t_generated_create_pipeline_schema_has_typed_steps(_Config) ->
    Schema = emqx_agent_schema_oai_tool_converter:to_json_schema([pipelines, items]),
    StepSchema = maps:get(
        <<"items">>,
        maps:get(<<"steps">>, maps:get(<<"properties">>, Schema))
    ),
    StepTypes = lists:sort([
        branch_type(Branch)
     || Branch <- maps:get(<<"anyOf">>, StepSchema)
    ]),
    ?assertEqual(
        [<<"break">>, <<"call_skill">>, <<"llm_loop">>, <<"wait_for_event">>],
        StepTypes
    ).

t_generated_pipeline_dynamic_maps_are_entry_arrays(_Config) ->
    Schema = emqx_agent_schema_oai_tool_converter:to_json_schema([pipelines, items]),
    StepSchema = maps:get(
        <<"items">>,
        maps:get(<<"steps">>, maps:get(<<"properties">>, Schema))
    ),
    Branches = maps:get(<<"anyOf">>, StepSchema),
    CallSkill = branch_by_type(<<"call_skill">>, Branches),
    LlmLoop = branch_by_type(<<"llm_loop">>, Branches),
    ?assertEqual(<<"array">>, property_type(<<"args">>, CallSkill)),
    ?assertEqual(<<"array">>, property_type(<<"input">>, LlmLoop)).

empty_object() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{},
        <<"required">> => [],
        <<"additionalProperties">> => false
    }.

one_field_schema() ->
    one_field_schema(#{<<"type">> => <<"string">>}).

one_field_schema(FieldSchema) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{<<"field">> => FieldSchema},
        <<"required">> => [<<"field">>],
        <<"additionalProperties">> => false
    }.

assert_error(Code, {error, Errors}) ->
    ?assert(
        lists:any(fun(#{error := Error}) -> Error =:= Code end, Errors),
        {expected_error, Code, Errors}
    ).

contains_any_key(Keys, Map) when is_map(Map) ->
    lists:any(fun(Key) -> maps:is_key(Key, Map) end, Keys) orelse
        lists:any(fun(Value) -> contains_any_key(Keys, Value) end, maps:values(Map));
contains_any_key(Keys, List) when is_list(List) ->
    lists:any(fun(Value) -> contains_any_key(Keys, Value) end, List);
contains_any_key(_Keys, _Value) ->
    false.

contains_property(Name, #{<<"properties">> := Properties} = Map) ->
    maps:is_key(Name, Properties) orelse
        contains_any_key([Name], maps:remove(<<"properties">>, Map));
contains_property(Name, Map) when is_map(Map) ->
    lists:any(fun(Value) -> contains_property(Name, Value) end, maps:values(Map));
contains_property(Name, List) when is_list(List) ->
    lists:any(fun(Value) -> contains_property(Name, Value) end, List);
contains_property(_Name, _Value) ->
    false.

branch_by_type(Type, Branches) ->
    hd([
        maps:get(hd(maps:get(<<"required">>, Branch)), maps:get(<<"properties">>, Branch))
     || Branch <- Branches,
        Type =:= branch_type(Branch)
    ]).

branch_type(Branch) ->
    WrapperName = hd(maps:get(<<"required">>, Branch)),
    Wrapped = maps:get(WrapperName, maps:get(<<"properties">>, Branch)),
    [Type] = maps:get(<<"enum">>, maps:get(<<"type">>, maps:get(<<"properties">>, Wrapped))),
    Type.

property_schema(Name, Schema) ->
    maps:get(Name, maps:get(<<"properties">>, Schema)).

property_type(Name, Schema) ->
    maps:get(<<"type">>, property_schema(Name, Schema)).
