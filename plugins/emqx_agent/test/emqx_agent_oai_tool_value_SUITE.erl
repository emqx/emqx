%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_oai_tool_value_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_valid_object(_Config) ->
    Value = #{
        <<"name">> => <<"Ada">>,
        <<"age">> => null,
        <<"role">> => <<"admin">>,
        <<"scores">> => [1, 2.5],
        <<"profile">> => #{<<"email">> => <<"ada@example.com">>}
    },
    ?assertEqual(ok, validate(Value)).

t_missing_required_property(_Config) ->
    Value = maps:remove(<<"age">>, valid_value()),
    assert_error(missing_required_property, validate(Value)).

t_unexpected_property(_Config) ->
    Value = value(#{<<"debug">> => true}),
    assert_error(unexpected_property, validate(Value)).

t_wrong_scalar_type(_Config) ->
    Value = value(#{<<"name">> => 10}),
    assert_error(wrong_type, validate(Value)).

t_integer_rejects_float(_Config) ->
    Schema = root(#{<<"n">> => #{<<"type">> => <<"integer">>}}),
    Result = emqx_agent_oai_tool_value:validate_value(Schema, #{<<"n">> => 1.5}),
    assert_error(wrong_type, Result).

t_number_accepts_integer_and_float(_Config) ->
    Schema = root(#{<<"n">> => #{<<"type">> => <<"number">>}}),
    ?assertEqual(ok, emqx_agent_oai_tool_value:validate_value(Schema, #{<<"n">> => 1})),
    ?assertEqual(ok, emqx_agent_oai_tool_value:validate_value(Schema, #{<<"n">> => 1.5})).

t_missing_nullable_field_is_error(_Config) ->
    Schema = root(#{<<"maybe">> => #{<<"type">> => [<<"string">>, <<"null">>]}}),
    assert_error(missing_required_property, emqx_agent_oai_tool_value:validate_value(Schema, #{})).

t_enum_mismatch(_Config) ->
    Value = value(#{<<"role">> => <<"owner">>}),
    assert_error(enum_mismatch, validate(Value)).

t_array_item_error_path(_Config) ->
    Value = value(#{<<"scores">> => [1, <<"bad">>]}),
    {error, Errors} = validate(Value),
    ?assert(
        lists:any(
            fun
                (#{error := wrong_type, path := [<<"scores">>, 1]}) -> true;
                (_) -> false
            end,
            Errors
        ),
        Errors
    ).

t_anyof_success_and_failure(_Config) ->
    Schema = root(#{
        <<"item">> => #{
            <<"anyOf">> => [
                #{<<"type">> => <<"string">>},
                #{<<"type">> => <<"integer">>}
            ]
        }
    }),
    ?assertEqual(ok, emqx_agent_oai_tool_value:validate_value(Schema, #{<<"item">> => <<"x">>})),
    ?assertEqual(ok, emqx_agent_oai_tool_value:validate_value(Schema, #{<<"item">> => 1})),
    assert_error(
        any_of_no_match, emqx_agent_oai_tool_value:validate_value(Schema, #{<<"item">> => true})
    ).

t_custom_null_value(_Config) ->
    Schema = root(#{<<"maybe">> => #{<<"type">> => [<<"string">>, <<"null">>]}}),
    ?assertEqual(
        ok,
        emqx_agent_oai_tool_value:validate_value(
            Schema,
            #{<<"maybe">> => undefined},
            #{null_value => undefined}
        )
    ).

validate(Value) ->
    emqx_agent_oai_tool_value:validate_value(schema(), Value).

schema() ->
    root(#{
        <<"name">> => #{<<"type">> => <<"string">>},
        <<"age">> => #{<<"type">> => [<<"integer">>, <<"null">>]},
        <<"role">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"admin">>, <<"user">>]},
        <<"scores">> => #{
            <<"type">> => <<"array">>,
            <<"items">> => #{<<"type">> => <<"number">>}
        },
        <<"profile">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"email">> => #{<<"type">> => <<"string">>}},
            <<"required">> => [<<"email">>],
            <<"additionalProperties">> => false
        }
    }).

valid_value() ->
    value(#{}).

value(Overrides) ->
    maps:merge(
        #{
            <<"name">> => <<"Ada">>,
            <<"age">> => 37,
            <<"role">> => <<"admin">>,
            <<"scores">> => [1, 2.5],
            <<"profile">> => #{<<"email">> => <<"ada@example.com">>}
        },
        Overrides
    ).

root(Properties) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Properties,
        <<"required">> => maps:keys(Properties),
        <<"additionalProperties">> => false
    }.

assert_error(Code, {error, Errors}) ->
    ?assert(
        lists:any(fun(#{error := Error}) -> Error =:= Code end, Errors),
        {expected_error, Code, Errors}
    ).
