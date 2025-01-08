%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TRANSFORMATIONS_PATH, "message_transformation.transformations").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bin(X) -> emqx_utils_conv:bin(X).

parse_and_check(InnerConfigs) ->
    RootBin = <<"message_transformation">>,
    InnerBin = <<"transformations">>,
    RawConf = #{RootBin => #{InnerBin => InnerConfigs}},
    #{RootBin := #{InnerBin := Checked}} = hocon_tconf:check_plain(
        emqx_message_transformation_schema,
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => false
        }
    ),
    Checked.

transformation(Name, Operations) ->
    transformation(Name, Operations, _Overrides = #{}).

transformation(Name, Operations0, Overrides) ->
    Operations = lists:map(fun normalize_operation/1, Operations0),
    Default = #{
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"description">> => <<"my transformation">>,
        <<"enable">> => true,
        <<"name">> => Name,
        <<"topics">> => [<<"t/+">>],
        <<"failure_action">> => <<"drop">>,
        <<"log_failure">> => #{<<"level">> => <<"warning">>},
        <<"payload_decoder">> => #{<<"type">> => <<"json">>},
        <<"payload_encoder">> => #{<<"type">> => <<"json">>},
        <<"operations">> => Operations
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

normalize_operation({K, V}) ->
    #{<<"key">> => bin(K), <<"value">> => bin(V)}.

dummy_operation() ->
    topic_operation(<<"concat([topic, '/', payload.t])">>).

topic_operation(VariformExpr) ->
    operation(topic, VariformExpr).

operation(Key, VariformExpr) ->
    {Key, VariformExpr}.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

schema_test_() ->
    [
        {"topics is always a list 1",
            ?_assertMatch(
                [#{<<"topics">> := [<<"t/1">>]}],
                parse_and_check([
                    transformation(
                        <<"foo">>,
                        [dummy_operation()],
                        #{<<"topics">> => <<"t/1">>}
                    )
                ])
            )},
        {"topics is always a list 2",
            ?_assertMatch(
                [#{<<"topics">> := [<<"t/1">>]}],
                parse_and_check([
                    transformation(
                        <<"foo">>,
                        [dummy_operation()],
                        #{<<"topics">> => [<<"t/1">>]}
                    )
                ])
            )},
        {"topics must be non-empty",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"at least one topic filter must be defined", _/binary>>,
                        value := [],
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    transformation(<<"foo">>, [dummy_operation()], #{<<"topics">> => []})
                ])
            )},
        {"names are unique",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated name:", _/binary>>,
                        path := ?TRANSFORMATIONS_PATH,
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    transformation(<<"foo">>, [dummy_operation()]),
                    transformation(<<"foo">>, [dummy_operation()])
                ])
            )},
        {"operations may be empty",
            ?_assertMatch(
                [#{<<"operations">> := []}],
                parse_and_check([
                    transformation(
                        <<"foo">>,
                        []
                    )
                ])
            )},
        {"bogus check type: decoder",
            ?_assertThrow(
                {_Schema, [
                    #{
                        expected := <<"none", _/binary>>,
                        kind := validation_error,
                        field_name := type
                    }
                ]},
                parse_and_check([
                    transformation(<<"foo">>, [dummy_operation()], #{
                        <<"payload_decoder">> => #{<<"type">> => <<"foo">>}
                    })
                ])
            )},
        {"bogus check type: encoder",
            ?_assertThrow(
                {_Schema, [
                    #{
                        expected := <<"none", _/binary>>,
                        kind := validation_error,
                        field_name := type
                    }
                ]},
                parse_and_check([
                    transformation(<<"foo">>, [dummy_operation()], #{
                        <<"payload_encoder">> => #{<<"type">> => <<"foo">>}
                    })
                ])
            )}
    ].

invalid_names_test_() ->
    [
        {InvalidName,
            ?_assertThrow(
                {_Schema, [
                    #{
                        kind := validation_error,
                        path := "message_transformation.transformations.1.name"
                    }
                ]},
                parse_and_check([transformation(InvalidName, [dummy_operation()])])
            )}
     || InvalidName <- [
            <<"">>,
            <<"_name">>,
            <<"name$">>,
            <<"name!">>,
            <<"some name">>,
            <<"nãme"/utf8>>,
            <<"test_哈哈"/utf8>>,
            %% long name
            binary:copy(<<"a">>, 256)
        ]
    ].
