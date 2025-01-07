%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(VALIDATIONS_PATH, "schema_validation.validations").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check(InnerConfigs) ->
    RootBin = <<"schema_validation">>,
    InnerBin = <<"validations">>,
    RawConf = #{RootBin => #{InnerBin => InnerConfigs}},
    #{RootBin := #{InnerBin := Checked}} = hocon_tconf:check_plain(
        emqx_schema_validation_schema,
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => false
        }
    ),
    Checked.

validation(Name, Checks) ->
    validation(Name, Checks, _Overrides = #{}).

validation(Name, Checks, Overrides) ->
    Default = #{
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"description">> => <<"my validation">>,
        <<"enable">> => true,
        <<"name">> => Name,
        <<"topics">> => <<"t/+">>,
        <<"strategy">> => <<"all_pass">>,
        <<"failure_action">> => <<"drop">>,
        <<"log_failure">> => #{<<"level">> => <<"warning">>},
        <<"checks">> => Checks
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

sql_check() ->
    sql_check(<<"select * where true">>).

sql_check(SQL) ->
    #{
        <<"type">> => <<"sql">>,
        <<"sql">> => SQL
    }.

schema_check(Type, SerdeName) ->
    schema_check(Type, SerdeName, _Overrides = #{}).

schema_check(Type, SerdeName, Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => emqx_utils_conv:bin(Type),
            <<"schema">> => SerdeName
        },
        Overrides
    ).

eval_sql(Message, SQL) ->
    {ok, Check} = emqx_schema_validation:parse_sql_check(SQL),
    Validation = #{log_failure => #{level => warning}, name => <<"validation">>},
    emqx_schema_validation:evaluate_sql_check(Check, Validation, Message).

message() ->
    message(_Opts = #{}).

message(Opts) ->
    Defaults = #{
        id => emqx_guid:gen(),
        qos => 0,
        from => emqx_guid:to_hexstr(emqx_guid:gen()),
        flags => #{retain => false},
        headers => #{
            proto_ver => v5,
            properties => #{'User-Property' => [{<<"a">>, <<"b">>}]}
        },
        topic => <<"t/t">>,
        payload => emqx_utils_json:encode(#{value => 10}),
        timestamp => 1710272561615,
        extra => []
    },
    emqx_message:from_map(emqx_utils_maps:deep_merge(Defaults, Opts)).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

schema_test_() ->
    [
        {"topics is always a list 1",
            ?_assertMatch(
                [#{<<"topics">> := [<<"t/1">>]}],
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [sql_check()],
                        #{<<"topics">> => <<"t/1">>}
                    )
                ])
            )},
        {"topics is always a list 2",
            ?_assertMatch(
                [#{<<"topics">> := [<<"t/1">>]}],
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [sql_check()],
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
                    validation(<<"foo">>, [sql_check()], #{<<"topics">> => []})
                ])
            )},
        {"foreach expression is not allowed",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := foreach_not_allowed,
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [sql_check(<<"foreach foo as f where true">>)]
                    )
                ])
            )},
        {"from clause is not allowed",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := non_empty_from_clause,
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [sql_check(<<"select * from t">>)]
                    )
                ])
            )},
        {"names are unique",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated name:", _/binary>>,
                        path := ?VALIDATIONS_PATH,
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [sql_check()]),
                    validation(<<"foo">>, [sql_check()])
                ])
            )},
        {"checks must be non-empty",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := "at least one check must be defined",
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        []
                    )
                ])
            )},
        {"bogus check type",
            ?_assertThrow(
                {_Schema, [
                    #{
                        expected := <<"sql", _/binary>>,
                        kind := validation_error,
                        field_name := type
                    }
                ]},
                parse_and_check([validation(<<"foo">>, [#{<<"type">> => <<"foo">>}])])
            )}
    ].

invalid_names_test_() ->
    [
        {InvalidName,
            ?_assertThrow(
                {_Schema, [
                    #{
                        kind := validation_error,
                        path := "schema_validation.validations.1.name"
                    }
                ]},
                parse_and_check([validation(InvalidName, [sql_check()])])
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

check_test_() ->
    [
        {"denied by payload 1",
            ?_assertNot(eval_sql(message(), <<"select * where payload.value > 15">>))},
        {"denied by payload 2",
            ?_assertNot(eval_sql(message(), <<"select payload.value as x where x > 15">>))},
        {"allowed by payload 1",
            ?_assert(eval_sql(message(), <<"select * where payload.value > 5">>))},
        {"allowed by payload 2",
            ?_assert(eval_sql(message(), <<"select payload.value as x where x > 5">>))},
        {"always passes 1", ?_assert(eval_sql(message(), <<"select * where true">>))},
        {"always passes 2", ?_assert(eval_sql(message(), <<"select * where 1 = 1">>))},
        {"never passes 1", ?_assertNot(eval_sql(message(), <<"select * where false">>))},
        {"never passes 2", ?_assertNot(eval_sql(message(), <<"select * where 1 = 2">>))},
        {"never passes 3", ?_assertNot(eval_sql(message(), <<"select * where true and false">>))}
    ].

duplicated_check_test_() ->
    [
        {"duplicated topics 1",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated topics: t/1">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.topics"
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [schema_check(json, <<"a">>)],
                        #{<<"topics">> => [<<"t/1">>, <<"t/1">>]}
                    )
                ])
            )},
        {"duplicated topics 2",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated topics: t/1">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.topics"
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [schema_check(json, <<"a">>)],
                        #{<<"topics">> => [<<"t/1">>, <<"t/#">>, <<"t/1">>]}
                    )
                ])
            )},
        {"duplicated topics 3",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated topics: t/1, t/2">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.topics"
                    }
                ]},
                parse_and_check([
                    validation(
                        <<"foo">>,
                        [schema_check(json, <<"a">>)],
                        #{
                            <<"topics">> => [
                                <<"t/1">>,
                                <<"t/#">>,
                                <<"t/1">>,
                                <<"t/2">>,
                                <<"t/2">>
                            ]
                        }
                    )
                ])
            )},
        {"duplicated sql checks are not checked",
            ?_assertMatch(
                [#{<<"checks">> := [_, _]}],
                parse_and_check([
                    validation(<<"foo">>, [sql_check(), sql_check()])
                ])
            )},
        {"different serdes with same name",
            ?_assertMatch(
                [#{<<"checks">> := [_, _, _]}],
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        schema_check(avro, <<"a">>),
                        schema_check(
                            protobuf,
                            <<"a">>,
                            #{<<"message_type">> => <<"a">>}
                        )
                    ])
                ])
            )},
        {"duplicated serdes 1",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated schema checks: json:a">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.checks"
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        schema_check(json, <<"a">>)
                    ])
                ])
            )},
        {"duplicated serdes 2",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated schema checks: json:a">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.checks"
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        sql_check(),
                        schema_check(json, <<"a">>)
                    ])
                ])
            )},
        {"duplicated serdes 3",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated schema checks: json:a">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.checks"
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        schema_check(json, <<"a">>),
                        sql_check()
                    ])
                ])
            )},
        {"duplicated serdes 4",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated schema checks: json:a">>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.checks"
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        schema_check(json, <<"a">>),
                        schema_check(json, <<"a">>)
                    ])
                ])
            )},
        {"duplicated serdes 4",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := <<"duplicated schema checks: ", _/binary>>,
                        kind := validation_error,
                        path := "schema_validation.validations.1.checks"
                    }
                ]},
                parse_and_check([
                    validation(<<"foo">>, [
                        schema_check(json, <<"a">>),
                        schema_check(json, <<"a">>),
                        schema_check(avro, <<"b">>),
                        schema_check(avro, <<"b">>)
                    ])
                ])
            )}
    ].
