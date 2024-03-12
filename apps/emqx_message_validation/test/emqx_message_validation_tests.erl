%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(VALIDATIONS_PATH, "message_validation.validations").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check(InnerConfigs) ->
    RootBin = <<"message_validation">>,
    InnerBin = <<"validations">>,
    RawConf = #{RootBin => #{InnerBin => InnerConfigs}},
    #{RootBin := #{InnerBin := Checked}} = hocon_tconf:check_plain(
        emqx_message_validation_schema,
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
        <<"log_failure_at">> => <<"warning">>,
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

eval_sql(Message, SQL) ->
    {ok, Check} = emqx_message_validation:parse_sql_check(SQL),
    emqx_message_validation:evaluate_sql_check(Check, Message).

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
