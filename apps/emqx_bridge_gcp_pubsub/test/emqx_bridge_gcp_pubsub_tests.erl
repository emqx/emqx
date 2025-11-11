%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Data section
%%===========================================================================

%%===========================================================================
%% Helper functions
%%===========================================================================

check_action(Overrides0) when is_map(Overrides0) ->
    DefaultOverrides = #{
        <<"connector">> => <<"x">>,
        <<"parameters">> => #{
            <<"attributes_template">> => [
                #{<<"key">> => <<"${payload.key}">>, <<"value">> => <<"fixed_value">>},
                #{<<"key">> => <<"${payload.key}2">>, <<"value">> => <<"${.payload.value}">>},
                #{<<"key">> => <<"fixed_key">>, <<"value">> => <<"fixed_value">>},
                #{<<"key">> => <<"fixed_key2">>, <<"value">> => <<"${.payload.value}">>}
            ],
            <<"ordering_key_template">> => <<"${.payload.ok}">>,
            <<"payload_template">> => <<"${.}">>
        }
    },
    Overrides = emqx_utils_maps:deep_merge(DefaultOverrides, Overrides0),
    emqx_bridge_gcp_pubsub_producer_SUITE:action_config(Overrides).

-define(validation_error(Reason, Value),
    {emqx_bridge_v2_schema, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

%%===========================================================================
%% Test cases
%%===========================================================================

producer_attributes_validator_test_() ->
    [
        {"base config",
            ?_assertMatch(
                #{<<"parameters">> := #{<<"attributes_template">> := [_, _, _, _]}},
                check_action(#{})
            )},
        {"empty key template",
            ?_assertThrow(
                ?validation_error("Key templates must not be empty", _),
                check_action(
                    #{
                        <<"parameters">> => #{
                            <<"attributes_template">> => [
                                #{
                                    <<"key">> => <<>>,
                                    <<"value">> => <<"some_value">>
                                }
                            ]
                        }
                    }
                )
            )},
        {"empty value template",
            ?_assertMatch(
                #{<<"parameters">> := #{<<"attributes_template">> := [_]}},
                check_action(
                    #{
                        <<"parameters">> => #{
                            <<"attributes_template">> => [
                                #{
                                    <<"key">> => <<"some_key">>,
                                    <<"value">> => <<>>
                                }
                            ]
                        }
                    }
                )
            )}
    ].

service_account_json_validator_test_() ->
    ValidServiceAccountJson = #{
        <<"type">> => <<"service_account">>,
        <<"project_id">> => <<"test-project">>,
        <<"private_key_id">> => <<"key123">>,
        <<"private_key">> =>
            <<"-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n">>,
        <<"client_email">> => <<"test@test-project.iam.gserviceaccount.com">>
    },
    ValidJsonString = emqx_utils_json:encode(ValidServiceAccountJson),
    InvalidJson = <<"not valid json">>,
    MissingFieldsJson = emqx_utils_json:encode(#{<<"type">> => <<"service_account">>}),
    WrongTypeJson = emqx_utils_json:encode(
        ValidServiceAccountJson#{<<"type">> => <<"wrong_type">>}
    ),
    [
        {"valid inline JSON string",
            ?_assertEqual(
                ok,
                emqx_bridge_gcp_pubsub:service_account_json_validator(ValidJsonString)
            )},
        {"valid JSON map",
            ?_assertEqual(
                ok,
                emqx_bridge_gcp_pubsub:service_account_json_validator(ValidServiceAccountJson)
            )},
        {"file reference should pass validation",
            ?_assertEqual(
                ok,
                emqx_bridge_gcp_pubsub:service_account_json_validator(
                    <<"file:///path/to/service_account.json">>
                )
            )},
        {"wrapped secret (function) should pass validation",
            ?_assertEqual(
                ok,
                emqx_bridge_gcp_pubsub:service_account_json_validator(
                    fun() -> ValidJsonString end
                )
            )},
        {"invalid JSON string",
            ?_assertEqual(
                {error, "not a json"},
                emqx_bridge_gcp_pubsub:service_account_json_validator(InvalidJson)
            )},
        {"missing required fields",
            ?_assertMatch(
                {error, #{missing_keys := [_ | _]}},
                emqx_bridge_gcp_pubsub:service_account_json_validator(MissingFieldsJson)
            )},
        {"wrong account type",
            ?_assertMatch(
                {error, #{wrong_type := <<"wrong_type">>}},
                emqx_bridge_gcp_pubsub:service_account_json_validator(WrongTypeJson)
            )}
    ].

service_account_json_converter_test_() ->
    ValidServiceAccountJson = #{
        <<"type">> => <<"service_account">>,
        <<"project_id">> => <<"test-project">>,
        <<"private_key_id">> => <<"key123">>,
        <<"private_key">> =>
            <<"-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n">>,
        <<"client_email">> => <<"test@test-project.iam.gserviceaccount.com">>
    },
    ValidJsonString = emqx_utils_json:encode(ValidServiceAccountJson),
    FileRef = <<"file:///path/to/service_account.json">>,
    [
        {"convert valid JSON string", fun() ->
            Result = emqx_bridge_gcp_pubsub:service_account_json_converter(
                ValidJsonString, #{}
            ),
            ?assert(is_function(Result, 0)),
            %% When unwrapped, should return the original JSON string
            ?assertEqual(ValidJsonString, emqx_secret:unwrap(Result))
        end},
        {"convert file reference", fun() ->
            Result = emqx_bridge_gcp_pubsub:service_account_json_converter(
                FileRef, #{}
            ),
            ?assert(is_function(Result, 0))
        %% Note: We can't test unwrapping without the actual file
        end},
        {"convert map to JSON and wrap", fun() ->
            Result = emqx_bridge_gcp_pubsub:service_account_json_converter(
                ValidServiceAccountJson, #{}
            ),
            ?assert(is_function(Result, 0)),
            %% When unwrapped, should be the JSON string representation
            ?assertEqual(ValidJsonString, emqx_secret:unwrap(Result))
        end},
        {"make_serializable with function", fun() ->
            Secret = emqx_secret:wrap(ValidJsonString),
            Result = emqx_bridge_gcp_pubsub:service_account_json_converter(
                Secret, #{make_serializable => true}
            ),
            ?assertEqual(ValidJsonString, Result)
        end},
        {"make_serializable preserves wrapped secrets", fun() ->
            %% When a secret is already wrapped, make_serializable should
            %% return its source representation
            Secret = emqx_secret:wrap(ValidJsonString),
            Result = emqx_bridge_gcp_pubsub:service_account_json_converter(
                Secret, #{make_serializable => true}
            ),
            %% For inline secrets, it should return the JSON string
            ?assertEqual(ValidJsonString, Result)
        end}
    ].
