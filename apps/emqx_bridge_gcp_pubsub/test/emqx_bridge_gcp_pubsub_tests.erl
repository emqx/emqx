%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
