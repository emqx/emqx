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

check_source(Overrides0) ->
    DefaultOverrides = #{
        <<"connector">> => <<"x">>
    },
    Overrides = emqx_utils_maps:deep_merge(DefaultOverrides, Overrides0),
    emqx_bridge_gcp_pubsub_consumer_SUITE:source_config(Overrides).

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

-doc """
Verifies that the topic of both producer actions and consumer sources accepts either a
bare topic name or a fully-qualified `projects/<project-id>/topics/<topic-name>` path,
and rejects other slash-containing values.
""".
pubsub_topic_validator_test_() ->
    ErrorMsg = <<
        "must be either a topic name or a fully-qualified topic path"
        " 'projects/<project-id>/topics/<topic-name>'"
    >>,
    ValidTopics = [
        <<"my-topic">>,
        <<"projects/other-project/topics/my-topic">>
    ],
    InvalidTopics = [
        <<"a/b">>,
        <<"projects/other-project/foo/my-topic">>,
        <<"projects/other-project/topics/my-topic/">>,
        <<"projects//topics/my-topic">>,
        <<"projects/other-project/topics/">>,
        <<"/projects/other-project/topics/my-topic">>,
        <<>>
    ],
    [
        {
            "producer accepts " ++ binary_to_list(Topic),
            ?_assertMatch(
                #{},
                check_action(#{<<"parameters">> => #{<<"pubsub_topic">> => Topic}})
            )
        }
     || Topic <- ValidTopics
    ] ++
        [
            {
                "consumer accepts " ++ binary_to_list(Topic),
                ?_assertMatch(
                    #{},
                    check_source(#{<<"parameters">> => #{<<"topic">> => Topic}})
                )
            }
         || Topic <- ValidTopics
        ] ++
        [
            {
                "producer rejects " ++ binary_to_list(Topic),
                ?_assertThrow(
                    ?validation_error(ErrorMsg, _),
                    check_action(#{<<"parameters">> => #{<<"pubsub_topic">> => Topic}})
                )
            }
         || Topic <- InvalidTopics
        ] ++
        [
            {
                "consumer rejects " ++ binary_to_list(Topic),
                ?_assertThrow(
                    ?validation_error(ErrorMsg, _),
                    check_source(#{<<"parameters">> => #{<<"topic">> => Topic}})
                )
            }
         || Topic <- InvalidTopics
        ].

-doc """
Verifies that we clamp the allowed range for ack deadline.
""".
consumer_ack_deadline_test_() ->
    ErrorMsg = <<"Value must be between 10 s and 600 s">>,
    [
        ?_assertThrow(
            ?validation_error(ErrorMsg, 9),
            check_source(#{<<"parameters">> => #{<<"ack_deadline">> => <<"9s">>}})
        ),
        ?_assertThrow(
            ?validation_error(ErrorMsg, 601),
            check_source(#{<<"parameters">> => #{<<"ack_deadline">> => <<"601s">>}})
        ),
        ?_assertMatch(
            #{},
            check_source(#{<<"parameters">> => #{<<"ack_deadline">> => <<"600s">>}})
        ),
        ?_assertMatch(
            #{},
            check_source(#{<<"parameters">> => #{<<"ack_deadline">> => <<"10s">>}})
        )
    ].

%% The validator reads the file of a `file://' secret only in a check of the global
%% config. In each other check it accepts the secret as it is.
service_account_json_file_secret_test() ->
    File = filename:join("/tmp", ?MODULE_STRING ++ "-" ++ os:getpid()),
    FileRef = iolist_to_binary(["file://", File]),
    Secret = emqx_schema_secret:convert_secret(FileRef, #{}),
    Validate = fun(Opts) ->
        emqx_bridge_gcp_pubsub_schema_lib:service_account_json_validator(Secret, Opts)
    end,
    Global = emqx_schema_secret:check_opts(global),
    ok = file:write_file(File, <<"not a json">>),
    try
        NoReads = emqx_common_test_helpers:file_reads(fun() ->
            ok = Validate(#{}),
            ok = Validate(#{maybe_obfuscated => true}),
            ok = Validate(emqx_schema_secret:check_opts(<<"ns1">>)),
            %% The whole connector config, as it is checked at boot, for the body of
            %% an API request and in a dry run.
            #{} = emqx_bridge_gcp_pubsub_producer_SUITE:connector_config(#{
                <<"authentication">> => #{
                    <<"type">> => <<"service_account_json">>,
                    <<"service_account_json">> => FileRef
                }
            })
        end),
        ?assertEqual([], NoReads),
        Reads = emqx_common_test_helpers:file_reads(fun() ->
            {error, "not a json"} = Validate(Global)
        end),
        ?assertEqual([File], Reads),
        ServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
        ok = file:write_file(File, emqx_utils_json:encode(ServiceAccount)),
        ?assertEqual(ok, Validate(Global))
    after
        file:delete(File)
    end.
