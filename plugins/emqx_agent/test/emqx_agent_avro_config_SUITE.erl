%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_avro_config_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_valid_full_config(_Config) ->
    ?assertMatch({ok, _}, decode(sample_config())).

t_publish_payload_schema_default_materialized(_Config) ->
    Publish0 = maps:get(<<"Message_Publish">>, publish_tool()),
    Publish = #{<<"Message_Publish">> => maps:remove(<<"payload_schema">>, Publish0)},
    {ok, Config} = encode_with_defaults(sample_config_with_tool(Publish)),
    [#{<<"Message_Publish">> := PublishWithDefault}] = maps:get(<<"tools">>, Config),
    PayloadSchema = emqx_utils_json:decode(maps:get(<<"payload_schema">>, PublishWithDefault)),
    ?assertEqual(
        #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"message">> => #{<<"type">> => <<"string">>}},
            <<"required">> => [<<"message">>],
            <<"additionalProperties">> => false
        },
        PayloadSchema
    ).

t_all_config_oai_schemas_valid(_Config) ->
    {ok, Config} = encode_with_defaults(sample_config()),
    ?assertEqual([], oai_schema_errors(Config)).

t_reject_missing_required_tool_field(_Config) ->
    Tool = #{
        <<"Message_Publish">> => maps:remove(
            <<"id">>, maps:get(<<"Message_Publish">>, publish_tool())
        )
    },
    Config = sample_config_with_tool(Tool),
    ?assertMatch({error, _}, decode(Config)).

t_reject_invalid_connection_enable_type(_Config) ->
    [Conn0] = maps:get(<<"connections">>, sample_config()),
    Conn = put_nested(Conn0, [<<"ConnectionPostgresql">>, <<"enable">>], <<"true">>),
    ?assertMatch({error, _}, decode((sample_config())#{<<"connections">> => [Conn]})).

t_reject_invalid_pipeline_step_union(_Config) ->
    [Pipeline0] = maps:get(<<"pipelines">>, sample_config()),
    BadStep = #{<<"UnknownStep">> => #{<<"id">> => <<"s1">>, <<"type">> => <<"unknown">>}},
    Pipeline = Pipeline0#{<<"steps">> => [BadStep]},
    ?assertMatch({error, _}, decode((sample_config())#{<<"pipelines">> => [Pipeline]})).

t_reject_invalid_name_value_type(_Config) ->
    Config = sample_config_with_tool(
        (http_tool())#{
            <<"Http">> => (maps:get(<<"Http">>, http_tool()))#{
                <<"headers">> => [#{<<"name">> => <<"x-api-key">>, <<"value">> => #{}}]
            }
        }
    ),
    ?assertMatch({error, _}, decode(Config)).

t_reject_pipeline_missing_call_tool_ref(_Config) ->
    Config = (sample_config())#{<<"pipelines">> => [pipeline_with_call_tool(<<"missing@tool">>)]},
    ?assertMatch({error, {missing_tools, [<<"missing@tool">>]}}, validate_config(Config)).

t_reject_pipeline_missing_llm_tool_ref(_Config) ->
    Config = (sample_config())#{<<"pipelines">> => [pipeline_with_llm_tool(<<"missing@tool">>)]},
    ?assertMatch({error, {missing_tools, [<<"missing@tool">>]}}, validate_config(Config)).

t_accept_pipeline_with_duplicate_existing_tool_refs(_Config) ->
    Config = (sample_config())#{
        <<"pipelines">> => [pipeline_with_duplicate_call_tool(<<"message__publish@pub">>)]
    },
    ?assertEqual(ok, validate_config(Config)).

decode(Config) ->
    try
        PrivDir = code:priv_dir(emqx_agent),
        {ok, AvscBin} = file:read_file(filename:join(PrivDir, "config_schema.avsc")),
        Name = <<"emqx_agent_test">>,
        Store0 = avro_schema_store:new([map]),
        Store = avro_schema_store:import_schema_json(Name, AvscBin, Store0),
        Opts = avro:make_decoder_options([
            {map_type, map},
            {record_type, map},
            {encoding, avro_json}
        ]),
        {ok, avro_json_decoder:decode_value(emqx_utils_json:encode(Config), Name, Store, Opts)}
    catch
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

encode_with_defaults(Config) ->
    emqx_agent_config:avro_config_with_defaults(Config, <<"emqx_agent_test">>).

validate_config(Config) ->
    {ok, ConfigWithDefaults} = encode_with_defaults(Config),
    emqx_agent_config:validate_config(ConfigWithDefaults).

oai_schema_errors(Config) ->
    tool_oai_schema_errors(maps:get(<<"tools">>, Config, [])) ++
        pipeline_oai_schema_errors(maps:get(<<"pipelines">>, Config, [])).

tool_oai_schema_errors(Tools) ->
    lists:flatmap(
        fun(Tool0) ->
            Tool = unwrap_union(Tool0),
            case maps:get(<<"type">>, Tool, undefined) of
                <<"message__publish">> ->
                    validate_field_schema(maps:get(<<"payload_schema">>, Tool));
                <<"message__request">> ->
                    validate_field_schema(maps:get(<<"request_payload_schema">>, Tool));
                <<"http">> ->
                    validate_root_schema(maps:get(<<"input_schema">>, Tool));
                _ ->
                    []
            end
        end,
        Tools
    ).

pipeline_oai_schema_errors(Pipelines) ->
    lists:flatmap(
        fun(Pipeline0) ->
            Pipeline = unwrap_union(Pipeline0),
            lists:flatmap(
                fun(Step0) ->
                    Step = unwrap_union(Step0),
                    case maps:get(<<"type">>, Step, undefined) of
                        <<"llm_loop">> ->
                            validate_root_schema(maps:get(<<"set_result_schema">>, Step));
                        _ ->
                            []
                    end
                end,
                maps:get(<<"steps">>, Pipeline, [])
            )
        end,
        Pipelines
    ).

validate_field_schema(SchemaString) ->
    Schema = emqx_agent_oai_tool_schema:json_schema_from_string(SchemaString, []),
    case emqx_agent_oai_tool_schema:validate_oai_schema_field(Schema) of
        ok -> [];
        {error, Reason} -> [Reason]
    end.

validate_root_schema(SchemaString) ->
    Schema = emqx_agent_oai_tool_schema:json_schema_from_string(SchemaString, []),
    case emqx_agent_oai_tool_schema:validate_oai_schema(Schema) of
        ok -> [];
        {error, Reason} -> [Reason]
    end.

unwrap_union(Map) when is_map(Map), map_size(Map) =:= 1 ->
    [{_Key, Value}] = maps:to_list(Map),
    Value;
unwrap_union(Value) ->
    Value.

sample_config_with_tool(Tool) ->
    (sample_config())#{<<"tools">> => [Tool]}.

sample_config() ->
    #{
        <<"tools">> => [
            publish_tool(),
            request_tool(),
            http_tool(),
            postgresql_tool(),
            simple_tool(<<"Agent_CreateTool">>, <<"agent__create_tool">>, <<"create-tool">>),
            simple_tool(
                <<"Agent_CreatePipeline">>, <<"agent__create_pipeline">>, <<"create-pipe">>
            ),
            simple_tool(<<"Agent_QueryTools">>, <<"agent__query_tools">>, <<"query-tools">>),
            simple_tool(
                <<"Agent_QueryProviders">>, <<"agent__query_providers">>, <<"query-providers">>
            ),
            simple_tool(
                <<"Agent_QueryPipelines">>, <<"agent__query_pipelines">>, <<"query-pipelines">>
            ),
            simple_tool(<<"Agent_DeleteTool">>, <<"agent__delete_tool">>, <<"delete-tool">>),
            simple_tool(
                <<"Agent_DeletePipeline">>, <<"agent__delete_pipeline">>, <<"delete-pipe">>
            ),
            simple_tool(
                <<"Agent_QueryConnections">>,
                <<"agent__query_connections">>,
                <<"query-connections">>
            ),
            simple_tool(<<"Agent_UpdateTool">>, <<"agent__update_tool">>, <<"update-tool">>),
            simple_tool(
                <<"Agent_UpdatePipeline">>, <<"agent__update_pipeline">>, <<"update-pipeline">>
            ),
            simple_tool(
                <<"Agent_DeletePipelineStep">>,
                <<"agent__delete_pipeline_step">>,
                <<"delete-pipeline-step">>
            ),
            simple_tool(
                <<"Agent_InsertPipelineStep">>,
                <<"agent__insert_pipeline_step">>,
                <<"insert-pipeline-step">>
            ),
            simple_tool(
                <<"Agent_UpdatePipelineStep">>,
                <<"agent__update_pipeline_step">>,
                <<"update-pipeline-step">>
            )
        ],
        <<"connections">> => [postgresql_connection()],
        <<"pipelines">> => [pipeline()]
    }.

publish_tool() ->
    #{
        <<"Message_Publish">> => #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"pub">>,
            <<"desc">> => <<"Publish message">>,
            <<"topic_prefix">> => <<"devices/">>,
            <<"payload_schema">> => schema_string()
        }
    }.

request_tool() ->
    #{
        <<"Message_Request">> => #{
            <<"type">> => <<"message__request">>,
            <<"id">> => <<"req">>,
            <<"desc">> => <<"Request message">>,
            <<"topic_prefix">> => <<"requests/">>,
            <<"request_payload_schema">> => schema_string()
        }
    }.

http_tool() ->
    #{
        <<"Http">> => #{
            <<"type">> => <<"http">>,
            <<"id">> => <<"http-main">>,
            <<"desc">> => <<"Call HTTP endpoint">>,
            <<"method">> => <<"post">>,
            <<"url">> => <<"https://example.com/api">>,
            <<"headers">> => [#{<<"name">> => <<"x-api-key">>, <<"value">> => <<"secret">>}],
            <<"input_schema">> => schema_string()
        }
    }.

postgresql_tool() ->
    #{
        <<"Postgresql_Query">> => #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => <<"pg-query">>,
            <<"desc">> => <<"Query PostgreSQL">>,
            <<"query">> => <<"select * from devices where id = ${device_id}">>,
            <<"resource">> => <<"pg-main">>
        }
    }.

simple_tool(RecordName, Type, Id) ->
    #{RecordName => #{<<"type">> => Type, <<"id">> => Id}}.

postgresql_connection() ->
    #{
        <<"ConnectionPostgresql">> => #{
            <<"type">> => <<"postgresql">>,
            <<"id">> => <<"pg-main">>,
            <<"enable">> => true,
            <<"config">> => #{
                <<"server">> => <<"127.0.0.1:5432">>,
                <<"database">> => <<"emqx">>,
                <<"username">> => <<"emqx">>,
                <<"password">> => <<"public">>,
                <<"pool_size">> => 4,
                <<"connect_timeout">> => 15000,
                <<"disable_prepared_statements">> => false,
                <<"ssl">> => #{
                    <<"enable">> => false,
                    <<"server_name_indication">> => <<"disable">>,
                    <<"verify">> => <<"verify_none">>,
                    <<"cacertfile">> => <<>>,
                    <<"certfile">> => <<>>,
                    <<"keyfile">> => <<>>
                }
            }
        }
    }.

pipeline() ->
    #{
        <<"pipeline_id">> => <<"pipe-main">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/devices/+">>},
        <<"steps">> => [
            #{
                <<"PipelineStepCallTool">> => #{
                    <<"id">> => <<"notify">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => <<"message__publish@pub">>,
                    <<"args">> => [
                        #{<<"name">> => <<"topic">>, <<"value">> => <<"$.event.device_id">>},
                        #{<<"name">> => <<"payload">>, <<"value">> => <<"$.event">>}
                    ],
                    <<"result_path">> => <<"$.notify">>
                }
            },
            #{
                <<"PipelineStepLlmLoop">> => #{
                    <<"id">> => <<"inspect">>,
                    <<"type">> => <<"llm_loop">>,
                    <<"provider_name">> => <<"openai">>,
                    <<"model">> => <<"gpt-4o">>,
                    <<"persistent">> => false,
                    <<"max_tokens">> => 2048,
                    <<"max_total_tokens">> => 50000,
                    <<"tools">> => [<<"message__publish@pub">>],
                    <<"instructions">> => <<"Inspect the event">>,
                    <<"input">> => [#{<<"name">> => <<"event">>, <<"value">> => <<"$.event">>}],
                    <<"set_result_schema">> => schema_string(),
                    <<"result_path">> => <<"$.inspection">>
                }
            },
            #{
                <<"PipelineStepBreak">> => #{
                    <<"id">> => <<"break">>,
                    <<"type">> => <<"break">>,
                    <<"path">> => <<"$.inspection.ok">>,
                    <<"not">> => false,
                    <<"eq">> => <<"true">>
                }
            }
        ]
    }.

pipeline_with_call_tool(Ref) ->
    #{
        <<"pipeline_id">> => <<"pipe-missing-call">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/missing/call">>},
        <<"steps">> => [
            #{
                <<"PipelineStepCallTool">> => #{
                    <<"id">> => <<"notify">>,
                    <<"type">> => <<"call_tool">>,
                    <<"tool">> => Ref,
                    <<"args">> => [],
                    <<"result_path">> => <<"$.notify">>
                }
            }
        ]
    }.

pipeline_with_llm_tool(Ref) ->
    #{
        <<"pipeline_id">> => <<"pipe-missing-tool">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/missing/tool">>},
        <<"steps">> => [
            #{
                <<"PipelineStepLlmLoop">> => #{
                    <<"id">> => <<"inspect">>,
                    <<"type">> => <<"llm_loop">>,
                    <<"provider_name">> => <<"openai">>,
                    <<"model">> => <<"gpt-4o">>,
                    <<"persistent">> => false,
                    <<"max_tokens">> => 2048,
                    <<"max_total_tokens">> => 50000,
                    <<"tools">> => [Ref],
                    <<"instructions">> => <<"Inspect the event">>,
                    <<"input">> => [],
                    <<"set_result_schema">> => schema_string(),
                    <<"result_path">> => <<"$.inspection">>
                }
            }
        ]
    }.

pipeline_with_duplicate_call_tool(Ref) ->
    #{
        <<"pipeline_id">> => <<"pipe-duplicate-call">>,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"$evt/duplicate/call">>},
        <<"steps">> => [
            pipeline_call_tool_step(<<"notify1">>, Ref),
            pipeline_call_tool_step(<<"notify2">>, Ref)
        ]
    }.

pipeline_call_tool_step(Id, Ref) ->
    #{
        <<"PipelineStepCallTool">> => #{
            <<"id">> => Id,
            <<"type">> => <<"call_tool">>,
            <<"tool">> => Ref,
            <<"args">> => [],
            <<"result_path">> => <<"$.notify">>
        }
    }.

schema_string() ->
    emqx_utils_json:encode(#{
        <<"type">> => <<"object">>,
        <<"properties">> => #{<<"message">> => #{<<"type">> => <<"string">>}},
        <<"required">> => [<<"message">>],
        <<"additionalProperties">> => false
    }).

put_nested(Map, [Key], Value) ->
    Map#{Key => Value};
put_nested(Map, [Key | Rest], Value) ->
    Map#{Key => put_nested(maps:get(Key, Map), Rest, Value)}.
