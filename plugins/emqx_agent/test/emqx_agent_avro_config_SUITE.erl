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

t_reject_missing_required_skill_field(_Config) ->
    Skill = #{
        <<"Message_Publish">> => maps:remove(
            <<"id">>, maps:get(<<"Message_Publish">>, publish_skill())
        )
    },
    Config = sample_config_with_skill(Skill),
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
    Config = sample_config_with_skill(
        (http_skill())#{
            <<"Http">> => (maps:get(<<"Http">>, http_skill()))#{
                <<"headers">> => [#{<<"name">> => <<"x-api-key">>, <<"value">> => #{}}]
            }
        }
    ),
    ?assertMatch({error, _}, decode(Config)).

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

sample_config_with_skill(Skill) ->
    (sample_config())#{<<"skills">> => [Skill]}.

sample_config() ->
    #{
        <<"skills">> => [
            publish_skill(),
            request_skill(),
            http_skill(),
            postgresql_skill(),
            simple_skill(<<"Agent_CreateSkill">>, <<"agent__create_skill">>, <<"create-skill">>),
            simple_skill(
                <<"Agent_CreatePipeline">>, <<"agent__create_pipeline">>, <<"create-pipe">>
            ),
            simple_skill(<<"Agent_QuerySkills">>, <<"agent__query_skills">>, <<"query-skills">>),
            simple_skill(
                <<"Agent_QueryProviders">>, <<"agent__query_providers">>, <<"query-providers">>
            ),
            simple_skill(
                <<"Agent_QueryPipelines">>, <<"agent__query_pipelines">>, <<"query-pipelines">>
            ),
            simple_skill(<<"Agent_DeleteSkill">>, <<"agent__delete_skill">>, <<"delete-skill">>),
            simple_skill(
                <<"Agent_DeletePipeline">>, <<"agent__delete_pipeline">>, <<"delete-pipe">>
            )
        ],
        <<"connections">> => [postgresql_connection()],
        <<"pipelines">> => [pipeline()]
    }.

publish_skill() ->
    #{
        <<"Message_Publish">> => #{
            <<"type">> => <<"message__publish">>,
            <<"id">> => <<"pub">>,
            <<"desc">> => <<"Publish message">>,
            <<"topic_prefix">> => <<"devices/">>,
            <<"payload_schema">> => schema_string()
        }
    }.

request_skill() ->
    #{
        <<"Message_Request">> => #{
            <<"type">> => <<"message__request">>,
            <<"id">> => <<"req">>,
            <<"desc">> => <<"Request message">>,
            <<"topic_prefix">> => <<"requests/">>,
            <<"request_payload_schema">> => schema_string()
        }
    }.

http_skill() ->
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

postgresql_skill() ->
    #{
        <<"Postgresql_Query">> => #{
            <<"type">> => <<"postgresql__query">>,
            <<"id">> => <<"pg-query">>,
            <<"desc">> => <<"Query PostgreSQL">>,
            <<"query">> => <<"select * from devices where id = ${device_id}">>,
            <<"resource">> => <<"pg-main">>
        }
    }.

simple_skill(RecordName, Type, Id) ->
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
        <<"trigger">> => #{<<"topic">> => <<"evt/devices/+">>},
        <<"steps">> => [
            #{
                <<"PipelineStepCallSkill">> => #{
                    <<"id">> => <<"notify">>,
                    <<"type">> => <<"call_skill">>,
                    <<"skill">> => <<"message__publish@pub">>,
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
                    <<"stop_on_finish">> => true,
                    <<"max_tokens">> => 2048,
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
