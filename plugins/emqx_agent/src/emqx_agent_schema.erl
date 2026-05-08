%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Hocon schema module for emqx_agent REST API.
%%
%% Defines three resource families:
%%   Skills         — cap-layer skill instances (message.publish, http, postgresql.query)
%%   Pipelines       — event-driven orchestration definitions

-module(emqx_agent_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema callbacks
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

%% Type helpers used by emqx_agent_api
-export([
    skill_entry_type/0,
    skill_create_type/0,
    pipeline_type/0,
    pipeline_step_type/0
]).

%% JSON Schema conversion / validation helpers
-export([
    json_schema_from_string/2,
    validate_oai_schema/1,
    validate_oai_schema_field/1
]).

%%--------------------------------------------------------------------
%% hocon_schema API
%%--------------------------------------------------------------------

namespace() -> agent.
roots() -> [].
tags() -> [<<"Agent">>].

%%--------------------------------------------------------------------
%% fields/1 — skill read response (what the registry stores)
%%--------------------------------------------------------------------

fields(skill_entry) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_entry_id)
            })},
        {type,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_entry_type)
            })},
        {display_name,
            mk(binary(), #{
                required => false,
                desc => ?DESC(skill_entry_display_name)
            })},
        {description,
            mk(binary(), #{
                required => false,
                desc => ?DESC(skill_entry_description)
            })},
        {input_schema,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_input_schema),
                converter => fun json_schema_from_string/2,
                validator => fun validate_oai_schema/1
            })}
    ];
%%--------------------------------------------------------------------
%% fields/1 — skill create variants (one per skill module type)
%%--------------------------------------------------------------------

fields(skill_publish_create) ->
    [
        {type,
            mk(enum(['message.publish']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_human_desc)
            })},
        {topic_prefix,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_publish_topic_prefix)
            })},
        {payload_schema,
            mk(map(), #{
                required => false,
                desc => ?DESC(skill_input_schema),
                converter => fun json_schema_from_string/2,
                validator => fun validate_oai_schema_field/1
            })}
    ];
fields(skill_http_create) ->
    [
        {type,
            mk(enum([http]), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_human_desc)
            })},
        {method,
            mk(enum([get, post, put, patch, delete]), #{
                required => true,
                desc => ?DESC(skill_http_method)
            })},
        {url,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_http_url)
            })},
        {headers,
            mk(hoconsc:array(ref(name_value_entry)), #{
                required => false,
                default => [],
                desc => ?DESC(skill_http_headers)
            })},
        {input_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_input_schema),
                converter => fun json_schema_from_string/2,
                validator => fun validate_oai_schema/1
            })}
    ];
fields(skill_mqtt_request_create) ->
    [
        {type,
            mk(enum(['message.request']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_human_desc)
            })},
        {topic_prefix,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_publish_topic_prefix)
            })},
        {request_payload_schema,
            mk(map(), #{
                required => false,
                desc => ?DESC(skill_request_payload_schema),
                converter => fun json_schema_from_string/2,
                validator => fun validate_oai_schema_field/1
            })}
    ];
fields(skill_postgresql_create) ->
    [
        {type,
            mk(enum(['postgresql.query']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_human_desc)
            })},
        {query,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_pg_query)
            })}
    ];
fields(skill_create_skill_create) ->
    [
        {type,
            mk(enum(['agent.create_skill']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_create_pipeline_create) ->
    [
        {type,
            mk(enum(['agent.create_pipeline']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_query_skills_create) ->
    [
        {type,
            mk(enum(['agent.query_skills']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_query_providers_create) ->
    [
        {type,
            mk(enum(['agent.query_providers']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_query_pipelines_create) ->
    [
        {type,
            mk(enum(['agent.query_pipelines']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_delete_skill_create) ->
    [
        {type,
            mk(enum(['agent.delete_skill']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
fields(skill_delete_pipeline_create) ->
    [
        {type,
            mk(enum(['agent.delete_pipeline']), #{
                required => true,
                desc => ?DESC(skill_type_discriminator)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })}
    ];
%%--------------------------------------------------------------------
%% fields/1 — pipeline definition
%%--------------------------------------------------------------------

fields(pipeline) ->
    [
        {pipeline_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_id)
            })},
        {active,
            mk(boolean(), #{
                required => false,
                default => true,
                desc => ?DESC(pipeline_active)
            })},
        {trigger,
            mk(ref(pipeline_trigger), #{
                required => true,
                desc => ?DESC(pipeline_trigger_field)
            })},
        {steps,
            mk(hoconsc:array(pipeline_step_type()), #{
                required => true,
                default => [],
                desc => ?DESC(pipeline_steps)
            })}
    ];
fields(pipeline_trigger) ->
    [
        {topic,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_trigger_topic)
            })}
    ];
fields(pipeline_step_call_skill) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_id)
            })},
        {type,
            mk(enum([call_skill]), #{
                required => true,
                desc => ?DESC(pipeline_step_type)
            })},
        {skill,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_skill)
            })},
        {args,
            mk(hoconsc:array(ref(name_value_entry)), #{
                required => false,
                default => [],
                desc => ?DESC(pipeline_step_args)
            })},
        {result_path,
            mk(binary(), #{
                required => false,
                desc => ?DESC(pipeline_step_result_path)
            })}
    ];
fields(pipeline_step_llm_loop) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_id)
            })},
        {type,
            mk(enum([llm_loop]), #{
                required => true,
                desc => ?DESC(pipeline_step_type)
            })},
        {provider_name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_provider_name)
            })},
        {model,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_model)
            })},
        {stop_on_finish,
            mk(boolean(), #{
                required => false,
                default => true,
                desc => ?DESC(pipeline_step_stop_on_finish)
            })},
        {max_tokens,
            mk(pos_integer(), #{
                required => false,
                default => 2048,
                desc => ?DESC(pipeline_step_max_tokens)
            })},
        {tools,
            mk(hoconsc:array(binary()), #{
                required => false,
                default => [],
                desc => ?DESC(pipeline_step_tools)
            })},
        {instructions,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_instructions)
            })},
        {input,
            mk(hoconsc:array(ref(name_value_entry)), #{
                required => false,
                default => [],
                desc => ?DESC(pipeline_step_input)
            })},
        {set_result_schema,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_set_result_schema),
                converter => fun json_schema_from_string/2,
                validator => fun validate_oai_schema/1
            })},
        {result_path,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_result_path)
            })}
    ];
fields(pipeline_step_wait_for_event) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_id)
            })},
        {type,
            mk(enum([wait_for_event]), #{
                required => true,
                desc => ?DESC(pipeline_step_type)
            })},
        {topic,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_topic)
            })},
        {where,
            mk(binary(), #{
                required => false,
                desc => ?DESC(pipeline_step_where)
            })},
        {result_path,
            mk(binary(), #{
                required => false,
                desc => ?DESC(pipeline_step_result_path)
            })}
    ];
fields(pipeline_step_break) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_id)
            })},
        {type,
            mk(enum([break]), #{
                required => true,
                desc => ?DESC(pipeline_step_type)
            })},
        {path,
            mk(binary(), #{
                required => true,
                desc => ?DESC(pipeline_step_path)
            })},
        {'not',
            mk(boolean(), #{
                required => false,
                default => false,
                desc => ?DESC(pipeline_step_not)
            })},
        {eq,
            mk(value_type(), #{
                required => false,
                default => true,
                desc => ?DESC(pipeline_step_eq)
            })}
    ];
fields(name_value_entry) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(name_value_entry_name)
            })},
        {value,
            mk(value_type(), #{
                required => true,
                desc => ?DESC(name_value_entry_value)
            })}
    ];
fields(_) ->
    [].

%%--------------------------------------------------------------------
%% desc/1 — struct-level descriptions
%%--------------------------------------------------------------------

desc(skill_entry) -> ?DESC(skill_entry);
desc(skill_publish_create) -> ?DESC(skill_publish_create);
desc(skill_http_create) -> ?DESC(skill_http_create);
desc(skill_mqtt_request_create) -> ?DESC(skill_mqtt_request_create);
desc(skill_postgresql_create) -> ?DESC(skill_postgresql_create);
desc(skill_create_skill_create) -> ?DESC(skill_create_skill_create);
desc(skill_create_pipeline_create) -> ?DESC(skill_create_pipeline_create);
desc(skill_query_skills_create) -> ?DESC(skill_query_skills_create);
desc(skill_query_providers_create) -> ?DESC(skill_query_providers_create);
desc(skill_query_pipelines_create) -> ?DESC(skill_query_pipelines_create);
desc(skill_delete_skill_create) -> ?DESC(skill_delete_skill_create);
desc(skill_delete_pipeline_create) -> ?DESC(skill_delete_pipeline_create);
desc(pipeline) -> ?DESC(pipeline);
desc(pipeline_trigger) -> ?DESC(pipeline_trigger);
desc(pipeline_step_call_skill) -> ?DESC(pipeline_step_call_skill);
desc(pipeline_step_llm_loop) -> ?DESC(pipeline_step_llm_loop);
desc(pipeline_step_wait_for_event) -> ?DESC(pipeline_step_wait_for_event);
desc(pipeline_step_break) -> ?DESC(pipeline_step_break);
desc(name_value_entry) -> ?DESC(name_value_entry);
desc(_) -> undefined.

%%--------------------------------------------------------------------
%% Schema type helpers
%%--------------------------------------------------------------------

-spec skill_entry_type() -> hocon_schema:schema().
skill_entry_type() ->
    ref(skill_entry).

-spec skill_create_type() -> hocon_schema:schema().
skill_create_type() ->
    emqx_schema:mkunion(<<"type">>, #{
        <<"message.publish">> => ref(skill_publish_create),
        <<"message.request">> => ref(skill_mqtt_request_create),
        <<"http">> => ref(skill_http_create),
        <<"postgresql.query">> => ref(skill_postgresql_create),
        <<"agent.create_skill">> => ref(skill_create_skill_create),
        <<"agent.create_pipeline">> => ref(skill_create_pipeline_create),
        <<"agent.query_skills">> => ref(skill_query_skills_create),
        <<"agent.query_providers">> => ref(skill_query_providers_create),
        <<"agent.query_pipelines">> => ref(skill_query_pipelines_create),
        <<"agent.delete_skill">> => ref(skill_delete_skill_create),
        <<"agent.delete_pipeline">> => ref(skill_delete_pipeline_create)
    }).

-spec pipeline_type() -> hocon_schema:schema().
pipeline_type() ->
    ref(pipeline).

-spec pipeline_step_type() -> hocon_schema:schema().
pipeline_step_type() ->
    emqx_schema:mkunion(<<"type">>, #{
        <<"call_skill">> => ref(pipeline_step_call_skill),
        <<"llm_loop">> => ref(pipeline_step_llm_loop),
        <<"wait_for_event">> => ref(pipeline_step_wait_for_event),
        <<"break">> => ref(pipeline_step_break)
    }).

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
enum(Values) -> hoconsc:enum(Values).
value_type() -> hoconsc:union([binary(), integer(), float(), boolean()]).

%%--------------------------------------------------------------------
%% JSON Schema converter / validator
%%--------------------------------------------------------------------

json_schema_from_string(undefined, _Opts) ->
    undefined;
json_schema_from_string(Str, _Opts) when is_binary(Str) ->
    case emqx_utils_json:safe_decode(Str) of
        {ok, Map} when is_map(Map) ->
            Map;
        {ok, _} ->
            throw({invalid_json_schema, "JSON schema must decode to an object"});
        {error, Reason} ->
            throw({invalid_json, Reason})
    end;
json_schema_from_string(Map, _Opts) when is_map(Map) ->
    Map.

validate_oai_schema(undefined) ->
    ok;
validate_oai_schema(Schema) when is_map(Schema) ->
    case emqx_agent_oai_tool_schema:validate_schema(Schema, root) of
        ok -> ok;
        {error, Errors} -> {error, format_schema_errors(Errors)}
    end.

validate_oai_schema_field(undefined) ->
    ok;
validate_oai_schema_field(Schema) when is_map(Schema) ->
    case emqx_agent_oai_tool_schema:validate_schema(Schema, field) of
        ok -> ok;
        {error, Errors} -> {error, format_schema_errors(Errors)}
    end.

format_schema_errors(Errors) ->
    iolist_to_binary([
        "Invalid JSON Schema: ",
        lists:join("; ", [format_schema_error(E) || E <- Errors])
    ]).

format_schema_error(#{error := Code, path := Path} = E) ->
    Extra = maps:without([error, path], E),
    Base = io_lib:format("~s at ~s", [Code, path_to_binary(Path)]),
    case maps:size(Extra) of
        0 -> Base;
        _ -> [Base, io_lib:format(" (~p)", [Extra])]
    end.

path_to_binary(Path) ->
    iolist_to_binary(lists:join("/", [to_binary(P) || P <- Path])).

to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> integer_to_binary(I).
