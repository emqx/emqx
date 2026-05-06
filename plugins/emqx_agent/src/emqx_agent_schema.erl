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
    pipeline_type/0
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
            mk(map(), #{
                required => false,
                desc => ?DESC(skill_entry_input_schema)
            })},
        {output_schema,
            mk(map(), #{
                required => false,
                desc => ?DESC(skill_entry_output_schema)
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
                desc => ?DESC(skill_input_schema)
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
            mk(map(), #{
                required => false,
                default => #{},
                desc => ?DESC(skill_http_headers)
            })},
        {input_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_input_schema)
            })},
        {output_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_output_schema)
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
                desc => ?DESC(skill_request_payload_schema)
            })},
        {response_schema,
            mk(map(), #{
                required => false,
                desc => ?DESC(skill_response_schema)
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
            })},
        {arg_keys,
            mk(hoconsc:array(binary()), #{
                required => false,
                default => [],
                desc => ?DESC(skill_pg_arg_keys)
            })},
        {input_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_input_schema)
            })},
        {output_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_output_schema)
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
            mk(hoconsc:array(map()), #{
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

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
enum(Values) -> hoconsc:enum(Values).
