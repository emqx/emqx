%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Hocon schema module for emqx_agent REST API.
%%
%% Defines three resource families:
%%   Skills         — cap-layer skill instances (message.publish, http, kv, postgresql.query)
%%   Session Profiles — LLM connection settings reusable across llm_loop steps
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
    session_profile_type/0,
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
fields(skill_kv_lookup_create) ->
    [
        {type,
            mk(enum(['kv.lookup']), #{
                required => true,
                desc => ?DESC(skill_kv_type)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_kv_desc)
            })},
        {data_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_kv_data_schema)
            })}
    ];
fields(skill_kv_put_create) ->
    [
        {type,
            mk(enum(['kv.put']), #{
                required => true,
                desc => ?DESC(skill_kv_type)
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_id)
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => ?DESC(skill_kv_desc)
            })},
        {data_schema,
            mk(map(), #{
                required => true,
                desc => ?DESC(skill_kv_data_schema)
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
%%--------------------------------------------------------------------
%% fields/1 — session profile
%%--------------------------------------------------------------------

fields(session_profile) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(session_profile_name)
            })},
        {api_key,
            mk(binary(), #{
                required => true,
                desc => ?DESC(session_profile_api_key)
            })},
        {base_url,
            mk(binary(), #{
                required => true,
                desc => ?DESC(session_profile_base_url)
            })},
        {model,
            mk(binary(), #{
                required => true,
                desc => ?DESC(session_profile_model)
            })},
        {instructions,
            mk(binary(), #{
                required => false,
                default => <<"You are a helpful assistant.">>,
                desc => ?DESC(session_profile_instructions)
            })},
        {output_schema,
            mk(map(), #{
                required => false,
                default => #{<<"type">> => <<"object">>},
                desc => ?DESC(session_profile_output_schema)
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
desc(skill_kv_lookup_create) -> ?DESC(skill_kv_create);
desc(skill_kv_put_create) -> ?DESC(skill_kv_create);
desc(skill_mqtt_request_create) -> ?DESC(skill_mqtt_request_create);
desc(skill_postgresql_create) -> ?DESC(skill_postgresql_create);
desc(session_profile) -> ?DESC(session_profile);
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
    hoconsc:union([
        ref(skill_publish_create),
        ref(skill_mqtt_request_create),
        ref(skill_http_create),
        ref(skill_kv_lookup_create),
        ref(skill_kv_put_create),
        ref(skill_postgresql_create)
    ]).

-spec session_profile_type() -> hocon_schema:schema().
session_profile_type() ->
    ref(session_profile).

-spec pipeline_type() -> hocon_schema:schema().
pipeline_type() ->
    ref(pipeline).

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
enum(Values) -> hoconsc:enum(Values).
