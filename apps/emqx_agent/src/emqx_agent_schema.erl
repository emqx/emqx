%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Hocon schema module for emqx_agent REST API.
%%
%% Defines three resource families:
%%   Skills         — cap-layer skill instances (message.publish, http, kv, clickhouse.history)
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
%% fields/1 — skill read response (what the registry stores, minus context)
%%--------------------------------------------------------------------

fields(skill_entry) ->
    [
        {id,
            mk(binary(), #{
                required => true,
                desc => <<"Skill instance ID">>
            })},
        {type,
            mk(binary(), #{
                required => true,
                desc => <<"Skill type identifier (e.g. message.publish, http, kv.lookup)">>
            })},
        {display_name,
            mk(binary(), #{
                required => false,
                desc => <<"Human-readable display name">>
            })},
        {description,
            mk(binary(), #{
                required => false,
                desc => <<"Description shown to the LLM as part of the tool manifest">>
            })},
        {input_schema,
            mk(map(), #{
                required => false,
                desc => <<"JSON Schema describing input arguments">>
            })},
        {output_schema,
            mk(map(), #{
                required => false,
                desc => <<"JSON Schema describing the skill response">>
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
                desc => <<"Skill type discriminator">>
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => <<"Unique skill instance ID">>
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => <<"Human-readable description">>
            })},
        {topic_prefix,
            mk(binary(), #{
                required => true,
                desc =>
                    <<
                        "MQTT topic prefix prepended to every agent-supplied topic suffix. "
                        "Constrains the skill to a specific namespace."
                    >>
            })}
    ];
fields(skill_http_create) ->
    [
        {type,
            mk(enum([http]), #{
                required => true,
                desc => <<"Skill type discriminator">>
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => <<"Unique skill instance ID">>
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => <<"Human-readable description">>
            })},
        {method,
            mk(enum([get, post, put, patch, delete]), #{
                required => true,
                desc => <<"HTTP method">>
            })},
        {url,
            mk(binary(), #{
                required => true,
                desc => <<"Base URL; query string appended for GET, JSON body for others">>
            })},
        {headers,
            mk(map(), #{
                required => false,
                default => #{},
                desc => <<"Static HTTP headers sent with every request">>
            })},
        {input_schema,
            mk(map(), #{
                required => true,
                desc => <<"JSON Schema for the skill input arguments">>
            })},
        {output_schema,
            mk(map(), #{
                required => true,
                desc => <<"JSON Schema for the skill response">>
            })}
    ];
fields(skill_kv_create) ->
    [
        {type,
            mk(enum([kv]), #{
                required => true,
                desc =>
                    <<
                        "Skill type discriminator. Creates kv.lookup (always) and "
                        "kv.put (when allow_put is true)."
                    >>
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => <<"Unique skill instance ID">>
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => <<"Human-readable description of the stored objects">>
            })},
        {data_schema,
            mk(map(), #{
                required => true,
                desc => <<"JSON Schema for the stored value type">>
            })},
        {allow_put,
            mk(boolean(), #{
                required => false,
                default => false,
                desc => <<"When true, a kv.put skill is registered alongside kv.lookup">>
            })}
    ];
fields(skill_clickhouse_create) ->
    [
        {type,
            mk(enum(['clickhouse.history']), #{
                required => true,
                desc => <<"Skill type discriminator">>
            })},
        {id,
            mk(binary(), #{
                required => true,
                desc => <<"Unique skill instance ID">>
            })},
        {desc,
            mk(binary(), #{
                required => true,
                desc => <<"Human-readable description">>
            })},
        {query,
            mk(binary(), #{
                required => true,
                desc => <<"ClickHouse SQL template; use ${variable} placeholders for input args">>
            })},
        {input_schema,
            mk(map(), #{
                required => true,
                desc => <<"JSON Schema for query parameters">>
            })},
        {output_schema,
            mk(map(), #{
                required => true,
                desc => <<"JSON Schema for query results">>
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
                desc => <<"Profile name, used in llm_loop step session_profile field">>
            })},
        {api_key,
            mk(binary(), #{
                required => true,
                desc => <<"LLM provider API key">>
            })},
        {base_url,
            mk(binary(), #{
                required => true,
                desc => <<"LLM API base URL (e.g. https://api.openai.com/v1)">>
            })},
        {model,
            mk(binary(), #{
                required => true,
                desc => <<"Model identifier (e.g. gpt-4o, llama3)">>
            })},
        {instructions,
            mk(binary(), #{
                required => false,
                default => <<"You are a helpful assistant.">>,
                desc => <<"System prompt injected at the start of every session">>
            })},
        {output_schema,
            mk(map(), #{
                required => false,
                default => #{<<"type">> => <<"object">>},
                desc => <<"JSON Schema constraining the final LLM response structure">>
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
                desc => <<"Unique pipeline definition identifier">>
            })},
        {trigger,
            mk(ref(pipeline_trigger), #{
                required => true,
                desc => <<"Trigger configuration — defines which event activates this pipeline">>
            })},
        {steps,
            mk(hoconsc:array(map()), #{
                required => true,
                default => [],
                desc =>
                    <<
                        "Ordered list of step definitions. Each step has a type field: "
                        "call_skill | wait_for_event | llm_loop | branch."
                    >>
            })}
    ];
fields(pipeline_trigger) ->
    [
        {topic,
            mk(binary(), #{
                required => true,
                desc =>
                    <<
                        "MQTT topic filter (supports + and # wildcards) that activates "
                        "a new pipeline instance when a matching message is published"
                    >>
            })}
    ];
fields(_) ->
    [].

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
        ref(skill_http_create),
        ref(skill_kv_create),
        ref(skill_clickhouse_create)
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
