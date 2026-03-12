%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% REST API for the agent subsystem.
%%
%% Resources:
%%   /agent/skills                   — list / create skill instances
%%   /agent/skills/:type/:id         — get / delete a skill instance
%%   /agent/session_profiles         — list / create session profiles
%%   /agent/session_profiles/:name   — get / update / delete a profile
%%   /agent/pipelines                — list / create pipeline definitions
%%   /agent/pipelines/:id            — get / update / delete a pipeline
%%
%% Skill types accepted on POST:
%%   message.publish  — MQTT publish capability scoped to a topic prefix
%%   http             — HTTP call capability
%%   kv               — Key-value store (creates kv.lookup + optionally kv.put)
%%   clickhouse.history — ClickHouse time-series query
%%
%% For GET/DELETE, use the actual registry type in the :type URL segment
%% (kv.lookup, kv.put, message.publish, http, clickhouse.history).
%% Deleting a kv.lookup or kv.put entry removes both variants.

-module(emqx_agent_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

%% minirest_api callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% Handler callbacks
-export([
    '/agent/ui'/2,
    '/agent/skills'/2,
    '/agent/skills/:type/:id'/2,
    '/agent/session_profiles'/2,
    '/agent/session_profiles/:name'/2,
    '/agent/pipelines'/2,
    '/agent/pipelines/:id'/2
]).

-define(TAGS, [<<"Agent">>]).

namespace() -> "agent".

%%--------------------------------------------------------------------
%% minirest_api — spec / paths
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/agent/ui",
        "/agent/skills",
        "/agent/skills/:type/:id",
        "/agent/session_profiles",
        "/agent/session_profiles/:name",
        "/agent/pipelines",
        "/agent/pipelines/:id"
    ].

%%--------------------------------------------------------------------
%% Schema definitions (Swagger)
%%--------------------------------------------------------------------

schema("/agent/ui") ->
    #{
        'operationId' => '/agent/ui',
        get => #{
            tags => ?TAGS,
            security => [],
            description => ?DESC(ui_get),
            responses => #{200 => <<"HTML page">>}
        }
    };
schema("/agent/skills") ->
    #{
        'operationId' => '/agent/skills',
        get => #{
            tags => ?TAGS,
            description => ?DESC(skills_list),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_agent_schema:skill_entry_type()),
                    skills_list_example()
                )
            }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC(skills_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:skill_create_type(),
                skill_create_example()
            ),
            responses => #{
                201 => <<"Skill instance registered">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(skill_bad_request)
                )
            }
        }
    };
schema("/agent/skills/:type/:id") ->
    #{
        'operationId' => '/agent/skills/:type/:id',
        get => #{
            tags => ?TAGS,
            description => ?DESC(skill_get),
            parameters => [skill_type_param(), skill_id_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:skill_entry_type(),
                    skill_entry_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC(skill_not_found))
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(skill_delete),
            parameters => [skill_type_param(), skill_id_param()],
            responses => #{
                204 => <<"Skill instance removed">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC(skill_not_found))
            }
        }
    };
schema("/agent/session_profiles") ->
    #{
        'operationId' => '/agent/session_profiles',
        get => #{
            tags => ?TAGS,
            description => ?DESC(session_profiles_list),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_agent_schema:session_profile_type()),
                    session_profiles_list_example()
                )
            }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC(session_profiles_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:session_profile_type(),
                session_profile_example()
            ),
            responses => #{
                201 => <<"Profile created">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(profile_bad_request)
                )
            }
        }
    };
schema("/agent/session_profiles/:name") ->
    #{
        'operationId' => '/agent/session_profiles/:name',
        get => #{
            tags => ?TAGS,
            description => ?DESC(session_profile_get),
            parameters => [profile_name_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:session_profile_type(),
                    session_profile_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(profile_not_found)
                )
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(session_profile_put),
            parameters => [profile_name_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:session_profile_type(),
                session_profile_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:session_profile_type(),
                    session_profile_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(profile_bad_request)
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(session_profile_delete),
            parameters => [profile_name_param()],
            responses => #{
                204 => <<"Profile deleted">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(profile_not_found)
                )
            }
        }
    };
schema("/agent/pipelines") ->
    #{
        'operationId' => '/agent/pipelines',
        get => #{
            tags => ?TAGS,
            description => ?DESC(pipelines_list),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_agent_schema:pipeline_type()),
                    pipelines_list_example()
                )
            }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC(pipelines_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:pipeline_type(),
                pipeline_example()
            ),
            responses => #{
                201 => <<"Pipeline registered">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(pipeline_bad_request)
                )
            }
        }
    };
schema("/agent/pipelines/:id") ->
    #{
        'operationId' => '/agent/pipelines/:id',
        get => #{
            tags => ?TAGS,
            description => ?DESC(pipeline_get),
            parameters => [pipeline_id_param()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:pipeline_type(),
                    pipeline_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(pipeline_not_found)
                )
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(pipeline_put),
            parameters => [pipeline_id_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:pipeline_type(),
                pipeline_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:pipeline_type(),
                    pipeline_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(pipeline_bad_request)
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(pipeline_delete),
            parameters => [pipeline_id_param()],
            responses => #{
                204 => <<"Pipeline removed">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], ?DESC(pipeline_not_found)
                )
            }
        }
    }.

%%--------------------------------------------------------------------
%% Handler — UI
%%--------------------------------------------------------------------

'/agent/ui'(get, _Params) ->
    PrivDir = code:priv_dir(emqx_agent),
    HtmlFile = filename:join(PrivDir, "index.html"),
    case file:read_file(HtmlFile) of
        {ok, Html} ->
            {200, #{<<"content-type">> => <<"text/html; charset=utf-8">>}, Html};
        {error, Reason} ->
            ?INTERNAL_ERROR(iolist_to_binary(io_lib:format("Cannot read UI: ~p", [Reason])))
    end.

%%--------------------------------------------------------------------
%% Handlers — Skills
%%--------------------------------------------------------------------

'/agent/skills'(get, _Params) ->
    Skills = emqx_agent_skill_registry:list(),
    ?OK([skill_to_map(S) || S <- Skills]);
'/agent/skills'(post, #{body := Body}) ->
    case do_create_skill(Body) of
        ok ->
            ?CREATED(#{});
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(
                iolist_to_binary(["Missing required field: ", field_to_str(Field)])
            );
        {error, unknown_type} ->
            ?BAD_REQUEST(
                <<"Unknown skill type. Valid types: message.publish, http, kv, clickhouse.history">>
            );
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/skills/:type/:id'(get, #{bindings := #{type := Type, id := Id}}) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {ok, Skill} ->
            ?OK(skill_to_map(Skill));
        {error, not_found} ->
            ?NOT_FOUND(<<"Skill not found">>)
    end;
'/agent/skills/:type/:id'(delete, #{bindings := #{type := Type, id := Id}}) ->
    ModType = module_type(Type),
    case find_skill_for_delete(ModType, Type, Id) of
        not_found ->
            ?NOT_FOUND(<<"Skill not found">>);
        found ->
            ok = do_destroy_skill(ModType, Id),
            ?NO_CONTENT
    end.

%%--------------------------------------------------------------------
%% Handlers — Session Profiles
%%--------------------------------------------------------------------

'/agent/session_profiles'(get, _Params) ->
    ?OK(emqx_agent_pipeline_registry:list_profiles());
'/agent/session_profiles'(post, #{body := Body}) ->
    case maps:get(<<"name">>, Body, undefined) of
        undefined ->
            ?BAD_REQUEST(<<"Missing required field: name">>);
        Name ->
            ok = emqx_agent_pipeline_registry:register_profile(Name, Body),
            ?CREATED(#{})
    end.

'/agent/session_profiles/:name'(get, #{bindings := #{name := Name}}) ->
    case emqx_agent_pipeline_registry:lookup_profile(Name) of
        {ok, Profile} -> ?OK(Profile);
        {error, not_found} -> ?NOT_FOUND(<<"Session profile not found">>)
    end;
'/agent/session_profiles/:name'(put, #{bindings := #{name := Name}, body := Body}) ->
    case maps:get(<<"name">>, Body, Name) of
        _ ->
            Body2 = Body#{<<"name">> => Name},
            ok = emqx_agent_pipeline_registry:register_profile(Name, Body2),
            ?OK(Body2)
    end;
'/agent/session_profiles/:name'(delete, #{bindings := #{name := Name}}) ->
    case emqx_agent_pipeline_registry:lookup_profile(Name) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Session profile not found">>);
        {ok, _} ->
            ok = emqx_agent_pipeline_registry:unregister_profile(Name),
            ?NO_CONTENT
    end.

%%--------------------------------------------------------------------
%% Handlers — Pipelines
%%--------------------------------------------------------------------

'/agent/pipelines'(get, _Params) ->
    ?OK(emqx_agent_pipeline_registry:list());
'/agent/pipelines'(post, #{body := Body}) ->
    case maps:get(<<"pipeline_id">>, Body, undefined) of
        undefined ->
            ?BAD_REQUEST(<<"Missing required field: pipeline_id">>);
        _ ->
            case emqx_agent_pipeline_registry:register(Body) of
                ok ->
                    ?CREATED(#{});
                {error, Reason} ->
                    ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
            end
    end.

'/agent/pipelines/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_agent_pipeline_registry:lookup(Id) of
        {ok, Pipeline} -> ?OK(Pipeline);
        {error, not_found} -> ?NOT_FOUND(<<"Pipeline not found">>)
    end;
'/agent/pipelines/:id'(put, #{bindings := #{id := Id}, body := Body}) ->
    Body2 = Body#{<<"pipeline_id">> => Id},
    case emqx_agent_pipeline_registry:register(Body2) of
        ok ->
            ?OK(Body2);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/pipelines/:id'(delete, #{bindings := #{id := Id}}) ->
    case emqx_agent_pipeline_registry:lookup(Id) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Pipeline not found">>);
        {ok, _} ->
            ok = emqx_agent_pipeline_registry:unregister(Id),
            ?NO_CONTENT
    end.

%%--------------------------------------------------------------------
%% Internal — Skill creation dispatch
%%--------------------------------------------------------------------

do_create_skill(#{
    <<"type">> := <<"message.publish">>,
    <<"id">> := Id,
    <<"desc">> := Desc,
    <<"topic_prefix">> := TopicPrefix
}) ->
    emqx_agent_skill_publish:create(#{
        skill_id => Id,
        desc => Desc,
        topic_prefix => TopicPrefix
    });
do_create_skill(#{<<"type">> := <<"message.publish">>} = Body) ->
    {error, {missing_field, first_missing(Body, [<<"id">>, <<"desc">>, <<"topic_prefix">>])}};
do_create_skill(
    #{
        <<"type">> := <<"http">>,
        <<"id">> := Id,
        <<"desc">> := Desc,
        <<"method">> := Method,
        <<"url">> := Url,
        <<"input_schema">> := InputSchema,
        <<"output_schema">> := OutputSchema
    } = Body
) ->
    emqx_agent_skill_http:create(#{
        skill_id => Id,
        desc => Desc,
        method => parse_http_method(Method),
        url => Url,
        headers => maps:get(<<"headers">>, Body, #{}),
        input_schema => InputSchema,
        output_schema => OutputSchema
    });
do_create_skill(#{<<"type">> := <<"http">>} = Body) ->
    {error,
        {missing_field,
            first_missing(Body, [
                <<"id">>,
                <<"desc">>,
                <<"method">>,
                <<"url">>,
                <<"input_schema">>,
                <<"output_schema">>
            ])}};
do_create_skill(
    #{
        <<"type">> := <<"kv">>,
        <<"id">> := Id,
        <<"desc">> := Desc,
        <<"data_schema">> := DataSchema
    } = Body
) ->
    emqx_agent_skill_kv:create(#{
        skill_id => Id,
        desc => Desc,
        data_schema => DataSchema,
        allow_put => maps:get(<<"allow_put">>, Body, false)
    });
do_create_skill(#{<<"type">> := <<"kv">>} = Body) ->
    {error, {missing_field, first_missing(Body, [<<"id">>, <<"desc">>, <<"data_schema">>])}};
do_create_skill(#{
    <<"type">> := <<"clickhouse.history">>,
    <<"id">> := Id,
    <<"desc">> := Desc,
    <<"query">> := Query,
    <<"input_schema">> := InputSchema,
    <<"output_schema">> := OutputSchema
}) ->
    emqx_agent_skill_clickhouse:create(#{
        skill_id => Id,
        desc => Desc,
        query => Query,
        input_schema => InputSchema,
        output_schema => OutputSchema
    });
do_create_skill(#{<<"type">> := <<"clickhouse.history">>} = Body) ->
    {error,
        {missing_field,
            first_missing(Body, [
                <<"id">>, <<"desc">>, <<"query">>, <<"input_schema">>, <<"output_schema">>
            ])}};
do_create_skill(#{<<"type">> := _}) ->
    {error, unknown_type};
do_create_skill(_Body) ->
    {error, {missing_field, type}}.

%% Returns the first field in `Required` that is absent from `Body`.
first_missing(Body, Required) ->
    case [F || F <- Required, not maps:is_key(F, Body)] of
        [First | _] -> First;
        [] -> unknown
    end.

%%--------------------------------------------------------------------
%% Internal — Skill deletion dispatch
%%--------------------------------------------------------------------

%% Maps the URL :type to the module responsible for destroy/1.
module_type(<<"message.publish">>) -> publish;
module_type(<<"http">>) -> http;
module_type(<<"kv.lookup">>) -> kv;
module_type(<<"kv.put">>) -> kv;
module_type(<<"clickhouse.history">>) -> clickhouse;
module_type(_) -> unknown.

%% Checks whether the skill exists before attempting deletion.
find_skill_for_delete(kv, _Type, Id) ->
    %% KV creates two entries; lookup either one.
    case emqx_agent_skill_registry:lookup(<<"kv.lookup">>, Id) of
        {ok, _} ->
            found;
        {error, not_found} ->
            case emqx_agent_skill_registry:lookup(<<"kv.put">>, Id) of
                {ok, _} -> found;
                {error, not_found} -> not_found
            end
    end;
find_skill_for_delete(unknown, _Type, _Id) ->
    not_found;
find_skill_for_delete(_ModType, Type, Id) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {ok, _} -> found;
        {error, not_found} -> not_found
    end.

do_destroy_skill(publish, Id) -> emqx_agent_skill_publish:destroy(Id);
do_destroy_skill(http, Id) -> emqx_agent_skill_http:destroy(Id);
do_destroy_skill(kv, Id) -> emqx_agent_skill_kv:destroy(Id);
do_destroy_skill(clickhouse, Id) -> emqx_agent_skill_clickhouse:destroy(Id);
do_destroy_skill(unknown, _Id) -> ok.

%%--------------------------------------------------------------------
%% Internal — Response helpers
%%--------------------------------------------------------------------

%% Converts a registry skill map (atom keys) to a JSON-friendly map (binary keys).
%% The `context` field is stripped — it may contain internal or sensitive details.
skill_to_map(Skill) ->
    Picked = maps:with(
        [skill_id, type, display_name, description, input_schema, output_schema], Skill
    ),
    maps:fold(
        fun(K, V, Acc) -> Acc#{atom_to_binary(K, utf8) => V} end,
        #{},
        Picked
    ).

field_to_str(F) when is_binary(F) -> F;
field_to_str(F) when is_atom(F) -> atom_to_binary(F, utf8).

parse_http_method(<<"get">>) -> get;
parse_http_method(<<"post">>) -> post;
parse_http_method(<<"put">>) -> put;
parse_http_method(<<"patch">>) -> patch;
parse_http_method(<<"delete">>) -> delete;
parse_http_method(M) -> {error, {invalid_method, M}}.

%%--------------------------------------------------------------------
%% Internal — URL parameters
%%--------------------------------------------------------------------

skill_type_param() ->
    {type,
        hoconsc:mk(binary(), #{
            required => true,
            in => path,
            desc => ?DESC(param_skill_type)
        })}.

skill_id_param() ->
    {id,
        hoconsc:mk(binary(), #{
            required => true,
            in => path,
            desc => ?DESC(param_skill_id)
        })}.

profile_name_param() ->
    {name,
        hoconsc:mk(binary(), #{
            required => true,
            in => path,
            desc => ?DESC(param_profile_name)
        })}.

pipeline_id_param() ->
    {id,
        hoconsc:mk(binary(), #{
            required => true,
            in => path,
            desc => ?DESC(param_pipeline_id)
        })}.

%%--------------------------------------------------------------------
%% Internal — Swagger examples
%%--------------------------------------------------------------------

skills_list_example() ->
    [skill_entry_example()].

skill_entry_example() ->
    #{
        <<"id">> => <<"slack-prod">>,
        <<"type">> => <<"message.publish">>,
        <<"display_name">> => <<"Slack Prod — Publish">>,
        <<"description">> => <<"Publish an MQTT message to a topic under the prefix: slack/prod/">>,
        <<"input_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"topic">> => #{<<"type">> => <<"string">>},
                <<"payload">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"topic">>, <<"payload">>]
        },
        <<"output_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}}
        }
    }.

skill_create_example() ->
    #{
        <<"type">> => <<"message.publish">>,
        <<"id">> => <<"slack-prod">>,
        <<"desc">> => <<"Publish messages to the Slack MQTT bridge">>,
        <<"topic_prefix">> => <<"slack/prod/">>
    }.

session_profiles_list_example() ->
    [session_profile_example()].

session_profile_example() ->
    #{
        <<"name">> => <<"hvac_triage_v1">>,
        <<"api_key">> => <<"sk-...">>,
        <<"base_url">> => <<"https://api.openai.com/v1">>,
        <<"model">> => <<"gpt-4o">>,
        <<"instructions">> =>
            <<"You are an HVAC triage expert. Diagnose anomalies and create ServiceNow incidents.">>,
        <<"output_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"incident_id">> => #{<<"type">> => <<"string">>},
                <<"severity">> => #{<<"type">> => <<"integer">>}
            }
        }
    }.

pipelines_list_example() ->
    [pipeline_example()].

pipeline_example() ->
    #{
        <<"pipeline_id">> => <<"hvac_incident_remediation">>,
        <<"trigger">> => #{<<"topic">> => <<"evt/hvac/anomaly">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"diagnose">>,
                <<"type">> => <<"llm_loop">>,
                <<"session_profile">> => <<"hvac_triage_v1">>,
                <<"tools">> => [
                    <<"clickhouse.history@ch-default">>,
                    <<"servicenow.create_incident@sn-prod">>
                ],
                <<"input">> => #{<<"event">> => <<"$.event">>},
                <<"result_path">> => <<"$.triage">>
            },
            #{
                <<"id">> => <<"wait_assignment">>,
                <<"type">> => <<"wait_for_event">>,
                <<"topic">> => <<"evt/cloud/servicenow.incident_updated">>,
                <<"where">> => <<"data.incident_id == $.triage.result.incident_id">>
            },
            #{
                <<"id">> => <<"notify">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"message.publish@slack-prod">>,
                <<"args">> => #{
                    <<"topic">> => <<"facilities-alerts">>,
                    <<"payload">> => <<"$.triage.result.summary">>
                }
            }
        ]
    }.
