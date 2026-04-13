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
%%   postgresql.query — PostgreSQL query
%%
%% For GET/DELETE, use the actual registry type in the :type URL segment
%% (kv.lookup, kv.put, message.publish, http, postgresql.query).
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
    '/agent/apple-box/ui'/2,
    '/agent/apple-box/img/:file'/2,
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
        "/agent/apple-box/ui",
        "/agent/apple-box/img/:file",
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
schema("/agent/apple-box/ui") ->
    #{
        'operationId' => '/agent/apple-box/ui',
        get => #{
            tags => ?TAGS,
            security => [],
            description => ?DESC(apple_box_ui_get),
            responses => #{200 => <<"HTML page">>}
        }
    };
schema("/agent/apple-box/img/:file") ->
    #{
        'operationId' => '/agent/apple-box/img/:file',
        get => #{
            tags => ?TAGS,
            security => [],
            description => ?DESC(apple_box_img_get),
            responses => #{200 => <<"Image file">>}
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
        put => #{
            tags => ?TAGS,
            description => ?DESC(skill_put),
            parameters => [skill_type_param(), skill_id_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_agent_schema:skill_create_type(),
                skill_create_example()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_agent_schema:skill_entry_type(),
                    skill_entry_example()
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], ?DESC(skill_bad_request)
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
    serve_html("index.html").

'/agent/apple-box/ui'(get, _Params) ->
    serve_html("apple-box.html").

'/agent/apple-box/img/:file'(get, #{bindings := #{file := File}}) ->
    PrivDir = code:priv_dir(emqx_agent),
    ImgFile = filename:join([PrivDir, "img", File]),
    case file:read_file(ImgFile) of
        {ok, Data} ->
            CT =
                case filename:extension(File) of
                    <<".png">> -> <<"image/png">>;
                    <<".jpg">> -> <<"image/jpeg">>;
                    <<".jpeg">> -> <<"image/jpeg">>;
                    _ -> <<"application/octet-stream">>
                end,
            {200, #{<<"content-type">> => CT}, Data};
        {error, _} ->
            {404, #{}, <<"not found">>}
    end.

serve_html(Filename) ->
    PrivDir = code:priv_dir(emqx_agent),
    HtmlFile = filename:join(PrivDir, Filename),
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
                <<"Unknown skill type. Valid types: message.publish, http, kv, postgresql.query">>
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
'/agent/skills/:type/:id'(put, #{bindings := #{type := Type, id := Id}, body := Body}) ->
    %% Force the id from the URL into the body so the registry key stays consistent.
    Body2 = Body#{<<"id">> => Id},
    case do_create_skill(Body2) of
        ok ->
            {ok, Skill} = emqx_agent_skill_registry:lookup(Type, Id),
            ?OK(skill_to_map(Skill));
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(<<"Missing required field: ", Field/binary>>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/skills/:type/:id'(delete, #{bindings := #{type := Type, id := Id}}) ->
    case find_and_destroy_skill(Type, Id) of
        not_found -> ?NOT_FOUND(<<"Skill not found">>);
        found -> ?NO_CONTENT
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

%% Parses and validates the raw API body using the hocon schema (same pattern as
%% emqx_mq_config:mq_from_raw_post/1), then delegates to the skill module.
do_create_skill(#{<<"type">> := Type} = Body) ->
    case skill_schema_for_type(Type) of
        unknown ->
            {error, unknown_type};
        SchemaRef ->
            Schema = #{roots => [{skill, SchemaRef}]},
            try hocon_tconf:check_plain(Schema, #{<<"skill">> => Body}, #{atom_key => true}) of
                #{skill := Ctx} ->
                    %% Rename id → skill_id (API uses "id"; internals use skill_id).
                    Ctx2 = maps:put(skill_id, maps:get(id, Ctx), maps:remove(id, Ctx)),
                    Mod = skill_module_for_type(Type),
                    Mod:create(Ctx2)
            catch
                throw:Error ->
                    {error, Error}
            end
    end;
do_create_skill(_Body) ->
    {error, {missing_field, <<"type">>}}.

%% Maps the API type string to the hocon schema ref used for validation/coercion.
skill_schema_for_type(<<"message.publish">>) ->
    hoconsc:ref(emqx_agent_schema, skill_publish_create);
skill_schema_for_type(<<"message.request">>) ->
    hoconsc:ref(emqx_agent_schema, skill_mqtt_request_create);
skill_schema_for_type(<<"http">>) ->
    hoconsc:ref(emqx_agent_schema, skill_http_create);
skill_schema_for_type(<<"kv.lookup">>) ->
    hoconsc:ref(emqx_agent_schema, skill_kv_lookup_create);
skill_schema_for_type(<<"kv.put">>) ->
    hoconsc:ref(emqx_agent_schema, skill_kv_put_create);
skill_schema_for_type(<<"postgresql.query">>) ->
    hoconsc:ref(emqx_agent_schema, skill_postgresql_create);
skill_schema_for_type(_) ->
    unknown.

%%--------------------------------------------------------------------
%% Internal — Skill deletion dispatch
%%--------------------------------------------------------------------

%% Checks whether the skill exists and, if so, destroys it.
%% Returns `found` or `not_found`.
find_and_destroy_skill(Type, Id) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {error, not_found} ->
            not_found;
        {ok, _} ->
            ok = do_destroy_skill(Type, Id),
            found
    end.

do_destroy_skill(<<"message.publish">>, Id) -> emqx_agent_skill_publish:destroy(Id);
do_destroy_skill(<<"message.request">>, Id) -> emqx_agent_skill_mqtt_request:destroy(Id);
do_destroy_skill(<<"http">>, Id) -> emqx_agent_skill_http:destroy(Id);
do_destroy_skill(<<"kv.lookup">>, Id) -> emqx_agent_skill_kv:destroy_lookup(Id);
do_destroy_skill(<<"kv.put">>, Id) -> emqx_agent_skill_kv:destroy_put(Id);
do_destroy_skill(<<"postgresql.query">>, Id) -> emqx_agent_skill_postgresql:destroy(Id).

%%--------------------------------------------------------------------
%% Internal — Response helpers
%%--------------------------------------------------------------------

skill_to_map(#{type := Type} = Skill) ->
    Mod = skill_module(Type),
    Mod:to_map(Skill).

skill_module(<<"http">>) -> emqx_agent_skill_http;
skill_module(<<"message.publish">>) -> emqx_agent_skill_publish;
skill_module(<<"message.request">>) -> emqx_agent_skill_mqtt_request;
skill_module(<<"kv.lookup">>) -> emqx_agent_skill_kv;
skill_module(<<"kv.put">>) -> emqx_agent_skill_kv;
skill_module(<<"postgresql.query">>) -> emqx_agent_skill_postgresql.

%% Used by do_create_skill — kv.lookup and kv.put are independent first-class types.
skill_module_for_type(<<"http">>) -> emqx_agent_skill_http;
skill_module_for_type(<<"message.publish">>) -> emqx_agent_skill_publish;
skill_module_for_type(<<"message.request">>) -> emqx_agent_skill_mqtt_request;
skill_module_for_type(<<"kv.lookup">>) -> emqx_agent_skill_kv;
skill_module_for_type(<<"kv.put">>) -> emqx_agent_skill_kv;
skill_module_for_type(<<"postgresql.query">>) -> emqx_agent_skill_postgresql;
skill_module_for_type(_) -> unknown.

field_to_str(F) when is_binary(F) -> F;
field_to_str(F) when is_atom(F) -> atom_to_binary(F, utf8).

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
        <<"api_key">> => <<"OPENAI_API_KEY">>,
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
                    <<"postgresql.query@pg-default">>,
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
                <<"id">> => <<"stop_if_not_assigned">>,
                <<"type">> => <<"break">>,
                <<"path">> => <<"$.event.data.is_assigned">>,
                <<"not">> => true
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
