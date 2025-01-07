%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_http_api).

-behaviour(minirest_api).

-include("emqx_schema_registry.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

-export([
    namespace/0,
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    '/schema_registry'/2,
    '/schema_registry/:name'/2,
    '/schema_registry_external'/2,
    '/schema_registry_external/registry/:name'/2
]).

-define(TAGS, [<<"Schema Registry">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "schema_registry_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/schema_registry",
        "/schema_registry/:name",
        "/schema_registry_external",
        "/schema_registry_external/registry/:name"
    ].

schema("/schema_registry") ->
    #{
        'operationId' => '/schema_registry',
        get => #{
            tags => ?TAGS,
            summary => <<"List registered schemas">>,
            description => ?DESC("desc_schema_registry_api_list"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:array(emqx_schema_registry_schema:api_schema("get")),
                            #{
                                sample =>
                                    #{value => sample_list_schemas_response()}
                            }
                        )
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Register a new schema">>,
            description => ?DESC("desc_schema_registry_api_post"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_schema_registry_schema:api_schema("post"),
                post_examples()
            ),
            responses =>
                #{
                    201 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:api_schema("post"),
                            post_examples()
                        ),
                    400 => error_schema('ALREADY_EXISTS', "Schema already exists")
                }
        }
    };
schema("/schema_registry/:name") ->
    #{
        'operationId' => '/schema_registry/:name',
        get => #{
            tags => ?TAGS,
            summary => <<"Get registered schema">>,
            description => ?DESC("desc_schema_registry_api_get"),
            parameters => [param_path_schema_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:api_schema("get"),
                            get_examples()
                        ),
                    404 => error_schema('NOT_FOUND', "Schema not found")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update a schema">>,
            description => ?DESC("desc_schema_registry_api_put"),
            parameters => [param_path_schema_name()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_schema_registry_schema:api_schema("put"),
                put_examples()
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:api_schema("put"),
                            post_examples()
                        ),
                    404 => error_schema('NOT_FOUND', "Schema not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete registered schema">>,
            description => ?DESC("desc_schema_registry_api_delete"),
            parameters => [param_path_schema_name()],
            responses =>
                #{
                    204 => <<"Schema deleted">>,
                    404 => error_schema('NOT_FOUND', "Schema not found")
                }
        }
    };
schema("/schema_registry_external") ->
    #{
        'operationId' => '/schema_registry_external',
        get => #{
            tags => ?TAGS,
            summary => <<"List external registries">>,
            description => ?DESC("external_registry_list"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:map(
                                name, emqx_schema_registry_schema:external_registries_type()
                            ),
                            #{
                                sample =>
                                    #{value => sample_list_external_registries_response()}
                            }
                        )
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Add a new external registry">>,
            description => ?DESC("external_registry_create"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                hoconsc:union(fun create_external_registry_union/1),
                create_external_registry_input_examples(post)
            ),
            responses =>
                #{
                    201 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:external_registry_type(),
                            create_external_registry_input_examples(get)
                        ),
                    400 => error_schema('ALREADY_EXISTS', "Schema already exists")
                }
        }
    };
schema("/schema_registry_external/registry/:name") ->
    #{
        'operationId' => '/schema_registry_external/registry/:name',
        get => #{
            tags => ?TAGS,
            summary => <<"Lookup external registry">>,
            description => ?DESC("external_registry_lookup"),
            parameters => [param_path_external_registry_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:external_registry_type(),
                            create_external_registry_input_examples(put)
                        ),
                    404 => error_schema('NOT_FOUND', "Schema not found")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update external registry">>,
            description => ?DESC("external_registry_update"),
            parameters => [param_path_external_registry_name()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_schema_registry_schema:external_registry_type(),
                create_external_registry_input_examples(put)
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:external_registry_type(),
                            create_external_registry_input_examples(put)
                        ),
                    404 => error_schema('NOT_FOUND', "Schema not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete external registry">>,
            description => ?DESC("external_registry_delete"),
            parameters => [param_path_external_registry_name()],
            responses => #{204 => <<"Deleted">>}
        }
    }.

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

'/schema_registry'(get, _Params) ->
    Schemas = emqx_schema_registry:list_schemas(),
    Response =
        lists:map(
            fun({Name, Params}) ->
                Params#{name => Name}
            end,
            maps:to_list(Schemas)
        ),
    ?OK(Response);
'/schema_registry'(post, #{body := Params0 = #{<<"name">> := Name}}) ->
    try
        ok = emqx_resource:validate_name(Name),
        Params = maps:without([<<"name">>], Params0),
        case emqx_schema_registry:get_schema(Name) of
            {error, not_found} ->
                case emqx_schema_registry:add_schema(Name, Params) of
                    ok ->
                        {ok, Res} = emqx_schema_registry:get_schema(Name),
                        {201, Res#{name => Name}};
                    {error, Error} ->
                        ?BAD_REQUEST(Error)
                end;
            {ok, _} ->
                ?BAD_REQUEST('ALREADY_EXISTS', <<"Schema already exists">>)
        end
    catch
        throw:#{kind := Kind, reason := Reason} ->
            Msg0 = ?ERROR_MSG('BAD_REQUEST', Reason),
            Msg = Msg0#{kind => Kind},
            {400, Msg}
    end.

'/schema_registry/:name'(get, #{bindings := #{name := Name}}) ->
    case emqx_schema_registry:get_schema(Name) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Schema not found">>);
        {ok, Schema} ->
            ?OK(Schema#{name => Name})
    end;
'/schema_registry/:name'(put, #{bindings := #{name := Name}, body := Params}) ->
    case emqx_schema_registry:get_schema(Name) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Schema not found">>);
        {ok, _} ->
            case emqx_schema_registry:add_schema(Name, Params) of
                ok ->
                    {ok, Res} = emqx_schema_registry:get_schema(Name),
                    ?OK(Res#{name => Name});
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
    end;
'/schema_registry/:name'(delete, #{bindings := #{name := Name}}) ->
    case emqx_schema_registry:get_schema(Name) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Schema not found">>);
        {ok, _} ->
            case emqx_schema_registry:delete_schema(Name) of
                ok ->
                    ?NO_CONTENT;
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
    end.

%% External registries
'/schema_registry_external'(get, _Params) ->
    Registries0 = emqx_schema_registry_config:list_external_registries_raw(),
    Registries = maps:map(
        fun(_Name, Registry) -> emqx_utils:redact(Registry) end,
        Registries0
    ),
    ?OK(Registries);
'/schema_registry_external'(post, #{body := Params0 = #{<<"name">> := Name}}) ->
    Params = maps:remove(<<"name">>, Params0),
    with_external_registry(
        Name,
        fun() ->
            ?BAD_REQUEST(<<"External registry already exists">>)
        end,
        fun() ->
            case emqx_schema_registry_config:upsert_external_registry(Name, Params) of
                {ok, Registry} ->
                    ?CREATED(external_registry_out(Registry));
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end
    ).

'/schema_registry_external/registry/:name'(get, #{bindings := #{name := Name}}) ->
    with_external_registry(
        Name,
        fun(Registry) ->
            ?OK(external_registry_out(Registry))
        end,
        not_found()
    );
'/schema_registry_external/registry/:name'(put, #{bindings := #{name := Name}, body := Params}) ->
    with_external_registry(
        Name,
        fun() ->
            case emqx_schema_registry_config:upsert_external_registry(Name, Params) of
                {ok, Registry} ->
                    ?OK(external_registry_out(Registry));
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end,
        not_found()
    );
'/schema_registry_external/registry/:name'(delete, #{bindings := #{name := Name}}) ->
    with_external_registry(
        Name,
        fun() ->
            case emqx_schema_registry_config:delete_external_registry(Name) of
                ok ->
                    ?NO_CONTENT;
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end,
        fun() -> ?NO_CONTENT end
    ).

%%-------------------------------------------------------------------------------------------------
%% Examples
%%-------------------------------------------------------------------------------------------------

sample_list_schemas_response() ->
    [sample_get_schema_response(avro)].

sample_get_schema_response(avro) ->
    #{
        type => <<"avro">>,
        name => <<"my_avro_schema">>,
        description => <<"My Avro Schema">>,
        source => <<
            "{\"type\":\"record\","
            "\"name\":\"test\","
            "\"fields\":[{\"type\":\"int\",\"name\":\"i\"},"
            "{\"type\":\"string\",\"name\":\"s\"}]}"
        >>
    }.

sample_get_schema_response(avro, put) ->
    maps:without([name], sample_get_schema_response(avro));
sample_get_schema_response(avro, _Method) ->
    sample_get_schema_response(avro).

put_examples() ->
    example_template(put).

post_examples() ->
    example_template(post).

get_examples() ->
    example_template(get).

example_template(Method) ->
    #{
        <<"avro_schema">> =>
            #{
                summary => <<"Avro">>,
                value => sample_get_schema_response(avro, Method)
            }
    }.

sample_list_external_registries_response() ->
    #{<<"my_registry">> => sample_get_external_registry_response(confluent)}.

sample_get_external_registry_response(confluent) ->
    #{
        type => <<"confluent">>,
        name => <<"test">>,
        url => <<"http://confluent_schema_registry:8081">>,
        auth => #{
            mechanism => <<"basic">>,
            username => <<"cpsruser">>,
            password => <<"******">>
        }
    }.

sample_get_external_registry_response(confluent, put) ->
    maps:without([name], sample_get_external_registry_response(confluent));
sample_get_external_registry_response(confluent, _Method) ->
    sample_get_external_registry_response(confluent).

create_external_registry_input_examples(Method) ->
    #{
        <<"confluent">> =>
            #{
                summary => <<"Confluent">>,
                value => sample_get_external_registry_response(confluent, Method)
            }
    }.

%%-------------------------------------------------------------------------------------------------
%% Schemas and hocon types
%%-------------------------------------------------------------------------------------------------

param_path_schema_name() ->
    {name,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_schema">>,
                desc => ?DESC("desc_param_path_schema_name")
            }
        )}.

param_path_external_registry_name() ->
    {name,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_registry">>,
                desc => ?DESC("param_path_external_registry_name")
            }
        )}.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

error_schema(Code, Message) when is_atom(Code) ->
    error_schema([Code], Message);
error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_list(Codes) andalso is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

external_registry_out(Registry) ->
    emqx_utils:redact(Registry).

create_external_registry_union(all_union_members) ->
    ?UNION(UnionFn) = emqx_schema_registry_schema:external_registry_type(),
    Refs = UnionFn(all_union_members),
    lists:map(
        fun(?R_REF(emqx_schema_registry_schema, Name)) ->
            NameStr = emqx_utils_conv:str(Name),
            Struct = "external_registry_api_create_" ++ NameStr,
            ?R_REF(emqx_schema_registry_schema, Struct)
        end,
        Refs
    );
create_external_registry_union({value, V}) ->
    ?UNION(UnionFn) = emqx_schema_registry_schema:external_registry_type(),
    %% will throw if there's no match; always return single match
    [?R_REF(emqx_schema_registry_schema, Name)] = UnionFn({value, V}),
    NameStr = emqx_utils_conv:str(Name),
    Struct = "external_registry_api_create_" ++ NameStr,
    [?R_REF(emqx_schema_registry_schema, Struct)].

not_found() -> fun() -> ?NOT_FOUND(<<"External registry not found">>) end.

with_external_registry(Name, FoundFn, NotFoundFn) ->
    case emqx_schema_registry_config:lookup_external_registry_raw(Name) of
        {ok, _Registry} when is_function(FoundFn, 0) ->
            FoundFn();
        {ok, Registry} when is_function(FoundFn, 1) ->
            FoundFn(Registry);
        {error, not_found} ->
            NotFoundFn()
    end.
