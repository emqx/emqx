%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    '/schema_registry/:name'/2
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
        "/schema_registry/:name"
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
                post_examples()
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_schema_registry_schema:api_schema("put"),
                            put_examples()
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
            "\"fields\":[{\"type\":\"int\",\"name\":\"i\"},"
            "{\"type\":\"string\",\"name\":\"s\"}]}"
        >>
    }.

put_examples() ->
    post_examples().

post_examples() ->
    get_examples().

get_examples() ->
    #{
        <<"avro_schema">> =>
            #{
                summary => <<"Avro">>,
                value => sample_get_schema_response(avro)
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
