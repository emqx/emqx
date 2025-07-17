%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_http_api).

-behaviour(minirest_api).

-include("emqx_schema_registry.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

-export([
    namespace/0,
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    '/schema_registry'/2,
    '/schema_registry_protobuf/bundle'/2,
    '/schema_registry/:name'/2,
    '/schema_registry_external'/2,
    '/schema_registry_external/registry/:name'/2
]).

-export([validate_protobuf_bundle_request/2]).

%% BPAPI RPC Targets
-export([
    lookup_resource_from_local_node_v1/2
]).

-define(TAGS, [<<"Schema Registry">>]).
-define(BPAPI_NAME, emqx_schema_registry_http_api).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "schema_registry_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/schema_registry",
        "/schema_registry_protobuf/bundle",
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
schema("/schema_registry_protobuf/bundle") ->
    #{
        'operationId' => '/schema_registry_protobuf/bundle',
        filter => fun ?MODULE:validate_protobuf_bundle_request/2,
        post => #{
            tags => ?TAGS,
            summary => <<"Upload a Protobuf bundle for a new schema">>,
            description => ?DESC("protobuf_bundle_create"),
            'requestBody' => protobuf_bundle_request_body(),
            responses =>
                #{
                    201 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ?R_REF(emqx_schema_registry_schema, "post_protobuf"),
                            post_examples()
                        ),
                    400 => emqx_dashboard_swagger:error_codes(
                        [
                            'ALREADY_EXISTS',
                            'BAD_FORM_DATA'
                        ]
                    )
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Upload a Protobuf bundle for an existing schema">>,
            description => ?DESC("protobuf_bundle_update"),
            'requestBody' => protobuf_bundle_request_body(),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ?R_REF(emqx_schema_registry_schema, "put_protobuf"),
                            put_examples()
                        ),
                    404 => error_schema('NOT_FOUND', "Schema not found"),
                    400 => emqx_dashboard_swagger:error_codes(['BAD_FORM_DATA'])
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

protobuf_bundle_request_body() ->
    #{
        content => #{
            'multipart/form-data' => #{
                schema => #{
                    type => object,
                    properties => #{
                        name => #{type => string},
                        root_proto_file => #{type => string},
                        description => #{type => string},
                        bundle => #{type => string, format => binary}
                    }
                },
                encoding => #{
                    name => #{'contentType' => 'text/plain'},
                    root_proto_file => #{'contentType' => 'text/plain'},
                    description => #{'contentType' => 'text/plain'},
                    bundle => #{'contentType' => 'application/gzip'}
                }
            }
        }
    }.

validate_protobuf_bundle_request(#{body := #{<<"bundle">> := _}} = Params0, #{method := put}) ->
    validate_protobuf_bundle_request_with_bundle(Params0);
validate_protobuf_bundle_request(#{body := #{} = Body} = Params0, #{method := put}) ->
    %% Update only root proto file and/or description.
    maybe
        {ok, Name} ?= find_or(<<"name">>, Body, <<"Missing `name`">>),
        ok ?= safe_validate_name(Name),
        {ok, RootFile} ?= find_or(<<"root_proto_file">>, Body, <<"Missing `root_proto_file`">>),
        {ok, RootPath} ?= validate_root_filename(RootFile),
        Description = maps:get(<<"description">>, Body, <<"">>),
        Params = Params0#{
            name => Name,
            description => Description,
            root_proto_path => RootPath
        },
        {ok, Params}
    else
        {error, Reason} ->
            ?BAD_REQUEST(Reason);
        _ ->
            bad_protobuf_bundle_error()
    end;
validate_protobuf_bundle_request(#{body := #{}} = Params0, _Meta) ->
    validate_protobuf_bundle_request_with_bundle(Params0);
validate_protobuf_bundle_request(_Params, _Meta) ->
    bad_protobuf_bundle_error().

validate_protobuf_bundle_request_with_bundle(#{body := #{} = Body} = Params0) ->
    maybe
        {ok, Name} ?= find_or(<<"name">>, Body, <<"Missing `name`">>),
        ok ?= safe_validate_name(Name),
        {ok, RootFile} ?= find_or(<<"root_proto_file">>, Body, <<"Missing `root_proto_file`">>),
        {ok, RootPath} ?= validate_root_filename(RootFile),
        Description = maps:get(<<"description">>, Body, <<"">>),
        {ok, Bundle} ?= find_or(<<"bundle">>, Body, <<"Missing `bundle`">>),
        [{_Filename, Bin}] ?= maps:to_list(maps:without([type], Bundle)),
        ok ?= validate_gzip_magic_number(Bin),
        Params = Params0#{
            name => Name,
            description => Description,
            root_proto_path => RootPath,
            bundle => Bin
        },
        {ok, Params}
    else
        {error, Reason} ->
            ?BAD_REQUEST(Reason);
        _ ->
            bad_protobuf_bundle_error()
    end.

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
                        {ok, Res} = emqx_schema_registry:get_schema_raw_with_defaults(Name),
                        {201, Res#{<<"name">> => Name}};
                    {error, Error} ->
                        ?BAD_REQUEST(Error)
                end;
            {ok, _} ->
                schema_already_exists_error()
        end
    catch
        throw:#{kind := Kind, reason := Reason} ->
            Msg0 = ?ERROR_MSG('BAD_REQUEST', Reason),
            Msg = Msg0#{kind => Kind},
            {400, Msg}
    end.

'/schema_registry/:name'(get, #{bindings := #{name := Name}}) ->
    case emqx_schema_registry:get_schema_raw_with_defaults(Name) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Schema not found">>);
        {ok, Schema} when ?IS_TYPE_WITH_RESOURCE(Schema) ->
            add_resource_info(Name, Schema);
        {ok, Schema} ->
            ?OK(Schema#{<<"name">> => Name})
    end;
'/schema_registry/:name'(put, #{bindings := #{name := Name}, body := Params}) ->
    with_schema(
        Name,
        fun() ->
            case emqx_schema_registry:add_schema(Name, Params) of
                ok ->
                    {ok, Res} = emqx_schema_registry:get_schema_raw_with_defaults(Name),
                    ?OK(Res#{<<"name">> => Name});
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        fun schema_not_found_error/0
    );
'/schema_registry/:name'(delete, #{bindings := #{name := Name}}) ->
    with_schema(
        Name,
        fun() ->
            case emqx_schema_registry:delete_schema(Name) of
                ok ->
                    ?NO_CONTENT;
                {error, #{} = Error} ->
                    ?BAD_REQUEST(emqx_mgmt_api_lib:to_json(Error));
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        fun schema_not_found_error/0
    ).

'/schema_registry_protobuf/bundle'(post, #{name := Name} = Params) ->
    with_schema(
        Name,
        fun schema_already_exists_error/0,
        fun() ->
            case handle_upload_protobuf_bundle(Params) of
                {ok, Res} ->
                    ?CREATED(Res);
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end
    );
'/schema_registry_protobuf/bundle'(put, #{name := Name, bundle := _} = Params) ->
    with_schema(
        Name,
        fun() ->
            case handle_upload_protobuf_bundle(Params) of
                {ok, Res} ->
                    ?OK(Res);
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end,
        fun schema_not_found_error/0
    );
'/schema_registry_protobuf/bundle'(put, #{name := Name} = Params) ->
    %% Updating only root file and/or description
    with_schema(
        Name,
        fun() ->
            case handle_update_protobuf_bundle_no_bundle(Params) of
                {ok, Res} ->
                    ?OK(Res);
                {error, {pre_config_update, _, {invalid_or_missing_imports, Details}}} ->
                    Msg0 = ?ERROR_MSG('BAD_REQUEST', invalid_or_missing_imports),
                    Msg = maps:merge(Msg0, Details),
                    {400, Msg};
                {error, Reason} ->
                    ?BAD_REQUEST(Reason)
            end
        end,
        fun schema_not_found_error/0
    ).

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
'/schema_registry_external/registry/:name'(put, #{bindings := #{name := Name}, body := NewParams0}) ->
    with_external_registry(
        Name,
        fun(CurrentParams) ->
            NewParams = emqx_utils:deobfuscate(NewParams0, CurrentParams),
            case emqx_schema_registry_config:upsert_external_registry(Name, NewParams) of
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
%% BPAPI RPC Targets
%%-------------------------------------------------------------------------------------------------

-spec lookup_resource_from_local_node_v1(serde_type(), schema_name()) ->
    {ok, map()} | {error, not_found}.
lookup_resource_from_local_node_v1(_Type, Name) ->
    maybe
        {ok, #serde{eval_context = #{resource_id := ResourceId}}} ?=
            emqx_schema_registry:get_serde(Name),
        {ok, ?SCHEMA_REGISTRY_RESOURCE_GROUP, #{status := Status}} ?=
            emqx_resource:get_instance(ResourceId),
        Formatted = #{status => Status},
        {ok, Formatted}
    else
        _ -> {error, not_found}
    end.

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

add_resource_info(Name, SchemaConfig) ->
    case lookup_resource_from_all_nodes(Name, SchemaConfig) of
        {ok, NodeResources} ->
            NodeStatus = nodes_status(NodeResources),
            Status = aggregate_status(NodeStatus),
            ?OK(SchemaConfig#{
                <<"name">> => Name,
                <<"status">> => Status,
                <<"node_status">> => NodeStatus
            });
        {error, NodeErrors} ->
            ?INTERNAL_ERROR(NodeErrors)
    end.

nodes_status(NodeResources) ->
    lists:map(
        fun({Node, Res}) -> {Node, maps:get(status, Res, undefined)} end,
        maps:to_list(NodeResources)
    ).

aggregate_status(NodeStatus) ->
    AllStatus = lists:map(fun({_Node, Status}) -> Status end, NodeStatus),
    case lists:usort(AllStatus) of
        [Status] ->
            Status;
        _ ->
            inconsistent
    end.

lookup_resource_from_all_nodes(Name, SchemaConfig) ->
    #{<<"type">> := Type} = SchemaConfig,
    Nodes = nodes_supporting_bpapi_version(1),
    Results = emqx_schema_registry_http_api_proto_v1:lookup_resource_from_all_nodes(
        Nodes, Type, Name
    ),
    NodeResults = lists:zip(Nodes, Results),
    sequence_node_results(NodeResults).

nodes_supporting_bpapi_version(Vsn) ->
    [
        N
     || N <- emqx:running_nodes(),
        case emqx_bpapi:supported_version(N, ?BPAPI_NAME) of
            undefined -> false;
            NVsn when is_number(NVsn) -> NVsn >= Vsn
        end
    ].

sequence_node_results(NodeResults) ->
    {Ok, Error} =
        lists:foldl(
            fun
                ({Node, {ok, {ok, Val}}}, {OkAcc, ErrAcc}) ->
                    {OkAcc#{Node => Val}, ErrAcc};
                ({Node, {ok, Error}}, {OkAcc, ErrAcc}) ->
                    {OkAcc, ErrAcc#{Node => Error}};
                ({Node, Error}, {OkAcc, ErrAcc}) ->
                    {OkAcc, ErrAcc#{Node => Error}}
            end,
            {#{}, #{}},
            NodeResults
        ),
    EmptyResults = map_size(Ok) == 0,
    case map_size(Error) == 0 of
        true when not EmptyResults ->
            {ok, Ok};
        true ->
            {error, empty_results};
        false ->
            {error, Error}
    end.

handle_upload_protobuf_bundle(Params) ->
    #{
        name := Name,
        bundle := Archive
    } = Params,
    maybe
        {ok, Contents} ?= extract_tar_gz(Archive),
        UpdateReq = prepare_protobuf_bundle_config_update_request(Contents, Params),
        ok ?= emqx_schema_registry:add_schema(Name, UpdateReq),
        {ok, Res} = emqx_schema_registry:get_schema_raw_with_defaults(Name),
        {ok, Res#{<<"name">> => Name}}
    end.

handle_update_protobuf_bundle_no_bundle(Params) ->
    #{name := Name} = Params,
    maybe
        UpdateReq = prepare_protobuf_bundle_config_update_request_no_bundle(Params),
        ok ?= emqx_schema_registry:add_schema(Name, UpdateReq),
        {ok, Res} = emqx_schema_registry:get_schema_raw_with_defaults(Name),
        {ok, Res#{<<"name">> => Name}}
    end.

bad_protobuf_bundle_error() ->
    {400, #{
        code => 'BAD_FORM_DATA',
        message =>
            <<"form-data should be `name=@schema-name;bundle=@filename.tar.gz`">>
    }}.

schema_not_found_error() ->
    ?NOT_FOUND(<<"Schema not found">>).

schema_already_exists_error() ->
    ?BAD_REQUEST('ALREADY_EXISTS', <<"Schema already exists">>).

find_or(Key, Map, Error) ->
    maybe
        error ?= maps:find(Key, Map),
        {error, Error}
    end.

safe_validate_name(Name) ->
    try
        emqx_resource:validate_name(Name)
    catch
        throw:#{reason := Reason} ->
            {error, Reason}
    end.

validate_root_filename(Filename) ->
    case filename:split(Filename) of
        [F] when
            F == <<".">>;
            F == <<"..">>;
            F == <<"/">>
        ->
            {error, <<"Invalid root filename">>};
        [F] ->
            {ok, F};
        _ ->
            {error, <<"Invalid root filename; must not be nested in any directory">>}
    end.

validate_gzip_magic_number(<<16#1f, 16#8b, _/bitstring>>) ->
    ok;
validate_gzip_magic_number(_) ->
    {error, <<"Not a valid gzip file">>}.

with_schema(Name, FoundFn, NotFoundFn) ->
    case emqx_schema_registry:get_schema(Name) of
        {error, not_found} ->
            NotFoundFn();
        {ok, _Schema} when is_function(FoundFn, 0) ->
            FoundFn()
    end.

extract_tar_gz(Archive) ->
    maybe
        {error, Reason} ?= erl_tar:extract({binary, Archive}, [compressed, memory]),
        {error, {bad_tar_gz, Reason}}
    end.

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

prepare_protobuf_bundle_config_update_request(Contents, Params) ->
    #{
        description := Description,
        root_proto_path := RootPath
    } = Params,
    RootPathStr = str(RootPath),
    Files = lists:map(
        fun({Filename, Bin}) ->
            protobuf_bundle_file_of(Filename, Bin, RootPathStr)
        end,
        Contents
    ),
    #{
        <<"type">> => <<"protobuf">>,
        <<"description">> => Description,
        <<"source">> => #{
            <<"type">> => <<"bundle">>,
            <<"files">> => Files
        }
    }.

prepare_protobuf_bundle_config_update_request_no_bundle(Params) ->
    #{
        name := Name,
        description := Description,
        root_proto_path := RootPath0
    } = Params,
    DataDir = emqx_schema_registry_config:protobuf_bundle_data_dir(Name),
    RootPath = filename:join([DataDir, RootPath0]),
    #{
        <<"type">> => <<"protobuf">>,
        <<"description">> => Description,
        <<"source">> => #{
            <<"type">> => <<"bundle">>,
            <<"root_proto_path">> => RootPath
        }
    }.

protobuf_bundle_file_of(Filename, Bin, RootPathStr) ->
    #{
        <<"path">> => bin(Filename),
        <<"contents">> => Bin,
        <<"root">> => Filename == RootPathStr
    }.
