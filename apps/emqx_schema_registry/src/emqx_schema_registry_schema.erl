%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_schema_registry_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_schema_registry.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    tags/0,
    union_member_selector/1
]).

%% `minirest_trails' API
-export([
    api_schema/1
]).

%% API
-export([
    external_registry_type/0,
    external_registries_type/0,
    parse_url/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

external_registry_type() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"confluent">> => ref(confluent_schema_registry)
        },
        <<"confluent">>
    ).

external_registries_type() ->
    hoconsc:map(name, external_registry_type()).

parse_url(URL) ->
    Parsed = emqx_utils_uri:parse(URL),
    case Parsed of
        #{scheme := undefined} ->
            {error, {invalid_url, {no_scheme, URL}}};
        #{authority := undefined} ->
            {error, {invalid_url, {no_host, URL}}};
        #{authority := #{userinfo := Userinfo}} when Userinfo =/= undefined ->
            {error, {invalid_url, {userinfo_not_supported, URL}}};
        #{fragment := Fragment} when Fragment =/= undefined ->
            {error, {invalid_url, {fragments_not_supported, URL}}};
        _ ->
            case emqx_utils_uri:request_base(Parsed) of
                {ok, Base} ->
                    Path = emqx_utils_uri:path(Parsed),
                    QueryParams = emqx_maybe:define(emqx_utils_uri:query(Parsed), <<"">>),
                    {ok, {Base, Path, QueryParams}};
                {error, Reason} ->
                    {error, {invalid_url, {invalid_base, Reason, URL}}}
            end
    end.

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() -> ?CONF_KEY_ROOT.

roots() ->
    [{?CONF_KEY_ROOT, mk(ref(?CONF_KEY_ROOT), #{required => false})}].

tags() ->
    [<<"Schema Registry">>].

fields(?CONF_KEY_ROOT) ->
    [
        {external,
            mk(
                external_registries_type(),
                #{
                    default => #{},
                    desc => ?DESC("confluent_schema_registry")
                }
            )},
        {schemas,
            mk(
                hoconsc:map(
                    name,
                    hoconsc:union(fun union_member_selector/1)
                ),
                #{
                    default => #{},
                    desc => ?DESC("schema_registry_schemas"),
                    validator => fun validate_name/1
                }
            )}
    ];
fields(avro) ->
    [
        {type, mk(?avro, #{required => true, desc => ?DESC("schema_type_avro")})}
        | common_fields(emqx_schema:json_binary())
    ];
fields(protobuf) ->
    Fields0 = common_fields(binary()),
    Fields1 = proplists:delete(source, Fields0),
    [
        {type, mk(?protobuf, #{required => true, desc => ?DESC("schema_type_protobuf")})},
        {source,
            mk(
                hoconsc:union([binary(), ref(protobuf_bundle_source)]),
                #{
                    required => true,
                    desc => ?DESC("schema_source"),
                    validator => fun protobuf_source_validator/1
                }
            )}
        | Fields1
    ];
fields(protobuf_bundle_source) ->
    [
        {type, mk(bundle, #{required => true, desc => ?DESC("protobuf_source_bundle_type")})},
        {files,
            mk(
                hoconsc:array(ref(protobuf_bundle_source_file)),
                #{required => false, importance => ?IMPORTANCE_HIDDEN}
            )},
        {root_proto_path,
            mk(binary(), #{
                required => false,
                desc => ?DESC("protobuf_source_bundle_root_proto_path")
            })}
    ];
fields(protobuf_bundle_source_file) ->
    [
        {path, mk(binary(), #{required => true})},
        {root, mk(boolean(), #{default => false})},
        {contents, mk(binary(), #{required => true})}
    ];
fields(json) ->
    [
        {type, mk(?json, #{required => true, desc => ?DESC("schema_type_json")})}
        | common_fields(emqx_schema:json_binary())
    ];
fields(external_http) ->
    [
        {type, mk(?external_http, #{required => true, desc => ?DESC("schema_type_external_http")})},
        {description, mk(binary(), #{default => <<>>, desc => ?DESC("schema_description")})},
        {parameters,
            mk(ref(external_http_params), #{required => true, desc => ?DESC("external_http_params")})}
    ];
fields(external_http_params) ->
    ConnectorFields0 = emqx_bridge_http_connector:fields(config),
    UnsupportedFields = [request, retry_interval, max_retries],
    ConnectorFields = lists:filter(
        fun({Field, _Sc}) -> not lists:member(Field, UnsupportedFields) end,
        ConnectorFields0
    ),
    [
        {url, mk(binary(), #{required => true, desc => ?DESC("external_http_url")})},
        {headers, mk(map(), #{default => #{}, desc => ?DESC("external_http_headers")})},
        {max_retries,
            mk(non_neg_integer(), #{
                default => 2, desc => ?DESC(emqx_bridge_http_schema, "config_max_retries")
            })},
        {request_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"10s">>, desc => ?DESC(emqx_bridge_http_connector, "request_timeout")
            })},
        {external_params,
            mk(binary(), #{default => <<"">>, desc => ?DESC("external_http_external_params")})}
        | ConnectorFields
    ];
fields(confluent_schema_registry) ->
    [
        {type,
            mk(confluent, #{default => confluent, desc => ?DESC("schema_registry_external_type")})},
        {url, mk(binary(), #{required => true, desc => ?DESC("confluent_schema_registry_url")})},
        {auth,
            mk(
                hoconsc:union([none, ref(confluent_schema_registry_auth_basic)]),
                #{default => none, desc => ?DESC("confluent_schema_registry_auth")}
            )}
    ];
fields(confluent_schema_registry_auth_basic) ->
    [
        {mechanism,
            mk(basic, #{
                required => true,
                default => basic,
                importance => ?IMPORTANCE_HIDDEN,
                desc => ?DESC("confluent_schema_registry_auth_basic")
            })},
        {username,
            mk(binary(), #{
                required => true,
                desc => ?DESC("confluent_schema_registry_auth_basic_username")
            })},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("confluent_schema_registry_auth_basic_password")
            })}
    ];
fields("external_registry_api_create_" ++ NameStr) ->
    Name = list_to_existing_atom(NameStr),
    [
        {name, mk(binary(), #{required => true, desc => ?DESC("external_registry_name")})}
        | fields(Name)
    ];
fields("get_avro") ->
    [{name, mk(binary(), #{required => true, desc => ?DESC("schema_name")})} | fields(avro)];
fields("get_protobuf") ->
    [{name, mk(binary(), #{required => true, desc => ?DESC("schema_name")})} | fields(protobuf)];
fields("get_json") ->
    [{name, mk(binary(), #{required => true, desc => ?DESC("schema_name")})} | fields(json)];
fields("get_external_http") ->
    %% TODO: move those structs related to HTTP API to the HTTP API module.
    [
        {name, mk(binary(), #{required => true, desc => ?DESC("schema_name")})}
        | fields(external_http) ++ fields(api_resource_status)
    ];
fields(api_resource_status) ->
    %% TODO: move those structs related to HTTP API to the HTTP API module.
    [
        {status, mk(binary(), #{})},
        {node_status, mk(hoconsc:array(ref(api_node_status)), #{})}
    ];
fields(api_node_status) ->
    %% TODO: move those structs related to HTTP API to the HTTP API module.
    [
        {node, mk(binary(), #{})},
        {status, mk(binary(), #{})}
    ];
fields("put_avro") ->
    fields(avro);
fields("put_protobuf") ->
    fields(protobuf);
fields("put_json") ->
    fields(json);
fields("put_external_http") ->
    fields(external_http);
fields("post_external_http") ->
    Fields = fields("get_external_http"),
    GetOnlyFields = [node_status, status],
    lists:filter(fun({Field, _Sc}) -> not lists:member(Field, GetOnlyFields) end, Fields);
fields("post_" ++ Type) ->
    fields("get_" ++ Type).

common_fields(SourceType) ->
    [
        {source, mk(SourceType, #{required => true, desc => ?DESC("schema_source")})},
        {description, mk(binary(), #{default => <<>>, desc => ?DESC("schema_description")})}
    ].

desc(?CONF_KEY_ROOT) ->
    ?DESC("schema_registry_root");
desc(avro) ->
    ?DESC("avro_type");
desc(protobuf) ->
    ?DESC("protobuf_type");
desc(json) ->
    ?DESC("json_type");
desc(external_http) ->
    ?DESC("external_http_type");
desc(external_http_params) ->
    ?DESC("external_http_params");
desc(confluent_schema_registry) ->
    ?DESC("confluent_schema_registry");
desc(confluent_schema_registry_auth_basic) ->
    ?DESC("confluent_schema_registry_auth");
desc(protobuf_bundle_source) ->
    ?DESC("protobuf_source_bundle_type");
desc(_) ->
    undefined.

union_member_selector(all_union_members) ->
    refs();
union_member_selector({value, V}) ->
    refs(V).

union_member_selector_api(Method) ->
    fun
        (all_union_members) ->
            refs_api(Method);
        ({value, V}) ->
            refs_api(Method, V)
    end.

validate_name(NameSchemaMap) ->
    case maps:is_key(?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME, NameSchemaMap) of
        true ->
            {error,
                <<"Illegal schema name ", ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME/binary>>};
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% `minirest_trails' "APIs"
%%------------------------------------------------------------------------------

api_schema("get") ->
    hoconsc:union(union_member_selector_api("get"));
api_schema("post") ->
    hoconsc:union(union_member_selector_api("post"));
api_schema("put") ->
    hoconsc:union(fun union_member_selector/1).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

supported_serde_types() ->
    [?avro, ?protobuf, ?json, ?external_http].

refs() ->
    [ref(Type) || Type <- supported_serde_types()].

refs(#{<<"type">> := TypeAtom} = Value) when is_atom(TypeAtom) ->
    refs(Value#{<<"type">> := atom_to_binary(TypeAtom)});
refs(#{<<"type">> := <<"avro">>}) ->
    [ref(avro)];
refs(#{<<"type">> := <<"protobuf">>}) ->
    [ref(protobuf)];
refs(#{<<"type">> := <<"json">>}) ->
    [ref(json)];
refs(#{<<"type">> := <<"external_http">>}) ->
    [ref(external_http)];
refs(_) ->
    Expected = lists:join(" | ", [atom_to_list(T) || T <- supported_serde_types()]),
    throw(#{
        field_name => type,
        expected => iolist_to_binary(Expected)
    }).

refs_api(Method) ->
    [ref(Method ++ "_" ++ atom_to_list(T)) || T <- supported_serde_types()].

refs_api(Method, #{<<"type">> := TypeAtom} = Value) when is_atom(TypeAtom) ->
    refs_api(Method, Value#{<<"type">> := atom_to_binary(TypeAtom)});
refs_api(Method, #{<<"type">> := <<"avro">>}) ->
    [ref(Method ++ "_avro")];
refs_api(Method, #{<<"type">> := <<"protobuf">>}) ->
    [ref(Method ++ "_protobuf")];
refs_api(Method, #{<<"type">> := <<"json">>}) ->
    [ref(Method ++ "_json")];
refs_api(Method, #{<<"type">> := <<"external_http">>}) ->
    [ref(Method ++ "_external_http")];
refs_api(_Method, _) ->
    Expected = lists:join(" | ", [atom_to_list(T) || T <- supported_serde_types()]),
    throw(#{
        field_name => type,
        expected => Expected
    }).

protobuf_source_validator(Bin) when is_binary(Bin) ->
    ok;
protobuf_source_validator(#{} = RawConf) ->
    HasRootPath = is_map_key(<<"root_proto_path">>, RawConf),
    HasFiles = is_map_key(<<"files">>, RawConf),
    case {HasRootPath, HasFiles} of
        {false, false} ->
            %% `files` is hidden and used by config import only.
            {error, <<"Must specify `root_proto_path`.">>};
        _ ->
            %% Even if both are defined, `files` will overwrite root path later when
            %% converted, so it's fine.
            ok
    end.
