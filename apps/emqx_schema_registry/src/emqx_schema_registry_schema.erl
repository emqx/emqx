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
    external_registries_type/0
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
        {type, mk(avro, #{required => true, desc => ?DESC("schema_type_avro")})}
        | common_fields(emqx_schema:json_binary())
    ];
fields(protobuf) ->
    [
        {type, mk(protobuf, #{required => true, desc => ?DESC("schema_type_protobuf")})}
        | common_fields(binary())
    ];
fields(json) ->
    [
        {type, mk(json, #{required => true, desc => ?DESC("schema_type_json")})}
        | common_fields(emqx_schema:json_binary())
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
fields("put_avro") ->
    fields(avro);
fields("put_protobuf") ->
    fields(protobuf);
fields("put_json") ->
    fields(json);
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
desc(confluent_schema_registry) ->
    ?DESC("confluent_schema_registry");
desc(confluent_schema_registry_auth_basic) ->
    ?DESC("confluent_schema_registry_auth");
desc(_) ->
    undefined.

union_member_selector(all_union_members) ->
    refs();
union_member_selector({value, V}) ->
    refs(V).

union_member_selector_get_api(all_union_members) ->
    refs_get_api();
union_member_selector_get_api({value, V}) ->
    refs_get_api(V).

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
    hoconsc:union(fun union_member_selector_get_api/1);
api_schema("post") ->
    api_schema("get");
api_schema("put") ->
    hoconsc:union(fun union_member_selector/1).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

supported_serde_types() ->
    [avro, protobuf, json].

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
refs(_) ->
    Expected = lists:join(" | ", [atom_to_list(T) || T <- supported_serde_types()]),
    throw(#{
        field_name => type,
        expected => Expected
    }).

refs_get_api() ->
    [ref("get_avro"), ref("get_protobuf"), ref("get_json")].

refs_get_api(#{<<"type">> := TypeAtom} = Value) when is_atom(TypeAtom) ->
    refs(Value#{<<"type">> := atom_to_binary(TypeAtom)});
refs_get_api(#{<<"type">> := <<"avro">>}) ->
    [ref("get_avro")];
refs_get_api(#{<<"type">> := <<"protobuf">>}) ->
    [ref("get_protobuf")];
refs_get_api(#{<<"type">> := <<"json">>}) ->
    [ref("get_json")];
refs_get_api(_) ->
    Expected = lists:join(" | ", [atom_to_list(T) || T <- supported_serde_types()]),
    throw(#{
        field_name => type,
        expected => Expected
    }).
