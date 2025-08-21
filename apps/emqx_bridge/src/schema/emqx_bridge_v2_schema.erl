%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([
    actions_get_response/0,
    actions_put_request/0,
    actions_post_request/0,
    action_api_schema/2,
    actions_examples/1,
    action_values/4
]).

-export([
    sources_get_response/0,
    sources_put_request/0,
    sources_post_request/0,
    source_api_schema/2,
    sources_examples/1,
    source_values/4
]).

%% Exported for mocking
%% TODO: refactor emqx_bridge_v1_compatibility_layer_SUITE so we don't need to
%% export this
-export([
    registered_actions_api_schemas/1,
    registered_sources_api_schemas/1,
    registered_action_types/0
]).

-export([action_types/0, action_types_sc/0]).
-export([source_types/0, source_types_sc/0]).
-export([action_resource_opts_fields/0, action_resource_opts_fields/1]).
-export([source_resource_opts_fields/0, source_resource_opts_fields/1]).

-export([
    api_fields/3,
    undefined_as_null_field/0
]).

-export([
    make_producer_action_schema/1, make_producer_action_schema/2,
    make_consumer_action_schema/1, make_consumer_action_schema/2,
    common_fields/0,
    common_action_fields/0,
    top_level_common_action_keys/0,
    top_level_common_source_keys/0,
    project_to_actions_resource_opts/1,
    project_to_sources_resource_opts/1, project_to_sources_resource_opts/2
]).

-export([actions_convert_from_connectors/1]).

-export_type([action_type/0, source_type/0]).

%% Should we explicitly list them here so dialyzer may be more helpful?
-type action_type() :: atom().
-type source_type() :: atom().
-type http_method() :: get | post | put.
-type schema_example_map() :: #{atom() => term()}.

%%======================================================================================
%% For HTTP APIs
%%======================================================================================

%%---------------------------------------------
%% Actions
%%---------------------------------------------

actions_get_response() ->
    actions_api_schema("get").

actions_put_request() ->
    actions_api_schema("put").

actions_post_request() ->
    actions_api_schema("post").

actions_api_schema(Method) ->
    APISchemas = ?MODULE:registered_actions_api_schemas(Method),
    hoconsc:union(bridge_api_union(APISchemas)).

action_api_schema(Method, BridgeV2Type) ->
    APISchemas = ?MODULE:registered_actions_api_schemas(Method),
    case lists:keyfind(atom_to_binary(BridgeV2Type), 1, APISchemas) of
        {_, SchemaRef} ->
            hoconsc:mk(SchemaRef);
        false ->
            unknown_bridge_schema(BridgeV2Type)
    end.

registered_actions_api_schemas(Method) ->
    RegisteredSchemas = emqx_action_info:registered_schema_modules_actions(),
    [
        api_ref(SchemaModule, atom_to_binary(BridgeV2Type), Method ++ "_bridge_v2")
     || {BridgeV2Type, SchemaModule} <- RegisteredSchemas
    ].

-spec action_values(http_method(), atom(), atom(), schema_example_map()) -> schema_example_map().
action_values(Method, ActionType, ConnectorType, ActionValues) ->
    ActionTypeBin = atom_to_binary(ActionType),
    ConnectorTypeBin = atom_to_binary(ConnectorType),
    lists:foldl(
        fun(M1, M2) ->
            maps:merge(M1, M2)
        end,
        #{
            enable => true,
            description => <<"My example ", ActionTypeBin/binary, " action">>,
            connector => <<ConnectorTypeBin/binary, "_connector">>,
            resource_opts => #{
                health_check_interval => <<"30s">>
            }
        },
        [
            ActionValues,
            method_values(action, Method, ActionType)
        ]
    ).

actions_examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, bridge_v2_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    SchemaModules = [Mod || {_, Mod} <- emqx_action_info:registered_schema_modules_actions()],
    lists:foldl(Fun, #{}, SchemaModules).

%%---------------------------------------------
%% Sources
%%---------------------------------------------

sources_get_response() ->
    sources_api_schema("get").

sources_put_request() ->
    sources_api_schema("put").

sources_post_request() ->
    sources_api_schema("post").

sources_api_schema(Method) ->
    APISchemas = ?MODULE:registered_sources_api_schemas(Method),
    hoconsc:union(bridge_api_union(APISchemas)).

source_api_schema(Method, SourceType) ->
    APISchemas = ?MODULE:registered_sources_api_schemas(Method),
    case lists:keyfind(atom_to_binary(SourceType), 1, APISchemas) of
        {_, SchemaRef} ->
            hoconsc:mk(SchemaRef);
        false ->
            unknown_source_schema(SourceType)
    end.

registered_sources_api_schemas(Method) ->
    RegisteredSchemas = emqx_action_info:registered_schema_modules_sources(),
    [
        api_ref(SchemaModule, atom_to_binary(BridgeV2Type), Method ++ "_source")
     || {BridgeV2Type, SchemaModule} <- RegisteredSchemas
    ].

-spec source_values(http_method(), atom(), atom(), schema_example_map()) -> schema_example_map().
source_values(Method, SourceType, ConnectorType, SourceValues) ->
    SourceTypeBin = atom_to_binary(SourceType),
    ConnectorTypeBin = atom_to_binary(ConnectorType),
    lists:foldl(
        fun(M1, M2) ->
            maps:merge(M1, M2)
        end,
        #{
            enable => true,
            description => <<"My example ", SourceTypeBin/binary, " source">>,
            connector => <<ConnectorTypeBin/binary, "_connector">>,
            resource_opts => #{
                health_check_interval => <<"30s">>
            }
        },
        [
            SourceValues,
            method_values(source, Method, SourceType)
        ]
    ).

sources_examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, source_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    SchemaModules = [Mod || {_, Mod} <- emqx_action_info:registered_schema_modules_sources()],
    lists:foldl(Fun, #{}, SchemaModules).

%%---------------------------------------------
%% Common helpers
%%---------------------------------------------

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

bridge_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                value => T,
                                reason => <<"unknown bridge type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    maps:values(Index)
            end
    end.

unknown_bridge_schema(BridgeV2Type) ->
    erroneous_value_schema(BridgeV2Type, <<"unknown bridge type">>).

unknown_source_schema(SourceType) ->
    erroneous_value_schema(SourceType, <<"unknown source type">>).

%% @doc Construct a schema that always emits validation error.
%% We need to silence dialyzer because inner anonymous function always throws.
-dialyzer({nowarn_function, [erroneous_value_schema/2]}).
erroneous_value_schema(Value, Reason) ->
    hoconsc:mk(typerefl:any(), #{
        validator => fun(_) ->
            throw(#{
                value => Value,
                reason => Reason
            })
        end
    }).

-spec method_values(action | source, http_method(), atom()) -> schema_example_map().
method_values(Kind, post, Type) ->
    KindBin = atom_to_binary(Kind),
    TypeBin = atom_to_binary(Type),
    #{
        name => <<TypeBin/binary, "_", KindBin/binary>>,
        type => TypeBin
    };
method_values(Kind, get, Type) ->
    maps:merge(
        method_values(Kind, post, Type),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
method_values(_Kind, put, _Type) ->
    #{}.

api_fields("get_bridge_v2", Type, Fields) ->
    lists:append(
        [
            emqx_bridge_schema:type_and_name_fields(Type),
            emqx_bridge_schema:status_fields(),
            Fields
        ]
    );
api_fields("post_bridge_v2", Type, Fields) ->
    lists:append(
        [
            emqx_bridge_schema:type_and_name_fields(Type),
            Fields
        ]
    );
api_fields("put_bridge_v2", _Type, Fields) ->
    Fields;
api_fields("get_source", Type, Fields) ->
    lists:append(
        [
            emqx_bridge_schema:type_and_name_fields(Type),
            emqx_bridge_schema:status_fields(),
            Fields
        ]
    );
api_fields("post_source", Type, Fields) ->
    lists:append(
        [
            emqx_bridge_schema:type_and_name_fields(Type),
            Fields
        ]
    );
api_fields("put_source", _Type, Fields) ->
    Fields.

undefined_as_null_field() ->
    {undefined_vars_as_null,
        ?HOCON(
            boolean(),
            #{
                default => false,
                desc => ?DESC("undefined_vars_as_null")
            }
        )}.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

namespace() -> "actions_and_sources".

tags() ->
    [<<"Actions">>, <<"Sources">>].

-dialyzer({nowarn_function, roots/0}).

roots() ->
    ActionsRoot =
        case fields(actions) of
            [] ->
                [
                    {actions,
                        ?HOCON(
                            hoconsc:map(name, typerefl:map()),
                            #{
                                importance => ?IMPORTANCE_LOW,
                                computed => fun fallback_actions_reverse_index_compute/2
                            }
                        )}
                ];
            _ ->
                [
                    {actions,
                        ?HOCON(?R_REF(actions), #{
                            importance => ?IMPORTANCE_LOW,
                            computed => fun fallback_actions_reverse_index_compute/2
                        })}
                ]
        end,
    SourcesRoot =
        [{sources, ?HOCON(?R_REF(sources), #{importance => ?IMPORTANCE_LOW})}],
    ActionsRoot ++ SourcesRoot.

fields(actions) ->
    registered_schema_fields_actions();
fields(sources) ->
    registered_schema_fields_sources();
fields(action_resource_opts) ->
    action_resource_opts_fields(_Overrides = []);
fields(source_resource_opts) ->
    source_resource_opts_fields(_Overrides = []);
fields(fallback_action_reference) ->
    [
        {kind, mk(reference, #{required => true, desc => ?DESC("fallback_action_kind")})},
        {type,
            mk(
                hoconsc:union(?MODULE:registered_action_types()),
                #{required => true, desc => ?DESC("fallback_action_reference_type")}
            )},
        {name,
            mk(
                binary(),
                #{required => true, desc => ?DESC("fallback_action_reference_name")}
            )}
    ];
fields(fallback_action_republish) ->
    [
        {kind, mk(republish, #{required => true, desc => ?DESC("fallback_action_kind")})},
        {args,
            mk(
                ref(emqx_rule_engine_schema, "republish_args"),
                #{
                    default => #{},
                    computed => fun fallback_actions_republish_compute/2
                }
            )}
    ].

registered_action_types() ->
    lists:map(
        fun({Type, _Module}) -> Type end,
        emqx_action_info:registered_schema_modules_actions()
    ).

registered_schema_fields_actions() ->
    [
        Module:fields(action)
     || {_BridgeV2Type, Module} <- emqx_action_info:registered_schema_modules_actions()
    ].

registered_schema_fields_sources() ->
    [
        Module:fields(source)
     || {_BridgeV2Type, Module} <- emqx_action_info:registered_schema_modules_sources()
    ].

desc(actions) ->
    ?DESC("desc_bridges_v2");
desc(sources) ->
    ?DESC("desc_sources");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(source_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(fallback_action_reference) ->
    ?DESC("fallback_action_reference");
desc(fallback_action_republish) ->
    ?DESC("fallback_action_republish");
desc(_) ->
    undefined.

-spec action_types() -> [action_type()].
action_types() ->
    proplists:get_keys(?MODULE:fields(actions)).

-spec action_types_sc() -> ?ENUM([action_type()]).
action_types_sc() ->
    hoconsc:enum(action_types()).

-spec source_types() -> [source_type()].
source_types() ->
    proplists:get_keys(?MODULE:fields(sources)).

-spec source_types_sc() -> ?ENUM([source_type()]).
source_types_sc() ->
    hoconsc:enum(source_types()).

action_resource_opts_fields() ->
    action_resource_opts_fields(_Overrides = []).

source_resource_opts_fields() ->
    source_resource_opts_fields(_Overrides = []).

common_action_resource_opts_subfields() ->
    [
        batch_size,
        batch_time,
        buffer_mode,
        buffer_seg_bytes,
        health_check_interval,
        health_check_interval_jitter,
        health_check_timeout,
        inflight_window,
        max_buffer_bytes,
        metrics_flush_interval,
        query_mode,
        request_ttl,
        resume_interval,
        worker_pool_size
    ].

common_source_resource_opts_subfields() ->
    [
        health_check_interval,
        health_check_interval_jitter,
        health_check_timeout,
        resume_interval
    ].

common_action_resource_opts_subfields_bin() ->
    lists:map(fun atom_to_binary/1, common_action_resource_opts_subfields()).

common_source_resource_opts_subfields_bin() ->
    lists:map(fun atom_to_binary/1, common_source_resource_opts_subfields()).

action_resource_opts_fields(Overrides) ->
    ActionROFields = common_action_resource_opts_subfields(),
    lists:filter(
        fun({Key, _Sc}) -> lists:member(Key, ActionROFields) end,
        emqx_resource_schema:create_opts(Overrides)
    ).

source_resource_opts_fields(Overrides) ->
    ActionROFields = common_source_resource_opts_subfields(),
    lists:filter(
        fun({Key, _Sc}) -> lists:member(Key, ActionROFields) end,
        emqx_resource_schema:create_opts(Overrides)
    ).

top_level_common_action_keys() ->
    [
        <<"connector">>,
        <<"tags">>,
        <<"description">>,
        <<"created_at">>,
        <<"last_modified_at">>,
        <<"enable">>,
        <<"local_topic">>,
        <<"parameters">>,
        <<"resource_opts">>
    ].

top_level_common_source_keys() ->
    [
        <<"connector">>,
        <<"tags">>,
        <<"description">>,
        <<"created_at">>,
        <<"last_modified_at">>,
        <<"enable">>,
        <<"parameters">>,
        <<"resource_opts">>
    ].

%%======================================================================================
%% Helper functions for making HOCON Schema
%%======================================================================================

make_producer_action_schema(ActionParametersRef) ->
    make_producer_action_schema(ActionParametersRef, _Opts = #{}).

make_producer_action_schema(ActionParametersRef, Opts) ->
    ResourceOptsRef = maps:get(resource_opts_ref, Opts, ref(?MODULE, action_resource_opts)),
    [
        {local_topic, mk(binary(), #{required => false, desc => ?DESC(mqtt_topic)})}
        | common_action_schema(ActionParametersRef, Opts)
    ] ++
        [
            {resource_opts,
                mk(ResourceOptsRef, #{
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "resource_opts")
                })}
        ].

make_consumer_action_schema(ParametersRef) ->
    make_consumer_action_schema(ParametersRef, _Opts = #{}).

make_consumer_action_schema(ParametersRef, Opts) ->
    ResourceOptsRef = maps:get(resource_opts_ref, Opts, ref(?MODULE, source_resource_opts)),
    common_consumer_schema(ParametersRef, Opts) ++
        [
            {resource_opts,
                mk(ResourceOptsRef, #{
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "resource_opts")
                })}
        ].

common_action_fields() ->
    [
        {fallback_actions,
            mk(
                hoconsc:array(
                    emqx_schema:mkunion(
                        kind,
                        #{
                            <<"reference">> => ref(?MODULE, fallback_action_reference),
                            <<"republish">> => ref(?MODULE, fallback_action_republish)
                        }
                    )
                ),
                #{
                    default => [],
                    desc => ?DESC("fallback_actions")
                }
            )}
        | common_fields()
    ].

common_fields() ->
    [
        {enable,
            mk(boolean(), #{
                desc => ?DESC("config_enable"),
                importance => ?IMPORTANCE_NO_DOC,
                default => true
            })},
        {connector,
            mk(binary(), #{
                desc => ?DESC(emqx_connector_schema, "connector_field"), required => true
            })},
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()},
        {created_at, mk(integer(), #{required => false, importance => ?IMPORTANCE_HIDDEN})},
        {last_modified_at, mk(integer(), #{required => false, importance => ?IMPORTANCE_HIDDEN})}
    ].

common_action_schema(ParametersRef, _Opts) ->
    [
        {parameters, ParametersRef}
        | common_action_fields()
    ].

common_consumer_schema(ParametersRef, _Opts) ->
    [
        {parameters, ParametersRef}
        | common_fields()
    ].

project_to_actions_resource_opts(OldResourceOpts) ->
    Subfields = common_action_resource_opts_subfields_bin(),
    maps:with(Subfields, OldResourceOpts).

project_to_sources_resource_opts(OldResourceOpts) ->
    project_to_sources_resource_opts(OldResourceOpts, common_source_resource_opts_subfields_bin()).

project_to_sources_resource_opts(OldResourceOpts, Subfields) ->
    maps:with(Subfields, OldResourceOpts).

actions_convert_from_connectors(RawConf = #{<<"actions">> := Actions}) ->
    Actions1 =
        maps:map(
            fun(ActionType, ActionMap) ->
                maps:map(
                    fun(_ActionName, Action) ->
                        #{<<"connector">> := ConnName} = Action,
                        ConnType = atom_to_binary(emqx_bridge_v2:connector_type(ActionType)),
                        ConnPath = [<<"connectors">>, ConnType, ConnName],
                        case emqx_utils_maps:deep_find(ConnPath, RawConf) of
                            {ok, ConnConf} ->
                                emqx_action_info:action_convert_from_connector(
                                    ActionType, ConnConf, Action
                                );
                            {not_found, _KeyPath, _Data} ->
                                Action
                        end
                    end,
                    ActionMap
                )
            end,
            Actions
        ),
    maps:put(<<"actions">>, Actions1, RawConf);
actions_convert_from_connectors(RawConf) ->
    RawConf.

fallback_actions_republish_compute(Args0, _HoconOpts) ->
    Args = emqx_utils_maps:unsafe_atom_key_map(Args0),
    emqx_rule_actions:pre_process_args(emqx_rule_actions, republish, Args).

%% Creates a mapping from Referenced Actions (`{Type :: binary(), Name :: binary()}`) to
%% lists of Referencing Actions that have it as their Fallback Action.
fallback_actions_reverse_index_compute(ActionsRootRawConfig, _HoconOpts) ->
    Index = fold_config(
        fun
            (ReferencingType, ReferencingName, #{<<"fallback_actions">> := FBAs}, Acc) ->
                add_fallback_actions_to_index(ReferencingType, ReferencingName, FBAs, Acc);
            (_Type, _Name, _Conf, Acc) ->
                Acc
        end,
        #{},
        ActionsRootRawConfig
    ),
    #{fallback_actions_index => Index}.

add_fallback_actions_to_index(ReferencingType, ReferencingName, FBAs, Index) ->
    lists:foldl(
        fun
            (
                #{<<"kind">> := reference, <<"type">> := T, <<"name">> := N},
                Acc
            ) ->
                ReferencingAction = #{
                    type => bin(ReferencingType),
                    name => bin(ReferencingName)
                },
                maps:update_with(
                    {bin(T), bin(N)},
                    fun(Refs) -> [ReferencingAction | Refs] end,
                    [ReferencingAction],
                    Acc
                );
            (_, Acc) ->
                Acc
        end,
        Index,
        FBAs
    ).

fold_config(Fn, Acc, RootRawConfig) ->
    maps:fold(
        fun(Type, NameAndConfs, Acc0) ->
            maps:fold(
                fun(Name, Conf, Acc1) ->
                    Fn(Type, Name, Conf, Acc1)
                end,
                Acc0,
                NameAndConfs
            )
        end,
        Acc,
        RootRawConfig
    ).

bin(X) -> emqx_utils_conv:bin(X).

-ifdef(TEST).
-include_lib("hocon/include/hocon_types.hrl").
schema_homogeneous_test() ->
    case
        lists:filtermap(
            fun({_Name, Schema}) ->
                is_bad_schema(Schema)
            end,
            fields(actions)
        )
    of
        [] ->
            ok;
        List ->
            throw(List)
    end.

is_bad_schema(#{type := ?MAP(_, ?R_REF(Module, TypeName))}) ->
    Fields = Module:fields(TypeName),
    ExpectedFieldNames = lists:map(fun binary_to_atom/1, top_level_common_action_keys()),
    MissingFields = lists:filter(
        fun(Name) -> lists:keyfind(Name, 1, Fields) =:= false end, ExpectedFieldNames
    ),
    case MissingFields of
        [] ->
            false;
        _ ->
            %% elasticsearch is new and doesn't have local_topic
            case MissingFields of
                [local_topic] when Module =:= emqx_bridge_es -> false;
                _ ->
                    {true, #{
                        schema_module => Module,
                        type_name => TypeName,
                        missing_fields => MissingFields
                    }}
            end
    end.

-endif.
