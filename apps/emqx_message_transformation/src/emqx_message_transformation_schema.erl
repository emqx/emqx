%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1
]).

%% `minirest_trails' API
-export([
    api_schema/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(BIF_MOD_STR, "emqx_message_transformation_bif").

-define(ALLOWED_ROOT_KEYS, [
    <<"payload">>,
    <<"qos">>,
    <<"retain">>,
    <<"topic">>,
    <<"user_property">>
]).

-type key() :: list(binary()) | binary().
-reflect_type([key/0]).

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() -> message_transformation.

roots() ->
    [
        {message_transformation,
            mk(ref(message_transformation), #{importance => ?IMPORTANCE_HIDDEN})}
    ].

fields(message_transformation) ->
    [
        {transformations,
            mk(
                hoconsc:array(ref(transformation)),
                #{
                    default => [],
                    desc => ?DESC("transformations"),
                    validator => fun validate_unique_names/1
                }
            )}
    ];
fields(transformation) ->
    [
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()},
        {enable,
            mk(boolean(), #{
                desc => ?DESC("config_enable"), default => true, importance => ?IMPORTANCE_NO_DOC
            })},
        {name,
            mk(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_resource:validate_name/1,
                    desc => ?DESC("name")
                }
            )},
        {topics,
            mk(
                hoconsc:union([binary(), hoconsc:array(binary())]),
                #{
                    desc => ?DESC("topics"),
                    converter => fun ensure_array/2,
                    validator => fun validate_unique_topics/1,
                    required => true
                }
            )},
        {failure_action,
            mk(
                hoconsc:enum([drop, disconnect, ignore]),
                #{desc => ?DESC("failure_action"), required => true}
            )},
        {log_failure,
            mk(
                ref(log_failure),
                #{desc => ?DESC("log_failure_at"), default => #{}}
            )},
        {payload_decoder,
            mk(
                hoconsc:union(fun payload_serde_member_selector/1),
                #{desc => ?DESC("payload_decoder"), default => #{<<"type">> => <<"none">>}}
            )},
        {payload_encoder,
            mk(
                hoconsc:union(fun payload_serde_member_selector/1),
                #{desc => ?DESC("payload_encoder"), default => #{<<"type">> => <<"none">>}}
            )},
        {operations,
            mk(
                hoconsc:array(ref(operation)),
                #{
                    desc => ?DESC("operation"),
                    default => []
                }
            )}
    ];
fields(log_failure) ->
    [
        {level,
            mk(
                hoconsc:enum([error, warning, notice, info, debug, none]),
                #{desc => ?DESC("log_failure_at"), default => info}
            )}
    ];
fields(operation) ->
    [
        %% TODO: more strict type check??
        {key,
            mk(
                typerefl:alias("string", key()), #{
                    desc => ?DESC("operation_key"),
                    required => true,
                    converter => fun parse_key_path/2
                }
            )},
        {value,
            mk(typerefl:alias("string", any()), #{
                desc => ?DESC("operation_value"),
                required => true,
                converter => fun compile_variform/2
            })}
    ];
fields(payload_serde_none) ->
    [{type, mk(none, #{default => none, desc => ?DESC("payload_serde_none_type")})}];
fields(payload_serde_json) ->
    [{type, mk(json, #{default => json, desc => ?DESC("payload_serde_json_type")})}];
fields(payload_serde_avro) ->
    [
        {type, mk(avro, #{default => avro, desc => ?DESC("payload_serde_avro_type")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("payload_serde_avro_schema")})}
    ];
fields(payload_serde_protobuf) ->
    [
        {type, mk(protobuf, #{default => protobuf, desc => ?DESC("payload_serde_protobuf_type")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("payload_serde_protobuf_schema")})},
        {message_type,
            mk(binary(), #{required => true, desc => ?DESC("payload_serde_protobuf_message_type")})}
    ].

%%------------------------------------------------------------------------------
%% `minirest_trails' API
%%------------------------------------------------------------------------------

api_schema(list) ->
    hoconsc:array(ref(transformation));
api_schema(lookup) ->
    ref(transformation);
api_schema(post) ->
    ref(transformation);
api_schema(put) ->
    ref(transformation).

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

payload_serde_member_selector(all_union_members) ->
    payload_serde_refs();
payload_serde_member_selector({value, V}) ->
    payload_serde_refs(V).

payload_serde_refs() ->
    [
        payload_serde_none,
        payload_serde_json,
        payload_serde_avro,
        payload_serde_protobuf
    ].
payload_serde_refs(#{<<"type">> := Type} = V) when is_atom(Type) ->
    payload_serde_refs(V#{<<"type">> := atom_to_binary(Type)});
payload_serde_refs(#{<<"type">> := <<"none">>}) ->
    [ref(payload_serde_none)];
payload_serde_refs(#{<<"type">> := <<"json">>}) ->
    [ref(payload_serde_json)];
payload_serde_refs(#{<<"type">> := <<"avro">>}) ->
    [ref(payload_serde_avro)];
payload_serde_refs(#{<<"type">> := <<"protobuf">>}) ->
    [ref(payload_serde_protobuf)];
payload_serde_refs(_Value) ->
    Expected = lists:join(
        " | ",
        [
            Name
         || T <- payload_serde_refs(),
            "payload_serde_" ++ Name <- [atom_to_list(T)]
        ]
    ),
    throw(#{
        field_name => type,
        expected => iolist_to_binary(Expected)
    }).

ensure_array(undefined, _) -> undefined;
ensure_array(L, _) when is_list(L) -> L;
ensure_array(B, _) -> [B].

validate_unique_names(Transformations0) ->
    Transformations = emqx_utils_maps:binary_key_map(Transformations0),
    do_validate_unique_names(Transformations, #{}).

do_validate_unique_names(_Transformations = [], _Acc) ->
    ok;
do_validate_unique_names([#{<<"name">> := Name} | _Rest], Acc) when is_map_key(Name, Acc) ->
    {error, <<"duplicated name: ", Name/binary>>};
do_validate_unique_names([#{<<"name">> := Name} | Rest], Acc) ->
    do_validate_unique_names(Rest, Acc#{Name => true}).

validate_unique_topics([]) ->
    {error, <<"at least one topic filter must be defined">>};
validate_unique_topics(Topics) ->
    Grouped = maps:groups_from_list(
        fun(T) -> T end,
        Topics
    ),
    DuplicatedMap = maps:filter(
        fun(_T, Ts) -> length(Ts) > 1 end,
        Grouped
    ),
    case maps:keys(DuplicatedMap) of
        [] ->
            ok;
        Duplicated ->
            Msg = iolist_to_binary([
                <<"duplicated topics: ">>,
                lists:join(", ", Duplicated)
            ]),
            {error, Msg}
    end.

compile_variform(Expression, #{make_serializable := true}) ->
    case is_binary(Expression) of
        true ->
            Expression;
        false ->
            emqx_variform:decompile(Expression)
    end;
compile_variform(Expression, _Opts) ->
    case emqx_variform:compile(Expression) of
        {ok, Compiled} ->
            transform_bifs(Compiled);
        {error, Reason} ->
            throw(#{expression => Expression, reason => Reason})
    end.

transform_bifs(#{form := Form} = Compiled) ->
    Compiled#{form := traverse_transform_bifs(Form)}.

traverse_transform_bifs({call, FnName, Args}) ->
    FQFnName = fully_qualify_local_bif(FnName),
    {call, FQFnName, lists:map(fun traverse_transform_bifs/1, Args)};
traverse_transform_bifs({array, Elems}) ->
    {array, lists:map(fun traverse_transform_bifs/1, Elems)};
traverse_transform_bifs(Node) ->
    Node.

fully_qualify_local_bif("json_encode") ->
    ?BIF_MOD_STR ++ ".json_encode";
fully_qualify_local_bif("json_decode") ->
    ?BIF_MOD_STR ++ ".json_decode";
fully_qualify_local_bif(FnName) ->
    FnName.

parse_key_path(<<"">>, _Opts) ->
    throw(#{reason => <<"key must be non-empty">>});
parse_key_path(Key, #{make_serializable := true}) ->
    case is_binary(Key) of
        true ->
            Key;
        false ->
            iolist_to_binary(lists:join(".", Key))
    end;
parse_key_path(Key, _Opts) when is_binary(Key) ->
    Parts = binary:split(Key, <<".">>, [global]),
    case lists:any(fun(P) -> P =:= <<"">> end, Parts) of
        true ->
            throw(#{invalid_key => Key});
        false ->
            ok
    end,
    case Parts of
        [<<"payload">> | _] ->
            ok;
        [<<"qos">>] ->
            ok;
        [<<"retain">>] ->
            ok;
        [<<"topic">>] ->
            ok;
        [<<"user_property">>, _] ->
            ok;
        [<<"user_property">>] ->
            throw(#{
                invalid_key => Key, reason => <<"must define exactly one key inside user property">>
            });
        [<<"user_property">> | _] ->
            throw(#{
                invalid_key => Key, reason => <<"must define exactly one key inside user property">>
            });
        _ ->
            throw(#{invalid_key => Key, allowed_root_keys => ?ALLOWED_ROOT_KEYS})
    end,
    Parts.
