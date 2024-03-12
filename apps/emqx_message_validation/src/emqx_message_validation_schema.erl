%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_validation_schema).

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

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() -> message_validation.

roots() ->
    [{message_validation, mk(ref(message_validation), #{importance => ?IMPORTANCE_HIDDEN})}].

fields(message_validation) ->
    [
        {validations,
            mk(
                hoconsc:array(ref(validation)),
                #{
                    default => [],
                    desc => ?DESC("validations"),
                    validator => fun validate_unique_names/1
                }
            )}
    ];
fields(validation) ->
    [
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()},
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {name, mk(binary(), #{required => true, desc => ?DESC("name")})},
        {topics,
            mk(
                hoconsc:union([binary(), hoconsc:array(binary())]),
                #{
                    desc => ?DESC("topics"),
                    converter => fun ensure_array/2,
                    required => true
                }
            )},
        {strategy,
            mk(
                hoconsc:enum([any_pass, all_pass]),
                #{desc => ?DESC("strategy"), required => true}
            )},
        {failure_action,
            mk(
                hoconsc:enum([drop, disconnect]),
                #{desc => ?DESC("failure_action"), required => true}
            )},
        {log_failure_at,
            mk(
                hoconsc:enum([error, warning, notice, info, debug]),
                #{desc => ?DESC("log_failure_at"), default => info}
            )},
        {checks,
            mk(
                hoconsc:array(
                    hoconsc:union(fun checks_union_member_selector/1)
                ),
                #{
                    required => true,
                    desc => ?DESC("checks"),
                    validator => fun
                        ([]) ->
                            {error, "at least one check must be defined"};
                        (_) ->
                            ok
                    end
                }
            )}
    ];
fields(check_sql) ->
    [
        {type, mk(sql, #{default => sql, desc => ?DESC("check_sql_type")})},
        {sql,
            mk(binary(), #{
                required => true,
                desc => ?DESC("check_sql_type"),
                validator => fun validate_sql/1
            })}
    ];
fields(check_json) ->
    [
        {type, mk(json, #{default => json, desc => ?DESC("check_json_type")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("check_json_type")})}
    ];
fields(check_protobuf) ->
    [
        {type, mk(protobuf, #{default => protobuf, desc => ?DESC("check_protobuf_type")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("check_protobuf_schema")})},
        {message_name,
            mk(binary(), #{required => true, desc => ?DESC("check_protobuf_message_name")})}
    ];
fields(check_avro) ->
    [
        {type, mk(avro, #{default => avro, desc => ?DESC("check_avro_type")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("check_avro_schema")})}
    ].

checks_union_member_selector(all_union_members) ->
    checks_refs();
checks_union_member_selector({value, V}) ->
    checks_refs(V).

checks_refs() ->
    [ref(CheckType) || CheckType <- check_types()].

check_types() ->
    [
        check_sql,
        check_json,
        check_avro,
        check_protobuf
    ].

checks_refs(#{<<"type">> := TypeAtom} = Value) when is_atom(TypeAtom) ->
    checks_refs(Value#{<<"type">> := atom_to_binary(TypeAtom)});
checks_refs(#{<<"type">> := <<"sql">>}) ->
    [ref(check_sql)];
checks_refs(#{<<"type">> := <<"json">>}) ->
    [ref(check_json)];
checks_refs(#{<<"type">> := <<"avro">>}) ->
    [ref(check_avro)];
checks_refs(#{<<"type">> := <<"protobuf">>}) ->
    [ref(check_protobuf)];
checks_refs(_Value) ->
    Expected = lists:join(
        " | ",
        [
            Name
         || T <- check_types(),
            "check_" ++ Name <- [atom_to_list(T)]
        ]
    ),
    throw(#{
        field_name => type,
        expected => iolist_to_binary(Expected)
    }).

%%------------------------------------------------------------------------------
%% `minirest_trails' API
%%------------------------------------------------------------------------------

api_schema(list) ->
    hoconsc:array(ref(validation));
api_schema(lookup) ->
    ref(validation);
api_schema(post) ->
    ref(validation);
api_schema(put) ->
    ref(validation).

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).

ensure_array(undefined, _) -> undefined;
ensure_array(L, _) when is_list(L) -> L;
ensure_array(B, _) -> [B].

validate_sql(SQL) ->
    case emqx_message_validation:parse_sql_check(SQL) of
        {ok, _Parsed} ->
            ok;
        Error = {error, _} ->
            Error
    end.

validate_unique_names(Validations0) ->
    Validations = emqx_utils_maps:binary_key_map(Validations0),
    do_validate_unique_names(Validations, #{}).

do_validate_unique_names(_Validations = [], _Acc) ->
    ok;
do_validate_unique_names([#{<<"name">> := Name} | _Rest], Acc) when is_map_key(Name, Acc) ->
    {error, <<"duplicated name: ", Name/binary>>};
do_validate_unique_names([#{<<"name">> := Name} | Rest], Acc) ->
    do_validate_unique_names(Rest, Acc#{Name => true}).
