%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_schema).

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

namespace() -> schema_validation.

roots() ->
    [{schema_validation, mk(ref(schema_validation), #{importance => ?IMPORTANCE_HIDDEN})}].

fields(schema_validation) ->
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
        {strategy,
            mk(
                hoconsc:enum([any_pass, all_pass]),
                #{desc => ?DESC("strategy"), required => true}
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
        {checks,
            mk(
                hoconsc:array(
                    hoconsc:union(fun checks_union_member_selector/1)
                ),
                #{
                    required => true,
                    desc => ?DESC("checks"),
                    validator => fun validate_unique_schema_checks/1
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
        {message_type,
            mk(binary(), #{required => true, desc => ?DESC("check_protobuf_message_type")})}
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
    case emqx_schema_validation:parse_sql_check(SQL) of
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

validate_unique_schema_checks([]) ->
    {error, "at least one check must be defined"};
validate_unique_schema_checks(Checks) ->
    Seen = sets:new([{version, 2}]),
    Duplicated = sets:new([{version, 2}]),
    do_validate_unique_schema_checks(Checks, Seen, Duplicated).

do_validate_unique_schema_checks(_Checks = [], _Seen, Duplicated) ->
    case sets:to_list(Duplicated) of
        [] ->
            ok;
        DuplicatedChecks0 ->
            DuplicatedChecks =
                lists:map(
                    fun({Type, SerdeName}) ->
                        [atom_to_binary(Type), ":", SerdeName]
                    end,
                    DuplicatedChecks0
                ),
            Msg = iolist_to_binary([
                <<"duplicated schema checks: ">>,
                lists:join(", ", DuplicatedChecks)
            ]),
            {error, Msg}
    end;
do_validate_unique_schema_checks(
    [#{<<"type">> := Type, <<"schema">> := SerdeName} | Rest],
    Seen0,
    Duplicated0
) ->
    Check = {Type, SerdeName},
    case sets:is_element(Check, Seen0) of
        true ->
            Duplicated = sets:add_element(Check, Duplicated0),
            do_validate_unique_schema_checks(Rest, Seen0, Duplicated);
        false ->
            Seen = sets:add_element(Check, Seen0),
            do_validate_unique_schema_checks(Rest, Seen, Duplicated0)
    end;
do_validate_unique_schema_checks([_Check | Rest], Seen, Duplicated) ->
    do_validate_unique_schema_checks(Rest, Seen, Duplicated).

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
