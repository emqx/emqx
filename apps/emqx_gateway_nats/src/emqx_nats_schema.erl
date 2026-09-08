%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_schema).

-include("emqx_nats.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1, deobfuscate/2]).

namespace() -> "gateway".

fields(nats) ->
    [
        {server_id,
            sc(binary(), #{
                desc => ?DESC(server_id),
                default => <<"emqx_nats_gateway">>
            })},
        {server_name,
            sc(binary(), #{
                desc => ?DESC(server_name),
                default => <<"emqx_nats_gateway">>
            })},
        {default_heartbeat_interval,
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(default_heartbeat_interval),
                    default => <<"30s">>
                }
            )},
        {heartbeat_wait_timeout,
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(heartbeat_wait_timeout),
                    default => <<"5s">>
                }
            )},
        {protocol, sc(ref(protocol))},
        {internal_authn,
            sc(
                hoconsc:array(hoconsc:union(fun internal_authn_union_member_selector/1)),
                #{
                    required => false,
                    default => [],
                    validator => fun validate_internal_authn/1,
                    desc => ?DESC(internal_authn)
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(tcp_ws_listeners), #{})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(internal_authn_token) ->
    [
        {type,
            sc(
                hoconsc:enum([token]),
                #{
                    required => true,
                    desc => ?DESC(type)
                }
            )},
        {token,
            sc(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_schema:non_empty_string/1,
                    desc => ?DESC(authn_token)
                }
            )}
    ];
fields(internal_authn_nkey) ->
    [
        {type,
            sc(
                hoconsc:enum([nkey]),
                #{
                    required => true,
                    desc => ?DESC(type)
                }
            )},
        {nkeys,
            sc(
                hoconsc:array(binary()),
                #{
                    required => true,
                    default => [],
                    desc => ?DESC(authn_nkeys)
                }
            )}
    ];
fields(internal_authn_jwt) ->
    [
        {type,
            sc(
                hoconsc:enum([jwt]),
                #{
                    required => true,
                    desc => ?DESC(type)
                }
            )}
    ] ++ fields(authn_jwt);
fields(authn_jwt) ->
    [
        {trusted_operators,
            sc(
                hoconsc:array(binary()),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC(trusted_operators)
                }
            )},
        {resolver,
            sc(
                hoconsc:union(fun jwt_resolver_union_member_selector/1),
                #{
                    default => memory,
                    required => false,
                    desc => ?DESC(resolver)
                }
            )}
    ];
fields(jwt_resolver_memory) ->
    [
        {type,
            sc(
                hoconsc:enum([memory]),
                #{
                    required => true,
                    desc => ?DESC(type)
                }
            )},
        {resolver_preload,
            sc(
                hoconsc:array(ref(jwt_resolver_preload_entry)),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC(resolver_preload)
                }
            )}
    ];
fields(jwt_resolver_preload_entry) ->
    [
        {pubkey,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(pubkey)
                }
            )},
        {jwt,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(jwt)
                }
            )}
    ];
fields(protocol) ->
    [
        {max_payload_size,
            sc(
                non_neg_integer(),
                #{
                    default => ?DEFAULT_MAX_PAYLOAD,
                    desc => ?DESC(max_payload_size)
                }
            )}
    ];
fields(tcp_ws_listeners) ->
    [
        {ws,
            sc(
                map(name, ref(ws_listener)),
                #{
                    desc => ?DESC(ws_listener)
                }
            )},
        {wss,
            sc(
                map(name, ref(wss_listener)),
                #{desc => ?DESC(wss_listener)}
            )}
    ] ++
        emqx_gateway_schema:fields(tcp_listeners);
fields(ws_listener) ->
    [
        {websocket, sc(ref(websocket), #{})}
    ] ++
        emqx_gateway_schema:ws_listener();
fields(wss_listener) ->
    [
        {websocket, sc(ref(websocket), #{})}
    ] ++
        emqx_gateway_schema:wss_listener();
fields(websocket) ->
    Override = #{
        path => <<>>,
        fail_if_no_subprotocol => false,
        supported_subprotocols => <<"NATS/1.0, NATS">>
    },
    emqx_gateway_schema:ws_opts(Override).

desc(nats) ->
    ?DESC(nats);
desc(internal_authn_token) ->
    ?DESC(internal_authn_token);
desc(internal_authn_nkey) ->
    ?DESC(internal_authn_nkey);
desc(internal_authn_jwt) ->
    ?DESC(internal_authn_jwt);
desc(authn_jwt) ->
    ?DESC(authn_jwt);
desc(jwt_resolver_memory) ->
    ?DESC(jwt_resolver_memory);
desc(jwt_resolver_preload_entry) ->
    ?DESC(jwt_resolver_preload_entry);
desc(protocol) ->
    ?DESC(protocol);
desc(tcp_ws_listeners) ->
    ?DESC(tcp_ws_listeners);
desc(ws_listener) ->
    ?DESC(ws_listener);
desc(wss_listener) ->
    ?DESC(wss_listener);
desc(websocket) ->
    ?DESC(websocket).

deobfuscate(NewConf, OldConf) when is_map(NewConf), is_map(OldConf) ->
    case map_get_any(NewConf, [internal_authn, <<"internal_authn">>], undefined) of
        undefined ->
            {ok, NewConf};
        NewMethods ->
            OldMethods = map_get_any(OldConf, [internal_authn, <<"internal_authn">>], []),
            case deobfuscate_internal_authn(NewMethods, OldMethods) of
                {ok, Methods} ->
                    {ok, put_map_value(NewConf, internal_authn, Methods)};
                {error, _} = Error ->
                    Error
            end
    end;
deobfuscate(NewConf, _OldConf) ->
    {ok, NewConf}.

%%--------------------------------------------------------------------
%% internal functions

deobfuscate_internal_authn(NewMethods, OldMethods) when
    is_list(NewMethods), is_list(OldMethods)
->
    case find_duplicate_method_type(NewMethods) of
        none ->
            deobfuscate_internal_authn(NewMethods, OldMethods, []);
        {duplicate, Type} ->
            duplicate_method_error(Type)
    end;
deobfuscate_internal_authn(NewMethods, _OldMethods) ->
    {ok, NewMethods}.

deobfuscate_internal_authn([], _OldMethods, Acc) ->
    {ok, lists:reverse(Acc)};
deobfuscate_internal_authn([NewMethod | Rest], OldMethods, Acc) ->
    NewType = method_type(NewMethod),
    case deobfuscate_method(NewMethod, OldMethods, NewType) of
        {ok, Method} ->
            deobfuscate_internal_authn(Rest, OldMethods, [Method | Acc]);
        {error, _} = Error ->
            Error
    end.

deobfuscate_method(NewMethod0, OldMethods, token) ->
    NewMethod = emqx_utils_maps:binary_key_map(NewMethod0),
    case is_redacted_field(NewMethod, token, <<"token">>) of
        true ->
            case find_unique_method(token, OldMethods) of
                {ok, OldMethod0} ->
                    OldMethod = emqx_utils_maps:binary_key_map(OldMethod0),
                    case has_map_key(OldMethod, token, <<"token">>) of
                        true ->
                            {ok, emqx_utils:deobfuscate(NewMethod, OldMethod)};
                        false ->
                            redacted_method_error(token)
                    end;
                _ ->
                    redacted_method_error(token)
            end;
        false ->
            {ok, NewMethod}
    end;
deobfuscate_method(NewMethod0, OldMethods, jwt) ->
    NewMethod = emqx_utils_maps:binary_key_map(NewMethod0),
    case has_redacted_jwt(NewMethod) of
        true ->
            case find_unique_method(jwt, OldMethods) of
                {ok, OldMethod0} ->
                    OldMethod = emqx_utils_maps:binary_key_map(OldMethod0),
                    deobfuscate_jwt_method(NewMethod, OldMethod);
                _ ->
                    redacted_method_error(jwt)
            end;
        false ->
            deobfuscate_jwt_resolver(NewMethod, #{})
    end;
deobfuscate_method(NewMethod, _OldMethods, _Type) ->
    case reject_redacted_method(NewMethod) of
        ok ->
            {ok, NewMethod};
        {error, _} = Error ->
            Error
    end.

deobfuscate_jwt_method(NewMethod, OldMethod) ->
    NewMethod1 = emqx_utils:deobfuscate(NewMethod, OldMethod),
    case deobfuscate_jwt_resolver(NewMethod1, OldMethod) of
        {ok, NewMethod2} ->
            ensure_no_redacted_jwt(NewMethod2);
        {error, _} = Error ->
            Error
    end.

ensure_no_redacted_jwt(Method) ->
    case has_redacted_jwt(Method) of
        true ->
            redacted_method_error(jwt);
        false ->
            {ok, Method}
    end.

deobfuscate_jwt_resolver(NewMethod, OldMethod) ->
    NewResolver = map_get_any(NewMethod, [resolver, <<"resolver">>], undefined),
    OldResolver = map_get_any(OldMethod, [resolver, <<"resolver">>], #{}),
    case {is_map(NewResolver), is_map(OldResolver)} of
        {true, true} ->
            NewEntries = map_get_any(
                NewResolver,
                [resolver_preload, <<"resolver_preload">>],
                []
            ),
            OldEntries = map_get_any(
                OldResolver,
                [resolver_preload, <<"resolver_preload">>],
                []
            ),
            case deobfuscate_jwt_entries(NewEntries, OldEntries) of
                {ok, Entries} ->
                    Resolver = put_map_value(NewResolver, resolver_preload, Entries),
                    {ok, put_map_value(NewMethod, resolver, Resolver)};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {ok, NewMethod}
    end.

deobfuscate_jwt_entries(NewEntries, OldEntries) when
    is_list(NewEntries), is_list(OldEntries)
->
    case find_duplicate_pubkey(NewEntries) of
        none ->
            deobfuscate_jwt_entries(NewEntries, OldEntries, []);
        duplicate ->
            duplicate_entry_error()
    end;
deobfuscate_jwt_entries(NewEntries, _OldEntries) ->
    {ok, NewEntries}.

deobfuscate_jwt_entries([], _OldEntries, Acc) ->
    {ok, lists:reverse(Acc)};
deobfuscate_jwt_entries([NewEntry0 | Rest], OldEntries, Acc) ->
    NewEntry = emqx_utils_maps:binary_key_map(NewEntry0),
    PubKey = map_get_any(NewEntry, [pubkey, <<"pubkey">>], undefined),
    case is_redacted_field(NewEntry, jwt, <<"jwt">>) of
        true ->
            case find_unique_entry(PubKey, OldEntries) of
                {ok, OldEntry0} ->
                    OldEntry = emqx_utils_maps:binary_key_map(OldEntry0),
                    case has_map_key(OldEntry, jwt, <<"jwt">>) of
                        true ->
                            Entry = emqx_utils:deobfuscate(NewEntry, OldEntry),
                            deobfuscate_jwt_entries(Rest, OldEntries, [Entry | Acc]);
                        false ->
                            redacted_entry_error(jwt)
                    end;
                _ ->
                    redacted_entry_error(jwt)
            end;
        false ->
            deobfuscate_jwt_entries(Rest, OldEntries, [NewEntry | Acc])
    end.

find_unique_method(Type, Methods) ->
    case [Method || Method <- Methods, method_type(Method) =:= Type] of
        [Method] ->
            {ok, Method};
        [] ->
            not_found;
        _ ->
            ambiguous
    end.

find_unique_entry(undefined, _Entries) ->
    not_found;
find_unique_entry(PubKey, Entries) ->
    case
        [
            Entry
         || Entry <- Entries,
            is_map(Entry),
            entry_pubkey_identity(Entry) =:= normalize_pubkey(PubKey)
        ]
    of
        [Entry] ->
            {ok, Entry};
        [] ->
            not_found;
        _ ->
            ambiguous
    end.

find_duplicate_method_type(Methods) ->
    Types = [
        Type
     || Method <- Methods,
        Type <- [method_type(Method)],
        Type =/= undefined
    ],
    case Types -- lists:usort(Types) of
        [Duplicate | _] ->
            {duplicate, Duplicate};
        [] ->
            none
    end.

find_duplicate_pubkey(Entries) ->
    PubKeys = [
        PubKey
     || Entry <- Entries,
        PubKey <- [entry_pubkey_identity(Entry)],
        PubKey =/= undefined
    ],
    case PubKeys -- lists:usort(PubKeys) of
        [_Duplicate | _] ->
            duplicate;
        [] ->
            none
    end.

entry_pubkey_identity(Entry) when is_map(Entry) ->
    normalize_pubkey(map_get_any(Entry, [pubkey, <<"pubkey">>], undefined));
entry_pubkey_identity(_Entry) ->
    undefined.

normalize_pubkey(undefined) ->
    undefined;
normalize_pubkey(PubKey) when is_binary(PubKey) ->
    emqx_nats_nkey:normalize(PubKey);
normalize_pubkey(PubKey) ->
    PubKey.

reject_redacted_method(Method) ->
    case is_redacted_field(Method, token, <<"token">>) of
        true ->
            redacted_method_error(token);
        false ->
            case has_redacted_jwt(Method) of
                true ->
                    redacted_method_error(jwt);
                false ->
                    ok
            end
    end.

has_redacted_jwt(Method) when is_map(Method) ->
    Resolver = map_get_any(Method, [resolver, <<"resolver">>], #{}),
    case is_map(Resolver) of
        true ->
            Entries = map_get_any(Resolver, [resolver_preload, <<"resolver_preload">>], []),
            case is_list(Entries) of
                true ->
                    lists:any(
                        fun(Entry) ->
                            is_map(Entry) andalso is_redacted_field(Entry, jwt, <<"jwt">>)
                        end,
                        Entries
                    );
                false ->
                    false
            end;
        false ->
            false
    end;
has_redacted_jwt(_Method) ->
    false.

method_type(Method) when is_map(Method) ->
    case map_get_any(Method, [type, <<"type">>], undefined) of
        token -> token;
        <<"token">> -> token;
        nkey -> nkey;
        <<"nkey">> -> nkey;
        jwt -> jwt;
        <<"jwt">> -> jwt;
        _ -> undefined
    end;
method_type(_Method) ->
    undefined.

is_redacted_field(Map, AtomKey, BinaryKey) when is_map(Map) ->
    case map_get_any(Map, [AtomKey, BinaryKey], undefined) of
        undefined ->
            false;
        Value ->
            emqx_utils:is_redacted(AtomKey, Value) orelse
                emqx_utils:is_redacted(BinaryKey, Value)
    end;
is_redacted_field(_Map, _AtomKey, _BinaryKey) ->
    false.

has_map_key(Map, AtomKey, BinaryKey) when is_map(Map) ->
    is_map_key(AtomKey, Map) orelse is_map_key(BinaryKey, Map);
has_map_key(_Map, _AtomKey, _BinaryKey) ->
    false.

put_map_value(Map, AtomKey, Value) ->
    BinaryKey = atom_to_binary(AtomKey, utf8),
    case maps:is_key(BinaryKey, Map) of
        true -> maps:put(BinaryKey, Value, Map);
        false -> maps:put(AtomKey, Value, Map)
    end.

redacted_method_error(Field) ->
    {error,
        {badconf, #{
            key => internal_authn,
            reason => "masked_secret_without_matching_old_value",
            value => Field
        }}}.

redacted_entry_error(Field) ->
    {error,
        {badconf, #{
            key => internal_authn,
            reason => "masked_secret_without_matching_old_value",
            value => Field
        }}}.

duplicate_method_error(Type) ->
    {error,
        {badconf, #{
            key => internal_authn,
            reason => "duplicate_authentication_method_type",
            value => Type
        }}}.

duplicate_entry_error() ->
    {error,
        {badconf, #{
            key => internal_authn,
            reason => "duplicate_resolver_preload_pubkey",
            value => pubkey
        }}}.

validate_internal_authn(Methods) when is_list(Methods) ->
    case find_duplicate_method_type(Methods) of
        none ->
            validate_internal_authn(Methods, 1);
        {duplicate, Type} ->
            {error,
                iolist_to_binary(
                    io_lib:format(
                        "duplicate authentication method type '~p'",
                        [Type]
                    )
                )}
    end;
validate_internal_authn(_) ->
    ok.

validate_internal_authn([Method | Rest], Pos) ->
    case map_get_any(Method, [type, <<"type">>], undefined) of
        nkey ->
            validate_internal_authn_nkey(Method, Rest, Pos);
        <<"nkey">> ->
            validate_internal_authn_nkey(Method, Rest, Pos);
        jwt ->
            validate_internal_authn_jwt(Method, Rest, Pos);
        <<"jwt">> ->
            validate_internal_authn_jwt(Method, Rest, Pos);
        _ ->
            validate_internal_authn(Rest, Pos + 1)
    end;
validate_internal_authn([], _Pos) ->
    ok.

validate_internal_authn_nkey(Method, Rest, Pos) ->
    case validate_authn_nkeys(Method) of
        ok ->
            validate_internal_authn(Rest, Pos + 1);
        {error, Reason} ->
            {error, format_internal_authn_error(Pos, Reason)}
    end.

validate_internal_authn_jwt(Method, Rest, Pos) ->
    case validate_authn_jwt(Method) of
        ok ->
            validate_internal_authn(Rest, Pos + 1);
        {error, Reason} ->
            {error, format_internal_authn_error(Pos, Reason)}
    end.

format_internal_authn_error(Pos, Reason) ->
    iolist_to_binary(io_lib:format("internal_authn[~B]: ~ts", [Pos, Reason])).

validate_authn_nkeys(Config) ->
    NKeys = map_get_any(Config, [nkeys, <<"nkeys">>], []),
    case NKeys of
        [] ->
            {error, <<"field `nkeys` must not be empty">>};
        _ ->
            case
                validate_nkey_list(
                    NKeys,
                    fun emqx_nats_nkey:decode_public/1,
                    fun(Index) ->
                        iolist_to_binary(
                            io_lib:format("field `nkeys[~B]` must be a valid user NKey", [Index])
                        )
                    end
                )
            of
                ok ->
                    validate_unique_nkeys(NKeys, <<"nkeys">>);
                {error, _} = Error ->
                    Error
            end
    end.

validate_nkey_list(Values, DecodeFun, FormatError) when is_list(Values) ->
    validate_nkey_list(Values, 1, DecodeFun, FormatError);
validate_nkey_list(_Values, _DecodeFun, _FormatError) ->
    ok.

validate_nkey_list([Value | Rest], Index, DecodeFun, FormatError) ->
    case DecodeFun(Value) of
        {ok, _PubKey} ->
            validate_nkey_list(Rest, Index + 1, DecodeFun, FormatError);
        {error, _Reason} ->
            {error, FormatError(Index)}
    end;
validate_nkey_list([], _Index, _DecodeFun, _FormatError) ->
    ok.

validate_nkey_with_prefix(Value, Prefix) ->
    case emqx_nats_nkey:decode_public_any(Value) of
        {ok, _PubKey} ->
            case emqx_nats_nkey:normalize(Value) of
                <<Prefix, _/binary>> ->
                    {ok, Value};
                _ ->
                    {error, invalid_nkey_prefix}
            end;
        {error, _Reason} ->
            {error, invalid_nkey_prefix}
    end.

validate_operator_nkey(Value) ->
    validate_nkey_with_prefix(Value, $O).

validate_account_nkey(Value) ->
    validate_nkey_with_prefix(Value, $A).

validate_resolver_preload_pubkeys(Values) when is_list(Values) ->
    validate_resolver_preload_pubkeys(Values, 1, #{});
validate_resolver_preload_pubkeys(_Values) ->
    ok.

validate_resolver_preload_pubkeys([Entry | Rest], Index, Seen) ->
    PubKey = map_get_any(Entry, [pubkey, <<"pubkey">>], undefined),
    case validate_account_nkey(PubKey) of
        {ok, _PubKey} ->
            PubKey1 = normalize_pubkey(PubKey),
            case maps:is_key(PubKey1, Seen) of
                true ->
                    {error, <<"field `resolver.resolver_preload.pubkey` must be unique">>};
                false ->
                    validate_resolver_preload_pubkeys(Rest, Index + 1, Seen#{PubKey1 => true})
            end;
        {error, _Reason} ->
            {error,
                iolist_to_binary(
                    io_lib:format(
                        "field `resolver.resolver_preload[~B].pubkey` must be a valid account NKey",
                        [Index]
                    )
                )}
    end;
validate_resolver_preload_pubkeys([], _Index, _Seen) ->
    ok.

validate_authn_jwt(Config) ->
    case validate_authn_jwt_required_fields(Config) of
        ok ->
            case validate_authn_jwt_nkey_material(Config) of
                ok ->
                    validate_authn_jwt_account_tokens(Config);
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

validate_authn_jwt_account_tokens(Config) ->
    case emqx_nats_authn:validate_jwt_config(Config) of
        ok ->
            ok;
        {error, Hint} ->
            {error, Hint}
    end.

validate_authn_jwt_required_fields(Config) ->
    case {jwt_has_trusted_operators(Config), jwt_has_resolver_preload(Config)} of
        {true, true} ->
            ok;
        {false, false} ->
            {error, <<"fields `trusted_operators` and `resolver.resolver_preload` are required">>};
        {false, true} ->
            {error, <<"field `trusted_operators` is required">>};
        {true, false} ->
            {error, <<"field `resolver.resolver_preload` is required">>}
    end.

validate_authn_jwt_nkey_material(Config) ->
    TrustedOperators = map_get_any(Config, [trusted_operators, <<"trusted_operators">>], []),
    case
        validate_nkey_list(
            TrustedOperators,
            fun validate_operator_nkey/1,
            fun(Index) ->
                iolist_to_binary(
                    io_lib:format(
                        "field `trusted_operators[~B]` must be a valid operator NKey",
                        [Index]
                    )
                )
            end
        )
    of
        ok ->
            case validate_unique_nkeys(TrustedOperators, <<"trusted_operators">>) of
                ok ->
                    Resolver = map_get_any(Config, [resolver, <<"resolver">>], #{}),
                    validate_resolver_preload_pubkeys(
                        map_get_any(Resolver, [resolver_preload, <<"resolver_preload">>], [])
                    );
                {error, _} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

validate_unique_nkeys(Values, Field) ->
    Identities = [nkey_identity(Value) || Value <- Values],
    case Identities -- lists:usort(Identities) of
        [_Duplicate | _] ->
            {error,
                iolist_to_binary(
                    io_lib:format("field `~ts` must be unique", [Field])
                )};
        [] ->
            ok
    end.

nkey_identity(Value) when is_binary(Value) ->
    case emqx_nats_nkey:decode_public_any(Value) of
        {ok, PubKey} ->
            PubKey;
        {error, _} ->
            Value
    end;
nkey_identity(Value) ->
    Value.

jwt_has_trusted_operators(Config) ->
    has_non_empty_list(map_get_any(Config, [trusted_operators, <<"trusted_operators">>], [])).

jwt_has_resolver_preload(Config) ->
    Resolver = map_get_any(Config, [resolver, <<"resolver">>], #{}),
    has_non_empty_list(map_get_any(Resolver, [resolver_preload, <<"resolver_preload">>], [])).

has_non_empty_list(Value) when is_list(Value) ->
    Value =/= [];
has_non_empty_list(_) ->
    false.

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).

map(Name, Type) ->
    hoconsc:map(Name, Type).

internal_authn_union_member_selector(all_union_members) ->
    [
        ref(internal_authn_token),
        ref(internal_authn_nkey),
        ref(internal_authn_jwt)
    ];
internal_authn_union_member_selector({value, V0}) ->
    V =
        case is_map(V0) of
            true -> emqx_utils_maps:binary_key_map(V0);
            false -> V0
        end,
    case V of
        #{<<"type">> := token} ->
            [ref(internal_authn_token)];
        #{<<"type">> := <<"token">>} ->
            [ref(internal_authn_token)];
        #{<<"type">> := nkey} ->
            [ref(internal_authn_nkey)];
        #{<<"type">> := <<"nkey">>} ->
            [ref(internal_authn_nkey)];
        #{<<"type">> := jwt} ->
            [ref(internal_authn_jwt)];
        #{<<"type">> := <<"jwt">>} ->
            [ref(internal_authn_jwt)];
        _ ->
            throw(#{
                field_name => internal_authn,
                expected => "{type = token, ...} | {type = nkey, ...} | {type = jwt, ...}"
            })
    end.

map_get_any(Map, [Key | More], Default) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            Value;
        error ->
            map_get_any(Map, More, Default)
    end;
map_get_any(_, [_Key | _More], Default) ->
    Default;
map_get_any(_, [], Default) ->
    Default.

jwt_resolver_union_member_selector(all_union_members) ->
    [memory, ref(jwt_resolver_memory)];
jwt_resolver_union_member_selector({value, V0}) ->
    V =
        case is_map(V0) of
            true -> emqx_utils_maps:binary_key_map(V0);
            false -> V0
        end,
    case V of
        memory ->
            [memory];
        <<"memory">> ->
            [memory];
        #{<<"type">> := memory} ->
            [ref(jwt_resolver_memory)];
        #{<<"type">> := <<"memory">>} ->
            [ref(jwt_resolver_memory)];
        _ ->
            throw(#{
                field_name => resolver,
                expected => "memory | {type = memory, ...}"
            })
    end.
