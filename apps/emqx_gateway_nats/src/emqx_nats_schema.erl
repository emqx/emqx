%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_schema).

-include("emqx_nats.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

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
            )},
        {verify_exp,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(verify_exp)
                }
            )},
        {verify_nbf,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(verify_nbf)
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

%%--------------------------------------------------------------------
%% internal functions

validate_internal_authn(Methods) when is_list(Methods) ->
    validate_internal_authn(Methods, 1);
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
            validate_nkey_list(
                NKeys,
                fun emqx_nats_nkey:decode_public/1,
                fun(Index) ->
                    iolist_to_binary(
                        io_lib:format("field `nkeys[~B]` must be a valid user NKey", [Index])
                    )
                end
            )
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
    validate_resolver_preload_pubkeys(Values, 1);
validate_resolver_preload_pubkeys(_Values) ->
    ok.

validate_resolver_preload_pubkeys([Entry | Rest], Index) ->
    PubKey = map_get_any(Entry, [pubkey, <<"pubkey">>], undefined),
    case validate_account_nkey(PubKey) of
        {ok, _PubKey} ->
            validate_resolver_preload_pubkeys(Rest, Index + 1);
        {error, _Reason} ->
            {error,
                iolist_to_binary(
                    io_lib:format(
                        "field `resolver.resolver_preload[~B].pubkey` must be a valid account NKey",
                        [Index]
                    )
                )}
    end;
validate_resolver_preload_pubkeys([], _Index) ->
    ok.

validate_authn_jwt(Config) ->
    case validate_authn_jwt_required_fields(Config) of
        ok ->
            validate_authn_jwt_nkey_material(Config);
        {error, _Reason} = Error ->
            Error
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
    case
        validate_nkey_list(
            map_get_any(Config, [trusted_operators, <<"trusted_operators">>], []),
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
            Resolver = map_get_any(Config, [resolver, <<"resolver">>], #{}),
            validate_resolver_preload_pubkeys(
                map_get_any(Resolver, [resolver_preload, <<"resolver_preload">>], [])
            );
        {error, _Reason} = Error ->
            Error
    end.

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
