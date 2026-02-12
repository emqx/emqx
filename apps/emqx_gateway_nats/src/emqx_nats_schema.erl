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
        {authn_token,
            sc(
                binary(),
                #{
                    required => false,
                    desc => ?DESC(authn_token)
                }
            )},
        {authn_nkeys,
            sc(
                hoconsc:array(binary()),
                #{
                    required => false,
                    desc => ?DESC(authn_nkeys),
                    default => []
                }
            )},
        {authn_jwt,
            sc(
                ref(authn_jwt),
                #{
                    required => false,
                    desc => ?DESC(authn_jwt)
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(tcp_ws_listeners), #{})}
    ] ++ emqx_gateway_schema:gateway_common_options();
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
        {cache_ttl,
            sc(
                emqx_schema:duration(),
                #{
                    default => <<"5m">>,
                    desc => ?DESC(cache_ttl)
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
desc(authn_jwt) ->
    ?DESC(authn_jwt);
desc(jwt_resolver_memory) ->
    ?DESC(jwt_resolver);
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
