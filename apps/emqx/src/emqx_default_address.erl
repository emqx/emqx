%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_default_address).

-moduledoc """
This module resolves the `EMQX_DEFAULT_ADDRESS` boot environment variable,
which sets the address of defaulted listener binds independently of the
security profile.

Valid values are `loopback`, `nodename`, `all`, or a literal IPv4/IPv6
address. When the variable is not set, the security profile policy decides
the default address.

NOTE: this module may be called without the EMQX application started,
e.g. in schema validation code. Schema validation also runs in the hocon
CLI, where the Erlang node is not alive and the node name is not known:
there the variable is only validated, never resolved, and defaulted binds
are left untouched. The running node checks the config again at boot,
before the listeners start, and applies the address then.
""".

-include("logger.hrl").

-define(PT_KEY, {?MODULE, value}).
-define(PT_NODENAME_KEY, {?MODULE, nodename}).
-define(ADDRESS_ENV_VAR, "EMQX_DEFAULT_ADDRESS").

-export([resolve/1, clear/0]).

-export_type([address/0]).

-type address() :: any | loopback | inet:ip_address().
-type value() :: default | loopback | all | nodename | inet:ip_address().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns the address for a defaulted listener bind in the given scope.

Returns the address resolved from the `EMQX_DEFAULT_ADDRESS` environment
variable when it is set, otherwise the security profile policy for the
scope. `any` means bind all interfaces without an explicit address;
`loopback` means the caller binds its own loopback address.

When the variable is set but the Erlang node is not alive (the hocon CLI),
returns `any` so defaulted binds stay untouched; see the module doc.
""".
-spec resolve(mqtt | dashboard) -> address().
resolve(Scope) ->
    case value() of
        default ->
            profile_policy(Scope);
        Value ->
            case is_alive() of
                true -> address(Value);
                false -> any
            end
    end.

-doc """
Clears the cached value. This function is intended for testing purposes only.
""".
clear() ->
    _ = persistent_term:erase(?PT_KEY),
    _ = persistent_term:erase(?PT_NODENAME_KEY),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

profile_policy(mqtt) ->
    emqx_security_profile:policy(mqtt_default_bind);
profile_policy(dashboard) ->
    emqx_security_profile:policy(dashboard_http_default_bind).

address(loopback) -> loopback;
address(all) -> {0, 0, 0, 0};
address(nodename) -> nodename_address();
address(IP) -> IP.

-spec value() -> value().
value() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            cache_value();
        Value ->
            Value
    end.

cache_value() ->
    Value =
        case os:getenv(?ADDRESS_ENV_VAR) of
            false -> default;
            "" -> default;
            "loopback" -> loopback;
            "all" -> all;
            "nodename" -> nodename;
            Literal -> parse_literal(Literal)
        end,
    _ = persistent_term:put(?PT_KEY, Value),
    Value.

parse_literal(Str) ->
    case inet:parse_address(Str) of
        {ok, IP} ->
            IP;
        {error, _} ->
            Message = io_lib:format(
                "Invalid default address(~p) value: ~p. "
                "Valid values are: `loopback', `nodename', `all', "
                "or a literal IPv4/IPv6 address.",
                [?ADDRESS_ENV_VAR, Str]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.

nodename_address() ->
    case persistent_term:get(?PT_NODENAME_KEY, undefined) of
        undefined ->
            cache_nodename_address();
        IP ->
            IP
    end.

cache_nodename_address() ->
    Host = host_part(atom_to_list(node())),
    IP =
        case inet:parse_address(Host) of
            {ok, Literal} -> Literal;
            {error, _} -> resolve_host(Host)
        end,
    ?SLOG(info, #{
        msg => "default_address_nodename_resolved",
        host => list_to_binary(Host),
        address => list_to_binary(inet:ntoa(IP))
    }),
    _ = persistent_term:put(?PT_NODENAME_KEY, IP),
    IP.

host_part(NodeStr) ->
    [_Name, Host] = string:split(NodeStr, "@"),
    Host.

resolve_host(Host) ->
    case inet:getaddrs(Host, inet) of
        {ok, [V4 | _]} -> V4;
        {error, _} -> resolve_host_inet6(Host)
    end.

resolve_host_inet6(Host) ->
    case inet:getaddrs(Host, inet6) of
        {ok, [V6 | _]} ->
            V6;
        {error, Reason} ->
            Message = io_lib:format(
                "~p is set to `nodename', but the node name host part ~p "
                "does not resolve to any address: ~p.",
                [?ADDRESS_ENV_VAR, Host, Reason]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.
