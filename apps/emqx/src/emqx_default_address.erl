%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_default_address).

-moduledoc """
This module resolves the `EMQX_DEFAULT_ADDRESS` boot environment variable,
which sets the address of listener binds that have no explicit address,
independently of the security profile.

Valid values are `loopback`, `nodename`, `all`, a literal IPv4/IPv6
address, or a hostname to resolve at boot. When the variable is not set,
the security profile policy decides the default address.

Schema defaults stay static bare ports; this module is called from
runtime code only, on the running node, when a listener is started.
""".

-include("logger.hrl").

-define(PT_KEY, {?MODULE, value}).
-define(PT_RESOLVED_KEY, {?MODULE, resolved_host}).
-define(ADDRESS_ENV_VAR, "EMQX_DEFAULT_ADDRESS").

-export([resolve/1, listen_on/2, clear/0]).

-export_type([address/0, scope/0]).

-type address() :: any | loopback | inet:ip_address().
-type scope() :: mqtt | dashboard | gateway.
-type value() ::
    default | loopback | all | nodename | {hostname, string()} | inet:ip_address().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns the address for a bare-port listener bind in the given scope.

Returns the address resolved from the `EMQX_DEFAULT_ADDRESS` environment
variable when it is set, otherwise the security profile policy for the
scope. `any` means bind all interfaces without an explicit address;
`loopback` means the caller binds its own loopback address.
""".
-spec resolve(scope()) -> address().
resolve(Scope) ->
    case value() of
        default -> profile_policy(Scope);
        Value -> address(Value)
    end.

-doc """
Applies the default address to a listener bind. A bare-port bind gets the
resolved address; a bind with an explicit address is returned unchanged.

Call this everywhere a bind from the configuration is turned into a
listen-on address, so all uses of one listener agree on the same value.
""".
-spec listen_on(scope(), Bind) -> Bind | {inet:ip_address(), inet:port_number()} when
    Bind :: term().
listen_on(Scope, Port) when is_integer(Port) ->
    case resolve(Scope) of
        any -> Port;
        loopback -> {{127, 0, 0, 1}, Port};
        IP -> {IP, Port}
    end;
listen_on(_Scope, Bind) ->
    Bind.

-doc """
Clears the cached value. This function is intended for testing purposes only.
""".
clear() ->
    _ = persistent_term:erase(?PT_KEY),
    _ = persistent_term:erase(?PT_RESOLVED_KEY),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

profile_policy(mqtt) ->
    emqx_security_profile:policy(mqtt_default_bind);
profile_policy(dashboard) ->
    emqx_security_profile:policy(dashboard_http_default_bind);
profile_policy(gateway) ->
    %% The security profile does not cover gateway binds.
    any.

address(loopback) -> loopback;
address(all) -> {0, 0, 0, 0};
address(nodename) -> host_address(host_part(atom_to_list(node())));
address({hostname, Host}) -> host_address(Host);
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
            Str -> parse_address_or_hostname(Str)
        end,
    _ = persistent_term:put(?PT_KEY, Value),
    Value.

parse_address_or_hostname(Str) ->
    case inet:parse_address(Str) of
        {ok, IP} ->
            IP;
        {error, _} ->
            validate_hostname(Str)
    end.

validate_hostname(Str) ->
    case is_valid_hostname(Str) of
        true ->
            {hostname, Str};
        false ->
            Message = io_lib:format(
                "Invalid default address(~p) value: ~p. "
                "Valid values are: `loopback', `nodename', `all', "
                "a literal IPv4/IPv6 address, or a hostname.",
                [?ADDRESS_ENV_VAR, Str]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.

is_valid_hostname(Str) ->
    Label = "[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?",
    RE = "^" ++ Label ++ "(\\." ++ Label ++ ")*$",
    length(Str) =< 253 andalso re:run(Str, RE, [{capture, none}]) =:= match.

host_part(NodeStr) ->
    [_Name, Host] = string:split(NodeStr, "@"),
    Host.

host_address(Host) ->
    case persistent_term:get(?PT_RESOLVED_KEY, undefined) of
        undefined ->
            cache_host_address(Host);
        IP ->
            IP
    end.

cache_host_address(Host) ->
    %% The nodename host part may itself be an IP address.
    IP =
        case inet:parse_address(Host) of
            {ok, Literal} -> Literal;
            {error, _} -> resolve_host(Host)
        end,
    ?SLOG(info, #{
        msg => "default_address_host_resolved",
        host => list_to_binary(Host),
        address => list_to_binary(inet:ntoa(IP))
    }),
    _ = persistent_term:put(?PT_RESOLVED_KEY, IP),
    IP.

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
                "~p: the host ~p does not resolve to any address: ~p.",
                [?ADDRESS_ENV_VAR, Host, Reason]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.
