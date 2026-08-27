%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_default_address).

-moduledoc """
This module resolves the `EMQX_DEFAULT_ADDRESS` boot environment variable,
which sets the address of defaulted listener binds independently of the
security profile.

Valid values are `loopback`, `hostname_i`, `all`, or a literal IPv4/IPv6
address. When the variable is not set, the security profile policy decides
the default address.

NOTE: this module may be called without the EMQX application started,
e.g. in schema validation code.
""".

-include("logger.hrl").

-define(PT_KEY, {?MODULE, address}).
-define(ADDRESS_ENV_VAR, "EMQX_DEFAULT_ADDRESS").

-export([resolve/1, clear/0]).

-export_type([address/0]).

-type address() :: any | loopback | inet:ip_address().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns the address for a defaulted listener bind in the given scope.

Returns the address resolved from the `EMQX_DEFAULT_ADDRESS` environment
variable when it is set, otherwise the security profile policy for the
scope. `any` means bind all interfaces without an explicit address;
`loopback` means the caller binds its own loopback address.
""".
-spec resolve(mqtt | dashboard) -> address().
resolve(Scope) ->
    case address() of
        default -> profile_policy(Scope);
        Address -> Address
    end.

-doc """
Clears the cached address. This function is intended for testing purposes only.
""".
clear() ->
    persistent_term:erase(?PT_KEY).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

profile_policy(mqtt) ->
    emqx_security_profile:policy(mqtt_default_bind);
profile_policy(dashboard) ->
    emqx_security_profile:policy(dashboard_http_default_bind).

address() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            cache_address();
        Address ->
            Address
    end.

cache_address() ->
    Address =
        case os:getenv(?ADDRESS_ENV_VAR) of
            false -> default;
            "" -> default;
            "loopback" -> loopback;
            "all" -> {0, 0, 0, 0};
            "hostname_i" -> resolve_hostname();
            Literal -> parse_literal(Literal)
        end,
    _ = persistent_term:put(?PT_KEY, Address),
    Address.

parse_literal(Str) ->
    case inet:parse_address(Str) of
        {ok, IP} ->
            IP;
        {error, _} ->
            Message = io_lib:format(
                "Invalid default address(~p) value: ~p. "
                "Valid values are: `loopback', `hostname_i', `all', "
                "or a literal IPv4/IPv6 address.",
                [?ADDRESS_ENV_VAR, Str]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.

resolve_hostname() ->
    {ok, Hostname} = inet:gethostname(),
    IP =
        case inet:getaddrs(Hostname, inet) of
            {ok, [V4 | _]} -> V4;
            {error, _} -> resolve_hostname_inet6(Hostname)
        end,
    ?SLOG(info, #{
        msg => "default_address_hostname_resolved",
        hostname => Hostname,
        address => list_to_binary(inet:ntoa(IP))
    }),
    IP.

resolve_hostname_inet6(Hostname) ->
    case inet:getaddrs(Hostname, inet6) of
        {ok, [V6 | _]} ->
            V6;
        {error, Reason} ->
            Message = io_lib:format(
                "~p is set to `hostname_i', but the hostname ~p "
                "does not resolve to any address: ~p.",
                [?ADDRESS_ENV_VAR, Hostname, Reason]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.
