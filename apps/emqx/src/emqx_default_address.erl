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
e.g. in schema validation code.
""".

-include("logger.hrl").

-define(PT_KEY, {?MODULE, address}).
-define(ADDRESS_ENV_VAR, "EMQX_DEFAULT_ADDRESS").
-define(NODE_NAME_ENV_VAR, "EMQX_NODE__NAME").

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
            "nodename" -> resolve_nodename();
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
                "Valid values are: `loopback', `nodename', `all', "
                "or a literal IPv4/IPv6 address.",
                [?ADDRESS_ENV_VAR, Str]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.

resolve_nodename() ->
    Host = nodename_host(),
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
    IP.

%% During boot, the config-generating escript calls run before the node is
%% started, so `node()' is not usable there; bin/emqx exports the resolved
%% node name as EMQX_NODE__NAME for those calls.
nodename_host() ->
    case is_alive() of
        true -> host_part(atom_to_list(node()));
        false -> nodename_host_from_env()
    end.

nodename_host_from_env() ->
    case os:getenv(?NODE_NAME_ENV_VAR) of
        false ->
            no_node_name_error();
        "" ->
            no_node_name_error();
        NodeStr ->
            host_part(NodeStr)
    end.

host_part(NodeStr) ->
    case string:split(NodeStr, "@") of
        [_Name, Host] ->
            Host;
        _ ->
            %% A short node name gets the short hostname as its host part.
            {ok, Host} = inet:gethostname(),
            Host
    end.

no_node_name_error() ->
    Message = io_lib:format(
        "~p is set to `nodename', but the Erlang node is not alive "
        "and ~p is not set, so there is no node name to take the "
        "address from.",
        [?ADDRESS_ENV_VAR, ?NODE_NAME_ENV_VAR]
    ),
    exit({invalid_default_address, iolist_to_binary(Message)}).

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
