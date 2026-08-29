%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_default_address).

-moduledoc """
This module resolves the `node.default_listener_address` config, which sets
the address of listener binds that have no explicit address, independently
of the security profile. The config is boot-only; the usual environment
override `EMQX_NODE__DEFAULT_LISTENER_ADDRESS` applies.

Valid values are `loopback`, `nodename`, `all`, a literal IPv4/IPv6
address, or a hostname to resolve at boot. When the config is not set,
the security profile policy decides the default address.

Schema defaults stay static bare ports, and the schema only validates the
value (`validate/1`); resolution happens in runtime code only, on the
running node, when a listener is started.
""".

-include("logger.hrl").

-define(PT_KEY, {?MODULE, value}).
-define(PT_RESOLVED_KEY, {?MODULE, resolved_host}).
-define(CONF_KEY, [node, default_listener_address]).
-define(CONF_NAME, "node.default_listener_address").

-export([
    resolve/1,
    listen_on/2,
    resolved_from/1,
    listen_on_from/2,
    validate/1,
    loopback_by_profile/0,
    clear/0
]).

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

Returns the address resolved from the `node.default_listener_address`
config when it is set, otherwise the security profile policy for the
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
Returns the source of the address `resolve/1` returns for the scope:
`<<"all">>`, `<<"loopback">>`, `<<"nodename">>`, or the literal
`node.default_listener_address` value (a hostname or IP string) when it is
set to one of those.

Unlike `resolve/1`, this stays the same across a cluster with identical
config even when the resolved address itself differs from node to node.
Under `nodename`, for example, every node resolves its own host part, so
`resolve/1` legitimately differs per node while this stays `<<"nodename">>`
everywhere: it explains that difference instead of leaving it looking like
a misconfiguration.
""".
-spec resolved_from(scope()) -> binary().
resolved_from(Scope) ->
    case value() of
        default -> profile_policy_label(Scope);
        loopback -> <<"loopback">>;
        all -> <<"all">>;
        nodename -> <<"nodename">>;
        {hostname, Host} -> unicode:characters_to_binary(Host);
        IP -> list_to_binary(inet:ntoa(IP))
    end.

-doc """
Same branching as `listen_on/2`: `<<"bind">>` for a bind that already has an
explicit address, so the address did not come from this module at all,
otherwise the category `resolved_from/1` reports for the scope.
""".
-spec listen_on_from(scope(), Bind :: term()) -> binary().
listen_on_from(_Scope, Bind) when not is_integer(Bind) ->
    <<"bind">>;
listen_on_from(Scope, Port) when is_integer(Port) ->
    resolved_from(Scope).

-doc """
Returns true when listeners bind loopback because the security profile says
so, and false when `node.default_listener_address` decides the address.

An operator who sets the address gets what they asked for. An operator who
only picks a security profile may not expect listeners to stop accepting
remote connections, so the caller warns about this case.
""".
-spec loopback_by_profile() -> boolean().
loopback_by_profile() ->
    value() =:= default andalso
        emqx_security_profile:policy(mqtt_default_bind) =:= loopback.

-doc """
Validates a `node.default_listener_address` value. Schema validator; pure,
it never resolves anything.
""".
-spec validate(string() | binary()) -> ok | {error, binary()}.
validate(Value) ->
    case parse(str(Value)) of
        {ok, _} -> ok;
        {error, Message} -> {error, Message}
    end.

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

profile_policy_label(Scope) ->
    case profile_policy(Scope) of
        any -> <<"all">>;
        loopback -> <<"loopback">>
    end.

address(loopback) -> loopback;
%% Bind all interfaces without an explicit address, which is what a bare
%% port already does. Keeping the bind a bare port means `all' leaves every
%% listener identifier exactly as it is without this config.
address(all) -> any;
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
        case emqx_config:get(?CONF_KEY, undefined) of
            undefined -> default;
            Conf -> parse_or_exit(str(Conf))
        end,
    _ = persistent_term:put(?PT_KEY, Value),
    Value.

parse_or_exit(Str) ->
    case parse(Str) of
        {ok, Value} ->
            Value;
        {error, Message} ->
            exit({invalid_default_address, Message})
    end.

-spec parse(string()) -> {ok, value()} | {error, binary()}.
parse("loopback") ->
    {ok, loopback};
parse("all") ->
    {ok, all};
parse("nodename") ->
    {ok, nodename};
parse(Str) ->
    case inet:parse_address(Str) of
        {ok, IP} ->
            {ok, IP};
        {error, _} ->
            parse_hostname(Str)
    end.

parse_hostname(Str) ->
    %% inet_parse:domain/1 accepts exactly what OTP's resolver treats as a
    %% hostname, so resolution at listener start gets only resolvable shapes.
    case inet_parse:domain(Str) of
        true ->
            {ok, {hostname, Str}};
        false ->
            Message = io_lib:format(
                "Invalid "
                ?CONF_NAME
                " value: ~p. "
                "Valid values are: `loopback', `nodename', `all', "
                "a literal IPv4/IPv6 address, or a hostname.",
                [Str]
            ),
            {error, iolist_to_binary(Message)}
    end.

str(Bin) when is_binary(Bin) -> unicode:characters_to_list(Bin);
str(List) when is_list(List) -> List.

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
                ?CONF_NAME ": the host ~p does not resolve to any address: ~p.",
                [Host, Reason]
            ),
            exit({invalid_default_address, iolist_to_binary(Message)})
    end.
