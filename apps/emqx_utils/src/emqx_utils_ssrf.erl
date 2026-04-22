%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_ssrf).
-moduledoc """
SSRF defence-in-depth checks for outbound target addresses
configured via the rule-engine (connectors).

The policy is cached in `persistent_term` and refreshed through
`refresh_cache/1` from a `post_config_update/5` callback. Callers use
`check_host/1` (resolves a hostname) or `check_address/1` (IP literal
only). Both return `ok` when the check is disabled or the address is
not denied, and `{error, Reason}` otherwise, where `Reason` carries
enough information for a user-facing error message.

This validation happens only at config-update time. It does not protect
against malicious DNS changes after validation has passed, so operators
must still enforce outbound egress restrictions at the network layer.
""".

-export([
    default_allow_cidrs/0,
    default_deny_cidrs/0,
    default_deny_hosts/0,
    refresh_cache/1,
    compile_cidrs/1,
    check_host/1,
    check_host/2,
    check_address/1,
    check_address/2,
    format_error/1
]).

-export_type([cidr/0, compiled/0, check_error/0]).

-define(CACHE_KEY, {?MODULE, cache}).
-define(EMPTY, #{enable => false, allow_cidrs => [], deny_cidrs => [], deny_hosts => []}).

-type cidr() :: binary() | string().
-type ip() :: inet:ip_address().
-type compiled() :: {ip(), 0..128}.
-type check_error() ::
    {denied, Host :: binary(), IP :: binary(), Cidr :: binary()}
    | {denied_host, Host :: binary()}
    | {invalid_address, Host :: binary()}.
cached() ->
    persistent_term:get(?CACHE_KEY, ?EMPTY).

-spec default_allow_cidrs() -> [binary()].
default_allow_cidrs() ->
    [].

-spec default_deny_cidrs() -> [binary()].
default_deny_cidrs() ->
    [
        <<"127.0.0.0/8">>,
        <<"::1/128">>,
        <<"169.254.0.0/16">>,
        <<"fe80::/10">>,
        <<"10.0.0.0/8">>,
        <<"172.16.0.0/12">>,
        <<"192.168.0.0/16">>,
        <<"fc00::/7">>,
        <<"0.0.0.0/32">>,
        <<"224.0.0.0/4">>,
        <<"ff00::/8">>,
        %% Aliyun metadata service
        <<"100.100.100.200/32">>,
        %% AWS External Metadata Service (literal IP in the wild)
        <<"69.254.169.253/32">>
    ].

-spec default_deny_hosts() -> [binary()].
default_deny_hosts() ->
    [
        <<"metadata.tencentyun.com">>,
        <<"metadata.google.internal">>,
        <<"metadata.azure.internal">>
    ].

-spec refresh_cache(map()) -> ok.
refresh_cache(#{enable := Enable} = Cfg) ->
    AllowCidrs = maps:get(allow_cidrs, Cfg, []),
    DenyCidrs = maps:get(deny_cidrs, Cfg, []),
    DenyHosts = maps:get(deny_hosts, Cfg, []),
    persistent_term:put(?CACHE_KEY, #{
        enable => Enable,
        allow_cidrs => compile_cidrs(AllowCidrs),
        deny_cidrs => compile_cidrs(DenyCidrs),
        deny_hosts => normalize_hosts(DenyHosts)
    }),
    ok;
refresh_cache(_) ->
    persistent_term:put(?CACHE_KEY, ?EMPTY),
    ok.

normalize_hosts(Hosts) ->
    [normalize_host(H) || H <- Hosts].

normalize_host(H) when is_binary(H) ->
    strip_trailing_dots(string:lowercase(H));
normalize_host(H) when is_list(H) ->
    strip_trailing_dots(string:lowercase(iolist_to_binary(H))).

strip_trailing_dots(<<>>) ->
    <<>>;
strip_trailing_dots(Host) ->
    re:replace(Host, <<"\\.+$">>, <<>>, [{return, binary}]).

%%--------------------------------------------------------------------
%% CIDR compilation
%%--------------------------------------------------------------------

-spec compile_cidrs([cidr()]) -> [compiled()].
compile_cidrs(Cidrs) ->
    lists:map(fun compile_cidr/1, Cidrs).

compile_cidr(Cidr) when is_binary(Cidr) ->
    compile_cidr(binary_to_list(Cidr));
compile_cidr(Cidr) when is_list(Cidr) ->
    case string:split(Cidr, "/") of
        [IpStr, PrefixStr] ->
            case {inet:parse_address(IpStr), to_int(PrefixStr)} of
                {{ok, IP}, {ok, Prefix}} ->
                    validate_prefix(IP, Prefix),
                    {IP, Prefix};
                _ ->
                    throw({invalid_cidr, list_to_binary(Cidr)})
            end;
        _ ->
            throw({invalid_cidr, list_to_binary(Cidr)})
    end.

validate_prefix({_, _, _, _}, P) when P >= 0, P =< 32 -> ok;
validate_prefix({_, _, _, _, _, _, _, _}, P) when P >= 0, P =< 128 -> ok;
validate_prefix(_, _) -> throw(invalid_prefix).

to_int(S) ->
    try
        {ok, list_to_integer(S)}
    catch
        _:_ -> error
    end.

%%--------------------------------------------------------------------
%% Checks
%%--------------------------------------------------------------------

-spec check_address(cidr() | ip()) -> ok | {error, check_error()}.
check_address(Addr) ->
    check_address(Addr, cached()).

-spec check_address(cidr() | ip(), map()) -> ok | {error, check_error()}.
check_address(_Addr, #{enable := false}) ->
    ok;
check_address(Addr, #{allow_cidrs := AllowCidrs, deny_cidrs := DenyCidrs}) ->
    case to_ip(Addr) of
        {ok, IP} ->
            check_ip(Addr, normalize_mapped(IP), AllowCidrs, DenyCidrs);
        error ->
            {error, {invalid_address, to_bin(Addr)}}
    end;
check_address(_Addr, _) ->
    ok.

-spec check_host(cidr()) -> ok | {error, check_error()}.
check_host(Host) ->
    check_host(Host, cached()).

-spec check_host(cidr(), map()) -> ok | {error, check_error()}.
check_host(_Host, #{enable := false}) ->
    ok;
check_host(Host, #{allow_cidrs := AllowCidrs, deny_cidrs := DenyCidrs} = Cfg) ->
    HostBin = to_bin(Host),
    HostStr = binary_to_list(HostBin),
    DenyHosts = maps:get(deny_hosts, Cfg, []),
    case match_host(HostBin, DenyHosts) of
        {error, _} = Err ->
            Err;
        ok ->
            case inet:parse_address(HostStr) of
                {ok, IP} ->
                    check_ip(HostBin, normalize_mapped(IP), AllowCidrs, DenyCidrs);
                {error, _} ->
                    resolve_and_check(HostBin, HostStr, Cfg)
            end
    end;
check_host(_Host, _) ->
    ok.

match_host(HostBin, Hosts) ->
    Normalized = normalize_host(HostBin),
    case lists:member(Normalized, Hosts) of
        true -> {error, {denied_host, HostBin}};
        false -> ok
    end.

resolve_and_check(HostBin, HostStr, #{allow_cidrs := AllowCidrs, deny_cidrs := DenyCidrs}) ->
    IPs = lists:usort(resolve(HostStr, inet) ++ resolve(HostStr, inet6)),
    case IPs of
        [] ->
            %% Validation is DNS-dependent and intentionally fail-open
            %% here to avoid blocking config updates on transient lookup
            %% failures. This module is therefore not a defense against
            %% malicious post-validation DNS changes; operators still
            %% need network-layer egress controls.
            ok;
        _ ->
            check_resolved_ips(HostBin, [normalize_mapped(IP) || IP <- IPs], AllowCidrs, DenyCidrs)
    end.

resolve(HostStr, Family) ->
    case inet:getaddrs(HostStr, Family) of
        {ok, L} -> L;
        {error, _} -> []
    end.

check_ip(Host, IP, AllowCidrs, DenyCidrs) ->
    case match_cidrs(IP, AllowCidrs) of
        {ok, _} ->
            ok;
        false ->
            case match_cidrs(IP, DenyCidrs) of
                {ok, Cidr} ->
                    {error, {denied, to_bin(Host), ip_to_bin(IP), cidr_to_bin(Cidr)}};
                false ->
                    ok
            end
    end.

check_resolved_ips(_Host, [], _AllowCidrs, _DenyCidrs) ->
    ok;
check_resolved_ips(Host, IPs, AllowCidrs, DenyCidrs) ->
    case first_match(IPs, AllowCidrs) of
        {ok, _AllowedIP, _AllowedCidr} ->
            ok;
        false ->
            case first_match(IPs, DenyCidrs) of
                {ok, DeniedIP, DeniedCidr} ->
                    {error, {denied, to_bin(Host), ip_to_bin(DeniedIP), cidr_to_bin(DeniedCidr)}};
                false ->
                    ok
            end
    end.

%% Collapse IPv4-mapped IPv6 (::ffff:a.b.c.d) to its IPv4 tuple so
%% denylist matching works regardless of which family a caller used
%% to express the literal.
normalize_mapped({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
    {AB bsr 8, AB band 16#ff, CD bsr 8, CD band 16#ff};
normalize_mapped(IP) ->
    IP.

first_match([], _Cidrs) ->
    false;
first_match([IP | Rest], Cidrs) ->
    case match_cidrs(IP, Cidrs) of
        {ok, Cidr} ->
            {ok, IP, Cidr};
        false ->
            first_match(Rest, Cidrs)
    end.

match_cidrs(_IP, []) ->
    false;
match_cidrs(IP, [{CidrIP, Prefix} = C | Rest]) ->
    case same_family(IP, CidrIP) andalso in_range(IP, CidrIP, Prefix) of
        true ->
            {ok, C};
        false ->
            match_cidrs(IP, Rest)
    end.

same_family(A, B) when tuple_size(A) =:= tuple_size(B) -> true;
same_family(_, _) -> false.

in_range(IP, CidrIP, Prefix) ->
    ip_to_int(IP) bsr shift(IP, Prefix) =:=
        ip_to_int(CidrIP) bsr shift(CidrIP, Prefix).

shift({_, _, _, _}, Prefix) -> 32 - Prefix;
shift({_, _, _, _, _, _, _, _}, Prefix) -> 128 - Prefix.

ip_to_int({A, B, C, D}) ->
    (A bsl 24) bor (B bsl 16) bor (C bsl 8) bor D;
ip_to_int({A, B, C, D, E, F, G, H}) ->
    (A bsl 112) bor (B bsl 96) bor (C bsl 80) bor (D bsl 64) bor
        (E bsl 48) bor (F bsl 32) bor (G bsl 16) bor H.

%%--------------------------------------------------------------------
%% Formatting
%%--------------------------------------------------------------------

-spec format_error(check_error()) -> binary().
format_error({denied, Host, IP, Cidr}) ->
    iolist_to_binary(
        io_lib:format(
            "Address resolves to a denied range: ~ts -> ~ts (matches ~ts). "
            "Adjust rule_engine.ssrf.deny_cidrs, "
            "rule_engine.ssrf.allow_cidrs, or set "
            "rule_engine.ssrf.enable=false.",
            [Host, IP, Cidr]
        )
    );
format_error({denied_host, Host}) ->
    iolist_to_binary(
        io_lib:format(
            "Host is on the SSRF denylist: ~ts. "
            "Adjust rule_engine.ssrf.deny_hosts or set "
            "rule_engine.ssrf.enable=false.",
            [Host]
        )
    );
format_error({invalid_address, Host}) ->
    iolist_to_binary(io_lib:format("Invalid address: ~ts", [Host])).

%%--------------------------------------------------------------------
%% Conversions
%%--------------------------------------------------------------------

to_ip(IP) when is_tuple(IP) -> {ok, IP};
to_ip(Bin) when is_binary(Bin) -> to_ip(binary_to_list(Bin));
to_ip(Str) when is_list(Str) ->
    case inet:parse_address(Str) of
        {ok, IP} -> {ok, IP};
        {error, _} -> error
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(T) when is_tuple(T) -> ip_to_bin(T).

ip_to_bin(IP) ->
    iolist_to_binary(inet:ntoa(IP)).

cidr_to_bin({IP, Prefix}) ->
    iolist_to_binary([inet:ntoa(IP), $/, integer_to_list(Prefix)]).
