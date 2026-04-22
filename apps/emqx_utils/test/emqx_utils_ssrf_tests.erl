%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_ssrf_tests).

-include_lib("eunit/include/eunit.hrl").

cfg(Enable, Cidrs) ->
    cfg(Enable, Cidrs, []).

cfg(Enable, Cidrs, Hosts) ->
    #{
        enable => Enable,
        allow_cidrs => [],
        deny_cidrs => emqx_utils_ssrf:compile_cidrs(Cidrs),
        deny_hosts => [string:lowercase(H) || H <- Hosts]
    }.

cfg(Enable, AllowCidrs, DenyCidrs, DenyHosts) ->
    #{
        enable => Enable,
        allow_cidrs => emqx_utils_ssrf:compile_cidrs(AllowCidrs),
        deny_cidrs => emqx_utils_ssrf:compile_cidrs(DenyCidrs),
        deny_hosts => [string:lowercase(H) || H <- DenyHosts]
    }.

default_cfg() ->
    cfg(
        true,
        emqx_utils_ssrf:default_allow_cidrs(),
        emqx_utils_ssrf:default_deny_cidrs(),
        emqx_utils_ssrf:default_deny_hosts()
    ).

disabled_allows_all_test() ->
    Cfg = cfg(false, [<<"127.0.0.0/8">>]),
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"127.0.0.1">>, Cfg)),
    ?assertEqual(ok, emqx_utils_ssrf:check_host(<<"localhost">>, Cfg)).

ipv4_literal_denied_test() ->
    Cfg = default_cfg(),
    {error, {denied, <<"169.254.169.254">>, <<"169.254.169.254">>, Cidr}} =
        emqx_utils_ssrf:check_address(<<"169.254.169.254">>, Cfg),
    ?assertEqual(<<"169.254.0.0/16">>, Cidr).

rfc1918_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"10.0.0.0/8">>}},
        emqx_utils_ssrf:check_address(<<"10.0.0.1">>, Cfg)
    ),
    ?assertMatch(
        {error, {denied, _, _, <<"172.16.0.0/12">>}},
        emqx_utils_ssrf:check_address(<<"172.16.5.5">>, Cfg)
    ),
    ?assertMatch(
        {error, {denied, _, _, <<"192.168.0.0/16">>}},
        emqx_utils_ssrf:check_address(<<"192.168.1.1">>, Cfg)
    ).

public_ipv4_allowed_test() ->
    Cfg = default_cfg(),
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"8.8.8.8">>, Cfg)),
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"1.1.1.1">>, Cfg)).

ipv6_loopback_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"::1/128">>}},
        emqx_utils_ssrf:check_address(<<"::1">>, Cfg)
    ).

ipv6_linklocal_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"fe80::/10">>}},
        emqx_utils_ssrf:check_address(<<"fe80::1">>, Cfg)
    ).

hostname_resolution_loopback_denied_test() ->
    Cfg = default_cfg(),
    %% "localhost" resolves to 127.0.0.1 and/or ::1 on all supported hosts
    ?assertMatch(
        {error, {denied, <<"localhost">>, _, _}},
        emqx_utils_ssrf:check_host(<<"localhost">>, Cfg)
    ).

invalid_address_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {invalid_address, <<"not-an-ip">>}},
        emqx_utils_ssrf:check_address(<<"not-an-ip">>, Cfg)
    ).

malformed_cidr_test() ->
    ?assertThrow(invalid_prefix, emqx_utils_ssrf:compile_cidrs([<<"10.0.0.0/99">>])),
    ?assertThrow({invalid_cidr, _}, emqx_utils_ssrf:compile_cidrs([<<"not-a-cidr">>])),
    ?assertThrow({invalid_cidr, _}, emqx_utils_ssrf:compile_cidrs([<<"10.0.0.0/abc">>])).

family_mismatch_does_not_match_test() ->
    %% An IPv4 literal must not be matched against an IPv6 CIDR.
    Cfg = cfg(true, [<<"::1/128">>]),
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"127.0.0.1">>, Cfg)).

allow_cidr_overrides_deny_for_literal_test() ->
    Cfg = cfg(true, [<<"10.0.0.0/8">>], emqx_utils_ssrf:default_deny_cidrs(), []),
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"10.0.0.1">>, Cfg)).

allow_cidr_overrides_deny_for_resolved_host_test() ->
    Cfg = cfg(true, [<<"127.0.0.0/8">>], emqx_utils_ssrf:default_deny_cidrs(), []),
    ?assertEqual(ok, emqx_utils_ssrf:check_host(<<"localhost">>, Cfg)).

ipv4_mapped_ipv6_loopback_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"127.0.0.0/8">>}},
        emqx_utils_ssrf:check_address(<<"::ffff:127.0.0.1">>, Cfg)
    ).

ipv4_mapped_ipv6_imds_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"169.254.0.0/16">>}},
        emqx_utils_ssrf:check_address(<<"::ffff:169.254.169.254">>, Cfg)
    ).

ipv4_mapped_ipv6_rfc1918_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"10.0.0.0/8">>}},
        emqx_utils_ssrf:check_address(<<"::ffff:10.0.0.1">>, Cfg)
    ).

aliyun_metadata_ip_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"100.100.100.200/32">>}},
        emqx_utils_ssrf:check_address(<<"100.100.100.200">>, Cfg)
    ).

aws_external_metadata_ip_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied, _, _, <<"69.254.169.253/32">>}},
        emqx_utils_ssrf:check_address(<<"69.254.169.253">>, Cfg)
    ).

aws_ipv6_metadata_denied_test() ->
    Cfg = default_cfg(),
    %% fd00:ec2::254 falls under the ULA fc00::/7 default
    ?assertMatch(
        {error, {denied, _, _, <<"fc00::/7">>}},
        emqx_utils_ssrf:check_address(<<"fd00:ec2::254">>, Cfg)
    ).

tencent_metadata_host_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied_host, <<"metadata.tencentyun.com">>}},
        emqx_utils_ssrf:check_host(<<"metadata.tencentyun.com">>, Cfg)
    ).

google_metadata_host_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied_host, <<"metadata.google.internal">>}},
        emqx_utils_ssrf:check_host(<<"metadata.google.internal">>, Cfg)
    ).

azure_metadata_host_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied_host, <<"metadata.azure.internal">>}},
        emqx_utils_ssrf:check_host(<<"metadata.azure.internal">>, Cfg)
    ).

metadata_host_case_insensitive_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied_host, _}},
        emqx_utils_ssrf:check_host(<<"Metadata.Google.Internal">>, Cfg)
    ).

metadata_host_trailing_dot_denied_test() ->
    Cfg = default_cfg(),
    ?assertMatch(
        {error, {denied_host, <<"metadata.google.internal.">>}},
        emqx_utils_ssrf:check_host(<<"metadata.google.internal.">>, Cfg)
    ).

refresh_cache_uses_cached_policy_test() ->
    emqx_utils_ssrf:refresh_cache(#{
        enable => true,
        allow_cidrs => [<<"8.8.8.0/24">>],
        deny_cidrs => [<<"127.0.0.0/8">>],
        deny_hosts => [<<"Metadata.Google.Internal">>]
    }),
    try
        ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"8.8.8.8">>)),
        ?assertMatch(
            {error, {denied, _, _, <<"127.0.0.0/8">>}},
            emqx_utils_ssrf:check_address(<<"127.0.0.1">>)
        ),
        ?assertMatch(
            {error, {denied_host, <<"metadata.google.internal">>}},
            emqx_utils_ssrf:check_host(<<"metadata.google.internal">>)
        )
    after
        persistent_term:erase({emqx_utils_ssrf, cache})
    end.

refresh_cache_invalid_input_resets_cache_test() ->
    persistent_term:put(
        {emqx_utils_ssrf, cache},
        #{
            enable => true,
            allow_cidrs => [],
            deny_cidrs => emqx_utils_ssrf:compile_cidrs([<<"127.0.0.0/8">>]),
            deny_hosts => []
        }
    ),
    emqx_utils_ssrf:refresh_cache(#{}),
    try
        ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"127.0.0.1">>))
    after
        persistent_term:erase({emqx_utils_ssrf, cache})
    end.

default_cfg_map_bypasses_checks_test() ->
    ?assertEqual(ok, emqx_utils_ssrf:check_address(<<"not-an-ip">>, #{})),
    ?assertEqual(ok, emqx_utils_ssrf:check_host(<<"localhost">>, #{})).

format_error_test() ->
    Msg = emqx_utils_ssrf:format_error(
        {denied, <<"host">>, <<"1.2.3.4">>, <<"1.0.0.0/8">>}
    ),
    ?assert(is_binary(Msg)),
    ?assert(binary:match(Msg, <<"host">>) =/= nomatch),
    ?assert(binary:match(Msg, <<"1.2.3.4">>) =/= nomatch),
    ?assert(binary:match(Msg, <<"1.0.0.0/8">>) =/= nomatch).
