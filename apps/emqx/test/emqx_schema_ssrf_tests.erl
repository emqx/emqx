%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_ssrf_tests).

-include_lib("eunit/include/eunit.hrl").

setup(Enable) ->
    persistent_term:put(
        {emqx_utils_ssrf, cache},
        #{
            enable => Enable,
            allow_cidrs => [],
            deny_cidrs => emqx_utils_ssrf:compile_cidrs(
                emqx_utils_ssrf:default_deny_cidrs()
            ),
            deny_hosts => []
        }
    ).

teardown() ->
    persistent_term:erase({emqx_utils_ssrf, cache}).

with_ssrf(Enable, Fun) ->
    setup(Enable),
    try
        Fun()
    after
        teardown()
    end.

parse_server_public_allowed_test() ->
    with_ssrf(true, fun() ->
        ?assertEqual(
            #{hostname => "example.com", port => 1883},
            emqx_schema:parse_server(
                <<"example.com:1883">>,
                #{default_port => 1883, ssrf_check => true}
            )
        )
    end).

parse_server_rejects_imds_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            "Address resolves" ++ _,
            emqx_schema:parse_server(
                <<"169.254.169.254:80">>,
                #{default_port => 80, ssrf_check => true}
            )
        )
    end).

parse_servers_rejects_rfc1918_in_list_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            "Address resolves" ++ _,
            emqx_schema:parse_servers(
                <<"example.com:9092,10.0.0.5:9092">>,
                #{default_port => 9092, ssrf_check => true}
            )
        )
    end).

disabled_allows_private_test() ->
    with_ssrf(false, fun() ->
        ?assertEqual(
            #{hostname => "10.0.0.5", port => 9092},
            emqx_schema:parse_server(
                <<"10.0.0.5:9092">>,
                #{default_port => 9092, ssrf_check => true}
            )
        )
    end).

%% Without ssrf_check opt the check must not fire even if enabled
%% and the target resolves into a denied range.
no_opt_bypasses_check_test() ->
    with_ssrf(true, fun() ->
        ?assertEqual(
            #{hostname => "10.0.0.5", port => 9092},
            emqx_schema:parse_server(
                <<"10.0.0.5:9092">>,
                #{default_port => 9092}
            )
        )
    end).
