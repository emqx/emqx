%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_resource_tests).

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

parse_url_allows_public_host_test() ->
    with_ssrf(true, fun() ->
        ?assertMatch(
            {_, _},
            emqx_connector_resource:parse_url(<<"http://example.com:8080/path">>)
        )
    end).

parse_url_rejects_imds_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            #{kind := validation_error, reason := <<"Address resolves", _/binary>>},
            emqx_connector_resource:parse_url(<<"http://169.254.169.254/">>)
        )
    end).

parse_url_rejects_rfc1918_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            #{kind := validation_error},
            emqx_connector_resource:parse_url(<<"http://10.0.0.1:8080/">>)
        )
    end).

parse_url_allows_private_when_disabled_test() ->
    with_ssrf(false, fun() ->
        ?assertMatch(
            {_, _},
            emqx_connector_resource:parse_url(<<"http://10.0.0.1:8080/">>)
        )
    end).
