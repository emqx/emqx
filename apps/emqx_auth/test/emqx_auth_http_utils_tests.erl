%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_http_utils_tests).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_auth_http_utils, [
    parse_url/1,
    parse_url_template/2,
    is_templated_host_url/1,
    parse_allowed_hosts/1,
    validate_allowed_hosts_field/1,
    render_host/3,
    format_url/4
]).

%% Defense-in-depth: even if some new render path bypasses the PP2 ingestion
%% sanitizer, the HTTP request generator must reject header values containing
%% bytes that would split the request line.
generate_request_rejects_crlf_in_header_test_() ->
    [
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_cr}},
            do_generate(<<"alice\r\nX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_lf}},
            do_generate(<<"alice\nX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_cr}},
            do_generate(<<"alice\rX-Override-Result: allow">>)
        ),
        ?_assertMatch(
            {error, {bad_http_header_value, _, contains_null}},
            do_generate(<<"alice", 0, "X-Override-Result: allow">>)
        )
    ].

generate_request_accepts_clean_header_test() ->
    ?assertMatch(
        {ok, _},
        do_generate(<<"alice.example.com">>)
    ).

do_generate(CN) ->
    {_, HeadersTpl} = emqx_authn_utils:parse_deep(
        maps:to_list(#{
            <<"content-type">> => <<"application/json">>,
            <<"x-cert-cn">> => <<"${cert_common_name}">>
        })
    ),
    {_, BodyTpl} = emqx_authn_utils:parse_deep(#{}),
    {_, QueryTpl} = emqx_authn_utils:parse_deep([]),
    {_, PathTpl} = emqx_authn_utils:parse_str(<<"/auth">>),
    State = #{
        method => post,
        headers_template => HeadersTpl,
        body_template => BodyTpl,
        base_path_template => PathTpl,
        base_query_template => QueryTpl
    },
    emqx_auth_http_utils:generate_request(State, #{cert_common_name => CN}).

%%--------------------------------------------------------------------
%% URL parsing / dynamic hostname resolution
%%--------------------------------------------------------------------

templates_test_() ->
    [
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com?client=${clientid}">>)
        ),
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"/path">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com/path?client=${clientid}">>)
        ),
        ?_assertEqual(
            {#{port => 80, scheme => http, host => "example.com"}, <<"/path">>, <<>>},
            parse_url(<<"http://example.com/path">>)
        )
    ].

parse_url_template_test_() ->
    [
        ?_assertEqual(
            {pooled, {#{port => 80, scheme => http, host => "example.com"}, <<"/path">>, <<>>}},
            parse_url_template(<<"http://example.com/path">>, static)
        ),
        ?_assertEqual(
            {templated_host, #{
                scheme => https,
                host_template => <<"${client_attrs.tns}.auth.example.com">>,
                port => 443,
                path => <<"/authn">>,
                query => <<>>
            }},
            parse_url_template(<<"https://${client_attrs.tns}.auth.example.com/authn">>, dynamic)
        ),
        ?_assertEqual(
            {templated_host, #{
                scheme => http,
                host_template => <<"${username}.example.com">>,
                port => 8080,
                path => <<"">>,
                query => <<"client=${clientid}">>
            }},
            parse_url_template(
                <<"http://${username}.example.com:8080?client=${clientid}">>, dynamic
            )
        ),
        %% a templated host parses as such regardless of the requested resolution;
        %% callers decide whether that combination is allowed
        ?_assertMatch(
            {templated_host, #{host_template := <<"${username}.example.com">>}},
            parse_url_template(<<"http://${username}.example.com">>, static)
        ),
        ?_assertThrow(
            {invalid_url, {templated_host_not_supported, _}},
            parse_url(<<"http://${username}.example.com/path">>)
        ),
        ?_assertThrow(
            {invalid_url, {unsupported_scheme, _}},
            parse_url_template(<<"ftp://${username}.example.com/path">>, dynamic)
        ),
        %% templated port makes the host "loose" (contains a colon)
        ?_assertThrow(
            {invalid_url, {unsupported_templated_host, _}},
            parse_url_template(<<"http://${username}.example.com:${port}/path">>, dynamic)
        ),
        ?_assertEqual(true, is_templated_host_url(<<"http://${username}.example.com">>)),
        ?_assertEqual(false, is_templated_host_url(<<"http://example.com/${username}">>)),
        ?_assertEqual(false, is_templated_host_url(<<"not a url">>))
    ].

parse_url_template_static_host_test_() ->
    [
        ?_assertEqual(
            {static_host, #{
                scheme => https,
                host => <<"example.com">>,
                port => 443,
                path => <<"/auth">>,
                query => <<"q=1">>
            }},
            parse_url_template(<<"https://example.com/auth?q=1">>, dynamic)
        ),
        ?_assertEqual(
            {static_host, #{
                scheme => http,
                host => <<"[::1]">>,
                port => 8080,
                path => <<"/auth">>,
                query => <<>>
            }},
            parse_url_template(<<"http://[::1]:8080/auth">>, dynamic)
        ),
        ?_assertThrow(
            {invalid_url, {unsupported_scheme, _}},
            parse_url_template(<<"ftp://example.com">>, dynamic)
        )
    ].

allowed_hosts_test_() ->
    [
        ?_assertEqual(
            [{exact, <<"auth.example.com">>}, {suffix, <<".auth.example.com">>}],
            parse_allowed_hosts([<<"Auth.Example.Com">>, <<"*.auth.example.com">>])
        ),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<>>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"*.">>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"bad host">>])),
        ?_assertThrow({invalid_allowed_host, _}, parse_allowed_hosts([<<"host:8080">>])),
        ?_assertEqual(ok, validate_allowed_hosts_field([<<"a.example.com">>])),
        ?_assertMatch({error, _}, validate_allowed_hosts_field([<<"a/b">>]))
    ].

render_host_test_() ->
    Template = fun(Str) ->
        {_Vars, T} = emqx_auth_template:parse_str(Str, ["username", {var_namespace, "client_attrs"}]),
        T
    end,
    AllowedHosts = [{suffix, <<".auth.example.com">>}, {exact, <<"fallback.example.com">>}],
    [
        ?_assertEqual(
            {ok, <<"t1.auth.example.com">>},
            render_host(
                Template(<<"${client_attrs.tns}.auth.example.com">>),
                AllowedHosts,
                #{client_attrs => #{<<"tns">> => <<"t1">>}}
            )
        ),
        ?_assertEqual(
            {ok, <<"fallback.example.com">>},
            render_host(
                Template(<<"fallback.EXAMPLE.com">>),
                AllowedHosts,
                #{}
            )
        ),
        ?_assertMatch(
            {error, {host_not_allowed, <<"t1.evil.example.com">>}},
            render_host(
                Template(<<"${client_attrs.tns}.evil.example.com">>),
                AllowedHosts,
                #{client_attrs => #{<<"tns">> => <<"t1">>}}
            )
        ),
        %% missing binding: fail-closed
        ?_assertMatch(
            {error, {failed_to_render_host, _}},
            render_host(
                Template(<<"${client_attrs.tns}.auth.example.com">>),
                AllowedHosts,
                #{}
            )
        ),
        %% rendered host with URL metacharacters: fail-closed
        ?_assertMatch(
            {error, {rendered_host_invalid, _}},
            render_host(
                Template(<<"${username}.auth.example.com">>),
                AllowedHosts,
                #{username => <<"evil.com/#">>}
            )
        ),
        %% exact match must not be bypassed by a subdomain
        ?_assertMatch(
            {error, {host_not_allowed, _}},
            render_host(
                Template(<<"${username}.fallback.example.com">>),
                [{exact, <<"fallback.example.com">>}],
                #{username => <<"sub">>}
            )
        )
    ].

format_url_test_() ->
    [
        ?_assertEqual(
            <<"https://h.example.com:443/a/b?q=1">>,
            format_url(https, <<"h.example.com">>, 443, [<<"/a/b">>, $?, <<"q=1">>])
        ),
        ?_assertEqual(
            <<"http://h.example.com:80/">>,
            format_url(http, <<"h.example.com">>, 80, <<>>)
        ),
        ?_assertEqual(
            <<"http://h.example.com:80/?q=1">>,
            format_url(http, <<"h.example.com">>, 80, <<"?q=1">>)
        )
    ].
